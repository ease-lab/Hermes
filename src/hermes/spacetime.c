//
// Created by akatsarakis on 04/05/18.
//
#include <config.h>
#include <spacetime.h>
#include <optik_mod.h>
#include <util.h>
/*
 * Initialize the spacetime using a Mica instances and adding the timestamps
 * and locks to the keys of mica structure
 */

struct spacetime_kv kv;
volatile spacetime_group_membership group_membership;

void meta_reset(struct spacetime_meta_stats* meta)
{
	meta->num_get_success = 0;
	meta->num_put_success = 0;
	meta->num_upd_success = 0;
	meta->num_inv_success = 0;
	meta->num_ack_success = 0;
	meta->num_get_stall = 0;
	meta->num_put_stall = 0;
	meta->num_upd_fail = 0;
	meta->num_inv_fail = 0;
	meta->num_ack_fail = 0;
	meta->num_get_miss = 0;
	meta->num_put_miss = 0;
	meta->num_unserved_get_miss = 0;
	meta->num_unserved_put_miss = 0;
}

void extended_meta_reset(struct extended_spacetime_meta_stats* meta)
{
	meta->num_hit = 0;
	meta->num_miss = 0;
	meta->num_stall = 0;
	meta->num_coherence_fail = 0;
	meta->num_coherence_success = 0;

	meta_reset(&meta->metadata);
}

static inline uint8_t is_last_ack(uint8_t const * gathered_acks, int worker_lid)
{
	//lock_free read of group membership & comparison with gathered acks
	int i = 0;
	spacetime_group_membership curr_membership;
	uint32_t debug_cntr = 0;
	do { //Lock free read of keys meta
		if (ENABLE_ASSERTIONS) {
			debug_cntr++;
			if (debug_cntr == M_4) {
				printf("Worker %u stuck on a local read \n", worker_lid);
				debug_cntr = 0;
			}
		}
		curr_membership = *((spacetime_group_membership*) &group_membership);
	} while (!(group_mem_timestamp_is_same_and_valid(
			curr_membership.optik_lock.version,
			group_membership.optik_lock.version)));
	for(i = 0; i < GROUP_MEMBERSHIP_ARRAY_SIZE; i++)
		if((gathered_acks[i] & curr_membership.group_membership[i]) !=
		   curr_membership.group_membership[i])
			return 0;
	return 1;
}

void spacetime_object_meta_init(spacetime_object_meta* ol)
{
	ol->tie_breaker_id = TIE_BREAKER_ID_EMPTY;
	ol->last_writer_id = LAST_WRITER_ID_EMPTY;
	ol->version = 0;
	ol->state = VALID_STATE;
	ol->write_buffer_index = WRITE_BUFF_EMPTY;
	ol->lock = OPTIK_FREE;
}

void spacetime_init(int instance_id, int num_threads)
{
	int i;
	///assert(sizeof(spacetime_object_meta) == 8); //make sure that the meta are 8B and thus can fit in mica unused key

	kv.num_threads = num_threads;
	//TODO add a Define for stats
	kv.total_ops_issued = 0;
	/// allocate and init metadata for the spacetime & threads
	extended_meta_reset(&kv.aggregated_meta);
	kv.meta = malloc(num_threads * sizeof(struct spacetime_meta_stats));
	for(i = 0; i < num_threads; i++)
		meta_reset(&kv.meta[i]);
	mica_init(&kv.hash_table, instance_id, KV_SOCKET, SPACETIME_NUM_BKTS, HERD_LOG_CAP);
	spacetime_populate_fixed_len(&kv, SPACETIME_NUM_KEYS, HERD_VALUE_SIZE);
}

void spacetime_populate_fixed_len(struct spacetime_kv* kv, int n, int val_len)
{
	assert(n > 0);
	assert(val_len > 0 && val_len <= MICA_MAX_VALUE);

	/* This is needed for the eviction message below to make sense */
	assert(kv->hash_table.num_insert_op == 0 && kv->hash_table.num_index_evictions == 0);

	int i;
	struct mica_op op;
	struct mica_resp resp;
	unsigned long long *op_key = (unsigned long long *) &op.key;
	spacetime_object_meta initial_meta;
	spacetime_object_meta_init(&initial_meta);

	/* Generate the keys to insert */
	uint128 *key_arr = mica_gen_keys(n);
	op.val_len = (uint8_t) (val_len >> SHIFT_BITS);
	op.opcode = ST_OP_PUT;
	spacetime_object_meta *value_ptr = (spacetime_object_meta*) op.value;
	memcpy((void*) value_ptr, (void*) &initial_meta, sizeof(spacetime_object_meta));
	for(i = n - 1; i >= 0; i--) {
		op_key[0] = key_arr[i].first;
		op_key[1] = key_arr[i].second;
		///printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock, op.key.meta.state, op.key.meta.version, op.key.meta.cid);
		uint8_t val = 'a' + (i % MAX_BATCH_OPS_SIZE);//(uint8_t) (op_key[1] & 0xff);

		memset((void*) &value_ptr[1], val, (uint8_t) ST_VALUE_SIZE);
		mica_insert_one(&kv->hash_table, &op, &resp);
	}

	assert(kv->hash_table.num_insert_op == n);
	yellow_printf("Spacetime: Populated instance %d with %d keys, length = %d. "
						  "Index eviction fraction = %.4f.\n",
				  kv->hash_table.instance_id, n, val_len,
				  (double) kv->hash_table.num_index_evictions / kv->hash_table.num_insert_op);
}

//TODO may merge all the batch_* func
void spacetime_batch_ops(int op_num, spacetime_op_t **op, int thread_id,
						 uint32_t refilled_ops_debug_cnt, uint32_t* ref_ops_dbg_array_cnt)
{
	int I, j;	/* I is batch index */
	long long stalled_brces = 0;
#if SPACETIME_DEBUG == 1
	//assert(kv.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if SPACETIME_DEBUG == 2
	for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif
    int i;
	unsigned int bkt[MAX_BATCH_OPS_SIZE];
	struct mica_bkt *bkt_ptr[MAX_BATCH_OPS_SIZE];
	unsigned int tag[MAX_BATCH_OPS_SIZE];
	int key_in_store[MAX_BATCH_OPS_SIZE];	/* Is this key in the datastore? */
	struct mica_op *kv_ptr[MAX_BATCH_OPS_SIZE];	/* Ptr to KV item in log */


	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
	for(I = 0; I < op_num; I++) {
		if ((*op)[I].state == ST_IN_PROGRESS_WRITE || (*op)[I].state == ST_PUT_SUCCESS) continue;
//			cyan_printf("Ops[%d]=== hash(1st 8B):%" PRIu64 "\n", I, ((uint64_t *) &(*op)[I].key)[1]);
		bkt[I] = (*op)[I].key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		if ((*op)[I].state == ST_IN_PROGRESS_WRITE || (*op)[I].state == ST_PUT_SUCCESS) continue;
		for(j = 0; j < 8; j++) {
			if(bkt_ptr[I]->slots[j].in_use == 1 &&
			   bkt_ptr[I]->slots[j].tag == tag[I]) {
				uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
									  kv.hash_table.log_mask;

				/*
				 * We can interpret the log entry as mica_op, even though it
				 * may not contain the full MICA_MAX_VALUE value.
				 */
				kv_ptr[I] = (struct mica_op*) &kv.hash_table.ht_log[log_offset];

				/* Small values (1--64 bytes) can span 2 cache lines */
				__builtin_prefetch(kv_ptr[I], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

				/* Detect if the head has wrapped around for this index entry */
				if(kv.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= kv.hash_table.log_cap) {
					kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
				}

				break;
			}
		}
	}

	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta prev_meta;
	for(I = 0; I < op_num; I++) {
		if ((*op)[I].state == ST_IN_PROGRESS_WRITE || (*op)[I].state == ST_PUT_SUCCESS) continue;
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].key;

//			if(key_ptr_log[0] == key_ptr_req[0] &&
//			   key_ptr_log[1] == key_ptr_req[1]) { //Key Found 16 Byte keys
			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;

				spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr[I]->value;
				uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1] ;
				if ((*op)[I].opcode == ST_OP_GET) {
					//Lock free reads through versioning (successful when version is even)
					uint8_t was_locked_read = 0;
					(*op)[I].state = ST_EMPTY;
					do {
						prev_meta = *curr_meta;
						//switch template with all states
						switch(curr_meta->state) {
							case VALID_STATE:
								memcpy((*op)[I].value, kv_value_ptr, ST_VALUE_SIZE);
								(*op)[I].state = ST_GET_SUCCESS;
								break;
							case INVALID_BUFF_STATE:
							case INVALID_WRITE_BUFF_STATE:
							case WRITE_BUFF_STATE:
							case REPLAY_BUFF_STATE:
							case REPLAY_WRITE_BUFF_STATE:
								(*op)[I].state = ST_GET_STALL;
								break;
							default:
								was_locked_read = 1;
								optik_lock(curr_meta);
								switch(curr_meta->state) {
									case VALID_STATE:
										memcpy((*op)[I].value, kv_value_ptr, ST_VALUE_SIZE);
										(*op)[I].state = ST_GET_SUCCESS;
										break;
									case INVALID_BUFF_STATE:
									case INVALID_WRITE_BUFF_STATE:
									case WRITE_BUFF_STATE:
									case REPLAY_BUFF_STATE:
									case REPLAY_WRITE_BUFF_STATE:
										break;
									case INVALID_STATE:
										curr_meta->state = INVALID_BUFF_STATE;
										break;
									case INVALID_WRITE_STATE:
										curr_meta->state = INVALID_WRITE_BUFF_STATE;
										break;
									case WRITE_STATE:
										curr_meta->state = WRITE_BUFF_STATE;
										break;
									case REPLAY_STATE:
										curr_meta->state = REPLAY_BUFF_STATE;
										break;
									case REPLAY_WRITE_STATE:
										curr_meta->state = REPLAY_WRITE_BUFF_STATE;
										break;
									default:
										printf("Wrong opcode %s\n",code_to_str(curr_meta->state));
										assert(0);
								}
								optik_unlock_decrement_version(curr_meta);
								if((*op)[I].state != ST_GET_SUCCESS)
									(*op)[I].state = ST_GET_STALL;
								break;
						}
					} while (!is_same_version_and_valid(&prev_meta, curr_meta) && was_locked_read == 0);
					if(refilled_ops_debug_cnt > M_4){
						printf("State: %s, Version: %d tie-id: %d,\n", code_to_str(curr_meta->state), curr_meta->version, curr_meta->tie_breaker_id);
					}
					///TODO may need to put this inside the above criticial section
					if((*op)[I].state == ST_GET_SUCCESS) {
						(*op)[I].val_len = kv_ptr[I]->val_len - sizeof(spacetime_object_meta);
						if (ENABLE_ASSERTIONS)
							assert((*op)[I].val_len == ST_VALUE_SIZE);
					}

				} else if ((*op)[I].opcode == ST_OP_PUT){
					if(ENABLE_ASSERTIONS)
						assert((*op)[I].val_len == ST_VALUE_SIZE);
					///Warning: even if a write is in progress write we may need to update the value of write_buffer_index
					(*op)[I].state = ST_EMPTY;
					optik_lock(curr_meta);
					if(ENABLE_ASSERTIONS)
						if(unlikely(ref_ops_dbg_array_cnt[I] > M_4)){
							red_printf("Worker %d is stacked in %d\n", thread_id, I);
							if(w_stats[thread_id].issued_invs_per_worker != w_stats[thread_id].received_acks_per_worker) {
								red_printf("\tCoordinator: issued_invs: %d received acks: %d, state: %s\n",
										   w_stats[thread_id].issued_invs_per_worker,
										   w_stats[thread_id].received_acks_per_worker, code_to_str(curr_meta->state));
							}else
								green_printf("\tCoordinator: issued_invs: %d received acks: %d, state: %s\n",
											 w_stats[thread_id].issued_invs_per_worker, w_stats[thread_id].received_acks_per_worker, code_to_str(curr_meta->state));
							for(j = 0; j < MAX_BATCH_OPS_SIZE; j++)
								if(((uint64_t *) &(*op)[I].key)[0] == ((uint64_t *) &(*op)[j].key)[0])
									red_printf("\t J: %d, hash(1st 8B):%" PRIu64 ", Op-State: %s\n",
											   j, ((uint64_t *) &(*op)[I].key)[0], code_to_str((*op)[j].state));
							ref_ops_dbg_array_cnt[I] = 0;
						}
					if(ENABLE_ASSERTIONS && refilled_ops_debug_cnt > M_4)
						yellow_printf("W%d--> Op[%d] | Key Hash:%" PRIu64 "\n\tOp: %s state: %s, version: %d, tie-breaker: %d, waits ack: %s\n",
									  thread_id, I, ((uint64_t *) &(*op)[I].key)[0],
									  code_to_str((*op)[I].state), code_to_str(curr_meta->state),
									  curr_meta->version - 1, curr_meta->tie_breaker_id,
									  curr_meta->write_buffer_index != WRITE_BUFF_EMPTY ? "y":"n");

					///TODO update the write_buff_index when move things from batch op in order to preserve session order
					switch(curr_meta->state) {
						case VALID_STATE:
						case INVALID_STATE:
						case INVALID_BUFF_STATE:
							if(curr_meta->write_buffer_index != WRITE_BUFF_EMPTY){
								///stall write: until all acks from last write arrive
//                                printf("[W%d]-->State:%s version:%d, tie: %d, buff index: %d, last_write-version: %d, last_write-id: %d !\n",
//									   thread_id, code_to_str(curr_meta->state), curr_meta->version - 1, curr_meta->tie_breaker_id,
//										curr_meta->write_buffer_index, curr_meta->last_writer_version, curr_meta->last_writer_id);
								optik_unlock_decrement_version((spacetime_object_meta*) curr_meta);
							} else {
								curr_meta->state = (uint8_t) (curr_meta->state == INVALID_BUFF_STATE? WRITE_BUFF_STATE : WRITE_STATE);
								memcpy(kv_value_ptr, (*op)[I].value, (*op)[I].val_len);
								kv_ptr[I]->val_len = (*op)[I].val_len + sizeof(spacetime_object_meta);
								///update group membership mask
								memcpy((void *) curr_meta->write_acks, (void *) group_membership.write_ack_init,
									   GROUP_MEMBERSHIP_ARRAY_SIZE);
								if(ENABLE_ASSERTIONS)
									assert(I < WRITE_BUFF_EMPTY);
								curr_meta->write_buffer_index = (uint8_t) I;
								curr_meta->last_writer_version = curr_meta->version + 1;
								optik_unlock_write((spacetime_object_meta*) curr_meta, (uint8) machine_id, &((*op)[I].version));
								(*op)[I].state = ST_PUT_SUCCESS;
							}
							break;

						case INVALID_WRITE_BUFF_STATE:
						case WRITE_BUFF_STATE:
						case REPLAY_BUFF_STATE:
						case REPLAY_WRITE_BUFF_STATE:
							optik_unlock_decrement_version((spacetime_object_meta*) curr_meta);
							break;
						default:
							switch(curr_meta->state) {
								case INVALID_WRITE_STATE:
									curr_meta->state = INVALID_WRITE_BUFF_STATE;
									break;
								case WRITE_STATE:
									curr_meta->state = WRITE_BUFF_STATE;
									break;
								case REPLAY_STATE:
									curr_meta->state = REPLAY_BUFF_STATE;
									break;
								case REPLAY_WRITE_STATE:
									curr_meta->state = REPLAY_WRITE_BUFF_STATE;
									break;
								default: assert(0);
							}
							optik_unlock_decrement_version(curr_meta);
							break;
					}
					if((*op)[I].state == ST_PUT_SUCCESS)
						(*op)[I].tie_breaker_id = (uint8_t) machine_id;
					else
						(*op)[I].state = ST_PUT_STALL;
				}else assert(0);
			}
		}

		if(key_in_store[I] == 0)  //KVS miss --> We get here if either tag or log key match failed
			assert(0);

	}
}

void spacetime_batch_invs(int op_num, spacetime_inv_t **op, int thread_id)
{
	int I, j;	/* I is batch index */
	long long stalled_brces = 0;
#if SPACETIME_DEBUG == 1
	//assert(kv.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if SPACETIME_DEBUG == 2
	for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif

	unsigned int bkt[INV_RECV_OPS_SIZE];
	struct mica_bkt *bkt_ptr[INV_RECV_OPS_SIZE];
	unsigned int tag[INV_RECV_OPS_SIZE];
	int key_in_store[INV_RECV_OPS_SIZE];	/* Is this key in the datastore? */
	struct mica_op *kv_ptr[INV_RECV_OPS_SIZE];	/* Ptr to KV item in log */

	if(ENABLE_BATCH_OP_PRINTS && ENABLE_INV_PRINTS && thread_id < MAX_THREADS_TO_PRINT)
		red_printf("[W%d] Batch INVs (op num: %d)!\n", thread_id, op_num);

	if(ENABLE_ASSERTIONS)
		assert(op_num <= INV_RECV_OPS_SIZE);
	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
	for(I = 0; I < op_num; I++) {
        if(ENABLE_ASSERTIONS){
			assert((*op)[I].version % 2 == 0);
            assert((*op)[I].opcode == ST_OP_INV);
			assert((*op)[I].val_len == ST_VALUE_SIZE);
			assert(REMOTE_MACHINES != 1 || (*op)[I].sender == REMOTE_MACHINES - machine_id);
			assert(REMOTE_MACHINES != 1 || (*op)[I].tie_breaker_id == REMOTE_MACHINES - machine_id);
//			red_printf("INVs: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
		}
		bkt[I] = (*op)[I].key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		for(j = 0; j < 8; j++) {
			if(bkt_ptr[I]->slots[j].in_use == 1 &&
			   bkt_ptr[I]->slots[j].tag == tag[I]) {
				uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
									  kv.hash_table.log_mask;

				/*
				 * We can interpret the log entry as mica_op, even though it
				 * may not contain the full MICA_MAX_VALUE value.
				 */
				kv_ptr[I] = (struct mica_op*) &kv.hash_table.ht_log[log_offset];

				/* Small values (1--64 bytes) can span 2 cache lines */
				__builtin_prefetch(kv_ptr[I], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

				/* Detect if the head has wrapped around for this index entry */
				if(kv.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= kv.hash_table.log_cap) {
					kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
				}

				break;
			}
		}
	}

	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta prev_meta;
	for(I = 0; I < op_num; I++) {
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].key;


//			if(key_ptr_log[0] == key_ptr_req[0] &&
//			   key_ptr_log[1] == key_ptr_req[1]) { //Key Found 16 Byte keys
			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;

				spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr[I]->value;
				uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1] ;
				if ((*op)[I].opcode != ST_OP_INV) assert(0);
				else{
					uint32_t debug_cntr = 0;
					do { //Lock free read of keys meta
						if (ENABLE_ASSERTIONS) {
							debug_cntr++;
							if (debug_cntr == M_4) {
								printf("Worker %u stuck on a local read \n", thread_id);
								debug_cntr = 0;
							}
						}
						prev_meta = *curr_meta;
					} while (!is_same_version_and_valid(&prev_meta, curr_meta));
					//lock and proceed iff remote.TS >= local.TS
					//if inv TS >= local timestamp
					if(!timestamp_is_smaller((*op)[I].version,  (*op)[I].tie_breaker_id,
											 prev_meta.version, prev_meta.tie_breaker_id)){
						//Lock and check again if inv TS > local timestamp
						optik_lock(curr_meta);
						///Warning: use op.version + 1 bellow since optik_lock() increases curr_meta->version by 1
						if(timestamp_is_smaller(curr_meta->version, curr_meta->tie_breaker_id,
												(*op)[I].version + 1,   (*op)[I].tie_breaker_id)){
//							printf("Received an invalidation with >= timestamp\n");
							///Update Value, TS and last_writer_id
							curr_meta->last_writer_id = (*op)[I].sender;
							if(ENABLE_ASSERTIONS)
								assert((*op)[I].val_len == ST_VALUE_SIZE);
							kv_ptr[I]->val_len = (*op)[I].val_len + sizeof(spacetime_object_meta);
							if(ENABLE_ASSERTIONS)
								assert(kv_ptr[I]->val_len == HERD_VALUE_SIZE);
							memcpy(kv_value_ptr, (*op)[I].value, (*op)[I].val_len);
							///Update state
							switch(curr_meta->state) {
								case VALID_STATE:
									curr_meta->state = INVALID_STATE;
									break;
								case INVALID_STATE:
								case INVALID_WRITE_STATE:
								case INVALID_BUFF_STATE:
								case INVALID_WRITE_BUFF_STATE:
									break;
								case WRITE_STATE:
									curr_meta->state = INVALID_WRITE_STATE;
									break;
								case WRITE_BUFF_STATE:
									curr_meta->state = INVALID_WRITE_BUFF_STATE;
									break;
								case REPLAY_STATE:
									curr_meta->state = INVALID_STATE;
									break;
								case REPLAY_BUFF_STATE:
									curr_meta->state = INVALID_BUFF_STATE;
									break;
								case REPLAY_WRITE_STATE:
									curr_meta->state = INVALID_WRITE_STATE;
									break;
								case REPLAY_WRITE_BUFF_STATE:
									curr_meta->state = INVALID_WRITE_BUFF_STATE;
									break;
								default: assert(0);
							}
							curr_meta->last_writer_id = (*op)[I].sender;
							optik_unlock(curr_meta, (*op)[I].tie_breaker_id, (*op)[I].version);
						} else if(timestamp_is_equal(curr_meta->version, curr_meta->tie_breaker_id,
													 (*op)[I].version,   (*op)[I].tie_breaker_id)) {
							if (curr_meta->state == WRITE_STATE || curr_meta->state == WRITE_BUFF_STATE)
								(*op)[I].opcode = ST_INV_OUT_OF_GROUP;
							curr_meta->last_writer_id = (*op)[I].sender;
							optik_unlock(curr_meta, (*op)[I].tie_breaker_id, (*op)[I].version);
						}else
							optik_unlock_decrement_version(curr_meta);
					}
				}
				if((*op)[I].opcode != ST_INV_OUT_OF_GROUP)
					(*op)[I].opcode = ST_INV_SUCCESS;
			}
		}
		if(key_in_store[I] == 0){//KVS miss --> We get here if either tag or log key match failed
			assert(0);
			(*op)[I].opcode = ST_MISS;
		}
		if(ENABLE_ASSERTIONS)
			assert((*op)[I].opcode == ST_INV_SUCCESS);
	}
}

void spacetime_batch_acks(int op_num, spacetime_ack_t **op,
						  spacetime_op_t* read_write_op, int thread_id)
{
    int complete_reqs_dbg_cnt = 0;
	int I, j;	/* I is batch index */
	long long stalled_brces = 0;
	int completed_writes_debug_cnt = 0;
#if SPACETIME_DEBUG == 1
	//assert(kv.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if SPACETIME_DEBUG == 2
	for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif
	if(ENABLE_BATCH_OP_PRINTS && ENABLE_ACK_PRINTS && thread_id < MAX_THREADS_TO_PRINT)
		red_printf("[W%d] Batch ACKs (op num: %d)!\n",thread_id, op_num);

	unsigned int bkt[ACK_RECV_OPS_SIZE];
	struct mica_bkt *bkt_ptr[ACK_RECV_OPS_SIZE];
	unsigned int tag[ACK_RECV_OPS_SIZE];
	int key_in_store[ACK_RECV_OPS_SIZE];	/* Is this key in the datastore? */
	struct mica_op *kv_ptr[ACK_RECV_OPS_SIZE];	/* Ptr to KV item in log */

	if(ENABLE_ASSERTIONS)
		assert(op_num <= ACK_RECV_OPS_SIZE);
	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
	for(I = 0; I < op_num; I++) {
        if(ENABLE_ASSERTIONS){
			assert((*op)[I].version % 2 == 0);
            assert((*op)[I].opcode == ST_OP_ACK);
			assert((*op)[I].tie_breaker_id == machine_id);
			assert(REMOTE_MACHINES != 1 || (*op)[I].sender == REMOTE_MACHINES - machine_id);
//			yellow_printf("ACKS: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
		}
		bkt[I] = (*op)[I].key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		for(j = 0; j < 8; j++) {
			if(bkt_ptr[I]->slots[j].in_use == 1 &&
			   bkt_ptr[I]->slots[j].tag == tag[I]) {
				uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
									  kv.hash_table.log_mask;

				/*
				 * We can interpret the log entry as mica_op, even though it
				 * may not contain the full MICA_MAX_VALUE value.
				 */
				kv_ptr[I] = (struct mica_op*) &kv.hash_table.ht_log[log_offset];

				/* Small values (1--64 bytes) can span 2 cache lines */
				__builtin_prefetch(kv_ptr[I], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

				/* Detect if the head has wrapped around for this index entry */
				if(kv.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= kv.hash_table.log_cap) {
					kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
				}

				break;
			}
		}
	}

	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta lock_free_read_meta;
	for(I = 0; I < op_num; I++) {
		int complete_buff_write = WRITE_BUFF_EMPTY;
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].key;

//			if(key_ptr_log[0] == key_ptr_req[0] &&
//			   key_ptr_log[1] == key_ptr_req[1]) { //Key Found 16 Byte keys
			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;

				spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr[I]->value;
				if ((*op)[I].opcode != ST_OP_ACK) assert(0);
				else{
					uint32_t debug_cntr = 0;
					do { //Lock free read of keys meta
						if (ENABLE_ASSERTIONS) {
							debug_cntr++;
							if (debug_cntr == M_4) {
								printf("Worker %u stuck on a local read \n", thread_id);
								debug_cntr = 0;
							}
						}
						lock_free_read_meta = *curr_meta;
					} while (!is_same_version_and_valid(&lock_free_read_meta, curr_meta));
                    if(ENABLE_ASSERTIONS){
//						if()
						assert(!timestamp_is_smaller(lock_free_read_meta.version, lock_free_read_meta.tie_breaker_id,
													(*op)[I].version, (*op)[I].tie_breaker_id));
					}
					///lock and proceed iff remote.TS == local.TS or TS  == of local.last_write_TS
					if(timestamp_is_equal(lock_free_read_meta.version, lock_free_read_meta.tie_breaker_id,
										  (*op)[I].version, (*op)[I].tie_breaker_id) ||
					   timestamp_is_equal(lock_free_read_meta.last_writer_version, (uint8_t) machine_id,
										  (*op)[I].version,   (*op)[I].tie_breaker_id)){
						///Lock and check again if ack TS == local timestamp
						optik_lock(curr_meta);
						///Warning: use op.version + 1 bellow since optik_lock() increases curr_meta->version by 1
						if(timestamp_is_equal(curr_meta->version, curr_meta->tie_breaker_id,
											  (*op)[I].version + 1,   (*op)[I].tie_breaker_id)){
							//ACK with TS == local.TS
							switch(curr_meta->state) {
								case VALID_STATE:
								case INVALID_STATE:
								case INVALID_BUFF_STATE:
								case INVALID_WRITE_STATE:
								case INVALID_WRITE_BUFF_STATE:
									//assert(0);
									break;///Findout which states should have assert and which just break;
								case WRITE_STATE:
								case WRITE_BUFF_STATE:
								case REPLAY_WRITE_STATE:
								case REPLAY_WRITE_BUFF_STATE:
									complete_buff_write = curr_meta->write_buffer_index;
                                    if(ENABLE_ASSERTIONS)
										assert(complete_buff_write != WRITE_BUFF_EMPTY);
									////WARNING! do not use break here
								case REPLAY_STATE:
								case REPLAY_BUFF_STATE:
									update_ack_bit_vector((*op)[I].sender, (uint8_t*) curr_meta->write_acks);
									if (is_last_ack((uint8*)curr_meta->write_acks, thread_id)) { //if last Ack
                                        if(curr_meta->state != REPLAY_STATE && curr_meta->state != REPLAY_BUFF_STATE)
											curr_meta->write_buffer_index = WRITE_BUFF_EMPTY; //reset the write buff index
										curr_meta->state = VALID_STATE;
										(*op)[I].opcode = ST_LAST_ACK_SUCCESS;
									}
									break;
								default: assert(0);
							}
							///else if ACK is for the last local write --> complete local write
						} else if(timestamp_is_equal(curr_meta->last_writer_version, (uint8_t) machine_id,
													 (*op)[I].version,   (*op)[I].tie_breaker_id)){
							update_ack_bit_vector((*op)[I].sender, (uint8_t*) curr_meta->write_acks);
							if (is_last_ack((uint8_t*) curr_meta->write_acks, thread_id)) { //if last local write completed
								complete_buff_write = curr_meta->write_buffer_index;
								if(ENABLE_ASSERTIONS)
									assert(complete_buff_write != WRITE_BUFF_EMPTY);
								curr_meta->write_buffer_index = WRITE_BUFF_EMPTY; //reset the write buff index
								(*op)[I].opcode = ST_LAST_ACK_PREV_WRITE_SUCCESS;
								switch (curr_meta->state) {
									case VALID_STATE:
									case INVALID_STATE:
									case INVALID_BUFF_STATE:
									case WRITE_STATE:
									case WRITE_BUFF_STATE:
									case REPLAY_STATE:
									case REPLAY_BUFF_STATE:
										///TODO check what happens here
										break;
									case INVALID_WRITE_STATE:
										curr_meta->state = INVALID_STATE;
										break;
									case INVALID_WRITE_BUFF_STATE:
										curr_meta->state = INVALID_BUFF_STATE;
										break;
									case REPLAY_WRITE_STATE:
										curr_meta->state = REPLAY_STATE;
										break;
									case REPLAY_WRITE_BUFF_STATE:
										curr_meta->state = REPLAY_BUFF_STATE;
										break;
									default:
										assert(0);
								}
							}
						}
						optik_unlock_decrement_version(curr_meta);
					}
					if(((*op)[I].opcode == ST_LAST_ACK_SUCCESS ||
						(*op)[I].opcode == ST_LAST_ACK_PREV_WRITE_SUCCESS) &&
					   complete_buff_write != WRITE_BUFF_EMPTY){
						///completed write --> remove it from the ops buffer
						if(ENABLE_ASSERTIONS){
//                            printf("ACK[%d]: Key Hash:%" PRIu64 " complete buff: %d\n\t op: %s, TS: %d | %d, sender: %d\n",
//								   I, ((uint64_t *) &(*op)[I].key)[1], complete_buff_write, code_to_str((*op)[I].opcode),
//								   (*op)[I].version, (*op)[I].tie_breaker_id, (*op)[I].sender);
							assert(read_write_op[complete_buff_write].state == ST_IN_PROGRESS_WRITE);
                            assert(((uint64_t *) &read_write_op[complete_buff_write].key)[0] == ((uint64_t *) &(*op)[I].key)[0]);
						}
						complete_reqs_dbg_cnt++;
						completed_writes_debug_cnt++;
						read_write_op[complete_buff_write].state = ST_PUT_COMPLETE;
					} else if(((*op)[I].opcode == ST_LAST_ACK_SUCCESS ||
						(*op)[I].opcode == ST_LAST_ACK_PREV_WRITE_SUCCESS) &&
							complete_buff_write == WRITE_BUFF_EMPTY)
                        assert(0);
				}
			}
			if((*op)[I].opcode != ST_LAST_ACK_SUCCESS)
				(*op)[I].opcode = ST_ACK_SUCCESS;
		}
		if(key_in_store[I] == 0) //KVS miss --> We get here if either tag or log key match failed
			assert(0);

	}

	if(ENABLE_ASSERTIONS)
		assert(REMOTE_MACHINES != 1 || complete_reqs_dbg_cnt == op_num);
}

void spacetime_batch_vals(int op_num, spacetime_val_t **op, int thread_id)
{
	int I, j;	/* I is batch index */
	long long stalled_brces = 0;
#if SPACETIME_DEBUG == 1
	//assert(kv.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if SPACETIME_DEBUG == 2
	for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif
	unsigned int bkt[VAL_RECV_OPS_SIZE];
	struct mica_bkt *bkt_ptr[VAL_RECV_OPS_SIZE];
	unsigned int tag[VAL_RECV_OPS_SIZE];
	int key_in_store[VAL_RECV_OPS_SIZE];	/* Is this key in the datastore? */
	struct mica_op *kv_ptr[VAL_RECV_OPS_SIZE];	/* Ptr to KV item in log */

	if(ENABLE_BATCH_OP_PRINTS && ENABLE_VAL_PRINTS && thread_id < MAX_THREADS_TO_PRINT)
		red_printf("[W%d] Batch VALs (op num: %d)!\n", thread_id, op_num);

	if(ENABLE_ASSERTIONS)
		assert(op_num <= VAL_RECV_OPS_SIZE);

	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */

	for(I = 0; I < op_num; I++) {
		if(ENABLE_ASSERTIONS){
			assert((*op)[I].version % 2 == 0);
            assert((*op)[I].opcode == ST_OP_VAL);
			assert(REMOTE_MACHINES != 1 || (*op)[I].sender == REMOTE_MACHINES - machine_id);
			assert(REMOTE_MACHINES != 1 || (*op)[I].tie_breaker_id == REMOTE_MACHINES - machine_id);
//			green_printf("VALS: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
		}
		bkt[I] = (*op)[I].key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		for(j = 0; j < 8; j++) {
			if(bkt_ptr[I]->slots[j].in_use == 1 &&
			   bkt_ptr[I]->slots[j].tag == tag[I]) {
				uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
									  kv.hash_table.log_mask;


				 // We can interpret the log entry as mica_op, even though it
				 // may not contain the full MICA_MAX_VALUE value.

				kv_ptr[I] = (struct mica_op*) &kv.hash_table.ht_log[log_offset];

				 //Small values (1--64 bytes) can span 2 cache lines
				__builtin_prefetch(kv_ptr[I], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

				//Detect if the head has wrapped around for this index entry
				if(kv.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= kv.hash_table.log_cap) {
					kv_ptr[I] = NULL;	//If so, we mark it "not found"
				}

				break;
			}
		}
	}

	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta lock_free_read_meta;
	for(I = 0; I < op_num; I++) {
		if(kv_ptr[I] != NULL) {
			// We had a tag match earlier. Now compare log entry.
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].key;

//			if(key_ptr_log[0] == key_ptr_req[0] &&
//			   key_ptr_log[1] == key_ptr_req[1]) { //Key Found 16 Byte keys
			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;

				spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr[I]->value;
				if ((*op)[I].opcode != ST_OP_VAL) assert(0);
				else{
					uint32_t debug_cntr = 0;
					do { //Lock free read of keys meta
						if (ENABLE_ASSERTIONS) {
							debug_cntr++;
							if (debug_cntr == M_4) {
								printf("Worker %u stuck on a local read \n", thread_id);
								debug_cntr = 0;
							}
						}
						lock_free_read_meta = *curr_meta;
					} while (!is_same_version_and_valid(&lock_free_read_meta, curr_meta));
					///lock and proceed iff remote.TS == local.TS
					if(timestamp_is_equal(lock_free_read_meta.version, lock_free_read_meta.tie_breaker_id,
										  (*op)[I].version,   (*op)[I].tie_breaker_id)){
						///Lock and check again if still TS == local timestamp
						optik_lock(curr_meta);
						///Warning: use op.version + 1 bellow since optik_lock() increases curr_meta->version by 1
						if(timestamp_is_equal(curr_meta->version, curr_meta->tie_breaker_id,
											  (*op)[I].version + 1,   (*op)[I].tie_breaker_id)){
//							printf("VAL with TS == local.TS\n");
							switch(curr_meta->state) {
								case WRITE_STATE:
								case WRITE_BUFF_STATE:
									assert(0); ///WARNING: this should not happen w/o failure suspicion of this node
								case INVALID_WRITE_STATE:
								case INVALID_WRITE_BUFF_STATE:
								case REPLAY_WRITE_STATE:
								case REPLAY_WRITE_BUFF_STATE:
								case VALID_STATE:
								case INVALID_STATE:
								case INVALID_BUFF_STATE:
								case REPLAY_STATE:
								case REPLAY_BUFF_STATE:
									curr_meta->state = VALID_STATE;
									break;
								default: assert(0);
							}
						}
						optik_unlock_decrement_version(curr_meta);
					}
				}
			}
			(*op)[I].opcode = ST_VAL_SUCCESS;
		}
		if(key_in_store[I] == 0)//KVS miss --> We get here if either tag or log key match failed
			assert(0);
	}
}

void group_membership_init(void)
{
	int i,j;
	for(i = 0; i < GROUP_MEMBERSHIP_ARRAY_SIZE; i++) {
		group_membership.group_membership[i] = 0;
		group_membership.write_ack_init[i] = 1;
		for (j = 0; j < 8; j++) {
			if (i * 8 + j == MACHINE_NUM){
//				green_printf("Group mem. bit array: "BYTE_TO_BINARY_PATTERN" \n",
//					   BYTE_TO_BINARY(group_membership.group_membership[i]));
//				green_printf("Write init bit array: "BYTE_TO_BINARY_PATTERN" \n",
//							 BYTE_TO_BINARY(group_membership.write_ack_init[i]));
				return;
			}
			group_membership.group_membership[i] = (uint8_t)
					((group_membership.group_membership[i] << 1) + 1);
			if(i * 8 + j < machine_id)
				group_membership.write_ack_init[i] = (uint8_t)
						group_membership.write_ack_init[i] << 1;
		}
	}
}
