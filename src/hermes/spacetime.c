//
// Created by akatsarakis on 04/05/18.
//
#include <config.h>
#include <spacetime.h>
#include <concur_ctrl.h>
#include <util.h>
#include <inline-util.h>

/*
 * Initialize the spacetime using a Mica instances and adding the timestamps
 * and locks to the keys of mica structure
 */

struct spacetime_kv kv;
spacetime_group_membership group_membership;

void
meta_reset(struct spacetime_meta_stats* meta)
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

void
extended_meta_reset(struct extended_spacetime_meta_stats* meta)
{
	meta->num_hit = 0;
	meta->num_miss = 0;
	meta->num_stall = 0;
	meta->num_coherence_fail = 0;
	meta->num_coherence_success = 0;

	meta_reset(&meta->metadata);
}

void
spacetime_object_meta_init(spacetime_object_meta* ol)
{
	cctrl_init(&ol->cctrl);
	ol->state = VALID_STATE;
	ol->last_writer_id = LAST_WRITER_ID_EMPTY;
	ol->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY;
}

void
spacetime_init(int instance_id, int num_threads)
{
	kv.num_threads = num_threads;
	//TODO add a Define for stats
	kv.total_ops_issued = 0;
	/// allocate and init metadata for the spacetime & threads
	extended_meta_reset(&kv.aggregated_meta);
	kv.meta = malloc(num_threads * sizeof(struct spacetime_meta_stats));
	for(int i = 0; i < num_threads; i++)
		meta_reset(&kv.meta[i]);
	mica_init(&kv.hash_table, instance_id, KV_SOCKET, SPACETIME_NUM_BKTS, SPACETIME_LOG_CAP);
	spacetime_populate_fixed_len(&kv, SPACETIME_NUM_KEYS, KVS_VALUE_SIZE);
}

void
spacetime_populate_fixed_len(struct spacetime_kv* _kv, int n, int val_len)
{
	assert(n > 0);
	assert(val_len > 0 && val_len <= KVS_VALUE_SIZE);

	/* This is needed for the eviction message below to make sense */
	assert(_kv->hash_table.num_insert_op == 0 && _kv->hash_table.num_index_evictions == 0);

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
	for(int i = n - 1; i >= 0; i--) {
		op_key[0] = key_arr[i].first;
		op_key[1] = key_arr[i].second;
		///printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock,
		/// op.key.meta.state, op.key.meta.version, op.key.meta.cid);
		uint8_t val = (uint8_t) ('a' + (i % 20));

		memset((void*) &value_ptr[1], val, ST_VALUE_SIZE);
		mica_insert_one(&_kv->hash_table, &op, &resp);
	}

	assert(_kv->hash_table.num_insert_op == n);
	yellow_printf("Spacetime: Populated instance %d with %d keys, length = %d. "
				  "Index eviction fraction = %.4f.\n",
				  _kv->hash_table.instance_id, n, val_len,
				  (double) _kv->hash_table.num_index_evictions / _kv->hash_table.num_insert_op);
}





uint8_t
node_of_missing_ack(spacetime_group_membership curr_membership, bit_vector_t write_acks)
{
	static_assert(MACHINE_NUM <= 8, "");

	bv_and(&write_acks, curr_membership.g_membership);

	for(uint8_t i = 0; i < MACHINE_NUM; ++i)
		if(bv_bit_get(write_acks, i) == 0)
			return i;

	assert(0);
}

int
find_suspected_node(spacetime_op_t *op, int thread_id,
                    spacetime_group_membership *curr_membership)
{
#if SPACETIME_DEBUG == 1
	//assert(kv.hash_table != NULL);
	assert(op != NULL);
	assert(index > 0 && index <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if SPACETIME_DEBUG == 2
	for(I = 0; I < index; I++)
		mica_print_op(&(*op)[I]);
#endif
	int i, suspected_node;	/* I is batch index */
	unsigned int bkt;
	struct mica_bkt *bkt_ptr;
	unsigned int tag;
	struct mica_op *kv_ptr;	/* Ptr to KV item in log */

	assert(op->op_meta.state == ST_IN_PROGRESS_REPLAY || op->op_meta.state == ST_IN_PROGRESS_PUT);
	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
	bkt = op->op_meta.key.bkt & kv.hash_table.bkt_mask;
	bkt_ptr = &kv.hash_table.ht_index[bkt];
	__builtin_prefetch(bkt_ptr, 0, 0);
	tag = op->op_meta.key.tag;

	kv_ptr = NULL;

	for(i = 0; i < 8; i++) {
		if(bkt_ptr->slots[i].in_use == 1 &&
		   bkt_ptr->slots[i].tag == tag) {
			uint64_t log_offset = bkt_ptr->slots[i].offset &
								  kv.hash_table.log_mask;
			/*
             * We can interpret the log entry as mica_op, even though it
             * may not contain the full MICA_MAX_VALUE value.
             */
			kv_ptr = (struct mica_op*) &kv.hash_table.ht_log[log_offset];

			/* Small values (1--64 bytes) can span 2 cache lines */
			__builtin_prefetch(kv_ptr, 0, 0);
			__builtin_prefetch((uint8_t *) kv_ptr + 64, 0, 0);

			/* Detect if the head has wrapped around for this index entry */
			if(kv.hash_table.log_head - bkt_ptr->slots[i].offset >= kv.hash_table.log_cap)
				kv_ptr = NULL;	/* If so, we mark it "not found" */

			break;
		}
	}

	if(kv_ptr != NULL) {
		/* We had a tag match earlier. Now compare log entry. */
		long long *key_ptr_log = (long long *) kv_ptr;
		long long *key_ptr_req = (long long *) &op->op_meta.key;

		if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
			spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
			cctrl_lock(&curr_meta->cctrl);
			suspected_node = node_of_missing_ack(*curr_membership, curr_meta->ack_bv);

//			printf("ops: (%s) ops-state %s state: %s\n", code_to_str(op->opcode),
//				   code_to_str(op->state), code_to_str(curr_meta->state));
//			yellow_printf("Write acks bit array: ");
//			bv_print(curr_meta->ack_bv);
//			printf("\n");
			cctrl_unlock_dec_version(&curr_meta->cctrl);
			if(ENABLE_ASSERTIONS){
				if(machine_id == suspected_node){
					//todo only for dbg
					printf("ack BV: ");
					bv_print(curr_meta->ack_bv);
					printf("\n g membership BV: ");
					bv_print(group_membership.g_membership);
					printf("\n w_ack init BV: ");
					bv_print(group_membership.w_ack_init);
					printf("\n");
				}
				assert(machine_id != suspected_node);
				assert(node_is_in_membership(*curr_membership, suspected_node));
			}
			return suspected_node;
		}
	}
	assert(0); //key is not in store!

	return -1;
}



//////////////////////////////////////////////////
/////////////////////// HERMES - Debricated
//////////////////////////////////////////////////

static uint64_t g_seed = 0xdeadbeef;
void
batch_ops_to_KVS(int op_num, spacetime_op_t **op, int thread_id,
				 spacetime_group_membership curr_membership)
{
	int I, j;	/* I is batch index */
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
		if ((*op)[I].op_meta.state == ST_PUT_SUCCESS ||
			(*op)[I].op_meta.state == ST_REPLAY_SUCCESS ||
			(*op)[I].op_meta.state == ST_IN_PROGRESS_PUT ||
			(*op)[I].op_meta.state == ST_IN_PROGRESS_REPLAY ||
			(*op)[I].op_meta.state == ST_OP_MEMBERSHIP_CHANGE ||
			(*op)[I].op_meta.state == ST_PUT_COMPLETE_SEND_VALS) continue;
//			cyan_printf("Ops[%d]=== hash(1st 8B):%" PRIu64 "\n", I, ((uint64_t *) &(*op)[I].key)[1]);
		bkt[I] = (*op)[I].op_meta.key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].op_meta.key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		if ((*op)[I].op_meta.state == ST_PUT_SUCCESS ||
			(*op)[I].op_meta.state == ST_REPLAY_SUCCESS ||
			(*op)[I].op_meta.state == ST_IN_PROGRESS_PUT ||
			(*op)[I].op_meta.state == ST_IN_PROGRESS_REPLAY ||
			(*op)[I].op_meta.state == ST_OP_MEMBERSHIP_CHANGE ||
			(*op)[I].op_meta.state == ST_PUT_COMPLETE_SEND_VALS) continue;
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
		if ((*op)[I].op_meta.state == ST_PUT_SUCCESS ||
			(*op)[I].op_meta.state == ST_REPLAY_SUCCESS ||
			(*op)[I].op_meta.state == ST_IN_PROGRESS_PUT ||
			(*op)[I].op_meta.state == ST_IN_PROGRESS_REPLAY ||
			(*op)[I].op_meta.state == ST_OP_MEMBERSHIP_CHANGE ||
			(*op)[I].op_meta.state == ST_PUT_COMPLETE_SEND_VALS) continue;
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].op_meta.key;

			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;

				spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr[I]->value;
				uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1] ;

				if ((*op)[I].op_meta.opcode == ST_OP_GET) {
					//Lock free reads through versioning (successful when version is even)
					uint8_t was_locked_read = 0;
					(*op)[I].op_meta.state = ST_EMPTY;
					do {
						uint8_t node_id; // used for virtual --> physical node ids mapping
						prev_meta = *curr_meta;
						//switch template with all states
						switch(curr_meta->state) {
							case VALID_STATE:
								memcpy((*op)[I].value, kv_value_ptr, ST_VALUE_SIZE);
								(*op)[I].op_meta.state = ST_GET_COMPLETE;
								(*op)[I].op_meta.val_len = kv_ptr[I]->val_len - sizeof(spacetime_object_meta);
								break;
							case INVALID_WRITE_STATE:
							case WRITE_STATE:
							case REPLAY_STATE:
								(*op)[I].op_meta.state = ST_GET_STALL;
								break;
							default:
								was_locked_read = 1;
								cctrl_lock(&curr_meta->cctrl);
								switch(curr_meta->state) {
									case VALID_STATE:
										memcpy((*op)[I].value, kv_value_ptr, ST_VALUE_SIZE);
										(*op)[I].op_meta.state = ST_GET_COMPLETE;
										(*op)[I].op_meta.val_len = kv_ptr[I]->val_len - sizeof(spacetime_object_meta);
										break;
									case INVALID_WRITE_STATE:
									case WRITE_STATE:
									case REPLAY_STATE:
										(*op)[I].op_meta.state = ST_GET_STALL;
										break;
									case INVALID_STATE:
										node_id = (uint8_t) (!ENABLE_VIRTUAL_NODE_IDS ?
															 curr_meta->last_writer_id :
															 curr_meta->last_writer_id % MACHINE_NUM);
										if(node_is_in_membership(curr_membership, node_id))
											(*op)[I].op_meta.state = ST_GET_STALL;
//										if(node_is_in_membership(curr_membership, curr_meta->last_writer_id))
//											(*op)[I].state = ST_GET_STALL;
										else if(curr_meta->op_buffer_index == ST_OP_BUFFER_INDEX_EMPTY) {
											///stall replay: until all acks from last write arrive
											///on multiple threads we can't complete writes / replays on VAL

											if(ENABLE_ASSERTIONS)
												assert(I < ST_OP_BUFFER_INDEX_EMPTY);

											yellow_printf("Write replay for i: %d\n", I);
											curr_meta->state = REPLAY_STATE;
											curr_meta->op_buffer_index = (uint8_t) I;
											curr_meta->last_local_write_ts.version= curr_meta->cctrl.ts.version - 1;
											curr_meta->last_local_write_ts.tie_breaker_id = curr_meta->cctrl.ts.tie_breaker_id;

											(*op)[I].op_meta.state = ST_REPLAY_SUCCESS;
											(*op)[I].op_meta.ts.version = curr_meta->cctrl.ts.version - 1;
											(*op)[I].op_meta.ts.tie_breaker_id = curr_meta->cctrl.ts.tie_breaker_id;
											(*op)[I].op_meta.val_len = ST_VALUE_SIZE;
											memcpy((*op)[I].value, kv_value_ptr, ST_VALUE_SIZE);
											///update group membership mask for replay acks
											bv_copy((bit_vector_t*) &curr_meta->ack_bv, curr_membership.w_ack_init);
										}
										break;
									default:
										printf("A1 Wrong opcode: (%d)", curr_meta->state);
										printf("%s\n",code_to_str(curr_meta->state));
										assert(0);
								}
								cctrl_unlock_dec_version(&curr_meta->cctrl);
								break;
						}
					} while (!cctrl_timestamp_is_same_and_valid(&prev_meta.cctrl, &curr_meta->cctrl) && was_locked_read == 0);

				} else if ((*op)[I].op_meta.opcode == ST_OP_PUT){
					if(ENABLE_ASSERTIONS)
						assert((*op)[I].op_meta.val_len == ST_VALUE_SIZE);
					///Warning: even if a write is in progress write we may need to update the value of write_buffer_index
					(*op)[I].op_meta.state = ST_EMPTY;

					cctrl_lock(&curr_meta->cctrl);
					uint8_t v_node_id = (uint8_t) machine_id;
					switch(curr_meta->state) {
						case VALID_STATE:
						case INVALID_STATE:
							if(curr_meta->op_buffer_index != ST_OP_BUFFER_INDEX_EMPTY){
								///stall write: until all acks from last write arrive
								/// on multiple threads we can't complete writes / replays on VAL
								cctrl_unlock_dec_version(&curr_meta->cctrl);
							} else {
								curr_meta->state = WRITE_STATE;
								memcpy(kv_value_ptr, (*op)[I].value, ST_VALUE_SIZE);
								kv_ptr[I]->val_len = (*op)[I].op_meta.val_len + sizeof(spacetime_object_meta);
								///update group membership mask
								bv_copy((bit_vector_t*) &curr_meta->ack_bv, curr_membership.w_ack_init);
								if(ENABLE_ASSERTIONS)
									assert(I < ST_OP_BUFFER_INDEX_EMPTY);
								curr_meta->op_buffer_index = (uint8_t) I;
								curr_meta->last_local_write_ts.version = curr_meta->cctrl.ts.version + 1;

								v_node_id = (uint8_t) (!ENABLE_VIRTUAL_NODE_IDS ? machine_id :
													   machine_id + MACHINE_NUM * (hrd_fastrand(&g_seed) % VIRTUAL_NODE_IDS_PER_NODE));
								curr_meta->last_local_write_ts.tie_breaker_id = v_node_id;
								cctrl_unlock_inc_version(&curr_meta->cctrl, v_node_id,
														 (uint32_t *) &((*op)[I].op_meta.ts.version));

								(*op)[I].op_meta.state = ST_PUT_SUCCESS;
							}
							break;
						case INVALID_WRITE_STATE:
						case WRITE_STATE:
						case REPLAY_STATE:
							cctrl_unlock_dec_version(&curr_meta->cctrl);
							break;
						default: assert(0);
							break;
					}
					//Fill this deterministic stuff after releasing the lock
					if((*op)[I].op_meta.state == ST_PUT_SUCCESS)
						(*op)[I].op_meta.ts.tie_breaker_id = v_node_id;
					else
						(*op)[I].op_meta.state = ST_PUT_STALL;

				}else assert(0);
			}
		}

		if(key_in_store[I] == 0)//KVS miss --> We get here if either tag or log key match failed
			(*op)[I].op_meta.state = ST_MISS;

	}
}

void
batch_invs_to_KVS(int op_num, spacetime_inv_t **op, spacetime_op_t *read_write_op, int thread_id,
				  int* node_suspected, uint32_t* refilled_per_ops_debug_cnt)
{
	int I, j;	/* I is batch index */
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
			assert((*op)[I].op_meta.ts.version % 2 == 0);
			assert((*op)[I].op_meta.opcode == ST_OP_INV ||
			       (*op)[I].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
			assert((*op)[I].op_meta.val_len == ST_VALUE_SIZE);
			assert(REMOTE_MACHINES != 1 ||
			       (*op)[I].op_meta.sender == REMOTE_MACHINES - machine_id);
			assert(REMOTE_MACHINES != 1 ||
			       (*op)[I].op_meta.ts.tie_breaker_id == REMOTE_MACHINES - machine_id);
//			red_printf("INVs: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
		}
		if((*op)[I].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE){
			printf("RECEIVED NODE SUSPICION: %d\n",(*op)[I].value[0]);
			*node_suspected = (*op)[I].value[0];
			continue;
		}
		bkt[I] = (*op)[I].op_meta.key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].op_meta.key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		if((*op)[I].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE) continue;
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
	spacetime_object_meta lock_free_meta;
	for(I = 0; I < op_num; I++) {
		if((*op)[I].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE) continue;
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].op_meta.key;


			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;

				spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr[I]->value;
				uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1] ;
				if ((*op)[I].op_meta.opcode != ST_OP_INV) assert(0);
				else{
					uint32_t debug_cntr = 0;
					do { //Lock free read of keys meta
						if (ENABLE_ASSERTIONS) {
							debug_cntr++;
							if (debug_cntr == M_4) {
								printf("Worker %u stuck on a lock-free read (for INV)\n", thread_id);
								debug_cntr = 0;
							}
						}
						lock_free_meta = *curr_meta;
					} while (!cctrl_timestamp_is_same_and_valid(&lock_free_meta.cctrl, &curr_meta->cctrl));
					//lock and proceed iff remote.TS >= local.TS
					//inv TS >= local timestamp
					if(!timestamp_is_smaller((*op)[I].op_meta.ts.version,  (*op)[I].op_meta.ts.tie_breaker_id,
											 lock_free_meta.cctrl.ts.version,
											 lock_free_meta.cctrl.ts.tie_breaker_id))
					{
						//Lock and check again if inv TS > local timestamp
						cctrl_lock(&curr_meta->cctrl);
						///Warning: use op.version + 1 bellow since optik_lock() increases curr_meta->version by 1
						if(timestamp_is_smaller(curr_meta->cctrl.ts.version - 1,
												curr_meta->cctrl.ts.tie_breaker_id,
												(*op)[I].op_meta.ts.version,
												(*op)[I].op_meta.ts.tie_breaker_id))
						{
//							printf("Received an invalidation with >= timestamp\n");
							///Update Value, TS and last_writer_id
							curr_meta->last_writer_id = (*op)[I].op_meta.sender;
							kv_ptr[I]->val_len = (*op)[I].op_meta.val_len + sizeof(spacetime_object_meta);
							if(ENABLE_ASSERTIONS){
								assert(kv_ptr[I]->val_len == KVS_VALUE_SIZE);
								assert((*op)[I].op_meta.val_len == ST_VALUE_SIZE);
							}
							memcpy(kv_value_ptr, (*op)[I].value, ST_VALUE_SIZE);
							///Update state
							switch(curr_meta->state) {
								case VALID_STATE:
									curr_meta->state = INVALID_STATE;
									break;
								case INVALID_STATE:
								case INVALID_WRITE_STATE:
									break;
								case WRITE_STATE:
								case REPLAY_STATE:
									curr_meta->state = INVALID_WRITE_STATE;
									break;
//								case REPLAY_STATE:
//									curr_meta->state = INVALID_WRITE_STATE;
//									curr_meta->state = INVALID_STATE;
//									//recover the read
//									if(ENABLE_ASSERTIONS){
//										assert(curr_meta->op_buffer_index != ST_OP_BUFFER_INDEX_EMPTY);
//										assert(read_write_op[curr_meta->op_buffer_index].state == ST_IN_PROGRESS_REPLAY);
//										assert(((uint64_t *) &read_write_op[curr_meta->op_buffer_index].key)[0] == ((uint64_t *) &(*op)[I].key)[0]);
//									}
//									read_write_op[curr_meta->op_buffer_index].state = ST_NEW;
//									curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY;
//									break;
								default: assert(0);
							}
							cctrl_unlock_custom_version(&curr_meta->cctrl, (*op)[I].op_meta.ts.tie_breaker_id,
														(*op)[I].op_meta.ts.version);
						} else if(timestamp_is_equal(curr_meta->cctrl.ts.version - 1,
													 curr_meta->cctrl.ts.tie_breaker_id,
													 (*op)[I].op_meta.ts.version,
													 (*op)[I].op_meta.ts.tie_breaker_id))
						{

							if (curr_meta->state == WRITE_STATE)
								(*op)[I].op_meta.opcode = ST_INV_OUT_OF_GROUP;

							curr_meta->last_writer_id = (*op)[I].op_meta.sender;
							cctrl_unlock_custom_version(&curr_meta->cctrl,
														(*op)[I].op_meta.ts.tie_breaker_id, (*op)[I].op_meta.ts.version);

						} else
							cctrl_unlock_dec_version(&curr_meta->cctrl);
					}
				}
				if((*op)[I].op_meta.opcode != ST_INV_OUT_OF_GROUP)
					(*op)[I].op_meta.opcode = ST_INV_SUCCESS;
			}
		}
		if(key_in_store[I] == 0){//KVS miss --> We get here if either tag or log key match failed
			assert(0);
			(*op)[I].op_meta.opcode = ST_MISS;
		}
		if(ENABLE_ASSERTIONS)
			assert((*op)[I].op_meta.opcode == ST_INV_SUCCESS || (*op)[I].op_meta.opcode == ST_INV_OUT_OF_GROUP);
	}
}

void
batch_acks_to_KVS(int op_num, spacetime_ack_t **op, spacetime_op_t *read_write_op,
				  spacetime_group_membership curr_membership, int thread_id)
{
	int I, j;	/* I is batch index */
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
			assert((*op)[I].ts.version % 2 == 0);
			assert((*op)[I].opcode == ST_OP_ACK || (*op)[I].opcode == ST_OP_MEMBERSHIP_CHANGE);
			assert(group_membership.num_of_alive_remotes != REMOTE_MACHINES ||
				   (*op)[I].ts.tie_breaker_id == machine_id ||
				   (ENABLE_VIRTUAL_NODE_IDS && (*op)[I].ts.tie_breaker_id  % MACHINE_NUM == machine_id));
			assert(REMOTE_MACHINES != 1 || (*op)[I].sender == REMOTE_MACHINES - machine_id);
//			yellow_printf("ACKS: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
		}
		if((*op)[I].opcode == ST_OP_MEMBERSHIP_CHANGE){
			//Could add measurments
			continue;
		}
		bkt[I] = (*op)[I].key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		if((*op)[I].opcode == ST_OP_MEMBERSHIP_CHANGE) continue;
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
		if((*op)[I].opcode == ST_OP_MEMBERSHIP_CHANGE) continue;
		int op_buff_indx = ST_OP_BUFFER_INDEX_EMPTY;
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].key;

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
								printf("Worker %u stuck on a lock-free read (for ACK)\n", thread_id);
								debug_cntr = 0;
							}
						}
						lock_free_read_meta = *curr_meta;
					} while (!cctrl_timestamp_is_same_and_valid(&lock_free_read_meta.cctrl, &curr_meta->cctrl));

					if(ENABLE_ASSERTIONS)
						assert(!timestamp_is_smaller(lock_free_read_meta.cctrl.ts.version,
													 lock_free_read_meta.cctrl.ts.tie_breaker_id,
													 (*op)[I].ts.version, (*op)[I].ts.tie_breaker_id));

					uint8_t prev_state = 10; //todo only for dbg
					if(timestamp_is_equal((*op)[I].ts.version,    (*op)[I].ts.tie_breaker_id,
										  lock_free_read_meta.last_local_write_ts.version,
										  lock_free_read_meta.last_local_write_ts.tie_breaker_id))
					{
						///Lock and check again if ack TS == last local write
						cctrl_lock(&curr_meta->cctrl);
						if(timestamp_is_equal((*op)[I].ts.version,    (*op)[I].ts.tie_breaker_id,
											  curr_meta->last_local_write_ts.version,
											  curr_meta->last_local_write_ts.tie_breaker_id))
						{
							bv_bit_set((bit_vector_t*) &curr_meta->ack_bv, (*op)[I].sender);
							if (is_last_ack(curr_meta->ack_bv, curr_membership)) { //if last local write completed
								op_buff_indx = curr_meta->op_buffer_index;
								prev_state = curr_meta->state; //todo only for dbg
								switch (curr_meta->state) {
									case VALID_STATE:
									case INVALID_STATE:
										(*op)[I].opcode = ST_LAST_ACK_NO_BCAST_SUCCESS;
										curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY; //reset the write buff index
										break;
									case INVALID_WRITE_STATE:
										curr_meta->state = INVALID_STATE;
										(*op)[I].opcode = ST_LAST_ACK_NO_BCAST_SUCCESS;
										curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY; //reset the write buff index
										break;
									case WRITE_STATE:
									case REPLAY_STATE:
										curr_meta->state = VALID_STATE;
										(*op)[I].opcode = ST_LAST_ACK_SUCCESS;
										curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY; //reset the write buff index
										break;
									default:
										assert(0);
								}
							}
						}
						cctrl_unlock_dec_version(&curr_meta->cctrl);
					}
					if((*op)[I].opcode == ST_LAST_ACK_SUCCESS ||
					   (*op)[I].opcode == ST_LAST_ACK_NO_BCAST_SUCCESS)
					{
						///completed read / write --> remove it from the ops buffer
						if(ENABLE_ASSERTIONS){
//							if(op_buff_indx == ST_OP_BUFFER_INDEX_EMPTY){
//								printf("DADAD\n");
//								printf("W%d--> ACK[%d]: Key state: %s, Key Hash:%" PRIu64
//									   " complete buff: %d\n\t op: %s, TS: %d | %d, sender: %d\n",
//									   thread_id, I, code_to_str(prev_state), ((uint64_t *) &(*op)[I].key)[1],
//									   op_buff_indx, code_to_str((*op)[I].opcode),
//									   (*op)[I].ts.version, (*op)[I].ts.tie_breaker_id,
//									   (*op)[I].sender);
//							}
							assert(op_buff_indx != ST_OP_BUFFER_INDEX_EMPTY);
							assert(read_write_op[op_buff_indx].op_meta.state == ST_IN_PROGRESS_PUT ||
								   read_write_op[op_buff_indx].op_meta.state == ST_OP_MEMBERSHIP_CHANGE ||
								   read_write_op[op_buff_indx].op_meta.state == ST_IN_PROGRESS_REPLAY);
							assert(((uint64_t *) &read_write_op[op_buff_indx].op_meta.key)[0] == ((uint64_t *) &(*op)[I].key)[0]);
						}
						if(read_write_op[op_buff_indx].op_meta.opcode == ST_OP_PUT)
							read_write_op[op_buff_indx].op_meta.state = ST_PUT_COMPLETE;
						else if(read_write_op[op_buff_indx].op_meta.opcode == ST_OP_GET)
							read_write_op[op_buff_indx].op_meta.state = ST_NEW;
						else
							assert(0);
					}
				}
			}
			if((*op)[I].opcode != ST_LAST_ACK_SUCCESS)
				(*op)[I].opcode = ST_ACK_SUCCESS;
		}
		if(key_in_store[I] == 0) //KVS miss --> We get here if either tag or log key match failed
			assert(0);
	}

	if (ENABLE_ASSERTIONS)
		for (I = 0; I < MAX_BATCH_OPS_SIZE; I++)
			assert(read_write_op[I].op_meta.opcode == ST_OP_GET ||
				   read_write_op[I].op_meta.state == ST_MISS ||
				   read_write_op[I].op_meta.state == ST_PUT_STALL ||
				   read_write_op[I].op_meta.state == ST_PUT_SUCCESS ||
				   read_write_op[I].op_meta.state == ST_PUT_COMPLETE ||
				   read_write_op[I].op_meta.state == ST_IN_PROGRESS_PUT ||
				   read_write_op[I].op_meta.state == ST_OP_MEMBERSHIP_CHANGE || ///TODO check this
				   read_write_op[I].op_meta.state == ST_IN_PROGRESS_REPLAY);
}

void
batch_vals_to_KVS(int op_num, spacetime_val_t **op, spacetime_op_t *read_write_op, int thread_id)
{
	int I, j;	/* I is batch index */
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
			assert((*op)[I].ts.version % 2 == 0);
			assert((*op)[I].opcode == ST_OP_VAL);
			assert(REMOTE_MACHINES != 1 || (*op)[I].sender == REMOTE_MACHINES - machine_id);
			assert(REMOTE_MACHINES != 1 || (*op)[I].ts.tie_breaker_id == REMOTE_MACHINES - machine_id);
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
								printf("Worker %u stuck on a lock-free read (for VAL)\n", thread_id);
								debug_cntr = 0;
							}
						}
						lock_free_read_meta = *curr_meta;
					} while (!cctrl_timestamp_is_same_and_valid(&lock_free_read_meta.cctrl, &curr_meta->cctrl));
					///lock and proceed iff remote.TS == local.TS
					if(timestamp_is_equal(lock_free_read_meta.cctrl.ts.version,
										  lock_free_read_meta.cctrl.ts.tie_breaker_id,
										  (*op)[I].ts.version,   (*op)[I].ts.tie_breaker_id))
					{
						///Lock and check again if still TS == local timestamp
						cctrl_lock(&curr_meta->cctrl);
						///Warning: use op.version + 1 bellow since optik_lock() increases curr_meta->version by 1
						if(timestamp_is_equal(curr_meta->cctrl.ts.version - 1,
											  curr_meta->cctrl.ts.tie_breaker_id,
											  (*op)[I].ts.version,   (*op)[I].ts.tie_breaker_id))
						{
							if(ENABLE_ASSERTIONS)
								assert(curr_meta->state != WRITE_STATE); ///WARNING: this should not happen w/o this node removed from the group
							curr_meta->state = VALID_STATE;
						}
						cctrl_unlock_dec_version(&curr_meta->cctrl);
					}
				}
			}
			(*op)[I].opcode = ST_VAL_SUCCESS;
		}
		if(key_in_store[I] == 0)//KVS miss --> We get here if either tag or log key match failed
			assert(0);
	}
}


//////////////////////////////////////////////////
/////////////////////// FAILURES
//////////////////////////////////////////////////

void
group_membership_init(void)
{
	group_membership.num_of_alive_remotes = REMOTE_MACHINES;
	seqlock_init(&group_membership.lock);
	bv_init((bit_vector_t*) &group_membership.g_membership);

	for(uint8_t i = 0; i < MACHINE_NUM; ++i)
		bv_bit_set((bit_vector_t*) &group_membership.g_membership, i);

	bv_copy((bit_vector_t*) &group_membership.w_ack_init, group_membership.g_membership);
	bv_reverse((bit_vector_t*) &group_membership.w_ack_init);
	bv_bit_set((bit_vector_t*) &group_membership.w_ack_init, (uint8_t) machine_id);
}

void
reconfigure_wrs(struct ibv_send_wr *inv_send_wr, struct ibv_sge *inv_send_sgl,
				struct ibv_send_wr *val_send_wr, struct ibv_sge *val_send_sgl,
				spacetime_group_membership last_g_membership, uint16_t worker_lid)
{
	int i_mod_remotes, sgl_index;
	uint16_t curr_machine_id, i , j;
	uint16_t rm_worker_gid, rm_id_index = 0;
	int remote_machine_ids[GROUP_MEMBERSHIP_ARRAY_SIZE * 8] = {0};
	//first get all alive remote ids
	for(i = 0; i < GROUP_MEMBERSHIP_ARRAY_SIZE; i++){
		for (j = 0; j < 8; j++){
			curr_machine_id = (uint16_t) (i * 8 + j);
			if(curr_machine_id == machine_id) continue;
			if(node_is_in_membership(last_g_membership, curr_machine_id))
				remote_machine_ids[rm_id_index++] = curr_machine_id;
		}
	}
	if(worker_lid == WORKER_EMULATING_FAILURE_DETECTOR){
		green_printf("Alive Remotes(%d): {", last_g_membership.num_of_alive_remotes);
		for(i = 0; i < rm_id_index; i++)
			yellow_printf(" %d,",remote_machine_ids[i]);
		green_printf("}\n");
	}

	for(i = 0; i < MAX_PCIE_BCAST_BATCH * last_g_membership.num_of_alive_remotes; i++){
		i_mod_remotes = i % group_membership.num_of_alive_remotes;
		sgl_index = i / group_membership.num_of_alive_remotes;
		rm_worker_gid = (uint16_t) (remote_machine_ids[i_mod_remotes] * WORKERS_PER_MACHINE + worker_lid);
		inv_send_wr[i].wr.ud.ah = remote_worker_qps[rm_worker_gid][INV_UD_QP_ID].ah;
		val_send_wr[i].wr.ud.ah = remote_worker_qps[rm_worker_gid][VAL_UD_QP_ID].ah;

		inv_send_wr[i].wr.ud.remote_qpn = (uint32) remote_worker_qps[rm_worker_gid][INV_UD_QP_ID].qpn;
		val_send_wr[i].wr.ud.remote_qpn = (uint32) remote_worker_qps[rm_worker_gid][VAL_UD_QP_ID].qpn;

		inv_send_wr[i].sg_list = &inv_send_sgl[sgl_index];
		val_send_wr[i].sg_list = &val_send_sgl[sgl_index];
	}


}

void
reset_bcast_send_buffers(spacetime_inv_packet_t *inv_send_packet_ops, int *inv_push_ptr,
						 spacetime_val_packet_t *val_send_packet_ops, int *val_push_ptr)
{
	int i , j;
	*inv_push_ptr = 0;
	*val_push_ptr = 0;

	for(i = 0; i < INV_SEND_OPS_SIZE; i++){
		inv_send_packet_ops[i].req_num = 0;
		for(j = 0; j < INV_MAX_REQ_COALESCE; j++)
			inv_send_packet_ops[i].reqs[j].op_meta.opcode = ST_EMPTY;
	}

	for(i = 0; i < VAL_SEND_OPS_SIZE; i++){
		val_send_packet_ops[i].req_num = 0;
		for(j = 0; j < VAL_MAX_REQ_COALESCE; j++)
			val_send_packet_ops[i].reqs[j].opcode = ST_EMPTY;
	}
}

void
complete_writes_and_replays_on_follower_removal(int op_num, spacetime_op_t **op,
												spacetime_group_membership curr_membership,
												int thread_id)
{
	int I, j;	/* I is batch index */
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
		if ((*op)[I].op_meta.state != ST_IN_PROGRESS_PUT &&
			(*op)[I].op_meta.state != ST_IN_PROGRESS_REPLAY &&
			(*op)[I].op_meta.state != ST_OP_MEMBERSHIP_CHANGE) continue;
//			cyan_printf("Ops[%d]=== hash(1st 8B):%" PRIu64 "\n", I, ((uint64_t *) &(*op)[I].key)[1]);
		bkt[I] = (*op)[I].op_meta.key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].op_meta.key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		if ((*op)[I].op_meta.state != ST_IN_PROGRESS_PUT &&
			(*op)[I].op_meta.state != ST_IN_PROGRESS_REPLAY &&
			(*op)[I].op_meta.state != ST_OP_MEMBERSHIP_CHANGE) continue;
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
		if ((*op)[I].op_meta.state != ST_IN_PROGRESS_PUT &&
			(*op)[I].op_meta.state != ST_IN_PROGRESS_REPLAY &&
			(*op)[I].op_meta.state != ST_OP_MEMBERSHIP_CHANGE) continue;
		uint8_t prev_state = (*op)[I].op_meta.state;
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I].op_meta.key;

			if(key_ptr_log[1] == key_ptr_req[0]) { //Key Found 8 Byte keys
				key_in_store[I] = 1;

				spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr[I]->value;

				uint32_t debug_cntr = 0;
				do { //Lock free read of keys meta
					if (ENABLE_ASSERTIONS) {
						debug_cntr++;
						if (debug_cntr == M_4) {
							printf("Worker %u stuck on a lock-free read (for completing writes)\n", thread_id);
							debug_cntr = 0;
						}
					}
					lock_free_read_meta = *curr_meta;
				} while (!cctrl_timestamp_is_same_and_valid(&lock_free_read_meta.cctrl, &curr_meta->cctrl));

				if(is_last_ack(lock_free_read_meta.ack_bv, curr_membership)) {
					cctrl_lock(&curr_meta->cctrl);
					if(is_last_ack(curr_meta->ack_bv, curr_membership)){
						if(ENABLE_ASSERTIONS)
							assert(curr_meta->op_buffer_index == I);
						printf("AAA\n");
						curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY; //reset the write buff index
						switch (curr_meta->state) {
							case VALID_STATE:
							case INVALID_STATE:
								(*op)[I].op_meta.state = ST_PUT_COMPLETE;
								break;
							case INVALID_WRITE_STATE:
								if(ENABLE_ASSERTIONS)
									assert((*op)[I].op_meta.state == ST_IN_PROGRESS_PUT ||
									       (*op)[I].op_meta.state == ST_OP_MEMBERSHIP_CHANGE);
								curr_meta->state = INVALID_STATE;
								(*op)[I].op_meta.state = ST_PUT_COMPLETE;
								break;
							case WRITE_STATE:
								if(ENABLE_ASSERTIONS)
									assert((*op)[I].op_meta.state == ST_IN_PROGRESS_PUT ||
									       (*op)[I].op_meta.state == ST_OP_MEMBERSHIP_CHANGE);
								curr_meta->state = VALID_STATE;
								(*op)[I].op_meta.ts.version = curr_meta->cctrl.ts.version - 1; // -1 because of optik lock does version + 1
								(*op)[I].op_meta.ts.tie_breaker_id = curr_meta->cctrl.ts.tie_breaker_id;
								(*op)[I].op_meta.state = DISABLE_VALS_FOR_DEBUGGING == 1 ? ST_PUT_COMPLETE : ST_PUT_COMPLETE_SEND_VALS; ///ops state for sending VALs
								break;
							case REPLAY_STATE:
								if(ENABLE_ASSERTIONS) assert((*op)[I].op_meta.state == ST_IN_PROGRESS_REPLAY);
								curr_meta->state = VALID_STATE;
								(*op)[I].op_meta.ts.version = curr_meta->cctrl.ts.version - 1; // -1 because of optik lock does version + 1
								(*op)[I].op_meta.ts.tie_breaker_id = curr_meta->cctrl.ts.tie_breaker_id;
								(*op)[I].op_meta.state = DISABLE_VALS_FOR_DEBUGGING == 1 ? ST_GET_COMPLETE : ST_REPLAY_COMPLETE; ///ops state for sending VALs
								break;
							default:
								assert(0);
						}
					}
					cctrl_unlock_dec_version(&curr_meta->cctrl);
				}
			}
		}
		if(prev_state == ST_OP_MEMBERSHIP_CHANGE)
			(*op)[I].op_meta.state = ST_OP_MEMBERSHIP_CHANGE;

		if(key_in_store[I] == 0)  //KVS miss --> We get here if either tag or log key match failed
			assert(0);

	}
}
