#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

// Optik Options
#define DEFAULT
#define CORE_NUM 4

#include <optik_mod.h>
#include "optik_mod.h"
#include "cache.h"

struct cache cache;

char* code_to_str(uint8_t code);
void cache_meta_aggregate(void);
void cache_meta_reset(struct cache_meta_stats* meta);
void extended_cache_meta_reset(struct extended_cache_meta_stats* meta);
void update_cache_stats(int op_num, int thread_id, struct extended_cache_op **op, struct mica_resp *resp, long long stalled_brcs);
void cache_reset_total_ops_issued(void);
void cache_add_2_total_ops_issued(long long ops_issued);



/*
 * Initialize the cache using a Mica instances and adding the timestamps
 * and locks to the keys of mica structure
 */
void cache_init(int cache_id, int num_threads) {
	int i;
	assert(sizeof(cache_meta) == 8); //make sure that the cache meta are 8B and thus can fit in mica unused key

	cache.num_threads = num_threads;
	//TODO add a Define for stats
	cache_reset_total_ops_issued();
	/// allocate and init metadata for the cache & threads
	extended_cache_meta_reset(&cache.aggregated_meta);
	cache.meta = malloc(num_threads * sizeof(struct cache_meta_stats));
	for(i = 0; i < num_threads; i++)
		cache_meta_reset(&cache.meta[i]);
	mica_init(&cache.hash_table, cache_id, CACHE_SOCKET, CACHE_NUM_BKTS, HERD_LOG_CAP);
	cache_populate_fixed_len(&cache.hash_table, CACHE_NUM_KEYS, HERD_VALUE_SIZE);
}

/* ---------------------------------------------------------------------------
------------------------------ STRONG CONSISTENCY--------------------------------
---------------------------------------------------------------------------*/

/* This is used to propagate all the regular ops from the trace to the cache,
 * The size of the op depends both on the key-value size and on the coalescing degree */
void cache_batch_op_sc_non_stalling_sessions(int op_num, int thread_id, struct extended_cache_op **op, struct mica_resp *resp) {
	protocol=STRONG_CONSISTENCY;
	int I, j;	/* I is batch index */
	long long stalled_brces = 0;
#if CACHE_DEBUG == 1
	//assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if CACHE_DEBUG == 2
	for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif

	unsigned int bkt[CACHE_BATCH_SIZE];
	struct mica_bkt *bkt_ptr[CACHE_BATCH_SIZE];
	unsigned int tag[CACHE_BATCH_SIZE];
	int key_in_store[CACHE_BATCH_SIZE];	/* Is this key in the datastore? */
	struct cache_op *kv_ptr[CACHE_BATCH_SIZE];	/* Ptr to KV item in log */


	// for(I = 0; I < op_num; I++) {
	// 	printf("%s\n", );
	// }

	/*
	 * We first lookup the key in the datastore. The first two @I loops work
	 * for both GETs and PUTs.
	 */
	for(I = 0; I < op_num; I++) {
		if(resp[I].type == UNSERVED_CACHE_MISS) continue;
		if (ENABLE_WAKE_UP == 1)
			if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
		bkt[I] = (*op)[I].key.bkt & cache.hash_table.bkt_mask;
		bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = (*op)[I].key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(I = 0; I < op_num; I++) {
		if(resp[I].type == UNSERVED_CACHE_MISS) continue;
		if (ENABLE_WAKE_UP == 1)
			if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
		for(j = 0; j < 8; j++) {
			if(bkt_ptr[I]->slots[j].in_use == 1 &&
			   bkt_ptr[I]->slots[j].tag == tag[I]) {
				uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
									  cache.hash_table.log_mask;

				/*
				 * We can interpret the log entry as mica_op, even though it
				 * may not contain the full MICA_MAX_VALUE value.
				 */
				kv_ptr[I] = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

				/* Small values (1--64 bytes) can span 2 cache lines */
				__builtin_prefetch(kv_ptr[I], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

				/* Detect if the head has wrapped around for this index entry */
				if(cache.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= cache.hash_table.log_cap) {
					kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
				}

				break;
			}
		}
	}

	// the following volatile variables used to validate atomicity between a lock-free read of an object
	volatile cache_meta prev_meta;
	for(I = 0; I < op_num; I++) {
		if(resp[I].type == UNSERVED_CACHE_MISS) continue;
		if (ENABLE_WAKE_UP == 1)
			if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
		if(kv_ptr[I] != NULL) {
			/* We had a tag match earlier. Now compare log entry. */
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &(*op)[I];

			//TODO except from the CRCW implementation (below) implement CREW too
			if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
				key_in_store[I] = 1;

				if ((*op)[I].opcode == CACHE_OP_GET) {
					//Lock free reads through versioning (successful when version is even)
					uint8_t was_locked_read = 0;
					do {
						memcpy((void*) &prev_meta, (void*) &(kv_ptr[I]->key.meta), sizeof(cache_meta));
						switch(kv_ptr[I]->key.meta.state) {
							case VALID_STATE:
								resp[I].val_ptr = kv_ptr[I]->value;
								resp[I].val_len = kv_ptr[I]->val_len;
								resp[I].type = CACHE_GET_SUCCESS;
								break;
							case INVALID_REPLAY_STATE:
							case WRITE_REPLAY_STATE:
								resp[I].type = CACHE_GET_STALL;
								break;
								//TODO the next 2 cases are changing the state!!
							default: //TODO transient, INVALID_STATE, WRITE_STATE, are handled with locking by default
								was_locked_read = 1;
								optik_lock(&kv_ptr[I]->key.meta);
								switch(kv_ptr[I]->key.meta.state) {
									case VALID_STATE:
										resp[I].val_ptr = kv_ptr[I]->value;
										resp[I].val_len = kv_ptr[I]->val_len;
										resp[I].type = CACHE_GET_SUCCESS;
										break;
									case INVALID_REPLAY_STATE:
									case WRITE_REPLAY_STATE:
										resp[I].type = CACHE_GET_STALL;
										break;
									case INVALID_STATE:
										kv_ptr[I]->key.meta.state = INVALID_REPLAY_STATE;
										resp[I].type = CACHE_GET_STALL;
										break;
									case WRITE_STATE:
										kv_ptr[I]->key.meta.state = WRITE_REPLAY_STATE;
										resp[I].type = CACHE_GET_STALL;
										break;
									default: assert(0);
								}
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								break;
						}
					} while (!optik_is_same_version_and_valid(prev_meta, kv_ptr[I]->key.meta) && was_locked_read == 0);
					(*op)[I].key.meta.state = kv_ptr[I]->key.meta.state; // TODO TODO TODO vasilis put this here for debugging
				} else if ((*op)[I].opcode == CACHE_OP_PUT) {
					assert((*op)[I].val_len == kv_ptr[I]->val_len); //TODO add Debug defines to this assert

					optik_lock(&kv_ptr[I]->key.meta);
					switch(kv_ptr[I]->key.meta.state) {
						case VALID_STATE:///WARNING: Do not use break here!
						case INVALID_STATE:
							kv_ptr[I]->key.meta.state = WRITE_STATE;
              memcpy(kv_ptr[I]->value, (*op)[I].value, kv_ptr[I]->val_len);
							kv_ptr[I]->key.meta.pending_acks = MACHINE_NUM - 1;
							optik_unlock_write(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (uint32_t*) &(*op)[I].key.meta.version);
							resp[I].type = CACHE_PUT_SUCCESS;
							break;
						case WRITE_STATE:
              if((*op)[I].key.meta.cid >= kv_ptr[I]->key.meta.cid) {
								memcpy(kv_ptr[I]->value, (*op)[I].value, kv_ptr[I]->val_len);
								kv_ptr[I]->key.meta.pending_acks = MACHINE_NUM - 1;
								optik_unlock_write(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (uint32_t*) &(*op)[I].key.meta.version);
								resp[I].type = CACHE_PUT_SUCCESS;
							}else{
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								resp[I].type = CACHE_PUT_FAIL;
							}
							break;
						case INVALID_REPLAY_STATE:
							optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
							resp[I].type = CACHE_PUT_STALL;
							break;
						case WRITE_REPLAY_STATE:
							if((*op)[I].key.meta.cid > kv_ptr[I]->key.meta.cid) {
								memcpy(kv_ptr[I]->value, (*op)[I].value, kv_ptr[I]->val_len);
								kv_ptr[I]->key.meta.pending_acks = MACHINE_NUM - 1;

								optik_unlock_write(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (uint32_t*) &(*op)[I].key.meta.version);
								resp[I].type = CACHE_PUT_SUCCESS;
							}else if((*op)[I].key.meta.cid == kv_ptr[I]->key.meta.cid) {
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								resp[I].type = CACHE_PUT_STALL;
							}else{
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								resp[I].type = CACHE_PUT_FAIL;
							}
							break;
						default: assert(0);
					}

					if(resp[I].type == CACHE_PUT_SUCCESS){
						(*op)[I].key.meta.cid = (uint8_t) machine_id;
						(*op)[I].opcode = CACHE_OP_BRC;

						resp[I].val_len = 0;
						resp[I].val_ptr = NULL;
					}

				} else if ((*op)[I].opcode == CACHE_OP_UPD) {
					assert((*op)[I].val_len == kv_ptr[I]->val_len); //TODO add Debug defines to this assert

					optik_lock(&kv_ptr[I]->key.meta);
					if (optik_is_same_version_plus_one(kv_ptr[I]->key.meta, (*op)[I].key.meta) &&
						(kv_ptr[I]->key.meta.state == INVALID_STATE ||
						 kv_ptr[I]->key.meta.state == INVALID_REPLAY_STATE)) {
						memcpy(kv_ptr[I]->value, (*op)[I].value, kv_ptr[I]->val_len);
						kv_ptr[I]->key.meta.state = VALID_STATE;
						optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
						resp[I].type = CACHE_UPD_SUCCESS;
					} else{
						optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
						resp[I].type = CACHE_UPD_FAIL;
					}

				}else if((*op)[I].opcode == CACHE_OP_INV){
					optik_lock(&kv_ptr[I]->key.meta);
					switch(kv_ptr[I]->key.meta.state) {
						case VALID_STATE:
						case INVALID_STATE:
							if (optik_is_greater_version(kv_ptr[I]->key.meta, (*op)[I].key.meta)) {
								kv_ptr[I]->key.meta.state = INVALID_STATE;
								optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
								resp[I].type = CACHE_INV_SUCCESS;
							}else {
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								resp[I].type = CACHE_INV_FAIL;
							}
							break;
						case WRITE_STATE:
							if (optik_is_greater_version_session(kv_ptr[I]->key.meta, (*op)[I].key.meta, machine_id)){
								kv_ptr[I]->key.meta.state = INVALID_STATE;
								optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
								resp[I].type = CACHE_INV_SUCCESS;
							}else{
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								resp[I].type = CACHE_INV_FAIL;
							}
							break;
						case INVALID_REPLAY_STATE:
							if(optik_is_greater_version(kv_ptr[I]->key.meta, (*op)[I].key.meta)) {
								kv_ptr[I]->key.meta.state = INVALID_REPLAY_STATE;
								optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
								resp[I].type = CACHE_INV_SUCCESS;
							}else{
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								resp[I].type = CACHE_INV_FAIL;
							}
							break;
						case WRITE_REPLAY_STATE:
							if(optik_is_greater_version_session(kv_ptr[I]->key.meta, (*op)[I].key.meta, machine_id)){
								kv_ptr[I]->key.meta.state  = INVALID_REPLAY_STATE;
								optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
								resp[I].type = CACHE_INV_SUCCESS;
							} else{
								optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
								resp[I].type = CACHE_INV_FAIL;
							}
							break;
						default: assert(0);
					}
					(*op)[I].opcode = CACHE_OP_ACK;

				}else if((*op)[I].opcode == CACHE_OP_ACK){
					//
					uint8_t m_id = (*op)[I].key.meta.cid; // TODO TODO vasilis put this here to allow acks to have their own cid
					(*op)[I].key.meta.cid = (uint8_t) machine_id; // TODO TODO vasilis put this here to allow acks to have their own cid
					// printf("Incoming cid from ack: %d changed to %d\n",m_id, (*op)[I].key.meta.cid);
					optik_lock(&kv_ptr[I]->key.meta);
					if(( kv_ptr[I]->key.meta.state  == WRITE_STATE ||
						kv_ptr[I]->key.meta.state  == WRITE_REPLAY_STATE ) &&
					   (kv_ptr[I]->key.meta.version == (*op)[I].key.meta.version + 1)){//TODO ensure that this lock free check is correct
						if(kv_ptr[I]->key.meta.pending_acks == 1){
							kv_ptr[I]->key.meta.state  = VALID_STATE;
							kv_ptr[I]->key.meta.cid = (uint8_t) machine_id;
							kv_ptr[I]->key.meta.pending_acks = 0;
							resp[I].type = CACHE_LAST_ACK_SUCCESS;
						}else{
							kv_ptr[I]->key.meta.pending_acks--;
							resp[I].type = CACHE_ACK_SUCCESS;
						}
						optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
					}else{
						optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
						resp[I].type = CACHE_ACK_FAIL;
					}
					// printf("And cchanged back to %d\n",m_id );
					(*op)[I].key.meta.cid = (uint8_t)m_id; // TODO TODO vasilis put this here to allow acks to have their own cid
				} else if ((*op)[I].opcode == CACHE_OP_BRC)
					stalled_brces++;
				else assert(0);
			}
		}

		if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
			resp[I].val_len = 0;
			resp[I].val_ptr = NULL;
			resp[I].type = CACHE_MISS;
		}
	}
	if(ENABLE_CACHE_STATS == 1)
		update_cache_stats(op_num, thread_id, op, resp, stalled_brces);
}

/* This is used to propagate the incoming upds/acks that are sized according to the key-value size
 * Unused parts are stripped!! */
void cache_batch_op_sc_non_stalling_sessions_with_cache_op(int op_num, int thread_id, struct cache_op **op, struct mica_resp *resp) {
  protocol=STRONG_CONSISTENCY;
  int I, j;	/* I is batch index */
  long long stalled_brces = 0;
#if CACHE_DEBUG == 1
  //assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if CACHE_DEBUG == 2
  for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif

  unsigned int bkt[CACHE_BATCH_SIZE];
  struct mica_bkt *bkt_ptr[CACHE_BATCH_SIZE];
  unsigned int tag[CACHE_BATCH_SIZE];
  int key_in_store[CACHE_BATCH_SIZE];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[CACHE_BATCH_SIZE];	/* Ptr to KV item in log */


  // for(I = 0; I < op_num; I++) {
  // 	printf("%s\n", );
  // }

  /*
   * We first lookup the key in the datastore. The first two @I loops work
   * for both GETs and PUTs.
   */
  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
    if (ENABLE_WAKE_UP == 1)
      if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
    bkt[I] = (*op)[I].key.bkt & cache.hash_table.bkt_mask;
    bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
    __builtin_prefetch(bkt_ptr[I], 0, 0);
    tag[I] = (*op)[I].key.tag;

    key_in_store[I] = 0;
    kv_ptr[I] = NULL;
  }

  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
    if (ENABLE_WAKE_UP == 1)
      if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
    for(j = 0; j < 8; j++) {
      if(bkt_ptr[I]->slots[j].in_use == 1 &&
         bkt_ptr[I]->slots[j].tag == tag[I]) {
        uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
                              cache.hash_table.log_mask;

        /*
         * We can interpret the log entry as mica_op, even though it
         * may not contain the full MICA_MAX_VALUE value.
         */
        kv_ptr[I] = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

        /* Small values (1--64 bytes) can span 2 cache lines */
        __builtin_prefetch(kv_ptr[I], 0, 0);
        __builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

        /* Detect if the head has wrapped around for this index entry */
        if(cache.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= cache.hash_table.log_cap) {
          kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
        }

        break;
      }
    }
  }

  // the following volatile variables used to validate atomicity between a lock-free read of an object
  volatile cache_meta prev_meta;
  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
    if (ENABLE_WAKE_UP == 1)
      if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
    if(kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) &(*op)[I];

      //TODO except from the CRCW implementation (below) implement CREW too
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;

        if ((*op)[I].opcode == CACHE_OP_UPD) {
          assert((*op)[I].val_len == kv_ptr[I]->val_len); //TODO add Debug defines to this assert

          optik_lock(&kv_ptr[I]->key.meta);
          if (optik_is_same_version_plus_one(kv_ptr[I]->key.meta, (*op)[I].key.meta) &&
              (kv_ptr[I]->key.meta.state == INVALID_STATE ||
               kv_ptr[I]->key.meta.state == INVALID_REPLAY_STATE)) {
            memcpy(kv_ptr[I]->value, (*op)[I].value, kv_ptr[I]->val_len);
            kv_ptr[I]->key.meta.state = VALID_STATE;
            optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
            resp[I].type = CACHE_UPD_SUCCESS;
          } else{
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
            resp[I].type = CACHE_UPD_FAIL;
          }

        }else if((*op)[I].opcode == CACHE_OP_ACK){
          //
          uint8_t m_id = (*op)[I].key.meta.cid; // TODO TODO vasilis put this here to allow acks to have their own cid
          (*op)[I].key.meta.cid = (uint8_t) machine_id; // TODO TODO vasilis put this here to allow acks to have their own cid
          // printf("Incoming cid from ack: %d changed to %d\n",m_id, (*op)[I].key.meta.cid);
          optik_lock(&kv_ptr[I]->key.meta);
          if(( kv_ptr[I]->key.meta.state  == WRITE_STATE ||
               kv_ptr[I]->key.meta.state  == WRITE_REPLAY_STATE ) &&
             (kv_ptr[I]->key.meta.version == (*op)[I].key.meta.version + 1)){//TODO ensure that this lock free check is correct
            if(kv_ptr[I]->key.meta.pending_acks == 1){
              kv_ptr[I]->key.meta.state  = VALID_STATE;
              kv_ptr[I]->key.meta.cid = (uint8_t) machine_id;
              kv_ptr[I]->key.meta.pending_acks = 0;
              resp[I].type = CACHE_LAST_ACK_SUCCESS;
            }else{
              kv_ptr[I]->key.meta.pending_acks--;
              resp[I].type = CACHE_ACK_SUCCESS;
            }
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
          }else{
            optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
            resp[I].type = CACHE_ACK_FAIL;
          }
          // printf("And cchanged back to %d\n",m_id );
          (*op)[I].key.meta.cid = (uint8_t)m_id; // TODO TODO vasilis put this here to allow acks to have their own cid
        } else if ((*op)[I].opcode == CACHE_OP_BRC)
          stalled_brces++;
        else assert(0);
      }
    }

    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      resp[I].val_len = 0;
      resp[I].val_ptr = NULL;
      resp[I].type = CACHE_MISS;
    }
  }
//  if(ENABLE_CACHE_STATS == 1)
//    update_cache_stats(op_num, thread_id, op, resp, stalled_brces);
}

/* This is used to propagate the incoming invs that are sized according to the key size
 * Unused parts are stripped!! */
void cache_batch_op_sc_non_stalling_sessions_with_small_cache_op(int op_num, int thread_id, struct small_cache_op **op, struct mica_resp *resp) {
  protocol=STRONG_CONSISTENCY;
  int I, j;	/* I is batch index */
  long long stalled_brces = 0;
#if CACHE_DEBUG == 1
  //assert(cache.hash_table != NULL);
	assert(op != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if CACHE_DEBUG == 2
  for(I = 0; I < op_num; I++)
		mica_print_op(&(*op)[I]);
#endif

  unsigned int bkt[CACHE_BATCH_SIZE];
  struct mica_bkt *bkt_ptr[CACHE_BATCH_SIZE];
  unsigned int tag[CACHE_BATCH_SIZE];
  int key_in_store[CACHE_BATCH_SIZE];	/* Is this key in the datastore? */
  struct cache_op *kv_ptr[CACHE_BATCH_SIZE];	/* Ptr to KV item in log */


  // for(I = 0; I < op_num; I++) {
  // 	printf("%s\n", );
  // }

  /*
   * We first lookup the key in the datastore. The first two @I loops work
   * for both GETs and PUTs.
   */
  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
    if (ENABLE_WAKE_UP == 1)
      if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
    bkt[I] = (*op)[I].key.bkt & cache.hash_table.bkt_mask;
    bkt_ptr[I] = &cache.hash_table.ht_index[bkt[I]];
    __builtin_prefetch(bkt_ptr[I], 0, 0);
    tag[I] = (*op)[I].key.tag;

    key_in_store[I] = 0;
    kv_ptr[I] = NULL;
  }

  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
    if (ENABLE_WAKE_UP == 1)
      if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
    for(j = 0; j < 8; j++) {
      if(bkt_ptr[I]->slots[j].in_use == 1 &&
         bkt_ptr[I]->slots[j].tag == tag[I]) {
        uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
                              cache.hash_table.log_mask;

        /*
         * We can interpret the log entry as mica_op, even though it
         * may not contain the full MICA_MAX_VALUE value.
         */
        kv_ptr[I] = (struct cache_op *) &cache.hash_table.ht_log[log_offset];

        /* Small values (1--64 bytes) can span 2 cache lines */
        __builtin_prefetch(kv_ptr[I], 0, 0);
        __builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

        /* Detect if the head has wrapped around for this index entry */
        if(cache.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= cache.hash_table.log_cap) {
          kv_ptr[I] = NULL;	/* If so, we mark it "not found" */
        }

        break;
      }
    }
  }

  // the following volatile variables used to validate atomicity between a lock-free read of an object
  volatile cache_meta prev_meta;
  for(I = 0; I < op_num; I++) {
    if(resp[I].type == UNSERVED_CACHE_MISS) continue;
    if (ENABLE_WAKE_UP == 1)
      if (resp[I].type == CACHE_GET_STALL || resp[I].type == CACHE_PUT_STALL) continue;
    if(kv_ptr[I] != NULL) {
      /* We had a tag match earlier. Now compare log entry. */
      long long *key_ptr_log = (long long *) kv_ptr[I];
      long long *key_ptr_req = (long long *) &(*op)[I];

      //TODO except from the CRCW implementation (below) implement CREW too
      if(key_ptr_log[1] == key_ptr_req[1]) { //Cache Hit
        key_in_store[I] = 1;

       if((*op)[I].opcode == CACHE_OP_INV){
          optik_lock(&kv_ptr[I]->key.meta);
          switch(kv_ptr[I]->key.meta.state) {
            case VALID_STATE:
            case INVALID_STATE:
              if (optik_is_greater_version(kv_ptr[I]->key.meta, (*op)[I].key.meta)) {
                kv_ptr[I]->key.meta.state = INVALID_STATE;
                optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
                resp[I].type = CACHE_INV_SUCCESS;
              }else {
                optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
                resp[I].type = CACHE_INV_FAIL;
              }
              break;
            case WRITE_STATE:
              if (optik_is_greater_version_session(kv_ptr[I]->key.meta, (*op)[I].key.meta, machine_id)){
                kv_ptr[I]->key.meta.state = INVALID_STATE;
                optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
                resp[I].type = CACHE_INV_SUCCESS;
              }else{
                optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
                resp[I].type = CACHE_INV_FAIL;
              }
              break;
            case INVALID_REPLAY_STATE:
              if(optik_is_greater_version(kv_ptr[I]->key.meta, (*op)[I].key.meta)) {
                kv_ptr[I]->key.meta.state = INVALID_REPLAY_STATE;
                optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
                resp[I].type = CACHE_INV_SUCCESS;
              }else{
                optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
                resp[I].type = CACHE_INV_FAIL;
              }
              break;
            case WRITE_REPLAY_STATE:
              if(optik_is_greater_version_session(kv_ptr[I]->key.meta, (*op)[I].key.meta, machine_id)){
                kv_ptr[I]->key.meta.state  = INVALID_REPLAY_STATE;
                optik_unlock(&kv_ptr[I]->key.meta, (*op)[I].key.meta.cid, (*op)[I].key.meta.version);
                resp[I].type = CACHE_INV_SUCCESS;
              } else{
                optik_unlock_decrement_version(&kv_ptr[I]->key.meta);
                resp[I].type = CACHE_INV_FAIL;
              }
              break;
            default: assert(0);
          }
          (*op)[I].opcode = CACHE_OP_ACK;

        }
        else assert(0);
      }
    }

    if(key_in_store[I] == 0) {  //Cache miss --> We get here if either tag or log key match failed
      resp[I].val_len = 0;
      resp[I].val_ptr = NULL;
      resp[I].type = CACHE_MISS;
    }
  }
//  if(ENABLE_CACHE_STATS == 1)
//    update_cache_stats(op_num, thread_id, op, resp, stalled_brces);
}


void cache_populate_fixed_len(struct mica_kv* kv, int n, int val_len) {
	//assert(cache != NULL);
	assert(n > 0);
	assert(val_len > 0 && val_len <= MICA_MAX_VALUE);

	/* This is needed for the eviction message below to make sense */
	assert(kv->num_insert_op == 0 && kv->num_index_evictions == 0);

	int i;
	struct cache_op op;
	struct mica_resp resp;
	unsigned long long *op_key = (unsigned long long *) &op.key;

	/* Generate the keys to insert */
	uint128 *key_arr = mica_gen_keys(n);

	for(i = n - 1; i >= 0; i--) {
		optik_init(&op.key.meta);
		op.key.meta.pending_acks = 0;
		op.key.meta.state = VALID_STATE;
		op_key[1] = key_arr[i].second;
		op.opcode = CACHE_OP_PUT;

		//printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock, op.key.meta.state, op.key.meta.version, op.key.meta.cid);
		op.val_len = (uint8_t) (val_len >> SHIFT_BITS);
		uint8_t val = 'a';//(uint8_t) (op_key[1] & 0xff);
		memset(op.value, val, (uint8_t) val_len);

		mica_insert_one(kv, (struct mica_op *) &op, &resp);
	}

	assert(kv->num_insert_op == n);
	// printf("Cache: Populated instance %d with %d keys, length = %d. "
	// 			   "Index eviction fraction = %.4f.\n",
	// 	   cache.hash_table.instance_id, n, val_len,
	// 	   (double) cache.hash_table.num_index_evictions / cache.hash_table.num_insert_op);
}

//refill empty slots of array from the trace
int batch_from_trace_to_cache(int trace_iter, int thread_id, struct trace_command *trace,
							  struct extended_cache_op *ops, struct mica_resp *resp, struct key_home* kh, int isSC, uint16_t next_op_i,
								struct latency_flags* latency_info, struct timespec* start, uint16_t* hottest_keys_pointers) {
	int i = next_op_i, j = 0;
	uint8_t is_update = 0;
	uint64_t seed = 0xdeadbeef;
	uint32_t empty_reqs = 0;

	if (ENABLE_ASSERTIONS == 1) {
		for (j = 0; j < next_op_i; j++){
			if (resp[j].type == EMPTY) {
				printf("Client %d, j = %d\n",thread_id, j );
				assert(false);}
			assert(!(resp[j].type == CACHE_GET_SUCCESS || ops[j].opcode == CACHE_OP_INV ||
							 resp[j].type == CACHE_PUT_FAIL));
		}
	}
  if ((ENABLE_HOT_KEY_TRACKING == 1) && (isSC == 0)) memset(hottest_keys_pointers, 0, sizeof(uint16_t) * HOTTEST_KEYS_TO_TRACK);
//  green_printf("ST_NEW BUFFER \n");
	while (i < CACHE_BATCH_SIZE && trace[trace_iter].opcode != NOP) {

    if ((ENABLE_HOT_KEY_TRACKING == 1) && (DISABLE_CACHE == 0)) {
      if (trace[trace_iter].key_id < HOTTEST_KEYS_TO_TRACK) {
        if (IS_READ(trace[trace_iter].opcode) == 1) {
          if (hottest_keys_pointers[trace[trace_iter].key_id] == 0) {
            hottest_keys_pointers[trace[trace_iter].key_id] = i + 1; //tag the op_i+1 such that '0' means unused
            if (isSC == 1) ops[i].value[0] = (uint8_t) trace[trace_iter].key_id; //the op tracks the key it stores
//          yellow_printf("Start tracking key %d , in op  %d\n", trace[trace_iter].key_id, i);
          } else {
            uint16_t op_i = hottest_keys_pointers[trace[trace_iter].key_id] - 1;
//            printf("opcode %d, op_i %d key %d key_pointer_to_op %d \n",
//                   ops[op_i].opcode, op_i, trace[trace_iter].key_id, hottest_keys_pointers[trace[trace_iter].key_id]);
            if (ENABLE_ASSERTIONS) {
              assert((hottest_keys_pointers[trace[trace_iter].key_id] > 0));
							assert(ops[op_i].opcode == CACHE_OP_GET);
							assert(ops[op_i].val_len < 255);
						}
            ops[op_i].val_len++;
//          yellow_printf("tracked key %d , in op  %d, new val_len %d\n", trace[trace_iter].key_id, op_i, ops[op_i].val_len);
            trace_iter++;
            if (trace[trace_iter].opcode == NOP)
              trace_iter = 0;
            continue;
          }
        }
        else hottest_keys_pointers[trace[trace_iter].key_id] = 0; // on a write
      }
    }
		*(uint128 *) &ops[i] = trace[trace_iter].key_hash;
		if (MEASURE_LATENCY == 1) ops[i].key.meta.state = 0;
		// *(uint128 *) &ops[i] = CityHash128((char *) &(trace[trace_iter].key_id), 4);
		// printf("Cache: Key = %d, tag: %d. \n", trace[trace_iter].key_id, ops[i].key.tag);
		is_update = (IS_WRITE(trace[trace_iter].opcode)) ? (uint8_t) 1 : (uint8_t) 0;
		if (ENABLE_ASSERTIONS == 1) assert(WRITE_RATIO > 0 || is_update == 0);
		ops[i].opcode = is_update ? (uint8_t) CACHE_OP_PUT : (uint8_t) CACHE_OP_GET;
		ops[i].val_len = is_update ? (uint8_t) (HERD_VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0; // if it is not an update the val_len will not be included in the packet

		if(is_update)
			if (ENABLE_ASSERTIONS == 1) assert(ops[i].val_len > 0);
		ops[i].key.meta.cid = (uint8_t) ((ENABLE_MULTIPLE_SESSIONS == 1) ?
							  (uint8_t)(hrd_fastrand(&seed) % SESSIONS_PER_CLIENT)
							  * CLIENTS_PER_MACHINE + thread_id : thread_id); // only for multiple sessions

		if (MEASURE_LATENCY == 1) start_measurement(start, latency_info, trace[trace_iter].home_machine_id,
											ops, i, thread_id, trace[trace_iter].opcode, isSC, next_op_i);

		kh[i].machine = (uint8_t) trace[trace_iter].home_machine_id;
		kh[i].worker = (uint8_t) trace[trace_iter].home_worker_id;
		resp[i].type = EMPTY;
		if(ops[i].opcode == CACHE_OP_PUT) //put the folowing value
			str_to_binary(ops[i].value, "Armonia is the key to success!  ", HERD_VALUE_SIZE);
		trace_iter++;
		empty_reqs++;
		if (DISABLE_CACHE == 1) resp[i].type = CACHE_MISS;
		i++;
		if (trace[trace_iter].opcode == NOP)
			trace_iter = 0;
	}
	empty_reqs *= 100;
	c_stats[thread_id].empty_reqs_per_trace = (double) empty_reqs / CACHE_BATCH_SIZE;
	c_stats[thread_id].tot_empty_reqs_per_trace += c_stats[thread_id].empty_reqs_per_trace;
	if (DISABLE_CACHE == 0) {
		if(isSC == 1) CACHE_BATCH_OP_SC(i, thread_id, &ops, resp);
		//else cache_batch_op_ec(i, thread_id, &ops, resp);
	}
	if (MEASURE_LATENCY == 1 && (DISABLE_CACHE == 0)) {
		hot_request_bookkeeping_for_latency_measurements(start, latency_info, ops, i, thread_id, isSC, resp);
	}

	if (ENABLE_ASSERTIONS == 1) {
		for (j = 0; j < CACHE_BATCH_SIZE; j++) { assert(resp[j].type != RETRY); }
	}
	return trace_iter;
}

//issuer array from the trace
void create_req_from_trace(int* trace_iter, int thread_id,
						   struct trace_command *trace, struct cache_op *op) {

	uint8_t is_update = 0;
	uint64_t seed = 0xdeadbeef;
    int curr_index = *trace_iter;

	if (trace[curr_index].opcode == NOP){
		*trace_iter = 0;
        curr_index = 0;
	}
    memcpy(op, &trace[curr_index].key_hash,sizeof(uint128));
	is_update = (IS_WRITE(trace[curr_index].opcode)) ? (uint8_t) 1 : (uint8_t) 0;

	if (ENABLE_ASSERTIONS == 1) assert(WRITE_RATIO > 0 || is_update == 0);
	op->opcode = is_update ? (uint8_t) CACHE_OP_PUT : (uint8_t) CACHE_OP_GET;
	op->val_len = is_update ? (uint8_t) (HERD_VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0; // if it is not an update the val_len will not be included in the packet

	if (ENABLE_ASSERTIONS == 1 && is_update) assert(op->val_len > 0);
	op->key.meta.cid = (uint8_t) ((ENABLE_MULTIPLE_SESSIONS == 1) ?
						  (uint8_t)(hrd_fastrand(&seed) % SESSIONS_PER_CLIENT)
						  * CLIENTS_PER_MACHINE + thread_id : thread_id); // only for multiple sessions

	if(op->opcode == CACHE_OP_PUT) //put the folowing value
		str_to_binary(op->value, "Armonia is the key to success!  ", HERD_VALUE_SIZE);
	*trace_iter = *trace_iter + 1;
}

//propagate cache misses and empty the ops array for any non-failed cache access
void manage_cache_response(int trace_iter, struct trace_command *trace, struct cache_op *ops, struct mica_resp *resp, struct key_home* kh){
	int i = 0;
	for (i = 0; i < CACHE_BATCH_SIZE; i++) {
#if CACHE_DEBUG == 2
		static int count = 0;
		printf("Trace->Key: %llu\n", trace[(trace_iter - CACHE_BATCH_SIZE) + i].key_id);
		printf("\t Trace->Type: %s\n", IS_HOT(trace[(trace_iter - CACHE_BATCH_SIZE) + i].opcode) ? "HOT" : "NORMAL");
		printf("\t Op code: %s\n", code_to_str(ops[i].opcode));
		printf("\t Response code: %s\n", code_to_str(resp[i].type));
		printf("\t Counter : %d\n", count++);
		printf("\t Home (Machine:core) : %d:%d\n", kh[i].machine, kh[i].worker);
		/*if(IS_HOT(trace[(trace_iter - CACHE_BATCH_SIZE) + i].opcode))
            assert(resp[i].type != CACHE_MISS);
        else
            assert(resp[i].type == CACHE_MISS);
         */
#endif

#if CACHE_DEBUG == 1
		if (resp[j].type == CACHE_GET_SUCCESS)
                printf("\t Value (%d): %s\n", resp[j].val_len, resp[j].val_ptr);
#endif
		if (resp[i].type == CACHE_GET_SUCCESS || resp[i].type == CACHE_PUT_SUCCESS || resp[i].type == CACHE_UPD_SUCCESS || resp[i].type == CACHE_UPD_FAIL)
			resp[i].type = EMPTY;
		else if (resp[i].type == CACHE_GET_STALL || resp[i].type == CACHE_PUT_STALL)
			resp[i].type = EMPTY;
		else if (resp[i].type == CACHE_MISS) {
			//TODO create a local or remote request for ops[j]
			//to the Worker: trace[trace_iter + (i - CACHE_BATCH_SIZE)].home_worker_id
			//in the machine: trace[trace_iter + (i - CACHE_BATCH_SIZE)].home_machine_id
			resp[i].type = EMPTY;
		} else{
			printf("The response code was: %s", code_to_str(resp[i].type));
			assert(0);
		}
	}
}

void update_cache_stats(int op_num, int thread_id, struct extended_cache_op **op, struct mica_resp *resp, long long stalled_brcs) {
	int i = 0;
	for(i = 0; i < op_num; ++i){
		switch(resp[i].type) {
			case CACHE_GET_SUCCESS:
				cache.meta[thread_id].num_get_success++;
				break;
			case CACHE_PUT_SUCCESS:
				cache.meta[thread_id].num_put_success++;
				break;
			case CACHE_UPD_SUCCESS:
				cache.meta[thread_id].num_upd_success++;
				break;
			case CACHE_INV_SUCCESS:
				cache.meta[thread_id].num_inv_success++;
				break;
			case CACHE_ACK_SUCCESS:
			case CACHE_LAST_ACK_SUCCESS:
				cache.meta[thread_id].num_ack_success++;
				break;
			case CACHE_MISS:
				if((*op)[i].opcode == CACHE_OP_GET)
					cache.meta[thread_id].num_get_miss++;
				else if((*op)[i].opcode == CACHE_OP_PUT)
					cache.meta[thread_id].num_put_miss++;
				else assert(0);
				break;
			case CACHE_GET_STALL:
				cache.meta[thread_id].num_get_stall++;
				break;
			case CACHE_PUT_STALL:
				cache.meta[thread_id].num_put_stall++;
				break;
			case CACHE_UPD_FAIL:
				cache.meta[thread_id].num_upd_fail++;
				break;
			case CACHE_INV_FAIL:
				cache.meta[thread_id].num_inv_fail++;
				break;
			case CACHE_ACK_FAIL:
				cache.meta[thread_id].num_ack_fail++;
				break;
			case UNSERVED_CACHE_MISS:
				if((*op)[i].opcode == CACHE_OP_GET)
					cache.meta[thread_id].num_unserved_get_miss++;
				else if((*op)[i].opcode == CACHE_OP_PUT)
					cache.meta[thread_id].num_unserved_put_miss++;
				else assert(0);
				break;
			default: assert(0);
		}
	}
	cache.meta[thread_id].num_put_success -= stalled_brcs;
}

void cache_meta_reset(struct cache_meta_stats* meta){
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

void extended_cache_meta_reset(struct extended_cache_meta_stats* meta){
	meta->num_hit = 0;
	meta->num_miss = 0;
	meta->num_stall = 0;
	meta->num_coherence_fail = 0;
	meta->num_coherence_success = 0;

	cache_meta_reset(&meta->metadata);
}

void cache_reset_total_ops_issued(){
	cache.total_ops_issued = 0;
}

void cache_add_2_total_ops_issued(long long ops_issued){
	cache.total_ops_issued += ops_issued;
}

void cache_meta_aggregate(){
	int i = 0;
	for(i = 0; i < cache.num_threads; i++){
		cache.aggregated_meta.metadata.num_get_success += cache.meta[i].num_get_success;
		cache.aggregated_meta.metadata.num_put_success += cache.meta[i].num_put_success;
		cache.aggregated_meta.metadata.num_upd_success += cache.meta[i].num_upd_success;
		cache.aggregated_meta.metadata.num_inv_success += cache.meta[i].num_inv_success;
		cache.aggregated_meta.metadata.num_ack_success += cache.meta[i].num_ack_success;
		cache.aggregated_meta.metadata.num_get_stall += cache.meta[i].num_get_stall;
		cache.aggregated_meta.metadata.num_put_stall += cache.meta[i].num_put_stall;
		cache.aggregated_meta.metadata.num_upd_fail += cache.meta[i].num_upd_fail;
		cache.aggregated_meta.metadata.num_inv_fail += cache.meta[i].num_inv_fail;
		cache.aggregated_meta.metadata.num_ack_fail += cache.meta[i].num_ack_fail;
		cache.aggregated_meta.metadata.num_get_miss += cache.meta[i].num_get_miss;
		cache.aggregated_meta.metadata.num_put_miss += cache.meta[i].num_put_miss;
		cache.aggregated_meta.metadata.num_unserved_get_miss += cache.meta[i].num_unserved_get_miss;
		cache.aggregated_meta.metadata.num_unserved_put_miss += cache.meta[i].num_unserved_put_miss;
	}
	cache.aggregated_meta.num_hit = cache.aggregated_meta.metadata.num_get_success + cache.aggregated_meta.metadata.num_put_success;
	cache.aggregated_meta.num_miss = cache.aggregated_meta.metadata.num_get_miss + cache.aggregated_meta.metadata.num_put_miss;
	cache.aggregated_meta.num_stall = cache.aggregated_meta.metadata.num_get_stall + cache.aggregated_meta.metadata.num_put_stall;
	cache.aggregated_meta.num_coherence_fail = cache.aggregated_meta.metadata.num_upd_fail + cache.aggregated_meta.metadata.num_inv_fail
											   + cache.aggregated_meta.metadata.num_ack_fail;
	cache.aggregated_meta.num_coherence_success = cache.aggregated_meta.metadata.num_upd_success + cache.aggregated_meta.metadata.num_inv_success
												  + cache.aggregated_meta.metadata.num_ack_success;
}

void str_to_binary(uint8_t* value, char* str, int size){
	int i;
	for(i = 0; i < size; i++)
		value[i] = (uint8_t) str[i] ;
	value[size] = '\0';
}

void print_IOPS_and_time(struct timespec start, long long ops, int id){
	struct timespec end;
	clock_gettime(CLOCK_REALTIME, &end);
	double seconds = (end.tv_sec - start.tv_sec) +
					 (double) (end.tv_nsec - start.tv_nsec) / 1000000000;
	printf("Cache %d: %.2f IOPS. time: %.2f\n", id, ops / seconds, seconds);
}

void print_cache_stats(struct timespec start, int cache_id){
	long long total_reads, total_writes, total_ops, total_retries;
	struct extended_cache_meta_stats* meta = &cache.aggregated_meta;
	extended_cache_meta_reset(meta);
	cache_meta_aggregate();
	total_reads = meta->metadata.num_get_success + meta->metadata.num_get_miss;
	total_writes = meta->metadata.num_put_success + meta->metadata.num_put_miss;
	total_retries = meta->metadata.num_get_stall + meta->metadata.num_put_stall;
	total_ops = total_reads + total_writes;

	printf("~~~~~~~~ Cache %d Stats ~~~~~~~ :\n", cache_id);
	if(total_ops != cache.total_ops_issued){
		printf("Total_ops: %llu, 2nd total ops: %llu\n",total_ops, cache.total_ops_issued);
	}
	//assert(total_ops == cache.total_ops_issued);
	print_IOPS_and_time(start, cache.total_ops_issued, cache_id);
	printf("\t Total (GET/PUT) Ops: %.2f %% %lld   \n", 100.0 * total_ops / total_retries, total_ops);
	printf("\t\t Hit Rate: %.2f %%  \n", 100.0 * (meta->metadata.num_get_success + meta->metadata.num_put_success)/ total_ops);
	printf("\t\t Total Retries: %lld   \n", total_retries);
	printf("\t\t\t Reads: %.2f %% (%lld)   \n", 100.0 * total_reads / total_ops, total_reads);
	printf("\t\t\t\t Read Hit Rate  : %.2f %%  \n", 100.0 * meta->metadata.num_get_success / total_reads);
	printf("\t\t\t\t\t Hits   : %lld \n", meta->metadata.num_get_success);
	printf("\t\t\t\t\t Misses : %lld \n", meta->metadata.num_get_miss);
	printf("\t\t\t\t\t Retries: %lld \n", meta->metadata.num_get_stall);
	printf("\t\t\t Writes: %.2f %% (%lld)   \n", 100.0 * total_writes / total_ops, total_writes);
	printf("\t\t\t\t Write Hit Rate: %.2f %% \n", 100.0 * meta->metadata.num_put_success / total_writes);
	printf("\t\t\t\t\t Hits  : %llu \n", meta->metadata.num_put_success);
	printf("\t\t\t\t\t Misses: %llu \n", meta->metadata.num_put_miss);
	printf("\t\t\t\t\t Retries: %lld \n", meta->metadata.num_put_stall);
	printf("\t Total Coherence Ops: %lld   \n", meta->num_coherence_fail + meta->num_coherence_success);
	printf("\t\t Successful: %lld  (%.2f %%) \n", meta->num_coherence_success, 100.0 * meta->num_coherence_success / (meta->num_coherence_fail + meta->num_coherence_success));
	printf("\t\t Failed: %lld  (%.2f %%) \n", meta->num_coherence_fail, 100.0 * meta->num_coherence_fail / (meta->num_coherence_fail + meta->num_coherence_success));
	printf("\t\t\t Updates: %.2f %% (%lld)   \n", 100.0 * meta->metadata.num_upd_success / (meta->num_coherence_fail + meta->num_coherence_success), meta->metadata.num_upd_success + meta->metadata.num_upd_fail);
	printf("\t\t\t\t Successful: %lld  (%.2f %%) \n", meta->metadata.num_upd_success, 100.0 * meta->metadata.num_upd_success / (meta->metadata.num_upd_success + meta->metadata.num_upd_fail));
	printf("\t\t\t\t Failed: %lld  (%.2f %%) \n", meta->metadata.num_upd_fail, 100.0 * meta->metadata.num_upd_fail / (meta->metadata.num_upd_success + meta->metadata.num_upd_fail));
	printf("\t\t\t Invalidates: %.2f %% (%lld)   \n", 100.0 * meta->metadata.num_inv_success / (meta->num_coherence_fail + meta->num_coherence_success), meta->metadata.num_inv_success + meta->metadata.num_inv_fail  );
	printf("\t\t\t\t Successful: %lld  (%.2f %%) \n", meta->metadata.num_inv_success, 100.0 * meta->metadata.num_inv_success / (meta->metadata.num_inv_success + meta->metadata.num_inv_fail));
	printf("\t\t\t\t Failed: %lld  (%.2f %%) \n", meta->metadata.num_inv_fail, 100.0 * meta->metadata.num_inv_fail / (meta->metadata.num_inv_success + meta->metadata.num_inv_fail));
	printf("\t\t\t Acks: %.2f %% (%lld)   \n", 100.0 * meta->metadata.num_ack_success / (meta->num_coherence_fail + meta->num_coherence_success), meta->metadata.num_ack_success + meta->metadata.num_ack_fail );
	printf("\t\t\t\t Successful: %lld  (%.2f %%) \n", meta->metadata.num_ack_success, 100.0 * meta->metadata.num_ack_success / (meta->metadata.num_ack_success + meta->metadata.num_ack_fail));
	printf("\t\t\t\t Failed: %lld  (%.2f %%) \n", meta->metadata.num_ack_fail, 100.0 * meta->metadata.num_ack_fail / (meta->metadata.num_ack_success + meta->metadata.num_ack_fail));
}


char* code_to_str(uint8_t code){
	switch (code){
		case EMPTY:
			return "ST_EMPTY";
		case RETRY:
			return "RETRY";
		case CACHE_GET_SUCCESS:
			return "CACHE_GET_SUCCESS";
		case CACHE_PUT_SUCCESS:
			return "CACHE_PUT_SUCCESS";
		case CACHE_UPD_SUCCESS:
			return "CACHE_UPD_SUCCESS";
		case CACHE_INV_SUCCESS:
			return "CACHE_INV_SUCCESS";
		case CACHE_ACK_SUCCESS:
			return "CACHE_ACK_SUCCESS";
		case CACHE_LAST_ACK_SUCCESS:
			return "CACHE_LAST_ACK_SUCCESS";
		case CACHE_MISS:
			return "CACHE_MISS";
		case CACHE_GET_STALL:
			return "CACHE_GET_STALL";
		case CACHE_PUT_STALL:
			return "CACHE_PUT_STALL";
		case CACHE_UPD_FAIL:
			return "CACHE_UPD_FAIL";
		case CACHE_INV_FAIL:
			return "CACHE_INV_FAIL";
		case CACHE_ACK_FAIL:
			return "CACHE_ACK_FAIL";
		case CACHE_PUT_FAIL:
			return "CACHE_PUT_FAIL";
		case CACHE_GET_FAIL:
			return "CACHE_GET_FAIL";
		case CACHE_OP_GET:
			return "CACHE_OP_GET";
		case CACHE_OP_PUT:
			return "CACHE_OP_PUT";
		case CACHE_OP_UPD:
			return "CACHE_OP_UPD";
		case CACHE_OP_INV:
			return "CACHE_OP_INV";
		case CACHE_OP_ACK:
			return "CACHE_OP_ACK";
		case CACHE_OP_BRC:
			return "CACHE_OP_BRC";
		case UNSERVED_CACHE_MISS:
			return "UNSERVED_CACHE_MISS";
		case VALID_STATE:
			return "VALID_STATE";
		case INVALID_STATE:
			return "INVALID_STATE";
		case INVALID_REPLAY_STATE:
			return "INVALID_REPLAY_STATE";
		case WRITE_STATE:
			return "WRITE_STATE";
		case WRITE_REPLAY_STATE:
			return "WRITE_REPLAY_STATE";
		default: {
			printf("Wrong code (%d)\n", code);
			assert(0);
		}
	}
}
