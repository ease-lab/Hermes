//
// Created by akatsarakis on 07/03/19.
//

#include <spacetime.h>
#include <util.h>


//////////////////////////////////////////////////
////////////////////  Chain Replication / CRAQ KVS
//////////////////////////////////////////////////



//////////////////////////////////////////////////
//////////// Helper functions ////////////////////
static inline uint8_t
head_id()
{
	return 0;
}

static inline uint8_t
tail_id()
{
	return MACHINE_NUM - 1;
}


//////////// Assertion functions
static inline void
cr_assertions_inv(spacetime_inv_t* inv_ptr)
{
	assert(inv_ptr->op_meta.ts.version % 2 == 0);
	assert(inv_ptr->op_meta.opcode == ST_OP_INV ||
		   inv_ptr->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
	assert(inv_ptr->op_meta.val_len == ST_VALUE_SIZE);
}


//////////// Skip functions

static inline uint8_t
cr_skip_op(spacetime_op_t *op_ptr)
{
	return (uint8_t)
	          ((op_ptr->op_meta.state == ST_PUT_SUCCESS ||
				op_ptr->op_meta.state == ST_IN_PROGRESS_GET ||
				op_ptr->op_meta.state == ST_IN_PROGRESS_PUT  ) ? 1 : 0);
}

static inline uint8_t
cr_skip_inv(spacetime_inv_t *inv_ptr)
{
	return (uint8_t) (inv_ptr->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE ? 1 : 0);
}

static inline uint8_t
cr_skip_ack(spacetime_ack_t *ack_ptr)
{
	return (uint8_t) (ack_ptr->opcode == ST_OP_MEMBERSHIP_CHANGE ? 1 : 0);
}

static inline uint8_t
cr_skip_remote_reads(spacetime_op_t *op_ptr)
{
	return (uint8_t) ((op_ptr->op_meta.state == ST_EMPTY) ? 1 : 0);
}

static inline uint8_t
cr_skip_remote_writes(spacetime_op_t *op_ptr)
{
	return (uint8_t)
			  ((op_ptr->op_meta.state == ST_EMPTY ||
	          	op_ptr->op_meta.state == ST_PUT_SUCCESS ||
				op_ptr->op_meta.state == ST_IN_PROGRESS_PUT) ? 1 : 0);
}


//////////// Exec functions
static inline void
cr_exec_write(spacetime_op_t *op_ptr, struct mica_op *kv_ptr)
{
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1];

	if(ENABLE_ASSERTIONS){
		assert(machine_id == head_id()); // Only head must exec writes
		assert(op_ptr->op_meta.opcode == ST_OP_PUT);
		assert(op_ptr->op_meta.val_len == ST_VALUE_SIZE);
	}


	op_ptr->op_meta.state = ST_EMPTY;

	cctrl_lock(&curr_meta->cctrl);
	switch(curr_meta->state) {
		case INVALID_STATE:
			// Do not initiate a new write until you get to valid state
			if(CR_ENABLE_BLOCKING_INVALID_WRITES_ON_HEAD){
				cctrl_unlock_dec_version(&curr_meta->cctrl);
				op_ptr->op_meta.state = ST_PUT_STALL;
				break;
			}
		case VALID_STATE:
			curr_meta->state = INVALID_STATE;
			memcpy(kv_value_ptr, op_ptr->value, ST_VALUE_SIZE);
			kv_ptr->val_len = op_ptr->op_meta.val_len + sizeof(spacetime_object_meta);

			cctrl_unlock_inc_version(&curr_meta->cctrl, (uint8_t) machine_id,
									 (uint32_t *) &(op_ptr->op_meta.ts.version));

			op_ptr->op_meta.state = ST_PUT_SUCCESS;
			op_ptr->op_meta.ts.tie_breaker_id = (uint8_t) machine_id;
			break;
		default: assert(0);
	}
}

static inline void
cr_exec_remote_reads(spacetime_op_t *op_ptr, struct mica_op *kv_ptr)
{
    if(ENABLE_ASSERTIONS){
    	assert(machine_id == tail_id());
		assert(op_ptr->op_meta.opcode == ST_OP_GET);
    }

	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta prev_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1];

	do {
		prev_meta = *curr_meta;
		//switch template with all states
		switch(curr_meta->state) {
			case VALID_STATE:
				memcpy(op_ptr->value, kv_value_ptr, ST_VALUE_SIZE);
				op_ptr->op_meta.state = ST_GET_COMPLETE;
				op_ptr->op_meta.val_len = kv_ptr->val_len - sizeof(spacetime_object_meta);
				break;
			case INVALID_STATE:
			default: assert(0);
		}
	} while (!cctrl_timestamp_is_same_and_valid(&prev_meta.cctrl, &curr_meta->cctrl));
}

static inline void
cr_exec_op(spacetime_op_t *op_ptr, struct mica_op *kv_ptr, uint8_t idx)
{
    if(ENABLE_ASSERTIONS)
        assert(idx < max_batch_size);

	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta prev_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1];

	if (op_ptr->op_meta.opcode == ST_OP_GET) {
		//Lock free reads through versioning (successful when version is even)
		op_ptr->op_meta.state = ST_EMPTY;


		do {
			prev_meta = *curr_meta;
			//switch template with all states
			switch(curr_meta->state) {
				case VALID_STATE:
					memcpy(op_ptr->value, kv_value_ptr, ST_VALUE_SIZE);
					op_ptr->op_meta.state = ST_GET_COMPLETE;
					op_ptr->op_meta.val_len = kv_ptr->val_len - sizeof(spacetime_object_meta);
					break;
				case INVALID_STATE:
					if(ENABLE_ASSERTIONS)
						assert(machine_id != tail_id()); // tail should always be valid
					op_ptr->op_meta.state = ST_GET_STALL;
					break;
				default: assert(0);
			}
		} while (!cctrl_timestamp_is_same_and_valid(&prev_meta.cctrl, &curr_meta->cctrl));

		if(op_ptr->op_meta.state == ST_GET_STALL)
			op_ptr->buff_idx = idx;

	}

	else if (op_ptr->op_meta.opcode == ST_OP_PUT) {
		if(machine_id == head_id()) // if it is head
			cr_exec_write(op_ptr, kv_ptr);
		else
			op_ptr->op_meta.state = ST_PUT_SUCCESS;

		if(op_ptr->op_meta.state == ST_PUT_SUCCESS)
			// Set idx that we cannot set while dispatching the req
			op_ptr->buff_idx = idx;
	}
}

static inline void
cr_complete_local_write(spacetime_op_t *read_write_op, uint8_t idx, const uint64_t *key)
{
	///completed read / write --> remove it from the ops buffer
	if(ENABLE_ASSERTIONS){
		assert(read_write_op[idx].op_meta.state == ST_IN_PROGRESS_PUT);
		assert(((uint64_t *) &read_write_op[idx].op_meta.key)[0] == key[0]);
	}

	if(read_write_op[idx].op_meta.opcode == ST_OP_PUT)
		read_write_op[idx].op_meta.state = ST_PUT_COMPLETE;
	else assert(0);
}

static inline void
cr_exec_inv(spacetime_inv_t *inv_ptr, struct mica_op *kv_ptr,
			spacetime_op_t *read_write_op)
{
	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta lock_free_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1] ;
    if (ENABLE_ASSERTIONS)
        assert(inv_ptr->op_meta.opcode == ST_OP_INV);

    uint32_t debug_cntr = 0;
    do { //Lock free read of keys meta
        if (ENABLE_ASSERTIONS) {
            debug_cntr++;
            if (debug_cntr == M_4) {
                printf("Worker stuck on a lock-free read (for INV)\n");
                debug_cntr = 0;
            }
        }
        lock_free_meta = *curr_meta;
    } while (!cctrl_timestamp_is_same_and_valid(&lock_free_meta.cctrl, &curr_meta->cctrl));

    //lock and proceed iff remote.TS >= local.TS
    //inv TS >= local timestamp
    if(!timestamp_is_smaller(inv_ptr->op_meta.ts.version,  inv_ptr->op_meta.ts.tie_breaker_id,
                             lock_free_meta.cctrl.ts.version,
                             lock_free_meta.cctrl.ts.tie_breaker_id))
    {
        //Lock and check again if inv TS > local timestamp
        cctrl_lock(&curr_meta->cctrl);
        ///Warning: use op.version + 1 bellow since optik_lock() increases curr_meta->version by 1
        if(timestamp_is_smaller(curr_meta->cctrl.ts.version - 1,
                                curr_meta->cctrl.ts.tie_breaker_id,
                                inv_ptr->op_meta.ts.version,
                                inv_ptr->op_meta.ts.tie_breaker_id))
        {
//							printf("Received an invalidation with >= timestamp\n");
            ///Update Value, TS and last_writer_id
//				curr_meta->last_writer_id = inv_ptr->op_meta.sender;
            kv_ptr->val_len = inv_ptr->op_meta.val_len + sizeof(spacetime_object_meta);
            if(ENABLE_ASSERTIONS){
//					assert(kv_ptr->val_len == KVS_VALUE_SIZE >> SHIFT_BITS);
                assert(inv_ptr->op_meta.val_len == ST_VALUE_SIZE >> SHIFT_BITS);
            }
            memcpy(kv_value_ptr, inv_ptr->value, ST_VALUE_SIZE);
            ///Update state

            switch(curr_meta->state) {
                case VALID_STATE:
                    if(machine_id != tail_id()) // Tail never gets invalid
                        curr_meta->state = INVALID_STATE;
                    break;
                case INVALID_STATE:
                    break;
                default: assert(0);
            }
            cctrl_unlock_custom_version(&curr_meta->cctrl, inv_ptr->op_meta.ts.tie_breaker_id,
                                        inv_ptr->op_meta.ts.version);
        } else if(timestamp_is_equal(curr_meta->cctrl.ts.version - 1,
                                     curr_meta->cctrl.ts.tie_breaker_id,
                                     inv_ptr->op_meta.ts.version,
                                     inv_ptr->op_meta.ts.tie_breaker_id))
            assert(0);
        else
            cctrl_unlock_dec_version(&curr_meta->cctrl);
    }
	inv_ptr->op_meta.opcode = ST_INV_SUCCESS;

	if(inv_ptr->op_meta.initiator == machine_id && machine_id == tail_id())
		cr_complete_local_write(read_write_op, inv_ptr->buff_idx, (uint64_t *) &inv_ptr->op_meta.key);

	if(ENABLE_ASSERTIONS)
		assert(inv_ptr->op_meta.opcode == ST_INV_SUCCESS);
}


static inline void
cr_exec_ack(spacetime_ack_t *ack_ptr, struct mica_op *kv_ptr,
			spacetime_op_t *read_write_op)
{
	if(ENABLE_ASSERTIONS)
		assert(machine_id != tail_id());

	// the following variables used to validate atomicity between a lock-free read of an object
	spacetime_object_meta lock_free_read_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	if (ack_ptr->opcode != ST_OP_ACK) assert(0);

	uint32_t debug_cntr = 0;
	do { //Lock free read of keys meta
		if (ENABLE_ASSERTIONS) {
			debug_cntr++;
			if (debug_cntr == M_4) {
				printf("Worker stuck on a lock-free read (for ACK)\n");
				debug_cntr = 0;
			}
		}
		lock_free_read_meta = *curr_meta;
	} while (!cctrl_timestamp_is_same_and_valid(&lock_free_read_meta.cctrl, &curr_meta->cctrl));

	if(ENABLE_ASSERTIONS)
		assert(!timestamp_is_smaller(lock_free_read_meta.cctrl.ts.version,
									 lock_free_read_meta.cctrl.ts.tie_breaker_id,
									 ack_ptr->ts.version, ack_ptr->ts.tie_breaker_id));

	if(timestamp_is_equal(ack_ptr->ts.version,    ack_ptr->ts.tie_breaker_id,
						  lock_free_read_meta.cctrl.ts.version,
						  lock_free_read_meta.cctrl.ts.tie_breaker_id))
	{
		///Lock and check again if ack TS == last local write
		cctrl_lock(&curr_meta->cctrl);
		if(timestamp_is_equal(ack_ptr->ts.version,    ack_ptr->ts.tie_breaker_id,
							  curr_meta->cctrl.ts.version - 1, curr_meta->cctrl.ts.tie_breaker_id))
		{
			switch (curr_meta->state) {
				case INVALID_STATE:
					curr_meta->state = VALID_STATE;
					ack_ptr->opcode = ST_LAST_ACK_SUCCESS;
					break;
				case VALID_STATE:
				default:
					assert(0);
			}
		}
		cctrl_unlock_dec_version(&curr_meta->cctrl);
	}

	if(machine_id == ack_ptr->initiator)
		cr_complete_local_write(read_write_op, ack_ptr->buff_idx, (uint64_t *) &ack_ptr->key);

	ack_ptr->opcode = ST_LAST_ACK_SUCCESS;
}

//////////// Dispatcher functions

static inline uint8_t
cr_skip_dispatcher(enum cr_type_t cr_type, void* ptr)
{
	switch (cr_type){
		case Local_ops:
			return cr_skip_op(ptr);
		case Invs:
			return cr_skip_inv(ptr);
		case Acks:
			return cr_skip_ack(ptr);
		case Remote_reads:
			return cr_skip_remote_reads(ptr);
		case Remote_writes:
			return cr_skip_remote_writes(ptr);
		default: assert(0);
	}
}

static inline void
cr_assertions_dispatcher(enum cr_type_t cr_type, void* ptr)
{
	if(ENABLE_ASSERTIONS)
		switch (cr_type){
			case Invs:
				cr_assertions_inv(ptr);
			case Acks:
			case Remote_writes:
			case Local_ops:
			case Remote_reads:
				break;
			default: assert(0);
		}
}


static inline void
cr_exec_dispatcher(enum cr_type_t cr_type, void* op_ptr, struct mica_op *kv_ptr,
				   uint8_t idx, spacetime_op_t *read_write_op)
{

	switch (cr_type){
		case Invs:
			cr_exec_inv(op_ptr, kv_ptr, read_write_op);
			break;
		case Acks:
			cr_exec_ack(op_ptr, kv_ptr, read_write_op);
			break;
		case Remote_writes:
			cr_exec_write(op_ptr, kv_ptr);
			break;
		case Local_ops:
			cr_exec_op(op_ptr, kv_ptr, idx);
			break;
		case Remote_reads:
			cr_exec_remote_reads(op_ptr, kv_ptr);
			break;
		default: assert(0);
	}
}





//////////////////////////////////////////////////
//////////// Batch function //////////////////////
#define CR_MAX_BATCH_SIZE MAX(MAX(MAX_BATCH_OPS_SIZE, ACK_RECV_OPS_SIZE), INV_RECV_OPS_SIZE)
void
cr_batch_ops_to_KVS(enum cr_type_t cr_type, uint8_t *op_array, int op_num,
					uint16_t sizeof_op_elem, spacetime_op_t *read_write_op)
{
#if SPACETIME_DEBUG == 1
	//assert(kv.hash_table != NULL);
	assert(op_array != NULL);
	assert(op_num > 0 && op_num <= CACHE_BATCH_SIZE);
	assert(resp != NULL);
#endif

#if SPACETIME_DEBUG == 2
	for(I = 0; I < op_num; I++)
		mica_print_op(&(*op_array)[I]);
#endif
	int key_in_store[CR_MAX_BATCH_SIZE];	    // Is this key in the datastore?
	unsigned int tag[CR_MAX_BATCH_SIZE];
//	unsigned int bkt[CR_MAX_BATCH_SIZE];
    uint64_t     bkt[CR_MAX_BATCH_SIZE];
	struct mica_bkt *bkt_ptr[CR_MAX_BATCH_SIZE];
	struct mica_op   *kv_ptr[CR_MAX_BATCH_SIZE];	// Ptr to KV item in log


	if(ENABLE_ASSERTIONS)
		assert(read_write_op != NULL || cr_type != Acks);

	// We first lookup the key in the datastore.
	// The first two @I loops work for both GETs and PUTs.
	for(int I = 0; I < op_num; I++) {
		spacetime_op_meta_t* op_ptr = (spacetime_op_meta_t *) &op_array[sizeof_op_elem * I];
		cr_assertions_dispatcher(cr_type, op_ptr);
	    if(cr_skip_dispatcher(cr_type, op_ptr)) continue;

		bkt[I] = op_ptr->key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = op_ptr->key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(int I = 0; I < op_num; I++) {
		spacetime_op_meta_t* op_ptr = (spacetime_op_meta_t *) &op_array[sizeof_op_elem * I];
		if(cr_skip_dispatcher(cr_type, op_ptr)) continue;
		for(int j = 0; j < 8; j++) {
			if(bkt_ptr[I]->slots[j].in_use == 1 &&
			   bkt_ptr[I]->slots[j].tag == tag[I]) {
				uint64_t log_offset = bkt_ptr[I]->slots[j].offset &
									  kv.hash_table.log_mask;
				// We can interpret the log entry as mica_op, even though it
				// may not contain the full MICA_MAX_VALUE value.
				kv_ptr[I] = (struct mica_op*) &kv.hash_table.ht_log[log_offset];

				// Small values (1--64 bytes) can span 2 cache lines
				__builtin_prefetch(kv_ptr[I], 0, 0);
				__builtin_prefetch((uint8_t *) kv_ptr[I] + 64, 0, 0);

				// Detect if the head has wrapped around for this index entry
				if(kv.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= kv.hash_table.log_cap) {
					kv_ptr[I] = NULL;	// If so, we mark it "not found"
				}

				break;
			}
		}
	}

	for(int I = 0; I < op_num; I++) {
		spacetime_op_meta_t* op_ptr = (spacetime_op_meta_t *) &op_array[sizeof_op_elem * I];
		if(cr_skip_dispatcher(cr_type, op_ptr)) continue;
		if(kv_ptr[I] != NULL) {
			// We had a tag match earlier. Now compare log entry.
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &op_ptr->key;

			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;
				cr_exec_dispatcher(cr_type, op_ptr, kv_ptr[I], (uint8_t) I, read_write_op);
			}
		}

		if(key_in_store[I] == 0) // KVS miss --> We get here if either tag or log key match failed
			op_ptr->state = ST_MISS;
	}

	if(ENABLE_ASSERTIONS)
		if(cr_type == Acks)
			for (int I = 0; I < max_batch_size; I++)
				assert(read_write_op[I].op_meta.opcode == ST_OP_GET ||
					   read_write_op[I].op_meta.state == ST_MISS ||
					   read_write_op[I].op_meta.state == ST_EMPTY ||
					   read_write_op[I].op_meta.state == ST_PUT_STALL ||
					   read_write_op[I].op_meta.state == ST_PUT_SUCCESS ||
					   read_write_op[I].op_meta.state == ST_PUT_COMPLETE ||
					   read_write_op[I].op_meta.state == ST_IN_PROGRESS_PUT ||
					   read_write_op[I].op_meta.state == ST_OP_MEMBERSHIP_CHANGE || ///TODO check this
					   read_write_op[I].op_meta.state == ST_IN_PROGRESS_REPLAY);
}
