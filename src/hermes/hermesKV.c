//
// Created by akatsarakis on 07/03/19.
//

#include <spacetime.h>
#include <inline-util.h>

//////////////////////////////////////////////////
/////////////////////// HERMES KVS (SPACETIME)
//////////////////////////////////////////////////

//////////// Assertion functions

static inline void
hermes_assertions_begin_inv(spacetime_inv_t *inv_ptr)
{
	assert(inv_ptr->op_meta.ts.version % 2 == 0);
	assert(inv_ptr->op_meta.opcode == ST_OP_INV ||
		   inv_ptr->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
	assert(inv_ptr->op_meta.val_len == ST_VALUE_SIZE);
	assert(REMOTE_MACHINES != 1 ||
		   inv_ptr->op_meta.sender == REMOTE_MACHINES - machine_id);
	assert(REMOTE_MACHINES != 1 ||
		   inv_ptr->op_meta.ts.tie_breaker_id == REMOTE_MACHINES - machine_id);
//			red_printf("INVs: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
}

static inline void
hermes_assertions_begin_ack(spacetime_ack_t *ack_ptr)
{
	assert(ack_ptr->ts.version % 2 == 0);
	assert(REMOTE_MACHINES != 1 || ack_ptr->sender == REMOTE_MACHINES - machine_id);
	assert(ack_ptr->opcode == ST_OP_ACK || ack_ptr->opcode == ST_OP_MEMBERSHIP_CHANGE);
	assert(group_membership.num_of_alive_remotes != REMOTE_MACHINES ||
		   ack_ptr->ts.tie_breaker_id == machine_id ||
		   (ENABLE_VIRTUAL_NODE_IDS && ack_ptr->ts.tie_breaker_id  % MACHINE_NUM == machine_id));
//			yellow_printf("ACKS: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
}

static inline void
hermes_assertions_begin_val(spacetime_val_t *val_ptr)
{
	assert(val_ptr->ts.version % 2 == 0);
	assert(val_ptr->opcode == ST_OP_VAL);
	assert(REMOTE_MACHINES != 1 || val_ptr->sender == REMOTE_MACHINES - machine_id);
	assert(REMOTE_MACHINES != 1 || val_ptr->ts.tie_breaker_id == REMOTE_MACHINES - machine_id);
//			green_printf("VALS: Ops[%d]vvv hash(1st 8B):%" PRIu64 " version: %d, tie: %d\n", I,
//					   ((uint64_t *) &(*op)[I].key)[0], (*op)[I].version, (*op)[I].tie_breaker_id);
}

static inline void
hermes_assertions_end_read_write_ops(spacetime_op_t* read_write_op)
{
	for (int i = 0; i < MAX_BATCH_OPS_SIZE; ++i)
			assert(read_write_op[i].op_meta.opcode == ST_OP_GET ||
				   read_write_op[i].op_meta.state == ST_MISS ||
				   read_write_op[i].op_meta.state == ST_PUT_STALL ||
				   read_write_op[i].op_meta.state == ST_PUT_SUCCESS ||
				   read_write_op[i].op_meta.state == ST_PUT_COMPLETE ||
				   read_write_op[i].op_meta.state == ST_IN_PROGRESS_PUT ||
				   read_write_op[i].op_meta.state == ST_OP_MEMBERSHIP_CHANGE || ///TODO check this
				   read_write_op[i].op_meta.state == ST_IN_PROGRESS_REPLAY);
}
//////////// Exec functions

static inline void
hermes_exec_read(spacetime_op_t *op_ptr, struct mica_op *kv_ptr,
				 uint8_t idx, spacetime_group_membership curr_membership)
{
	spacetime_object_meta prev_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1];

	if(ENABLE_ASSERTIONS)
		assert(op_ptr->op_meta.opcode == ST_OP_GET);

	//Lock free reads through versioning (successful when version is even)
	uint8_t was_locked_read = 0;
	op_ptr->op_meta.state = ST_EMPTY;
	do {
		uint8_t node_id; // used for virtual --> physical node ids mapping
		prev_meta = *curr_meta;
		//switch template with all states
		switch(curr_meta->state) {
			case VALID_STATE:
				memcpy(op_ptr->value, kv_value_ptr, ST_VALUE_SIZE);
				op_ptr->op_meta.state = ST_GET_COMPLETE;
				op_ptr->op_meta.val_len = kv_ptr->val_len - sizeof(spacetime_object_meta);
				break;
			case INVALID_WRITE_STATE:
			case WRITE_STATE:
			case REPLAY_STATE:
				op_ptr->op_meta.state = ST_GET_STALL;
				break;
			default:
				was_locked_read = 1;
				cctrl_lock(&curr_meta->cctrl);
				switch(curr_meta->state) {
					case VALID_STATE:
						memcpy(op_ptr->value, kv_value_ptr, ST_VALUE_SIZE);
						op_ptr->op_meta.state = ST_GET_COMPLETE;
						op_ptr->op_meta.val_len = kv_ptr->val_len - sizeof(spacetime_object_meta);
						break;
					case INVALID_WRITE_STATE:
					case WRITE_STATE:
					case REPLAY_STATE:
						op_ptr->op_meta.state = ST_GET_STALL;
						break;
					case INVALID_STATE:
						node_id = (uint8_t) (!ENABLE_VIRTUAL_NODE_IDS ?
											 curr_meta->last_writer_id :
											 curr_meta->last_writer_id % MACHINE_NUM);
						if(node_is_in_membership(curr_membership, node_id))
							op_ptr->op_meta.state = ST_GET_STALL;
						else if(curr_meta->op_buffer_index == ST_OP_BUFFER_INDEX_EMPTY) {
							///stall replay: until all acks from last write arrive
							///on multiple threads we can't complete writes / replays on VAL

							if(ENABLE_ASSERTIONS)
								assert(idx < ST_OP_BUFFER_INDEX_EMPTY);

							yellow_printf("Write replay for i: %d\n", idx);
							curr_meta->state = REPLAY_STATE;
							curr_meta->op_buffer_index = (uint8_t) idx;
							curr_meta->last_local_write_ts.version= curr_meta->cctrl.ts.version - 1;
							curr_meta->last_local_write_ts.tie_breaker_id = curr_meta->cctrl.ts.tie_breaker_id;

							op_ptr->op_meta.state = ST_REPLAY_SUCCESS;
							op_ptr->op_meta.ts.version = curr_meta->cctrl.ts.version - 1;
							op_ptr->op_meta.ts.tie_breaker_id = curr_meta->cctrl.ts.tie_breaker_id;
							op_ptr->op_meta.val_len = ST_VALUE_SIZE;
							memcpy(op_ptr->value, kv_value_ptr, ST_VALUE_SIZE);
							///update group membership mask for replay acks
							bv_copy((bit_vector_t*) &curr_meta->ack_bv, curr_membership.w_ack_init);
						}
						break;
					default:
						assert(0);
				}
				cctrl_unlock_dec_version(&curr_meta->cctrl);
				break;
		}
	} while (!cctrl_timestamp_is_same_and_valid(&prev_meta.cctrl, &curr_meta->cctrl) && was_locked_read == 0);

	if(ENABLE_READ_COMPLETE_AFTER_VAL_RECV_OF_HOT_REQS &&
	   op_ptr->op_meta.state == ST_GET_STALL  )
	{
		if(op_ptr->op_meta.ts.version == 0 && op_ptr->op_meta.ts.tie_breaker_id == 0){
			// if its the first time we stall on this read store the timestamp
			op_ptr->op_meta.ts.version = curr_meta->cctrl.ts.version;
			op_ptr->op_meta.ts.tie_breaker_id = curr_meta->cctrl.ts.tie_breaker_id;

		} else if(op_ptr->op_meta.ts.version + 1 < curr_meta->cctrl.ts.version){
			// if the timestamp we saw initially has smaller than 2 versions complete the read;
			// TODO we also need to get the value here
			op_ptr->op_meta.state = ST_GET_COMPLETE;
		}
	}
}

static uint64_t g_seed = 0xdeadbeef;
static inline void
hermes_exec_write(spacetime_op_t *op_ptr, struct mica_op *kv_ptr,
				 uint8_t idx, spacetime_group_membership curr_membership)
{
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1];

	if(ENABLE_ASSERTIONS){
		assert(op_ptr->op_meta.opcode == ST_OP_PUT);
		assert(op_ptr->op_meta.val_len == ST_VALUE_SIZE);
	}

	///Warning: even if a write is in progress write we may need to update the value of write_buffer_index
	op_ptr->op_meta.state = ST_EMPTY;
	cctrl_lock(&curr_meta->cctrl);
	uint16_t curr_ts = (uint16_t) (curr_meta->cctrl.ts.version - 1);
	uint8_t v_node_id = (uint8_t) machine_id;
	switch(curr_meta->state) {
		case VALID_STATE:
		case INVALID_STATE:
			if(curr_meta->op_buffer_index != ST_OP_BUFFER_INDEX_EMPTY){
				///stall write: until all acks from last write arrive
				/// on multiple threads we can't complete writes / replays on VAL
				cctrl_unlock_dec_version(&curr_meta->cctrl);
				if(ENABLE_WRITE_COALESCE_TO_THE_SAME_KEY_IN_SAME_NODE && op_ptr->op_meta.ts.version == 0){
					// if its the first time we stall on this read store the timestamp
					op_ptr->op_meta.ts.version = curr_ts;
					op_ptr->op_meta.state = ST_IN_PROGRESS_PUT;
				}
			} else {
				curr_meta->state = WRITE_STATE;
				memcpy(kv_value_ptr, op_ptr->value, ST_VALUE_SIZE);
				kv_ptr->val_len = op_ptr->op_meta.val_len + sizeof(spacetime_object_meta);
				///update group membership mask
				bv_copy((bit_vector_t*) &curr_meta->ack_bv, curr_membership.w_ack_init);
				if(ENABLE_ASSERTIONS)
					assert(idx < ST_OP_BUFFER_INDEX_EMPTY);
				curr_meta->op_buffer_index = (uint8_t) idx;
				curr_meta->last_local_write_ts.version = curr_meta->cctrl.ts.version + 1;

				v_node_id = (uint8_t) (!ENABLE_VIRTUAL_NODE_IDS ? machine_id :
									   machine_id + MACHINE_NUM * (hrd_fastrand(&g_seed) % VIRTUAL_NODE_IDS_PER_NODE));
				curr_meta->last_local_write_ts.tie_breaker_id = v_node_id;
				cctrl_unlock_inc_version(&curr_meta->cctrl, v_node_id,
										 (uint32_t *) &(op_ptr->op_meta.ts.version));

				op_ptr->op_meta.state = ST_PUT_SUCCESS;
			}
			break;
		case INVALID_WRITE_STATE:
		case WRITE_STATE:
			if(ENABLE_WRITE_COALESCE_TO_THE_SAME_KEY_IN_SAME_NODE && op_ptr->op_meta.ts.version == 0){
				// if its the first time we stall on this read store the timestamp
				op_ptr->op_meta.ts.version = curr_ts;
				op_ptr->op_meta.state = ST_IN_PROGRESS_PUT;
			}

		case REPLAY_STATE:
			cctrl_unlock_dec_version(&curr_meta->cctrl);
			break;
		default: assert(0);
			break;
	}

	//Fill this deterministic stuff after releasing the lock
	if(op_ptr->op_meta.state == ST_PUT_SUCCESS)
		op_ptr->op_meta.ts.tie_breaker_id = v_node_id;
	else
		op_ptr->op_meta.state = ST_PUT_STALL;

	if(ENABLE_WRITE_COALESCE_TO_THE_SAME_KEY_IN_SAME_NODE && op_ptr->op_meta.state == ST_PUT_STALL)
		if(op_ptr->op_meta.ts.version > 0 && op_ptr->op_meta.ts.version + 1 < curr_ts){
			// if the timestamp we saw initially has smaller than 2 versions it means that
			// the local write we coalesced with is completed
			op_ptr->op_meta.state = ST_PUT_COMPLETE;
		}
}

static inline void
hermes_exec_inv(spacetime_inv_t *inv_ptr, struct mica_op *kv_ptr)
{
	spacetime_object_meta lock_free_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;
	uint8_t* kv_value_ptr = (uint8_t*) &curr_meta[1] ;

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
//			printf("Received an invalidation with >= timestamp\n");
			///Update Value, TS and last_writer_id
			curr_meta->last_writer_id = inv_ptr->op_meta.sender;
			kv_ptr->val_len = inv_ptr->op_meta.val_len + sizeof(spacetime_object_meta);
			if(ENABLE_ASSERTIONS){
				assert(kv_ptr->val_len == KVS_VALUE_SIZE);
				assert(inv_ptr->op_meta.val_len == ST_VALUE_SIZE);
			}
			memcpy(kv_value_ptr, inv_ptr->value, ST_VALUE_SIZE);
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
//				case REPLAY_STATE:
//					curr_meta->state = INVALID_WRITE_STATE;
//					curr_meta->state = INVALID_STATE;
//					//recover the read
//					if(ENABLE_ASSERTIONS){
//						assert(curr_meta->op_buffer_index != ST_OP_BUFFER_INDEX_EMPTY);
//						assert(read_write_op[curr_meta->op_buffer_index].state == ST_IN_PROGRESS_REPLAY);
//						assert(((uint64_t *) &read_write_op[curr_meta->op_buffer_index].key)[0] == ((uint64_t *) &(*op)[I].key)[0]);
//					}
//					read_write_op[curr_meta->op_buffer_index].state = ST_NEW;
//					curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY;
//					break;
				default: assert(0);
			}
			cctrl_unlock_custom_version(&curr_meta->cctrl, inv_ptr->op_meta.ts.tie_breaker_id,
										inv_ptr->op_meta.ts.version);
		} else if(timestamp_is_equal(curr_meta->cctrl.ts.version - 1,
									 curr_meta->cctrl.ts.tie_breaker_id,
									 inv_ptr->op_meta.ts.version,
									 inv_ptr->op_meta.ts.tie_breaker_id))
		{

			if (curr_meta->state == WRITE_STATE)
				inv_ptr->op_meta.opcode = ST_INV_OUT_OF_GROUP;

			curr_meta->last_writer_id = inv_ptr->op_meta.sender;
			cctrl_unlock_custom_version(&curr_meta->cctrl, inv_ptr->op_meta.ts.tie_breaker_id,
										inv_ptr->op_meta.ts.version);

		} else
			cctrl_unlock_dec_version(&curr_meta->cctrl);
	}

	if(inv_ptr->op_meta.opcode != ST_INV_OUT_OF_GROUP)
		inv_ptr->op_meta.opcode = ST_INV_SUCCESS;

	if(ENABLE_ASSERTIONS)
		assert(inv_ptr->op_meta.opcode == ST_INV_SUCCESS ||
		       inv_ptr->op_meta.opcode == ST_INV_OUT_OF_GROUP);
}

static inline void
hermes_exec_ack(spacetime_ack_t *ack_ptr, struct mica_op *kv_ptr,
				spacetime_group_membership curr_membership, spacetime_op_t *read_write_op)
{
	int op_buff_indx = ST_OP_BUFFER_INDEX_EMPTY;
	spacetime_object_meta lock_free_read_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;

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

	uint8_t prev_state = 10; //todo only for dbg
	if(timestamp_is_equal(ack_ptr->ts.version,    ack_ptr->ts.tie_breaker_id,
						  lock_free_read_meta.last_local_write_ts.version,
						  lock_free_read_meta.last_local_write_ts.tie_breaker_id))
	{
		///Lock and check again if ack TS == last local write
		cctrl_lock(&curr_meta->cctrl);
		if(timestamp_is_equal(ack_ptr->ts.version,    ack_ptr->ts.tie_breaker_id,
							  curr_meta->last_local_write_ts.version,
							  curr_meta->last_local_write_ts.tie_breaker_id))
		{
			bv_bit_set((bit_vector_t*) &curr_meta->ack_bv, ack_ptr->sender);
			if (is_last_ack(curr_meta->ack_bv, curr_membership)) { //if last local write completed
				op_buff_indx = curr_meta->op_buffer_index;
				prev_state = curr_meta->state; //todo only for dbg
				switch (curr_meta->state) {
					case VALID_STATE:
					case INVALID_STATE:
						ack_ptr->opcode = ST_LAST_ACK_NO_BCAST_SUCCESS;
						curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY; //reset the write buff index
						break;
					case INVALID_WRITE_STATE:
						curr_meta->state = INVALID_STATE;
						ack_ptr->opcode = ST_LAST_ACK_NO_BCAST_SUCCESS;
						curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY; //reset the write buff index
						break;
					case WRITE_STATE:
					case REPLAY_STATE:
						curr_meta->state = VALID_STATE;
						ack_ptr->opcode = ST_LAST_ACK_SUCCESS;
						curr_meta->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY; //reset the write buff index
						break;
					default:
						assert(0);
				}
			}
		}
		cctrl_unlock_dec_version(&curr_meta->cctrl);
	}


	if(ack_ptr->opcode == ST_LAST_ACK_SUCCESS ||
	   ack_ptr->opcode == ST_LAST_ACK_NO_BCAST_SUCCESS)
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
			assert(((uint64_t *) &read_write_op[op_buff_indx].op_meta.key)[0] == ((uint64_t *) &ack_ptr->key)[0]);
		}
		if(read_write_op[op_buff_indx].op_meta.opcode == ST_OP_PUT)
			read_write_op[op_buff_indx].op_meta.state = ST_PUT_COMPLETE;
		else if(read_write_op[op_buff_indx].op_meta.opcode == ST_OP_GET)
			read_write_op[op_buff_indx].op_meta.state = ST_NEW;
		else
			assert(0);
	}

	if(ack_ptr->opcode != ST_LAST_ACK_SUCCESS)
		ack_ptr->opcode = ST_ACK_SUCCESS;
}



static inline void
hermes_exec_val(spacetime_val_t *val_ptr, struct mica_op *kv_ptr)
{
	spacetime_object_meta lock_free_read_meta;
	spacetime_object_meta *curr_meta = (spacetime_object_meta *) kv_ptr->value;

	uint32_t debug_cntr = 0;
	do { //Lock free read of keys meta
		if (ENABLE_ASSERTIONS) {
			debug_cntr++;
			if (debug_cntr == M_4) {
				printf("Worker stuck on a lock-free read (for VAL)\n");
				debug_cntr = 0;
			}
		}
		lock_free_read_meta = *curr_meta;
	} while (!cctrl_timestamp_is_same_and_valid(&lock_free_read_meta.cctrl, &curr_meta->cctrl));

	///lock and proceed iff remote.TS == local.TS
	if(timestamp_is_equal(lock_free_read_meta.cctrl.ts.version,
						  lock_free_read_meta.cctrl.ts.tie_breaker_id,
						  val_ptr->ts.version,   val_ptr->ts.tie_breaker_id))
	{
		///Lock and check again if still TS == local timestamp
		cctrl_lock(&curr_meta->cctrl);
		///Warning: use op.version + 1 bellow since optik_lock() increases curr_meta->version by 1
		if(timestamp_is_equal(curr_meta->cctrl.ts.version - 1,
							  curr_meta->cctrl.ts.tie_breaker_id,
							  val_ptr->ts.version,   val_ptr->ts.tie_breaker_id))
		{
			if(ENABLE_ASSERTIONS)
				assert(curr_meta->state != WRITE_STATE); ///WARNING: this should not happen w/o this node removed from the group
			curr_meta->state = VALID_STATE;
		}
		cctrl_unlock_dec_version(&curr_meta->cctrl);
	}
	val_ptr->opcode = ST_VAL_SUCCESS;
}

//////////// Skip functions
static inline uint8_t
hermes_skip_op(spacetime_op_t *op_ptr)
{
	return (uint8_t) ((op_ptr->op_meta.state == ST_PUT_SUCCESS ||
	                   op_ptr->op_meta.state == ST_REPLAY_SUCCESS ||
	                   op_ptr->op_meta.state == ST_IN_PROGRESS_PUT ||
	                   op_ptr->op_meta.state == ST_IN_PROGRESS_REPLAY ||
	                   op_ptr->op_meta.state == ST_OP_MEMBERSHIP_CHANGE ||
	                   op_ptr->op_meta.state == ST_PUT_COMPLETE_SEND_VALS) ? 1 : 0);
}

static inline uint8_t
hermes_skip_inv(spacetime_inv_t *inv_ptr, int* node_suspected)
{
	if(inv_ptr->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE) {
		//TODO we need to do this only on the first skip
		*node_suspected = inv_ptr->value[0];
		printf("RECEIVED NODE SUSPICION: %d\n", *node_suspected);
		return 1;
	}
	return 0;
}

static inline uint8_t
hermes_skip_ack(spacetime_ack_t *ack_ptr)
{
	return (uint8_t) ((ack_ptr->state == ST_OP_MEMBERSHIP_CHANGE) ? 1 : 0);
}

//////////// Dispatcher functions

static inline uint8_t
hermes_skip_dispatcher(enum hermes_batch_type_t type, void* ptr, int* node_suspected)
{
	switch (type){
		case local_ops:
			return hermes_skip_op(ptr);
		case invs:
			return hermes_skip_inv(ptr, node_suspected);
		case acks:
			return hermes_skip_ack(ptr);
		case vals:
			return 0;
		default: assert(0);
	}
}

static inline void
hermes_assertions_begin_dispatcher(enum hermes_batch_type_t type, void* ptr)
{
	if(ENABLE_ASSERTIONS)
		switch (type){
			case local_ops:
				break;
			case invs:
				hermes_assertions_begin_inv(ptr);
				break;
			case acks:
				hermes_assertions_begin_ack(ptr);
				break;
			case vals:
				hermes_assertions_begin_val(ptr);
				break;
			default: assert(0);
		}
}

static inline void
hermes_print_dispatcher(enum hermes_batch_type_t type, int op_num, uint8_t thread_id)
{
	if(ENABLE_BATCH_OP_PRINTS)
		switch (type){
			case local_ops:
				break;
			case invs:
				if(ENABLE_INV_PRINTS && thread_id < MAX_THREADS_TO_PRINT)
					red_printf("[W] Batch INVs (op num: %d)!\n", thread_id, op_num);
				break;
			case acks:
				if(ENABLE_ACK_PRINTS && thread_id < MAX_THREADS_TO_PRINT)
					red_printf("[W%d] Batch ACKs (op num: %d)!\n",thread_id, op_num);
				break;
			case vals:
				if(ENABLE_VAL_PRINTS && thread_id < MAX_THREADS_TO_PRINT)
					red_printf("[W%d] Batch VALs (op num: %d)!\n", thread_id, op_num);
				break;
			default: assert(0);
		}
}

static inline void
hermes_assertions_end_dispatcher(enum hermes_batch_type_t type, spacetime_op_t* read_write_ops)
{
	if(ENABLE_ASSERTIONS)
		switch (type){
			case local_ops:
			case invs:
				break;
			case acks:
			    hermes_assertions_end_read_write_ops(read_write_ops);
				break;
			case vals:
				break;
			default: assert(0);
		}
}

static inline void
hermes_exec_dispatcher(enum hermes_batch_type_t type, void* op_ptr, struct mica_op *kv_ptr,
					   spacetime_group_membership curr_membership,
					   uint8_t idx, spacetime_op_t *read_write_op)
{
	switch (type){
		case local_ops:
		    if(((spacetime_op_t*) op_ptr)->op_meta.opcode == ST_OP_GET)
		        hermes_exec_read(op_ptr, kv_ptr, idx, curr_membership);
		    else if(((spacetime_op_t*) op_ptr)->op_meta.opcode == ST_OP_PUT)
				hermes_exec_write(op_ptr, kv_ptr, idx, curr_membership);
		    else assert(0);
			break;
		case invs:
		    hermes_exec_inv(op_ptr, kv_ptr);
			break;
		case acks:
			hermes_exec_ack(op_ptr, kv_ptr, curr_membership, read_write_op);
			break;
		case vals:
		    hermes_exec_val(op_ptr, kv_ptr);
			break;
		default: assert(0);
	}
}




#define HERMES_MAX_BATCH_SIZE MAX(MAX(MAX(MAX_BATCH_OPS_SIZE, ACK_RECV_OPS_SIZE), INV_RECV_OPS_SIZE), VAL_RECV_OPS_SIZE)
void
hermes_batch_ops_to_KVS(enum hermes_batch_type_t type, uint8_t *op_array, int op_num,
						uint16_t sizeof_op_elem, spacetime_group_membership curr_membership,
						int *node_suspected, spacetime_op_t *read_write_ops, uint8_t thread_id)
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
	int key_in_store[HERMES_MAX_BATCH_SIZE];	    // Is this key in the datastore?
	unsigned int tag[HERMES_MAX_BATCH_SIZE];
	unsigned int bkt[HERMES_MAX_BATCH_SIZE];
	struct mica_bkt *bkt_ptr[HERMES_MAX_BATCH_SIZE];
	struct mica_op   *kv_ptr[HERMES_MAX_BATCH_SIZE];	// Ptr to KV item in log



	if(ENABLE_ASSERTIONS){
		assert(op_num <= HERMES_MAX_BATCH_SIZE);
		assert(read_write_ops != NULL || type != acks);
		assert(node_suspected != NULL || type != invs);
	}

	hermes_print_dispatcher(type, op_num, thread_id);
	// We first lookup the key in the datastore.
	// The first two @I loops work for both GETs and PUTs.
	for(int I = 0; I < op_num; I++) {
		spacetime_op_meta_t* op_ptr = (spacetime_op_meta_t *) &op_array[sizeof_op_elem * I];
		hermes_assertions_begin_dispatcher(type, op_ptr);
	    if(hermes_skip_dispatcher(type, op_ptr, node_suspected)) continue;

		bkt[I] = op_ptr->key.bkt & kv.hash_table.bkt_mask;
		bkt_ptr[I] = &kv.hash_table.ht_index[bkt[I]];
		__builtin_prefetch(bkt_ptr[I], 0, 0);
		tag[I] = op_ptr->key.tag;

		key_in_store[I] = 0;
		kv_ptr[I] = NULL;
	}

	for(int I = 0; I < op_num; I++) {
		spacetime_op_meta_t* op_ptr = (spacetime_op_meta_t *) &op_array[sizeof_op_elem * I];
		if(hermes_skip_dispatcher(type, op_ptr, node_suspected)) continue;
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
				if(kv.hash_table.log_head - bkt_ptr[I]->slots[j].offset >= kv.hash_table.log_cap)
					kv_ptr[I] = NULL;	// If so, we mark it "not found"

				break;
			}
		}
	}

	for(int I = 0; I < op_num; I++) {
		spacetime_op_meta_t* op_ptr = (spacetime_op_meta_t *) &op_array[sizeof_op_elem * I];
		if(hermes_skip_dispatcher(type, op_ptr, node_suspected)) continue;
		if(kv_ptr[I] != NULL) {
			// We had a tag match earlier. Now compare log entry.
			long long *key_ptr_log = (long long *) kv_ptr[I];
			long long *key_ptr_req = (long long *) &op_ptr->key;

			if(key_ptr_log[1] == key_ptr_req[0]){ //Key Found 8 Byte keys
				key_in_store[I] = 1;
				hermes_exec_dispatcher(type, op_ptr, kv_ptr[I], curr_membership, (uint8_t) I, read_write_ops);
			}
		}

		if(key_in_store[I] == 0) // KVS miss --> We get here if either tag or log key match failed
			op_ptr->state = ST_MISS;
	}

	hermes_assertions_end_dispatcher(type, read_write_ops);
}

