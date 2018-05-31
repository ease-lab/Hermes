//
// Created by akatsarakis on 23/05/18.
//

#ifndef HERMES_INLINE_UTIL_H
#define HERMES_INLINE_UTIL_H

#include <infiniband/verbs.h>
#include "spacetime.h"
#include "config.h"
#include "util.h"

/* ---------------------------------------------------------------------------
------------------------------------GENERIC-----------------------------------
---------------------------------------------------------------------------*/
static inline int poll_completions(int num_of_completions, struct ibv_cq* completion_q, struct ibv_wc* work_completions){
	int completions_found = ibv_poll_cq(completion_q, num_of_completions, work_completions);
//	if(ENABLE_ASSERTIONS == 1){
//        if(completions_found != num_of_completions)
//			red_printf("Completions found: %d, vs completions wanted: %d\n",completions_found, num_of_completions);
//		assert(completions_found == num_of_completions);
//	}
	return completions_found;
}
///TODO ADD Batching to the NIC for post RECEIVES
static inline void post_receives(struct hrd_ctrl_blk *cb, uint16_t num_of_receives,
								 uint8_t buff_type, void *recv_buff, int *push_ptr){
	static int k = 0;
    int i,qp_id, req_size, recv_q_depth;
    void* next_buff_addr;
    uint8_t* next_buff_opcode;
    switch(buff_type){
        case ST_INV_BUFF:
            req_size = sizeof(ud_req_inv_t);
            recv_q_depth = RECV_INV_Q_DEPTH;
            qp_id = INV_UD_QP_ID;
			k++;
            break;
        case ST_ACK_BUFF:
            req_size = sizeof(ud_req_ack_t);
            recv_q_depth = RECV_ACK_Q_DEPTH;
            qp_id = ACK_UD_QP_ID;
			k++;
            break;
        case ST_VAL_BUFF:
            req_size = sizeof(ud_req_val_t);
            recv_q_depth = RECV_VAL_Q_DEPTH;
            qp_id = VAL_UD_QP_ID;
            break;
        case ST_CRD_BUFF:
            req_size = sizeof(ud_req_crd_t);
            recv_q_depth = RECV_CRD_Q_DEPTH;
            qp_id = CRD_UD_QP_ID;
            break;
        default: assert(0);
    }

    for(i = 0; i < num_of_receives; i++) {
        next_buff_addr = ((uint8_t*) recv_buff) + (*push_ptr * req_size);
        switch(buff_type){
            case ST_INV_BUFF:
                next_buff_opcode = &((ud_req_inv_t*)recv_buff)[*push_ptr].req.opcode;
                break;
            case ST_ACK_BUFF:
                next_buff_opcode = &((ud_req_ack_t*)recv_buff)[*push_ptr].req.opcode;
                break;
            case ST_VAL_BUFF:
                next_buff_opcode = &((ud_req_val_t*)recv_buff)[*push_ptr].req.opcode;
                break;
            case ST_CRD_BUFF:
                next_buff_opcode = &((ud_req_crd_t*)recv_buff)[*push_ptr].req.opcode;
                break;
            default: assert(0);
        }
//		printf("POST: Push_ptr: %d, recv_q_depth: %d, addr: %d, op: %d\n",*push_ptr, recv_q_depth, next_buff_addr, next_buff_opcode);
        *next_buff_opcode = ST_EMPTY;
        hrd_post_dgram_recv(cb->dgram_qp[qp_id],
                            next_buff_addr,
                            req_size, cb->dgram_buf_mr->lkey);
        HRD_MOD_ADD(*push_ptr, recv_q_depth);
    }
}

/*
static inline void poll_buff(void* incoming_buff, uint8_t buff_type, int* buf_pull_ptr,
							 void* recv_ops, int* ops_push_ptr, uint16_t outstanding_ops,
							 struct ibv_cq* completion_q, struct ibv_wc* work_completions,
							 uint8_t credits[][MACHINE_NUM], uint16_t worker_lid)
{
	uint8_t* next_opcode_ptr;
	void* next_req_ptr;
	int index = 0, recv_q_depth = 0, max_credits = 0, i = 0, reqs_polled = 0;
	uint8_t qp_credits_to_inc = 0, sender = 0;
	size_t req_size = 0;
	switch(buff_type){
		case ST_INV_BUFF:
			qp_credits_to_inc = ACK_UD_QP_ID;
			recv_q_depth = RECV_INV_Q_DEPTH;
			req_size = sizeof(spacetime_inv_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = ACK_CREDITS;
			break;
		case ST_ACK_BUFF:
			qp_credits_to_inc = INV_UD_QP_ID;
			recv_q_depth = RECV_ACK_Q_DEPTH;
			req_size = sizeof(spacetime_ack_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = INV_CREDITS;
			break;
		case ST_VAL_BUFF:
			qp_credits_to_inc = CRD_UD_QP_ID;
			recv_q_depth = RECV_VAL_Q_DEPTH;
			req_size = sizeof(spacetime_val_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = CRD_CREDITS;
			break;
		case ST_CRD_BUFF:
			qp_credits_to_inc = VAL_UD_QP_ID;
			recv_q_depth = RECV_CRD_Q_DEPTH;
			req_size = sizeof(spacetime_crd_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = VAL_CREDITS;
			break;
		default: assert(0);
	}

	reqs_polled = poll_completions(MAX_BATCH_OPS_SIZE - outstanding_ops - *ops_push_ptr , completion_q, work_completions);
//	while (*ops_push_ptr < MAX_BATCH_OPS_SIZE) {
	if(reqs_polled > 0)
		printf("FOUND %d completions\n",reqs_polled);
	for(i = 0; i < reqs_polled; i++){
		printf("iter %d \n",i);
		index = (*buf_pull_ptr + 1) % recv_q_depth;
		switch (buff_type) {
			case ST_INV_BUFF:
				sender = ((ud_req_inv_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_inv_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_inv_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_INV || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_ACK_BUFF:
				sender = ((ud_req_ack_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_ack_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_ack_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_ACK || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_VAL_BUFF:
				sender = ((ud_req_val_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_val_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_val_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_VAL || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_CRD_BUFF:
				sender = ((ud_req_crd_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_crd_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_crd_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_CRD || *next_opcode_ptr == ST_EMPTY);
				break;
			default:
				assert(0);
		}
		if (*next_opcode_ptr == ST_EMPTY) {
            assert(0);
//			break;
		} else {
//			if (outstanding_ops + *ops_push_ptr == MAX_BATCH_OPS_SIZE) break; // Back pressure if no empty slots
			void *recv_op_ptr = ((uint8_t *) recv_ops) + (*ops_push_ptr * req_size);
			memcpy(recv_op_ptr, next_req_ptr, req_size);
			*next_opcode_ptr = ST_EMPTY;
			HRD_MOD_ADD(*buf_pull_ptr, recv_q_depth);
//			printf("POLL: Pull_ptr: %d, q_depth: %d, addr: %d, op: %d\n",
//				   *buf_pull_ptr, recv_q_depth, recv_op_ptr, next_opcode_ptr);
			(*ops_push_ptr)++;
			//increment credits
			credits[qp_credits_to_inc][sender]++;
//			reqs_polled++;
            if(ENABLE_ASSERTIONS == 1)
				assert(credits[qp_credits_to_inc][sender] <= max_credits);
		}
	}
	if(reqs_polled > 0){
		//poll completion q
//		poll_completions(reqs_polled, completion_q, work_completions);
		if (ENABLE_STAT_COUNTING == 1)
			switch(buff_type){
				case ST_INV_BUFF:
					w_stats[worker_lid].received_invs_per_worker += reqs_polled;
					cyan_printf("Polled: INVs %d, (total: %d)!\n", reqs_polled, w_stats[worker_lid].received_invs_per_worker);
					yellow_printf("Credits: ACK incremented (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: INVs %d\n",reqs_polled);
//			            green_printf("Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, sender: %d, value: %s\n",
//						 ((uint64_t *) &inv_ops[*inv_ops_i - 1].key)[1],
//						 code_to_str(inv_ops[*inv_ops_i - 1].opcode),inv_ops[*inv_ops_i - 1].version,
//						 inv_ops[*inv_ops_i - 1].tie_breaker_id, inv_ops[*inv_ops_i - 1].sender,
//						 inv_ops[*inv_ops_i - 1].value);
					break;
				case ST_ACK_BUFF:
					w_stats[worker_lid].received_acks_per_worker += reqs_polled;
					cyan_printf("Polled: ACKs %d, (total: %d)!\n", reqs_polled, w_stats[worker_lid].received_acks_per_worker);
					yellow_printf("Credits: INV incremented to (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: ACKs %d\n",reqs_polled);
					break;
				case ST_VAL_BUFF:
					w_stats[worker_lid].received_vals_per_worker += reqs_polled;
					cyan_printf("VALs polled %d, (total: %d)!\n", reqs_polled, w_stats[worker_lid].received_vals_per_worker);
					yellow_printf("Credits: CRD incremented to (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: VALs %d\n",reqs_polled);
					break;
				case ST_CRD_BUFF:
					yellow_printf("Credits: VAL incremented to (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: CRDs %d\n",reqs_polled);
					break;
				default: assert(0);
			}
	}
}
*/
static inline void poll_buff(void* incoming_buff, uint8_t buff_type, int* buf_pull_ptr,
							 void* recv_ops, int* ops_push_ptr, uint16_t outstanding_ops,
							 struct ibv_cq* completion_q, struct ibv_wc* work_completions,
							 uint8_t credits[][MACHINE_NUM], uint16_t worker_lid)
{
	uint8_t* next_opcode_ptr;
	void* next_req_ptr;
	int index = 0, recv_q_depth = 0, max_credits = 0;
	uint8_t qp_credits_to_inc = 0, sender = 0, reqs_polled = 0;
	size_t req_size = 0;
	switch(buff_type){
		case ST_INV_BUFF:
			qp_credits_to_inc = ACK_UD_QP_ID;
			recv_q_depth = RECV_INV_Q_DEPTH;
			req_size = sizeof(spacetime_inv_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = ACK_CREDITS;
			break;
		case ST_ACK_BUFF:
			qp_credits_to_inc = INV_UD_QP_ID;
			recv_q_depth = RECV_ACK_Q_DEPTH;
			req_size = sizeof(spacetime_ack_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = INV_CREDITS;
			break;
		case ST_VAL_BUFF:
			qp_credits_to_inc = CRD_UD_QP_ID;
			recv_q_depth = RECV_VAL_Q_DEPTH;
			req_size = sizeof(spacetime_val_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = CRD_CREDITS;
			break;
		case ST_CRD_BUFF:
			qp_credits_to_inc = VAL_UD_QP_ID;
			recv_q_depth = RECV_CRD_Q_DEPTH;
			req_size = sizeof(spacetime_crd_t);
			if(ENABLE_ASSERTIONS == 1) max_credits = VAL_CREDITS;
			break;
		default: assert(0);
	}

	while (*ops_push_ptr < MAX_BATCH_OPS_SIZE) {
		index = (*buf_pull_ptr + 1) % recv_q_depth;
		switch (buff_type) {
			case ST_INV_BUFF:
				sender = ((ud_req_inv_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_inv_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_inv_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_INV || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_ACK_BUFF:
				sender = ((ud_req_ack_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_ack_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_ack_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_ACK || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_VAL_BUFF:
				sender = ((ud_req_val_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_val_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_val_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_VAL || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_CRD_BUFF:
				sender = ((ud_req_crd_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_crd_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_crd_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS == 1)
					assert(*next_opcode_ptr == ST_OP_CRD || *next_opcode_ptr == ST_EMPTY);
				break;
			default:
				assert(0);
		}
		if (*next_opcode_ptr == ST_EMPTY) {
			break;
		} else {
			if (outstanding_ops + *ops_push_ptr == MAX_BATCH_OPS_SIZE) break; // Back pressure if no empty slots
			void *recv_op_ptr = ((uint8_t *) recv_ops) + (*ops_push_ptr * req_size);
			memcpy(recv_op_ptr, next_req_ptr, req_size);
			*next_opcode_ptr = ST_EMPTY;
			HRD_MOD_ADD(*buf_pull_ptr, recv_q_depth);
//			printf("POLL: Pull_ptr: %d, q_depth: %d, addr: %d, op: %d\n",
//				   *buf_pull_ptr, recv_q_depth, recv_op_ptr, next_opcode_ptr);
			(*ops_push_ptr)++;
			//increment credits
			credits[qp_credits_to_inc][sender]++;
			reqs_polled++;
            if(ENABLE_ASSERTIONS == 1)
				assert(credits[qp_credits_to_inc][sender] <= max_credits);
		}
	}
	if(reqs_polled > 0){
		//poll completion q
		poll_completions(reqs_polled, completion_q, work_completions);
		if (ENABLE_STAT_COUNTING == 1)
			switch(buff_type){
				case ST_INV_BUFF:
					w_stats[worker_lid].received_invs_per_worker += reqs_polled;
					cyan_printf("Polled: INVs %d, (total: %d)!\n", reqs_polled, w_stats[worker_lid].received_invs_per_worker);
					yellow_printf("Credits: ACK incremented (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: INVs %d\n",reqs_polled);
//			            green_printf("Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, sender: %d, value: %s\n",
//						 ((uint64_t *) &inv_ops[*inv_ops_i - 1].key)[1],
//						 code_to_str(inv_ops[*inv_ops_i - 1].opcode),inv_ops[*inv_ops_i - 1].version,
//						 inv_ops[*inv_ops_i - 1].tie_breaker_id, inv_ops[*inv_ops_i - 1].sender,
//						 inv_ops[*inv_ops_i - 1].value);
					break;
				case ST_ACK_BUFF:
					w_stats[worker_lid].received_acks_per_worker += reqs_polled;
					cyan_printf("Polled: ACKs %d, (total: %d)!\n", reqs_polled, w_stats[worker_lid].received_acks_per_worker);
					yellow_printf("Credits: INV incremented to (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: ACKs %d\n",reqs_polled);
					break;
				case ST_VAL_BUFF:
					w_stats[worker_lid].received_vals_per_worker += reqs_polled;
					cyan_printf("VALs polled %d, (total: %d)!\n", reqs_polled, w_stats[worker_lid].received_vals_per_worker);
					yellow_printf("Credits: CRD incremented to (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: VALs %d\n",reqs_polled);
					break;
				case ST_CRD_BUFF:
					yellow_printf("Credits: VAL incremented to (%d) of sender %d!\n", credits[qp_credits_to_inc][sender], sender);
					red_printf("Poll Completions: CRDs %d\n",reqs_polled);
					break;
				default: assert(0);
			}
	}
}

// Poll for credits and increment the credits
///TODO I don't need to pass the whole credit array just the VAL_UD_QP_ID
static inline void poll_credits(struct ibv_cq* credit_recv_cq,
								struct ibv_wc* credit_wc, uint8_t credits[][MACHINE_NUM])
{
	int j = 0, credits_found = 0;
    spacetime_crd_t crd_tmp;
	credits_found = ibv_poll_cq(credit_recv_cq, SEND_CRD_Q_DEPTH, credit_wc);
	if(credits_found > 0) {
		if(unlikely(credit_wc[credits_found -1].status != 0)) {
			fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
			exit(0);
		}

		///TODO change imm_data to machine num+ virtual channel it should increment
		for (j = 0; j < credits_found; j++){
			memcpy(&crd_tmp, &credit_wc[j].imm_data, sizeof(spacetime_crd_t));
			assert(crd_tmp.opcode == ST_OP_CRD);
			credits[VAL_UD_QP_ID][crd_tmp.sender] += crd_tmp.val_credits;
		}
		///increment_credits(credits_found, credit_wc, credits);
	}
	else if(unlikely(credits_found < 0)) {
		printf("ERROR In the credit CQ\n"); exit(0);
	}
}

static inline bool check_broadcast_credits(
		uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
		struct ibv_wc* credit_wc, uint32_t* credit_debug_cnt,
		uint16_t qp_id)
{
	uint16_t poll_for_credits = 0, j;
	for (j = 0; j < MACHINE_NUM; j++) {
		if (j == machine_id) continue; // skip the local machine
		if (credits[qp_id][j] == 0) {
			poll_for_credits = 1;
			break;
		}
	}
	if (poll_for_credits == 1 && qp_id == VAL_UD_QP_ID) {
		poll_credits(cb->dgram_recv_cq[CRD_UD_QP_ID], credit_wc, credits);
		// We polled for credits, if we did not find enough just break
		for (j = 0; j < MACHINE_NUM; j++) {
			if (j == machine_id) continue; // skip the local machine
			if (credits[qp_id][j] == 0) {
				(*credit_debug_cnt)++;
				return false;
			}
		}
	} else if(poll_for_credits == 1)
		return false;
	*credit_debug_cnt = 0;
	return true;
}

/* ---------------------------------------------------------------------------
------------------------------------INVS--------------------------------------
---------------------------------------------------------------------------*/





static inline void forge_bcast_inv_wrs(spacetime_op_t* op, spacetime_op_resp_t* resp,
									   spacetime_inv_t* inv_send_ops, int* inv_push_ops_ptr,
									   struct ibv_send_wr* send_inv_wr, struct ibv_sge* send_inv_sgl,
									   struct hrd_ctrl_blk* cb, long long* br_tx,
									   uint16_t br_i, uint16_t worker_lid)
{
	struct ibv_wc signal_send_wc;

	///TODO we need to empty the buffer space
	///coh_message_count[ACK_VC][ack_bcast_ops[*ack_pop_ptr].key.meta.cid]++; // we are emptying that buffer space
	inv_send_ops[*inv_push_ops_ptr].opcode = ST_OP_INV;
	inv_send_ops[*inv_push_ops_ptr].sender = (uint8_t) machine_id;
	inv_send_ops[*inv_push_ops_ptr].version = resp->version;
	inv_send_ops[*inv_push_ops_ptr].tie_breaker_id = resp->tie_breaker_id;
	inv_send_ops[*inv_push_ops_ptr].val_len = resp->val_len;
	memcpy(&inv_send_ops[*inv_push_ops_ptr].value, resp->val_ptr, resp->val_len);
	memcpy(&inv_send_ops[*inv_push_ops_ptr].key, &op->key, KEY_SIZE);
	send_inv_sgl[br_i].addr = (uint64_t) (uintptr_t) (inv_send_ops + (*inv_push_ops_ptr));

	///TODO add code for non-inlining
	w_stats[worker_lid].invs_per_worker++;
	HRD_MOD_ADD(*inv_push_ops_ptr, MAX_BATCH_OPS_SIZE);

	// Do a Signaled Send every BROADCAST_SS_BATCH broadcasts (BROADCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
	if ((*br_tx) % BROADCAST_SS_BATCH == 0) send_inv_wr[0].send_flags |= IBV_SEND_SIGNALED;
	if(*br_tx > 0 && (*br_tx) % BROADCAST_SS_BATCH == BROADCAST_SS_BATCH - 1){
		printf("INV completion\n");
		hrd_poll_cq(cb->dgram_send_cq[INV_UD_QP_ID], 1, &signal_send_wc);
	}
	(*br_tx)++;
	// SET THE LAST SEND_WR TO POINT TO NULL
//	if (br_i > 0)
//		send_inv_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = &send_inv_wr[br_i * MAX_MESSAGES_IN_BCAST];
}

static inline void broadcasts_invs(spacetime_op_t* ops, spacetime_op_resp_t* resp,
								   spacetime_inv_t* inv_send_ops, int* inv_push_ptr,
								   struct ibv_send_wr* send_inv_wr, struct ibv_sge* send_inv_sgl,
								   uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
                                   long long* br_tx, ud_req_ack_t* incoming_acks, int* ack_push_ptr,
								   uint16_t worker_lid)
{
	uint8_t missing_credits = 0;
	uint16_t i = 0, br_i = 0, j, credit_recv_counter = 0;
	int ret;
	struct ibv_send_wr *bad_send_wr;
	static int inv_issued = 0; ///TODO just for debugging

	// traverse all of the ops to find invs
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (resp[i].resp_opcode != ST_PUT_SUCCESS && resp[i].resp_opcode!= ST_BUFFERED_REPLAY)
			continue;

		for (j = 0; j < MACHINE_NUM; j++) {
			if (j == machine_id) continue; // skip the local machine
			if (credits[INV_UD_QP_ID][j] == 0) {
				missing_credits = 1;
				break;
			}
		}
		// if not enough credits for a Broadcast
		if (missing_credits == 1) break;

		if(resp[i].resp_opcode== ST_PUT_SUCCESS){
			ops[i].opcode = ST_IN_PROGRESS_WRITE;
		}else if(resp[i].resp_opcode == ST_BUFFERED_REPLAY){
			ops[i].opcode = ST_BUFFERED_IN_PROGRESS_REPLAY;
		} else assert(0);

		// Create the broadcast messages
		forge_bcast_inv_wrs(&ops[i], &resp[i], inv_send_ops, inv_push_ptr, send_inv_wr,
							send_inv_sgl, cb, br_tx, br_i, worker_lid);
		for (j = 0; j < MACHINE_NUM; j++)
			credits[INV_UD_QP_ID][j]--;
		yellow_printf("Credits: INVs decremented %d (for machine %d)\n",
					  credits[INV_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					  (machine_id + 1) % MACHINE_NUM);
		if ((*br_tx) % CREDITS_IN_MESSAGE == 0) credit_recv_counter++;
		br_i++;
		if (br_i == MAX_BCAST_BATCH) {
			red_printf("Post Receives: ACK %d, Push_ptr %d\n",br_i * (MACHINE_NUM - 1), *ack_push_ptr);
			post_receives(cb, (uint16_t) (br_i * (MACHINE_NUM - 1)), ST_ACK_BUFF, incoming_acks, ack_push_ptr);
//			post_credit_recvs_and_batch_bcasts_to_NIC(br_i, cb, send_inv_wr, credit_recv_wr,
//													  &credit_recv_counter, INV_UD_QP_ID);
			ret = ibv_post_send(cb->dgram_qp[INV_UD_QP_ID], &send_inv_wr[0], &bad_send_wr);
			CPE(ret, "Broadcast ibv_post_send error", ret);
			inv_issued+=br_i;
			green_printf("Send: INVs %d (Total %d)\n", br_i, inv_issued);
			br_i = 0;
		}
	}
}

/* ---------------------------------------------------------------------------
------------------------------------ACKS--------------------------------------
---------------------------------------------------------------------------*/

static inline void forge_ack_wr(spacetime_inv_t* inv_recv_op, spacetime_ack_t* ack_send_ops,
								int* ack_push_ptr, struct ibv_send_wr* send_ack_wr,
								struct ibv_sge* send_ack_sgl, struct hrd_ctrl_blk* cb,
								long long* send_ack_tx, uint16_t send_ack_count, uint16_t local_client_id)
{
	struct ibv_wc signal_send_wc;

	ack_send_ops[*ack_push_ptr].opcode = ST_OP_ACK;
	ack_send_ops[*ack_push_ptr].sender = (uint8_t) machine_id;
	ack_send_ops[*ack_push_ptr].version =  inv_recv_op->version;
	ack_send_ops[*ack_push_ptr].tie_breaker_id = inv_recv_op->tie_breaker_id;
	memcpy(&ack_send_ops[*ack_push_ptr].key, &inv_recv_op->key, KEY_SIZE);

	send_ack_sgl[send_ack_count].addr = (uint64_t) (uintptr_t) (ack_send_ops + (*ack_push_ptr));
	send_ack_wr[send_ack_count].wr.ud.ah = remote_worker_qps[inv_recv_op->sender][ACK_UD_QP_ID].ah;
	send_ack_wr[send_ack_count].wr.ud.remote_qpn = (uint32) remote_worker_qps[inv_recv_op->sender][ACK_UD_QP_ID].qpn;

	///TODO add code for non-inlining
	w_stats[local_client_id].acks_per_worker++;
	HRD_MOD_ADD(*ack_push_ptr, MAX_BATCH_OPS_SIZE);

	if (send_ack_count > 0)
		send_ack_wr[send_ack_count - 1].next = &send_ack_wr[send_ack_count];
	if ((*send_ack_tx) % ACK_SS_BATCH == 0) {
		send_ack_wr[send_ack_count].send_flags |= IBV_SEND_SIGNALED;
		// if (local_client_id == 0) green_printf("Sending ack %llu signaled \n", *send_ack_tx);
	}
	if(*send_ack_tx > 0 && (*send_ack_tx) % ACK_SS_BATCH == ACK_SS_BATCH - 1) {
		// if (local_client_id == 0) green_printf("Polling for ack  %llu \n", *send_ack_tx);
		printf("ACK completion\n");
		hrd_poll_cq(cb->dgram_send_cq[ACK_UD_QP_ID], 1, &signal_send_wc);
	}
	(*send_ack_tx)++; // Selective signaling
}

static inline void issue_acks(spacetime_inv_t *inv_recv_ops, spacetime_ack_t* ack_send_ops,
							  int* ack_push_ptr, long long int* send_ack_tx,
							  struct ibv_send_wr *send_ack_wr, struct ibv_sge *send_ack_sgl,
					          uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                              ud_req_inv_t* incoming_invs, int* inv_push_ptr,
							  uint16_t worker_lid)
{
	static int ack_issued = 0; ///TODO just for debugging
	int ret;
	struct ibv_send_wr *bad_send_wr;
	uint16_t i = 0, send_ack_count = 0;
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (inv_recv_ops[i].opcode == ST_EMPTY)
			continue;
		if(credits[ACK_UD_QP_ID][inv_recv_ops[i].sender] == 0)
			continue; //TODO we may need to push this to the top of the stack + (seems ok to batch this again to the KVS)
		//reduce credits
		credits[ACK_UD_QP_ID][inv_recv_ops[i].sender]--;
		yellow_printf("Credits: ACKs decrement %d (for machine %d)\n",
					  credits[ACK_UD_QP_ID][inv_recv_ops[i].sender],
					  inv_recv_ops[i].sender);
		// Create the broadcast messages
		forge_ack_wr(&inv_recv_ops[i], &ack_send_ops[i], ack_push_ptr, send_ack_wr,
					 send_ack_sgl, cb, send_ack_tx, send_ack_count, worker_lid);
		send_ack_count++;
		//empty inv buffer
		if(inv_recv_ops[i].opcode == ST_INV_SUCCESS)
			inv_recv_ops[i].opcode = ST_EMPTY;
		else{
            printf("Code: %s\n",code_to_str(inv_recv_ops[i].opcode));
			assert(0);
		}
	}

	if (send_ack_count > 0) {
		//before sending acks post receives for INVs
		red_printf("Post receives:  INV %d, Push_ptr %d\n",send_ack_count, *inv_push_ptr);
		post_receives(cb, send_ack_count, ST_INV_BUFF, incoming_invs, inv_push_ptr);
		send_ack_wr[send_ack_count - 1].next = NULL;
	    if(ENABLE_ASSERTIONS == 1){
			assert(send_ack_wr[0].opcode == IBV_WR_SEND);
			assert(send_ack_wr[0].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(send_ack_wr[0].sg_list == &send_ack_sgl[0]);
			assert(send_ack_wr[0].sg_list->length == sizeof(spacetime_ack_t));
			assert(send_ack_wr[0].num_sge == 1);
//			assert(send_ack_wr[0].send_flags == IBV_SEND_INLINE);
		}
		ret = ibv_post_send(cb->dgram_qp[ACK_UD_QP_ID], &send_ack_wr[0], &bad_send_wr);
		CPE(ret, "ibv_post_send error while sending ACKs", ret);
        ack_issued+=send_ack_count;
		green_printf("Send: ACKs %d (Total %d)\n", send_ack_count, ack_issued);
	}
}

/* ---------------------------------------------------------------------------
------------------------------------VALs--------------------------------------
---------------------------------------------------------------------------*/
// Broadcast logic uses this function to post appropriate number of credit recvs before sending broadcasts
static inline void post_credit_recvs_and_batch_bcasts_to_NIC(
		uint16_t br_i, struct hrd_ctrl_blk *cb, struct ibv_send_wr *send_wr,
		struct ibv_recv_wr *credit_recv_wr, uint16_t *credit_recv_counter, uint8_t qp_id)
{
	assert(qp_id < TOTAL_WORKER_UD_QPs);
	uint16_t j;
	int ret;
	struct ibv_send_wr *bad_send_wr;
	struct ibv_recv_wr *bad_recv_wr;


	if (*credit_recv_counter > 0) { // Must post receives for credits
//		if (ENABLE_ASSERTIONS == 1) assert ((*credit_recv_counter) * (MACHINE_NUM - 1) <= MAX_CREDIT_RECVS);
		for (j = 0; j < (MACHINE_NUM - 1) * (*credit_recv_counter); j++) {
			credit_recv_wr[j].next = (j == ((MACHINE_NUM - 1) * (*credit_recv_counter)) - 1) ?	NULL : &credit_recv_wr[j + 1];
		}
//		printf("Credit_recv_counter %d\n",*credit_recv_counter);
		ret = ibv_post_recv(cb->dgram_qp[CRD_UD_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs for credits before broadcasting", ret);
		*credit_recv_counter = 0;
	}

	// Batch the broadcasts to the NIC
	if (br_i > 0) {
		///TODO we may need to change MESSAGES_IN_BCAST to machines in group
//		send_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = NULL;
		ret = ibv_post_send(cb->dgram_qp[qp_id], &send_wr[0], &bad_send_wr);
		CPE(ret, "Broadcast ibv_post_send error", ret);
		///TODO send non-inline aswell
//		if (CLIENT_ENABLE_INLINING == 1) coh_send_wr[0].send_flags = IBV_SEND_INLINE;
//		else if (!CLIENT_ENABLE_INLINING && protocol != STRONG_CONSISTENCY) coh_send_wr[0].send_flags = 0;
	}
}

static inline void forge_bcast_val_wrs(spacetime_ack_t* ack_op, spacetime_val_t* val_send_ops,
									   int* val_push_ptr, struct ibv_send_wr* send_val_wr,
									   struct ibv_sge* send_val_sgl, struct hrd_ctrl_blk* cb,
									   long long* br_tx, uint16_t br_i, uint16_t worker_lid)
{
	struct ibv_wc signal_send_wc;

	///TODO we need to empty the buffer space
	///coh_message_count[ACK_VC][ack_bcast_ops[*ack_pop_ptr].key.meta.cid]++; // we are emptying that buffer space
	memcpy(&val_send_ops[*val_push_ptr], ack_op, sizeof(spacetime_val_t));
	val_send_ops[*val_push_ptr].opcode = ST_OP_VAL;
	val_send_ops[*val_push_ptr].sender = (uint8_t) machine_id;
	send_val_sgl[br_i].addr = (uint64_t) (uintptr_t) (val_send_ops + (*val_push_ptr));

	///TODO add code for non-inlining
	w_stats[worker_lid].vals_per_worker++;
	HRD_MOD_ADD(*val_push_ptr, MAX_BATCH_OPS_SIZE);

	// Do a Signaled Send every BROADCAST_SS_BATCH broadcasts (BROADCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
	if ((*br_tx) % BROADCAST_SS_BATCH == 0) send_val_wr[0].send_flags |= IBV_SEND_SIGNALED;
	(*br_tx)++;
	if((*br_tx) % BROADCAST_SS_BATCH == BROADCAST_SS_BATCH - 1)
		hrd_poll_cq(cb->dgram_send_cq[VAL_UD_QP_ID], 1, &signal_send_wc);

	// SET THE LAST SEND_WR TO POINT TO NULL
//	if (br_i > 0)
//		send_inv_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = &send_inv_wr[br_i * MAX_MESSAGES_IN_BCAST];
}

static inline void broadcasts_vals(spacetime_ack_t* ack_ops, spacetime_val_t* val_send_ops, int* val_push_ptr,
								   struct ibv_send_wr* send_val_wr, struct ibv_sge* send_val_sgl,
								   uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
								   struct ibv_wc* credit_wc, uint32_t* credit_debug_cnt,
								   long long* br_tx, struct ibv_recv_wr* credit_recv_wr, uint16_t worker_lid)
{
	uint16_t i = 0, br_i = 0, j, credit_recv_counter = 0;
	static int val_issued = 0; ///TODO just for debugging
	int ret;
	struct ibv_send_wr *bad_send_wr;
	////TODO I don't think we need to traverse the whole array (MAX_BATCH_OPS_SIZE)
	// traverse all of the ops to find invs
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (ack_ops[i].opcode != ST_LAST_ACK_SUCCESS)
			continue;

		// if not enough credits for a Broadcast
		if (!check_broadcast_credits(credits, cb, credit_wc, credit_debug_cnt, VAL_UD_QP_ID))
			break;

		ack_ops[i].opcode = ST_EMPTY;

		// Create the broadcast messages
		forge_bcast_val_wrs(&ack_ops[i], val_send_ops, val_push_ptr, send_val_wr,
									   send_val_sgl, cb, br_tx, br_i, worker_lid);

		for (j = 0; j < MACHINE_NUM; j++)
			credits[VAL_UD_QP_ID][j]--;

		yellow_printf("Credits: VALs decremented %d (for machine %d)\n",
					  credits[VAL_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					  (machine_id + 1) % MACHINE_NUM);
		if ((*br_tx) % CREDITS_IN_MESSAGE == 0) credit_recv_counter++;
		br_i++;
		if (br_i == MAX_BCAST_BATCH) {
			//before sending acks post receives for INVs
//            yellow_printf("VAL\n");
			red_printf("Post receives: CRD %d \n", credit_recv_counter);
//	        post_receives(cb, (uint16_t) (br_i * (MACHINE_NUM - 1)), ST_CRD_BUFF, incoming_crds, crd_push_ptr);
			post_credit_recvs_and_batch_bcasts_to_NIC(br_i, cb, send_val_wr, credit_recv_wr,
													  &credit_recv_counter, VAL_UD_QP_ID);
//			ret = ibv_post_send(cb->dgram_qp[VAL_UD_QP_ID], &send_val_wr[0], &bad_send_wr);
//			CPE(ret, "Broadcast ibv_post_send error", ret);
			val_issued+=br_i;
			green_printf("Send: VALs %d (Total %d)\n", br_i, val_issued);
			br_i = 0;
		}
	}
}
/* ---------------------------------------------------------------------------
------------------------------------CRDs--------------------------------------
---------------------------------------------------------------------------*/
// Post Receives for VALs and then Send out the credits
static inline void send_credits(uint16_t credit_wr_i, struct ibv_sge* recv_val_sgl, struct hrd_ctrl_blk* cb,
								int* push_ptr, struct ibv_recv_wr* recv_val_wr, struct ibv_qp* coh_recv_qp,
								struct ibv_send_wr* credit_send_wr, uint16_t credits_in_message, uint32_t clt_buf_slots,
								void* buf)
{
	// For every credit message we send back, prepare x receives for the broadcasts
	uint16_t i, j, recv_iter = 0;
	struct ibv_recv_wr *bad_recv_wr;
	struct ibv_send_wr *bad_send_wr;
	for(i = 0; i < credit_wr_i; i++) {
		for (j = 0; j < credits_in_message; j++) {
			recv_iter = i * credits_in_message + j;
			recv_val_sgl[recv_iter].addr = (uintptr_t) buf + ((*push_ptr) * VAL_RECV_REQ_SIZE);
			HRD_MOD_ADD(*push_ptr, clt_buf_slots);
//			if (*push_ptr == 0) *push_ptr = 1;
			recv_val_wr[recv_iter].next = (recv_iter == (credit_wr_i * credits_in_message) - 1) ?
										  NULL : &recv_val_wr[recv_iter + 1];
		}
	}
	int ret = ibv_post_recv(cb->dgram_qp[VAL_UD_QP_ID], &recv_val_wr[0], &bad_recv_wr);
	CPE(ret, "ibv_post_recv error", ret);

	credit_send_wr[credit_wr_i - 1].next = NULL;
	// yellow_printf("I am sending %d credit message(s)\n", credit_wr_i);
	ret = ibv_post_send(cb->dgram_qp[CRD_UD_QP_ID], &credit_send_wr[0], &bad_send_wr);
	CPE(ret, "ibv_post_send error in credits", ret);
}
/* ---------------------------------------------------------------------------
------------------------------------DEB--------------------------------------
---------------------------------------------------------------------------*/
// Poll inv region
static inline void poll_invs(ud_req_inv_t* incoming_invs, int* pull_ptr,
							 spacetime_inv_t* inv_ops,     int* inv_ops_i,
							 uint16_t outstanding_invs, uint16_t worker_lid)
{
	static int invs_polled = 0; ///TODO only for debugging
	uint16_t worker_gid = (uint16_t) (machine_id * WORKERS_PER_MACHINE + worker_lid);	/* Global ID of this worker thread*/
	while (*inv_ops_i < MAX_BATCH_OPS_SIZE) {
		if (incoming_invs[(*pull_ptr + 1) % RECV_INV_Q_DEPTH].req.opcode != ST_OP_INV){
			//Empty or wrong
			if (ENABLE_ASSERTIONS == 1)
				if (incoming_invs[(*pull_ptr + 1) % RECV_INV_Q_DEPTH].req.opcode != ST_EMPTY){
					red_printf("Worker %d Opcode seen %s (%d)\n", worker_gid,
							   code_to_str(incoming_invs[(*pull_ptr + 1) % RECV_INV_Q_DEPTH].req.opcode),
							   incoming_invs[(*pull_ptr + 1) % RECV_INV_Q_DEPTH].req.opcode);
					assert(false);
				}
			break;
		}else{
			if (outstanding_invs + *inv_ops_i == MAX_BATCH_OPS_SIZE) break; // Back pressure if no empty slots
			HRD_MOD_ADD(*pull_ptr, RECV_INV_Q_DEPTH);
			memcpy(inv_ops + *inv_ops_i, &(incoming_invs[*pull_ptr].req), sizeof(spacetime_inv_t));
			incoming_invs[*pull_ptr].req.opcode = ST_EMPTY;
			assert(inv_ops[*inv_ops_i].tie_breaker_id < MACHINE_NUM);
//        yellow_printf("CLIENT %d I see a message %s for key %d with version %d, inv_ops_i %d\n", local_client_id,
//                      code_to_str(incoming_reqs[*pull_ptr].m_op.opcode), 	inv_ops[*inv_ops_i].key.tag,
//                                  inv_ops[*inv_ops_i].key.meta.version, (*inv_ops_i));
			(*inv_ops_i)++;
			if (ENABLE_STAT_COUNTING == 1) w_stats[worker_lid].received_invs_per_worker++;
			if (ENABLE_ASSERTIONS == 1) assert(*pull_ptr >= 0);
			yellow_printf("%d INVs polled\n", ++invs_polled);
//			green_printf("Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, sender: %d, value: %s\n",
//						 ((uint64_t *) &inv_ops[*inv_ops_i - 1].key)[1],
//						 code_to_str(inv_ops[*inv_ops_i - 1].opcode),inv_ops[*inv_ops_i - 1].version,
//						 inv_ops[*inv_ops_i - 1].tie_breaker_id, inv_ops[*inv_ops_i - 1].sender,
//						 inv_ops[*inv_ops_i - 1].value);
		}
	}
}

#endif //HERMES_INLINE_UTIL_H
