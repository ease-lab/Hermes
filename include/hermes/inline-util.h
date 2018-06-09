//
// Created by akatsarakis on 23/05/18.
//

#ifndef HERMES_INLINE_UTIL_H
#define HERMES_INLINE_UTIL_H

#include <infiniband/verbs.h>
#include "spacetime.h"
#include "config.h"
#include "util.h"


static inline void poll_credits(struct ibv_cq* credit_recv_cq,
								struct ibv_wc* credit_wc, uint8_t credits[][MACHINE_NUM]);
/* ---------------------------------------------------------------------------
------------------------------------GENERIC-----------------------------------
---------------------------------------------------------------------------*/
///TODO ADD Batching to the NIC for post RECEIVES
static inline void post_receives(struct hrd_ctrl_blk *cb, uint16_t num_of_receives,
								 uint8_t buff_type, void *recv_buff, int *push_ptr)
{
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
        memset(next_buff_addr, 0, (size_t) req_size);
        *next_buff_opcode = ST_EMPTY;
        hrd_post_dgram_recv(cb->dgram_qp[qp_id],
                            next_buff_addr,
                            req_size, cb->dgram_buf_mr->lkey);
        HRD_MOD_ADD(*push_ptr, recv_q_depth);
    }
}

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
			if(ENABLE_ASSERTIONS) max_credits = ACK_CREDITS;
			break;
		case ST_ACK_BUFF:
			qp_credits_to_inc = INV_UD_QP_ID;
			recv_q_depth = RECV_ACK_Q_DEPTH;
			req_size = sizeof(spacetime_ack_t);
			if(ENABLE_ASSERTIONS) max_credits = INV_CREDITS;
			break;
		case ST_VAL_BUFF:
			qp_credits_to_inc = CRD_UD_QP_ID;
			recv_q_depth = RECV_VAL_Q_DEPTH;
			req_size = sizeof(spacetime_val_t);
			if(ENABLE_ASSERTIONS) max_credits = CRD_CREDITS;
			break;
		case ST_CRD_BUFF:
			qp_credits_to_inc = VAL_UD_QP_ID;
			recv_q_depth = RECV_CRD_Q_DEPTH;
			req_size = sizeof(spacetime_crd_t);
			if(ENABLE_ASSERTIONS) max_credits = VAL_CREDITS;
			break;
		default: assert(0);
	}

	reqs_polled = ibv_poll_cq(completion_q, MAX_BATCH_OPS_SIZE - outstanding_ops - *ops_push_ptr, work_completions);
//	if(reqs_polled > 0)
//		printf("FOUND %d completions pull_ptr %d\n",reqs_polled, *buf_pull_ptr);
	for(i = 0; i < reqs_polled; i++){
		index = (*buf_pull_ptr + 1) % recv_q_depth;
		switch (buff_type) {
			case ST_INV_BUFF:
				sender = ((ud_req_inv_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_inv_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_inv_t *) incoming_buff)[index].req.opcode;
//                printf("Op Ptr: %d Code: %s sender %d\n", next_opcode_ptr, code_to_str(*next_opcode_ptr), sender);
                spacetime_inv_t* tmp = next_req_ptr;
//				green_printf("Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, sender: %d, value: %s\n",
//						 ((uint64_t *) &tmp->key)[1],
//						 code_to_str(tmp->opcode),tmp->version,
//						 tmp->tie_breaker_id, tmp->sender,
//						 tmp->value);
				if (ENABLE_ASSERTIONS)
					assert(*next_opcode_ptr == ST_OP_INV || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_ACK_BUFF:
				sender = ((ud_req_ack_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_ack_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_ack_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS)
					assert(*next_opcode_ptr == ST_OP_ACK || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_VAL_BUFF:
				sender = ((ud_req_val_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_val_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_val_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS)
					assert(*next_opcode_ptr == ST_OP_VAL || *next_opcode_ptr == ST_EMPTY);
				break;
			case ST_CRD_BUFF:
				sender = ((ud_req_crd_t *) incoming_buff)[index].req.sender;
				next_req_ptr = &((ud_req_crd_t *) incoming_buff)[index].req;
				next_opcode_ptr = &((ud_req_crd_t *) incoming_buff)[index].req.opcode;
				if (ENABLE_ASSERTIONS)
					assert(*next_opcode_ptr == ST_OP_CRD || *next_opcode_ptr == ST_EMPTY);
				break;
			default:
				assert(0);
		}
		if (*next_opcode_ptr == ST_EMPTY) {
            assert(0);
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
            if(ENABLE_ASSERTIONS)
				assert(credits[qp_credits_to_inc][sender] <= max_credits);
		}
	}
	if(reqs_polled > 0){
		//poll completion q
//		poll_completions(reqs_polled, completion_q, work_completions);
		if (ENABLE_STAT_COUNTING)
			switch(buff_type){
				case ST_INV_BUFF:
					w_stats[worker_lid].received_invs_per_worker += reqs_polled;
                    if(ENABLE_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: \033[31mINVs\033[0m %d, (total: %d)!\n", worker_lid, reqs_polled, w_stats[worker_lid].received_invs_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[33mACKs\033[0m \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",worker_lid, credits[qp_credits_to_inc][sender], sender);
//			            green_printf("Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, sender: %d, value: %s\n",
//						 ((uint64_t *) &inv_ops[*inv_ops_i - 1].key)[1],
//						 code_to_str(inv_ops[*inv_ops_i - 1].opcode),inv_ops[*inv_ops_i - 1].version,
//						 inv_ops[*inv_ops_i - 1].tie_breaker_id, inv_ops[*inv_ops_i - 1].sender,
//						 inv_ops[*inv_ops_i - 1].value);
					break;
				case ST_ACK_BUFF:
					w_stats[worker_lid].received_acks_per_worker += reqs_polled;
					if(ENABLE_RECV_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: \033[33mACKs\033[0m %d, (total: %d)!\n", worker_lid, reqs_polled, w_stats[worker_lid].received_acks_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[31mINVs\033[0m \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n", worker_lid, credits[qp_credits_to_inc][sender], sender);
					break;
				case ST_VAL_BUFF:
					w_stats[worker_lid].received_vals_per_worker += reqs_polled;
					if(ENABLE_RECV_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: \033[1m\033[32mVALs\033[0m %d, (total: %d)!\n", worker_lid, reqs_polled, w_stats[worker_lid].received_vals_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[1m\033[36mCRDs\033[0m \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",worker_lid, credits[qp_credits_to_inc][sender], sender);
					break;
				case ST_CRD_BUFF:
					w_stats[worker_lid].received_crds_per_worker += reqs_polled;
					if(ENABLE_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: \033[1m\033[36mCRDs\033[0m %d, (total: %d)!\n",worker_lid, reqs_polled, w_stats[worker_lid].received_vals_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[1m\033[32mVALs\033[0m \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",worker_lid, credits[qp_credits_to_inc][sender], sender);
					break;
				default: assert(0);
			}
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
static inline void forge_bcast_inv_wrs(spacetime_ops_t* op, spacetime_inv_t* inv_send_ops,
									   int* inv_push_ops_ptr, struct ibv_send_wr* send_inv_wr,
									   struct ibv_sge* send_inv_sgl, struct hrd_ctrl_blk* cb,
									   long long* br_tx, uint16_t br_i, uint16_t worker_lid)
{
	static int total_completions = 0;
	struct ibv_wc signal_send_wc;

	inv_send_ops[*inv_push_ops_ptr].opcode = ST_OP_INV;
	inv_send_ops[*inv_push_ops_ptr].sender = (uint8_t) machine_id;
    inv_send_ops[*inv_push_ops_ptr].version = op->version;
	inv_send_ops[*inv_push_ops_ptr].tie_breaker_id = op->tie_breaker_id;
	inv_send_ops[*inv_push_ops_ptr].val_len = op->val_len;
	if(ENABLE_ASSERTIONS)
		assert(op->val_len == ST_VALUE_SIZE);
	memcpy(&inv_send_ops[*inv_push_ops_ptr].value, op->value, op->val_len);

//	inv_send_ops[*inv_push_ops_ptr].version = resp->version;
//	inv_send_ops[*inv_push_ops_ptr].tie_breaker_id = resp->tie_breaker_id;
//	inv_send_ops[*inv_push_ops_ptr].val_len = resp->val_len;
//	memcpy(&inv_send_ops[*inv_push_ops_ptr].value, resp->val_ptr, resp->val_len);
	memcpy(&inv_send_ops[*inv_push_ops_ptr].key, &op->key, KEY_SIZE);
	send_inv_sgl[br_i].addr = (uint64_t) (uintptr_t) (inv_send_ops + (*inv_push_ops_ptr));

	///TODO add code for non-inlining
	w_stats[worker_lid].invs_per_worker++;
	HRD_MOD_ADD(*inv_push_ops_ptr, MAX_BATCH_OPS_SIZE);


	if(br_i == 0) //reset possibly signaled wr from the prev round
		send_inv_wr[0].send_flags = IBV_SEND_INLINE;
	// Do a Signaled Send every INV_SS_GRANULARITY broadcasts (INV_SS_GRANULARITY * (MACHINE_NUM - 1) messages)
	if ((*br_tx) % INV_SS_GRANULARITY == 0){
        if(ENABLE_SS_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("vvv Send SS[W%d]: \033[31mINV\033[0m \n", worker_lid);
		send_inv_wr[0].send_flags |= IBV_SEND_SIGNALED;
	}
	if(*br_tx > 0 && (*br_tx) % INV_SS_GRANULARITY == INV_SS_GRANULARITY - 1){
		hrd_poll_cq(cb->dgram_send_cq[INV_UD_QP_ID], 1, &signal_send_wc);
		if(ENABLE_SS_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("^^^ Polled SS completion[W%d]: \033[31mINV\033[0m %d (total %d)\n", worker_lid, 1, ++total_completions);
	}
	(*br_tx)++;
	// SET THE LAST SEND_WR TO POINT TO NULL
	if (br_i > 0)
		send_inv_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = &send_inv_wr[br_i * MAX_MESSAGES_IN_BCAST];
}


static inline void broadcasts_invs(spacetime_ops_t* ops, spacetime_inv_t* inv_send_ops, int* inv_push_ptr,
								   struct ibv_send_wr* send_inv_wr, struct ibv_sge* send_inv_sgl,
								   uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
                                   long long* br_tx, ud_req_ack_t* incoming_acks, int* ack_push_ptr,
								   uint16_t worker_lid, uint32_t* credit_debug_cnt)
{
	uint8_t missing_credits = 0;
	uint16_t i = 0, br_i = 0, j = 0, k = 0;
	int ret;
	struct ibv_send_wr *bad_send_wr;
	static int total_invs_issued[WORKERS_PER_MACHINE]= { 0 };///TODO just for debugging

	// traverse all of the ops to find invs
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (ops[i].state != ST_PUT_SUCCESS && ops[i].state != ST_BUFFERED_REPLAY)
			continue;

		for (j = 0; j < MACHINE_NUM; j++) {
			if (j == machine_id) continue; // skip the local machine
			if (credits[INV_UD_QP_ID][j] == 0) {
				missing_credits = 1;
				(*credit_debug_cnt)++;
				break;
			}
		}
		// if not enough credits for a Broadcast
		if (missing_credits == 1) break;
		*credit_debug_cnt = 0;

		if(ops[i].state == ST_PUT_SUCCESS)
			ops[i].state = ST_IN_PROGRESS_WRITE;
		else if(ops[i].state == ST_BUFFERED_REPLAY)
			ops[i].state = ST_BUFFERED_IN_PROGRESS_REPLAY;
		else assert(0);

		// Create the broadcast messages
		forge_bcast_inv_wrs(&ops[i], inv_send_ops, inv_push_ptr, send_inv_wr,
							send_inv_sgl, cb, br_tx, br_i, worker_lid);
		for (j = 0; j < MACHINE_NUM; j++)
			credits[INV_UD_QP_ID][j]--;
		if(ENABLE_CREDIT_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			printf("$$$ Credits[W%d]: \033[31mINVs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					  worker_lid, credits[INV_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					  (machine_id + 1) % MACHINE_NUM);
		br_i++;
		if (br_i == MAX_PCIE_BCAST_BATCH) {
			send_inv_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = NULL;
//			if(ENABLE_ASSERTIONS){
//				assert(send_inv_wr[0].next == NULL);
//				assert(send_inv_wr[0].num_sge== 1);
//				assert(send_inv_wr[0].opcode == IBV_SEND_INLINE | IBV_SEND_SIGNALED);
//				assert(send_inv_wr[0].sg_list == &send_inv_sgl[0]);
//				assert(send_inv_sgl[0].length == sizeof(spacetime_inv_t));
//				assert(send_inv_wr[0].wr.ud.ah == remote_worker_qps[MACHINE_NUM - machine_id - 1][INV_UD_QP_ID].ah);
//				assert(send_inv_wr[0].wr.ud.remote_qpn == remote_worker_qps[MACHINE_NUM - machine_id - 1][INV_UD_QP_ID].qpn);
//			}
			if(ENABLE_POST_RECV_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				yellow_printf("vvv Post Receives[W%d]: \033[33mACKs\033[0m %d\n", worker_lid, br_i * (MACHINE_NUM - 1));
			post_receives(cb, (uint16_t) (br_i * (MACHINE_NUM - 1)), ST_ACK_BUFF, incoming_acks, ack_push_ptr);
			total_invs_issued[worker_lid]+=br_i;
			if(ENABLE_SEND_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				cyan_printf(">>> Send[W%d]: \033[31mINVs\033[0m %d (Total %d)\n", worker_lid, br_i, total_invs_issued[worker_lid]);
			ret = ibv_post_send(cb->dgram_qp[INV_UD_QP_ID], &send_inv_wr[0], &bad_send_wr);
			CPE(ret, "INVs ibv_post_send error", ret);
			br_i = 0;
		}
	}
   	if(br_i > 0){
		send_inv_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = NULL;
		if(ENABLE_POST_RECV_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			yellow_printf("vvv Post Receives[W%d]: \033[33mACKs\033[0m %d\n", worker_lid, br_i * (MACHINE_NUM - 1));
		post_receives(cb, (uint16_t) (br_i * (MACHINE_NUM - 1)), ST_ACK_BUFF, incoming_acks, ack_push_ptr);
		total_invs_issued[worker_lid]+=br_i;
		if(ENABLE_SEND_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			cyan_printf(">>> Send[W%d]: \033[31mINVs\033[0m %d (Total %d)\n", worker_lid, br_i, total_invs_issued[worker_lid]);
		ret = ibv_post_send(cb->dgram_qp[INV_UD_QP_ID], &send_inv_wr[0], &bad_send_wr);
		CPE(ret, "INVs ibv_post_send error", ret);
	}
}

/* ---------------------------------------------------------------------------
------------------------------------ACKS--------------------------------------
---------------------------------------------------------------------------*/

static inline void forge_ack_wr(spacetime_inv_t* inv_recv_op, spacetime_ack_t* ack_send_ops,
								int* ack_push_ptr, struct ibv_send_wr* send_ack_wr,
								struct ibv_sge* send_ack_sgl, struct hrd_ctrl_blk* cb,
								long long* send_ack_tx, uint16_t send_ack_count, uint16_t worker_lid)
{
	static int total_completions = 0;
	struct ibv_wc signal_send_wc;
	uint16_t dst_worker_gid = (uint16_t) (inv_recv_op->sender * WORKERS_PER_MACHINE + worker_lid);

	ack_send_ops->opcode = ST_OP_ACK;
	ack_send_ops->sender = (uint8_t) machine_id;
	ack_send_ops->version =  inv_recv_op->version;
	ack_send_ops->tie_breaker_id = inv_recv_op->tie_breaker_id;
	memcpy(&ack_send_ops->key, &inv_recv_op->key, KEY_SIZE);

	send_ack_sgl[send_ack_count].addr = (uint64_t) ack_send_ops;
	send_ack_wr[send_ack_count].wr.ud.ah = remote_worker_qps[dst_worker_gid][ACK_UD_QP_ID].ah;
	send_ack_wr[send_ack_count].wr.ud.remote_qpn = (uint32) remote_worker_qps[dst_worker_gid][ACK_UD_QP_ID].qpn;
	send_ack_wr[send_ack_count].send_flags = IBV_SEND_INLINE;

	///TODO add code for non-inlining
	w_stats[worker_lid].acks_per_worker++;
//	HRD_MOD_ADD(*ack_push_ptr, MAX_BATCH_OPS_SIZE);

	if (send_ack_count > 0)
		send_ack_wr[send_ack_count - 1].next = &send_ack_wr[send_ack_count];
	if ((*send_ack_tx) % ACK_SS_GRANULARITY == 0) {
		send_ack_wr[send_ack_count].send_flags |= IBV_SEND_SIGNALED;
		if(ENABLE_SS_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("vvv Send SS[W%d]: \033[33mACK\033[0m\n",worker_lid);
	}
	if(*send_ack_tx > 0 && (*send_ack_tx) % ACK_SS_GRANULARITY == ACK_SS_GRANULARITY - 1) {
		hrd_poll_cq(cb->dgram_send_cq[ACK_UD_QP_ID], 1, &signal_send_wc);
		if(ENABLE_SS_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("^^^ Polled SS completion[W%d]: \033[33mACK\033[0m %d (total %d)\n", worker_lid, 1, ++total_completions);
	}
	(*send_ack_tx)++; // Selective signaling
}

static inline void issue_acks(spacetime_inv_t *inv_recv_ops, spacetime_ack_t* ack_send_ops,
							  int* ack_push_ptr, long long int* send_ack_tx,
							  struct ibv_send_wr *send_ack_wr, struct ibv_sge *send_ack_sgl,
					          uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
                              ud_req_inv_t* incoming_invs, int* inv_push_ptr,
							  uint16_t worker_lid, uint32_t* credit_debug_cnt)
{
	static int total_acks_issued[WORKERS_PER_MACHINE]= { 0 }; ///TODO just for debugging
	int ret;
	struct ibv_send_wr *bad_send_wr;
	uint16_t i = 0, send_ack_count = 0;
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (inv_recv_ops[i].opcode == ST_EMPTY)
			continue;
		if(credits[ACK_UD_QP_ID][inv_recv_ops[i].sender] == 0){
			(*credit_debug_cnt)++;
			continue; //TODO we may need to push this to the top of the stack + (seems ok to batch this again to the KVS)
		}
		*credit_debug_cnt = 0;
		//reduce credits
		credits[ACK_UD_QP_ID][inv_recv_ops[i].sender]--;
		if(ENABLE_CREDIT_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			printf("$$$ Credits[W%d]: \033[33mACKs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					  worker_lid, credits[ACK_UD_QP_ID][inv_recv_ops[i].sender],
					  inv_recv_ops[i].sender);
		// Create the broadcast messages
		forge_ack_wr(&inv_recv_ops[i], &ack_send_ops[send_ack_count], ack_push_ptr, send_ack_wr,
					 send_ack_sgl, cb, send_ack_tx, send_ack_count, worker_lid);


        //empty inv buffer
		if(inv_recv_ops[i].opcode == ST_INV_SUCCESS)
			inv_recv_ops[i].opcode = ST_EMPTY;
		else{
            printf("Unexpected Code: %s\n",code_to_str(inv_recv_ops[i].opcode));
			assert(0);
		}
		send_ack_count++;
		if (send_ack_count == MAX_SEND_ACK_WRS) {
			//before sending acks post receives for INVs
			if(ENABLE_POST_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				yellow_printf("vvv Post Receives[W%d]: \033[31mINVs\033[0m %d\n", worker_lid, send_ack_count);
			post_receives(cb, send_ack_count, ST_INV_BUFF, incoming_invs, inv_push_ptr);
			send_ack_wr[send_ack_count - 1].next = NULL;
			if(ENABLE_ASSERTIONS){
				assert(send_ack_wr[0].opcode == IBV_WR_SEND);
				assert(send_ack_wr[0].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
				assert(send_ack_wr[0].sg_list == &send_ack_sgl[0]);
				assert(send_ack_wr[0].sg_list->length == sizeof(spacetime_ack_t));
				assert(send_ack_wr[0].num_sge == 1);
//			assert(send_ack_wr[0].send_flags == IBV_SEND_INLINE);
				assert(((spacetime_inv_t*) send_ack_sgl[0].addr)->sender == machine_id);
			}
			total_acks_issued[worker_lid] +=send_ack_count;
			if(ENABLE_SEND_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				cyan_printf(">>> Send[W%d]: \033[33mACKs\033[0m %d (Total %d)\n", worker_lid, send_ack_count, total_acks_issued[worker_lid]);
			ret = ibv_post_send(cb->dgram_qp[ACK_UD_QP_ID], &send_ack_wr[0], &bad_send_wr);
			CPE(ret, "ibv_post_send error while sending ACKs", ret);
			send_ack_count = 0;
		}
	}

	if (send_ack_count > 0) {
		//before sending acks post receives for INVs
		if(ENABLE_POST_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			yellow_printf("vvv Post Receives[W%d]: \033[31mINVs\033[0m %d\n", worker_lid, send_ack_count);
		post_receives(cb, send_ack_count, ST_INV_BUFF, incoming_invs, inv_push_ptr);
		send_ack_wr[send_ack_count - 1].next = NULL;
	    if(ENABLE_ASSERTIONS){
			assert(send_ack_wr[0].opcode == IBV_WR_SEND);
			assert(send_ack_wr[0].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(send_ack_wr[0].sg_list == &send_ack_sgl[0]);
			assert(send_ack_wr[0].sg_list->length == sizeof(spacetime_ack_t));
			assert(send_ack_wr[0].num_sge == 1);
//			assert(send_ack_wr[0].send_flags == IBV_SEND_INLINE);
			assert(((spacetime_inv_t*) send_ack_sgl[0].addr)->sender == machine_id);
		}
		total_acks_issued[worker_lid]+=send_ack_count;
		if(ENABLE_SEND_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			cyan_printf(">>> Send[W%d]: \033[33mACKs\033[0m %d (Total %d)\n", worker_lid, send_ack_count, total_acks_issued[worker_lid]);
		ret = ibv_post_send(cb->dgram_qp[ACK_UD_QP_ID], &send_ack_wr[0], &bad_send_wr);
		CPE(ret, "ibv_post_send error while sending ACKs", ret);
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
	if(ENABLE_ASSERTIONS)
		assert(qp_id < TOTAL_WORKER_UD_QPs);
	uint16_t j;
	int ret;
	struct ibv_send_wr *bad_send_wr;
	struct ibv_recv_wr *bad_recv_wr;


	if (*credit_recv_counter > 0) { // Must post receives for credits
		for (j = 0; j < (MACHINE_NUM - 1) * (*credit_recv_counter); j++)
			credit_recv_wr[j].next = (j == ((MACHINE_NUM - 1) * (*credit_recv_counter)) - 1) ?	NULL : &credit_recv_wr[j + 1];
		ret = ibv_post_recv(cb->dgram_qp[CRD_UD_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs for credits before broadcasting", ret);
		*credit_recv_counter = 0;
	}

	// Batch the broadcasts to the NIC
	if (br_i > 0) {
		send_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = NULL;
		ret = ibv_post_send(cb->dgram_qp[qp_id], &send_wr[0], &bad_send_wr);
		CPE(ret, "VALs ibv_post_send error", ret);
		///TODO send non-inline aswell
//		if (CLIENT_ENABLE_INLINING) coh_send_wr[0].send_flags = IBV_SEND_INLINE;
//		else if (!CLIENT_ENABLE_INLINING && protocol != STRONG_CONSISTENCY) coh_send_wr[0].send_flags = 0;
	}
}

static inline void post_credit_recvs(struct hrd_ctrl_blk *cb,
									 struct ibv_recv_wr *credit_recv_wr,
									 uint16_t *credit_recv_counter)
{
	uint16_t j;
	int ret;
	struct ibv_recv_wr *bad_recv_wr;
	if (*credit_recv_counter > 0) { // Must post receives for credits
		for (j = 0; j < (MACHINE_NUM - 1) * (*credit_recv_counter); j++)
			credit_recv_wr[j].next = (j == ((MACHINE_NUM - 1) * (*credit_recv_counter)) - 1) ?	NULL : &credit_recv_wr[j + 1];
		ret = ibv_post_recv(cb->dgram_qp[CRD_UD_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs for credits before broadcasting", ret);
		*credit_recv_counter = 0;
	}
}

static inline void forge_bcast_val_wrs(spacetime_ack_t* ack_op, spacetime_val_t* val_send_ops,
									   int* val_push_ptr, struct ibv_send_wr* send_val_wr,
									   struct ibv_sge* send_val_sgl, struct hrd_ctrl_blk* cb,
									   long long* br_tx, uint16_t br_i, uint16_t worker_lid)
{
	static int total_completions = 0;
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

	if(br_i == 0) //reset possibly signaled wr from the prev round
		send_val_wr[br_i].send_flags = IBV_SEND_INLINE;
	// Do a Signaled Send every VAL_SS_GRANULARITY broadcasts (VAL_SS_GRANULARITY * (MACHINE_NUM - 1) messages)
	if ((*br_tx) % VAL_SS_GRANULARITY == 0){
		if(ENABLE_SS_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("vvv Send SS[W%d]: \033[1m\033[32mVAL\033[0m \n", worker_lid);
		send_val_wr[0].send_flags |= IBV_SEND_SIGNALED;
	}

	if(*br_tx > 0 && (*br_tx) % VAL_SS_GRANULARITY == VAL_SS_GRANULARITY - 1){
		hrd_poll_cq(cb->dgram_send_cq[VAL_UD_QP_ID], 1, &signal_send_wc);
		if(ENABLE_SS_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("^^^ Polled SS completion[W%d]: \033[1m\033[32mVAL\033[0m %d (total %d)\n", worker_lid, 1, ++total_completions);
	}
	(*br_tx)++;
	// SET THE LAST SEND_WR TO POINT TO NULL
	if (br_i > 0)
		send_val_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = &send_val_wr[br_i * MAX_MESSAGES_IN_BCAST];
}

static inline void broadcasts_vals(spacetime_ack_t* ack_ops, spacetime_val_t* val_send_ops, int* val_push_ptr,
								   struct ibv_send_wr* send_val_wr, struct ibv_sge* send_val_sgl,
								   uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
								   struct ibv_wc* credit_wc, uint32_t* credit_debug_cnt,
								   long long* br_tx, struct ibv_recv_wr* credit_recv_wr, uint16_t worker_lid)
{
	uint16_t i = 0, br_i = 0, j, credit_recv_counter = 0;
	static int total_vals_issued[WORKERS_PER_MACHINE]= { 0 }; ///TODO just for debugging
	int ret;
	struct ibv_send_wr *bad_send_wr;
	////TODO I don't think we need to traverse the whole array (MAX_BATCH_OPS_SIZE)
	// traverse all of the ops to find bcasts
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

		if(ENABLE_CREDIT_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			printf("$$$ Credits[W%d]: \033[1m\033[32mVALs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					  worker_lid, credits[VAL_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					  (machine_id + 1) % MACHINE_NUM);
		if ((*br_tx) % CRDS_IN_MESSAGE == 0) credit_recv_counter++;
		br_i++;
		if (br_i == MAX_PCIE_BCAST_BATCH) {
			send_val_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = NULL;
			if(ENABLE_POST_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				yellow_printf("vvv Post Receives[W%d]: \033[1m\033[36mCRDs\033[0m %d\n", worker_lid, credit_recv_counter);
			post_credit_recvs(cb, credit_recv_wr, &credit_recv_counter);
			total_vals_issued[worker_lid]+=br_i;
			if(ENABLE_SEND_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				cyan_printf(">>> Send[W%d]: \033[1m\033[32mVALs\033[0m %d (Total %d)\n", worker_lid, br_i, total_vals_issued[worker_lid]);
			ret = ibv_post_send(cb->dgram_qp[VAL_UD_QP_ID], &send_val_wr[0], &bad_send_wr);
			CPE(ret, "Broadcast VALs ibv_post_send error", ret);
			br_i = 0;
		}
	}
	if(br_i > 0){
		send_val_wr[(br_i * MAX_MESSAGES_IN_BCAST) - 1].next = NULL;
		if(ENABLE_POST_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			yellow_printf("vvv Post Receives[W%d]: \033[1m\033[36mCRDs\033[0m %d\n", worker_lid, credit_recv_counter);
		post_credit_recvs(cb,credit_recv_wr,&credit_recv_counter);
		total_vals_issued[worker_lid]+=br_i;
		if(ENABLE_SEND_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			cyan_printf(">>> Send[W%d]: \033[1m\033[32mVALs\033[0m %d (Total %d)\n", worker_lid, br_i, total_vals_issued[worker_lid]);
		ret = ibv_post_send(cb->dgram_qp[VAL_UD_QP_ID], &send_val_wr[0], &bad_send_wr);
		CPE(ret, "Broadcast VALs ibv_post_send error", ret);
	}
}

/* ---------------------------------------------------------------------------
------------------------------------CRDs--------------------------------------
---------------------------------------------------------------------------*/
// Poll and increment credits
///TODO I don't need to pass the whole credit array just the VAL_UD_QP_ID
static inline void poll_credits(struct ibv_cq* credit_recv_cq, struct ibv_wc* credit_wc,
								uint8_t credits[][MACHINE_NUM])
{
	spacetime_crd_t crd_tmp;
	int i = 0, credits_found = 0;

	credits_found = ibv_poll_cq(credit_recv_cq, RECV_CRD_Q_DEPTH, credit_wc);
	if(credits_found > 0) {
		if(unlikely(credit_wc[credits_found -1].status != 0)) {
			fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
			exit(0);
		}

		for (i = 0; i < credits_found; i++){
			memcpy(&crd_tmp, &credit_wc[i].imm_data, sizeof(spacetime_crd_t));
            if(ENABLE_ASSERTIONS)
				assert(crd_tmp.opcode == ST_OP_CRD);
			credits[VAL_UD_QP_ID][crd_tmp.sender] += crd_tmp.val_credits;
		}
	} else if(unlikely(credits_found < 0)) {
		printf("ERROR In the credit CQ\n");
		exit(0);
	}
}

static inline void forge_crd_wr(spacetime_val_t* val_recv_op, struct ibv_send_wr* send_crd_wr,
								struct hrd_ctrl_blk* cb, long long* send_crd_tx,
								uint16_t send_crd_count, uint16_t worker_lid)
{
	static int total_completions = 0;
	struct ibv_wc signal_send_wc;
	uint16_t dst_worker_gid = (uint16_t) (val_recv_op->sender * WORKERS_PER_MACHINE + worker_lid);

    ///TODO may want to change / have variable CRDS in msg
//	spacetime_crd_t crd_tmp;
//	crd_tmp.opcode = ST_OP_CRD;
//	crd_tmp.sender = (uint8_t) machine_id;
//	crd_tmp.val_credits = CRDS_IN_MESSAGE;
//	memcpy(&send_crd_wr[send_crd_count].imm_data, &crd_tmp, sizeof(spacetime_crd_t));

	send_crd_wr[send_crd_count].wr.ud.ah = remote_worker_qps[dst_worker_gid][CRD_UD_QP_ID].ah;
	send_crd_wr[send_crd_count].wr.ud.remote_qpn = (uint32) remote_worker_qps[dst_worker_gid][CRD_UD_QP_ID].qpn;
	send_crd_wr[send_crd_count].send_flags = IBV_SEND_INLINE;


	///TODO add code for non-inlining
	w_stats[worker_lid].crds_per_worker++;

	if (send_crd_count > 0)
		send_crd_wr[send_crd_count - 1].next = &send_crd_wr[send_crd_count];
	if ((*send_crd_tx) % CRD_SS_GRANULARITY == 0) {
		send_crd_wr[send_crd_count].send_flags |= IBV_SEND_SIGNALED;
		if(ENABLE_SS_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("vvv Send SS[W%d]: \033[1m\033[36mCRD\033[0m\n", worker_lid);
	}
	if(*send_crd_tx > 0 && (*send_crd_tx) % CRD_SS_GRANULARITY == CRD_SS_GRANULARITY - 1) {
		hrd_poll_cq(cb->dgram_send_cq[CRD_UD_QP_ID], 1, &signal_send_wc);
		if(ENABLE_SS_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("^^^ Polled SS completion[W%d]: \033[1m\033[36mCRD\033[0m %d (total %d)\n", worker_lid, 1, ++total_completions);
	}
	(*send_crd_tx)++; // Selective signaling
}


static inline void issue_credits(spacetime_val_t *val_recv_ops, long long int* send_crd_tx,
								 struct ibv_send_wr *send_crd_wr, uint8_t credits[][MACHINE_NUM],
								 struct hrd_ctrl_blk *cb, ud_req_val_t* incoming_vals,
								 int* val_push_ptr, uint16_t worker_lid, uint32_t* credit_debug_cnt)
{
	static int total_crds_issued[WORKERS_PER_MACHINE]= { 0 }; ///TODO just for debugging
	int ret;
	struct ibv_send_wr *bad_send_wr;
	uint16_t i = 0, send_crd_count = 0;
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (val_recv_ops[i].opcode == ST_EMPTY)
			continue;
		if(credits[CRD_UD_QP_ID][val_recv_ops[i].sender] == 0){
			*(credit_debug_cnt)++;
			continue; //TODO we may need to push this to the top of the stack + (seems ok to batch this again to the KVS)
		}
		*credit_debug_cnt = 0;
		//reduce credits
		credits[CRD_UD_QP_ID][val_recv_ops[i].sender]--;
		if(ENABLE_CREDIT_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			printf("$$$ Credits[W%d]: \033[1m\033[36mCRDs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					  worker_lid, credits[CRD_UD_QP_ID][val_recv_ops[i].sender],
					  val_recv_ops[i].sender);
		// Create the broadcast messages
		forge_crd_wr(&val_recv_ops[i], send_crd_wr, cb,
					 send_crd_tx, send_crd_count, worker_lid);

        //empty inv buffer
		if(val_recv_ops[i].opcode == ST_VAL_SUCCESS)
			val_recv_ops[i].opcode = ST_EMPTY;
		else{
            printf("Unexpected Code: %s\n",code_to_str(val_recv_ops[i].opcode));
			assert(0);
		}
		send_crd_count++;
		if (send_crd_count == MAX_SEND_CRD_WRS) {
			//before sending acks post receives for INVs
			if(ENABLE_POST_RECV_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				yellow_printf("vvv Post Receives[W%d]: \033[1m\033[32mVALs\033[0m %d\n", worker_lid, send_crd_count);
			post_receives(cb, send_crd_count, ST_VAL_BUFF, incoming_vals, val_push_ptr);
			send_crd_wr[send_crd_count - 1].next = NULL;
			if(ENABLE_ASSERTIONS){
				assert(send_crd_wr[0].opcode == IBV_WR_SEND_WITH_IMM);
				assert(send_crd_wr[0].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
				assert(send_crd_wr[0].sg_list->length == 0);
				assert(send_crd_wr[0].num_sge == 0);
			}
			total_crds_issued[worker_lid] += send_crd_count;
			if(ENABLE_SEND_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				cyan_printf(">>> Send[W%d]: \033[1m\033[36mCRDs\033[0m %d (Total %d)\n", worker_lid, send_crd_count, total_crds_issued[worker_lid]);
			ret = ibv_post_send(cb->dgram_qp[CRD_UD_QP_ID], &send_crd_wr[0], &bad_send_wr);
			CPE(ret, "ibv_post_send error while sending CRDs", ret);
			send_crd_count = 0;
		}
	}

	if (send_crd_count > 0) {
        //before sending acks post receives for INVs
		if(ENABLE_POST_RECV_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			yellow_printf("vvv Post Receives[W%d]: \033[1m\033[32mVALs\033[0m %d\n", worker_lid, send_crd_count);
		post_receives(cb, send_crd_count, ST_VAL_BUFF, incoming_vals, val_push_ptr);
		send_crd_wr[send_crd_count - 1].next = NULL;
		if(ENABLE_ASSERTIONS){
			assert(send_crd_wr[0].opcode == IBV_WR_SEND_WITH_IMM);
			assert(send_crd_wr[0].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(send_crd_wr[0].sg_list->length == 0);
			assert(send_crd_wr[0].num_sge == 0);
		}
		total_crds_issued[worker_lid] += send_crd_count;
		if(ENABLE_SEND_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			cyan_printf(">>> Send[W%d]: \033[1m\033[36mCRDs\033[0m %d (Total %d)\n", worker_lid, send_crd_count, total_crds_issued[worker_lid]);
		ret = ibv_post_send(cb->dgram_qp[CRD_UD_QP_ID], &send_crd_wr[0], &bad_send_wr);
		CPE(ret, "ibv_post_send error while sending CRDs", ret);
	}
}


/* ---------------------------------------------------------------------------
------------------------------------OTHERS------------------------------------
---------------------------------------------------------------------------*/
static inline uint32_t refill_ops(uint32_t* trace_iter, uint16_t worker_lid,
								  struct spacetime_trace_command *trace,
								  spacetime_ops_t *ops)
{
    int i = 0;
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (ops[i].opcode != ST_EMPTY) continue;
		ops[i].state = ST_NEW;
		if(ENABLE_ASSERTIONS)
			assert(trace[*trace_iter].opcode == ST_OP_PUT || trace[*trace_iter].opcode == ST_OP_GET);

		ops[i].opcode = trace[*trace_iter].opcode;
//		ops[i].opcode = worker_lid % 2 == 0 ? ST_OP_PUT : ST_OP_GET;
//		ops[i].opcode =  ST_OP_PUT;
		memcpy(&ops[i].key, &trace[*trace_iter].key_hash, sizeof(spacetime_key_t));

		if (ops[i].opcode == ST_OP_PUT)
			memset(ops[i].value, ((uint8_t) 'x' + machine_id), ST_VALUE_SIZE);
//			memset(ops[i].value, ((uint8_t) machine_id % 2 == 0 ? 'x' : 'y'), ST_VALUE_SIZE);
		ops[i].val_len = (uint8) (ops[i].opcode == ST_OP_PUT ? ST_VALUE_SIZE : 0);
		if(ENABLE_REQ_PRINTS &&  worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("W%d--> Op: %s, hash(1st 8B):%" PRIu64 "\n",
					   worker_lid, code_to_str(ops[i].opcode), ((uint64_t *) &ops[i].key)[1]);
		HRD_MOD_ADD(*trace_iter, TRACE_SIZE);
	}

}
#endif //HERMES_INLINE_UTIL_H
