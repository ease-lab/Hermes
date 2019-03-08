//
// Created by akatsarakis on 23/05/18.
//

#ifndef HERMES_INLINE_UTIL_H
#define HERMES_INLINE_UTIL_H

#include <infiniband/verbs.h>
#include <concur_ctrl.h>
#include "spacetime.h"
#include "config.h"
#include "util.h"


static inline void
poll_cq_for_credits(struct ibv_cq *credit_recv_cq, struct ibv_wc *credit_wc,
					struct ibv_recv_wr* credit_recv_wr, struct hrd_ctrl_blk* cb,
					uint8_t credits[][MACHINE_NUM], uint16_t worker_lid);

static inline uint8_t
node_is_in_membership(spacetime_group_membership last_group_membership, int node_id);

/* ---------------------------------------------------------------------------
------------------------------------GENERIC-----------------------------------
---------------------------------------------------------------------------*/

static inline void
post_receives(struct hrd_ctrl_blk *cb, uint16_t num_of_receives,
			  uint8_t buff_type, void *recv_buff, int *push_ptr,
			  struct ibv_recv_wr *recv_wr)
{
	int i, ret, qp_id, req_size, recv_q_depth, max_recv_wrs;
	struct ibv_recv_wr *bad_recv_wr;
	void* next_buff_addr;

	switch(buff_type){
		case ST_INV_BUFF:
			req_size = INV_RECV_REQ_SIZE;
			recv_q_depth = RECV_INV_Q_DEPTH;
			max_recv_wrs = MAX_RECV_INV_WRS;
			qp_id = INV_UD_QP_ID;
			break;
		case ST_ACK_BUFF:
			req_size = ACK_RECV_REQ_SIZE;
			recv_q_depth = RECV_ACK_Q_DEPTH;
			max_recv_wrs = MAX_RECV_ACK_WRS;
			qp_id = ACK_UD_QP_ID;
			break;
		case ST_VAL_BUFF:
			req_size = VAL_RECV_REQ_SIZE;
			recv_q_depth = RECV_VAL_Q_DEPTH;
			max_recv_wrs = MAX_RECV_VAL_WRS;
			qp_id = VAL_UD_QP_ID;
			break;
		default: assert(0);
	}
	if(ENABLE_ASSERTIONS)
		assert(num_of_receives <= max_recv_wrs);

	for(i = 0; i < num_of_receives; i++) {
		next_buff_addr = ((uint8_t*) recv_buff) + (*push_ptr * req_size);
		memset(next_buff_addr, 0, (size_t) req_size); //reset the buffer before posting the receive
		if(BATCH_POST_RECVS_TO_NIC)
			recv_wr[i].sg_list->addr = (uintptr_t) next_buff_addr;
		else
			hrd_post_dgram_recv(cb->dgram_qp[qp_id], next_buff_addr,
								req_size, cb->dgram_buf_mr->lkey);
		HRD_MOD_ADD(*push_ptr, recv_q_depth);
	}

	if(BATCH_POST_RECVS_TO_NIC) {
		recv_wr[num_of_receives - 1].next = NULL;
		if (ENABLE_ASSERTIONS) {
			for (i = 0; i < num_of_receives - 1; i++) {
				assert(recv_wr[i].num_sge == 1);
				assert(recv_wr[i].next == &recv_wr[i + 1]);
				assert(recv_wr[i].sg_list->length == req_size);
				assert(recv_wr[i].sg_list->lkey == cb->dgram_buf_mr->lkey);
			}
			assert(recv_wr[i].next == NULL);
		}
		ret = ibv_post_recv(cb->dgram_qp[qp_id], &recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs for credits before val bcast", ret);
		//recover next ptr of last wr to NULL
		recv_wr[num_of_receives - 1].next = (max_recv_wrs == num_of_receives - 1) ?
											NULL : &recv_wr[num_of_receives];
	}
}

static inline void
poll_buff_and_post_recvs(void *incoming_buff, uint8_t buff_type, int *buf_pull_ptr,
						 void *recv_ops, int *ops_push_ptr,
						 struct ibv_cq *completion_q, struct ibv_wc *work_completions,
						 struct hrd_ctrl_blk *cb, int *recv_push_ptr,
						 struct ibv_recv_wr *recv_wr,
						 spacetime_group_membership last_group_membership,
						 uint8_t credits[][MACHINE_NUM], uint16_t worker_lid)
{
	void* next_packet_reqs, *recv_op_ptr, *next_req;
	uint8_t *next_packet_req_num_ptr;
	int index = 0, recv_q_depth = 0, max_credits = 0, i = 0, j = 0,
			packets_polled = 0, reqs_polled = 0;
	uint8_t qp_credits_to_inc = 0, sender = 0;
	size_t req_size = 0;
    if(ENABLE_ASSERTIONS)
		assert(*ops_push_ptr == 0);

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
		default: assert(0);
	}

	//poll completion q
	packets_polled = ibv_poll_cq(completion_q, MAX_BATCH_OPS_SIZE - *ops_push_ptr, work_completions);

	for(i = 0; i < packets_polled; i++){
		index = (*buf_pull_ptr + 1) % recv_q_depth;
		switch (buff_type) {
			case ST_INV_BUFF:
				sender = ((ud_req_inv_t *) incoming_buff)[index].packet.reqs[0].op_meta.sender;
				next_packet_reqs = &((ud_req_inv_t *) incoming_buff)[index].packet.reqs;
				next_packet_req_num_ptr = &((ud_req_inv_t *) incoming_buff)[index].packet.req_num;
				if(ENABLE_ASSERTIONS){
				    if(!(*next_packet_req_num_ptr > 0 && *next_packet_req_num_ptr <= INV_MAX_REQ_COALESCE)) {
						printf("AA: %d, coalesce: %d\n", *next_packet_req_num_ptr, INV_MAX_REQ_COALESCE);
						printf("opcode: %d, state: %d\n",
							   (((spacetime_inv_t *) next_packet_reqs)[j].op_meta.opcode),
							   ((spacetime_inv_t *) next_packet_reqs)[j].op_meta.state);
						printf("opcode: %s\n",
							   code_to_str(((spacetime_inv_t *) next_packet_reqs)[j].op_meta.opcode));
						printf("opcode: %s, state: %s\n",
							   code_to_str(((spacetime_inv_t *) next_packet_reqs)[j].op_meta.opcode),
							   code_to_str(((spacetime_inv_t *) next_packet_reqs)[j].op_meta.state));
					}
					assert(*next_packet_req_num_ptr > 0 && *next_packet_req_num_ptr <= INV_MAX_REQ_COALESCE);
					for(j = 0; j < *next_packet_req_num_ptr; j++){
						assert(((spacetime_inv_t*) next_packet_reqs)[j].op_meta.ts.version % 2 == 0);
						assert(((spacetime_inv_t*) next_packet_reqs)[j].op_meta.opcode == ST_OP_INV ||
						       ((spacetime_inv_t*) next_packet_reqs)[j].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
						assert(((spacetime_inv_t*) next_packet_reqs)[j].op_meta.val_len == ST_VALUE_SIZE);
						assert(REMOTE_MACHINES != 1 ||
						       ((spacetime_inv_t*) next_packet_reqs)[j].op_meta.ts.tie_breaker_id ==
						       REMOTE_MACHINES - machine_id);
						assert(REMOTE_MACHINES != 1 ||
						       ((spacetime_inv_t*) next_packet_reqs)[j].op_meta.sender ==
						       REMOTE_MACHINES - machine_id);
					}
				}
				break;
			case ST_ACK_BUFF:
				sender = ((ud_req_ack_t *) incoming_buff)[index].packet.reqs[0].sender;
				next_packet_reqs = &((ud_req_ack_t *) incoming_buff)[index].packet.reqs;
				next_packet_req_num_ptr = &((ud_req_ack_t *) incoming_buff)[index].packet.req_num;
				if(ENABLE_ASSERTIONS){
					assert(*next_packet_req_num_ptr > 0 && *next_packet_req_num_ptr <= ACK_MAX_REQ_COALESCE);
					for(j = 0; j < *next_packet_req_num_ptr; j++){
						assert(((spacetime_ack_t*) next_packet_reqs)[j].ts.version % 2 == 0);
						assert(((spacetime_ack_t*) next_packet_reqs)[j].opcode == ST_OP_ACK ||
							   ((spacetime_ack_t*) next_packet_reqs)[j].opcode == ST_OP_MEMBERSHIP_CHANGE);
						assert(REMOTE_MACHINES != 1 || ((spacetime_ack_t*) next_packet_reqs)[j].sender == REMOTE_MACHINES - machine_id);
						assert(group_membership.num_of_alive_remotes != REMOTE_MACHINES ||
							   ENABLE_VIRTUAL_NODE_IDS ||
							   ((spacetime_ack_t*) next_packet_reqs)[j].ts.tie_breaker_id == machine_id);
					}
				}
				break;
			case ST_VAL_BUFF:
				sender = ((ud_req_val_t *) incoming_buff)[index].packet.reqs[0].sender;
				next_packet_reqs = &((ud_req_val_t *) incoming_buff)[index].packet.reqs;
				next_packet_req_num_ptr = &((ud_req_val_t *) incoming_buff)[index].packet.req_num;
				if(ENABLE_ASSERTIONS){
					assert(*next_packet_req_num_ptr > 0 && *next_packet_req_num_ptr <= VAL_MAX_REQ_COALESCE);
					for(j = 0; j < *next_packet_req_num_ptr; j++){
						assert(((spacetime_val_t*) next_packet_reqs)[j].ts.version % 2 == 0);
						assert(((spacetime_val_t*) next_packet_reqs)[j].opcode == ST_OP_VAL);
						assert(REMOTE_MACHINES != 1 || ((spacetime_val_t*) next_packet_reqs)[j].ts.tie_breaker_id == REMOTE_MACHINES - machine_id);
						assert(REMOTE_MACHINES != 1 || ((spacetime_val_t*) next_packet_reqs)[j].sender == REMOTE_MACHINES - machine_id);
					}
				}
				break;
			default:
				assert(0);
		}

        if(node_is_in_membership(last_group_membership, sender))
			for(j = 0; j < *next_packet_req_num_ptr; j++){
				recv_op_ptr = ((uint8_t *) recv_ops) + (*ops_push_ptr * req_size);
				next_req = &((uint8_t*) next_packet_reqs)[j * req_size];
				memcpy(recv_op_ptr, next_req, req_size);
				reqs_polled++;
				(*ops_push_ptr)++;
				credits[qp_credits_to_inc][sender]++; //increment packet credits
			}

		*next_packet_req_num_ptr = 0; //TODO can be removed since we already reset on posting receives
		HRD_MOD_ADD(*buf_pull_ptr, recv_q_depth);

		if(ENABLE_ASSERTIONS){
			assert(credits[qp_credits_to_inc][sender] <= max_credits);
			assert(reqs_polled == *ops_push_ptr);
		}
	}

	//Refill recvs
	if(packets_polled > 0)
		post_receives(cb, (uint16_t) packets_polled, buff_type, incoming_buff, recv_push_ptr, recv_wr);

	if (ENABLE_RECV_PRINTS || ENABLE_CREDIT_PRINTS || ENABLE_POST_RECV_PRINTS || ENABLE_STAT_COUNTING)
		if(packets_polled > 0){
			switch(buff_type){
				case ST_INV_BUFF:
					w_stats[worker_lid].received_invs_per_worker += reqs_polled;
					w_stats[worker_lid].received_packet_invs_per_worker += packets_polled;
					if(ENABLE_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: %d packets \033[31mINVs\033[0m %d, (total packets: %d, reqs %d)!\n",
									 worker_lid, packets_polled, reqs_polled,
									 w_stats[worker_lid].received_packet_invs_per_worker, w_stats[worker_lid].received_invs_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[33mACKs\033[0m \033[1m\033[32mincremented\033[0m "
									   "to %d (for machine %d)\n", worker_lid, credits[qp_credits_to_inc][sender], sender);
					if (ENABLE_POST_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						yellow_printf("vvv Post Receives[W%d]: \033[31mINVs\033[0m %d\n", worker_lid, packets_polled);
					if(ENABLE_ASSERTIONS)
						assert(INV_MAX_REQ_COALESCE != 1 || packets_polled == reqs_polled);
					break;
				case ST_ACK_BUFF:
					w_stats[worker_lid].received_acks_per_worker += reqs_polled;
					w_stats[worker_lid].received_packet_acks_per_worker += packets_polled;
					if(ENABLE_RECV_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: %d packets \033[33mACKs\033[0m %d, (total packets: %d, reqs: %d)!\n",
									 worker_lid, packets_polled, reqs_polled,
									 w_stats[worker_lid].received_packet_acks_per_worker, w_stats[worker_lid].received_acks_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[31mINVs\033[0m \033[1m\033[32mincremented\033[0m "
									   "to %d (for machine %d)\n", worker_lid, credits[qp_credits_to_inc][sender], sender);
					if (ENABLE_POST_RECV_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						yellow_printf("vvv Post Receives[W%d]: \033[33mACKs\033[0m %d\n", worker_lid, packets_polled);
					if(ENABLE_ASSERTIONS)
						assert(ACK_MAX_REQ_COALESCE != 1 || packets_polled == reqs_polled);
					break;
				case ST_VAL_BUFF:
					w_stats[worker_lid].received_vals_per_worker += reqs_polled;
					w_stats[worker_lid].received_packet_vals_per_worker += packets_polled;
					if(ENABLE_RECV_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: %d packets \033[1m\033[32mVALs\033[0m %d, (total packets: %d reqs: %d)!\n",
									 worker_lid, packets_polled, reqs_polled,
									 w_stats[worker_lid].received_packet_vals_per_worker, w_stats[worker_lid].received_vals_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[1m\033[36mCRDs\033[0m \033[1m\033[32mincremented\033[0m"
									   " to %d (for machine %d)\n", worker_lid, credits[qp_credits_to_inc][sender], sender);
					if(ENABLE_POST_RECV_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						yellow_printf("vvv Post Receives[W%d]: \033[1m\033[32mVALs\033[0m %d\n", worker_lid, packets_polled);
					if(ENABLE_ASSERTIONS)
						assert(VAL_MAX_REQ_COALESCE != 1 || packets_polled == reqs_polled);
					break;
				default: assert(0);
			}
		}
}



/* ---------------------------------------------------------------------------
------------------------------------INVS--------------------------------------
---------------------------------------------------------------------------*/
static inline void
forge_bcast_inv_wrs(spacetime_op_t* op, spacetime_inv_packet_t* inv_send_op,
					struct ibv_send_wr* send_inv_wr, struct ibv_sge* send_inv_sgl,
					uint8_t num_of_alive_remotes,
					struct hrd_ctrl_blk* cb, long long* total_inv_bcasts,
					uint16_t br_i, uint16_t worker_lid, uint8_t opcode)
{
	struct ibv_wc signal_send_wc;

	if(ENABLE_ASSERTIONS)
		assert(sizeof(spacetime_inv_t) == sizeof(spacetime_op_t));

	memcpy(&inv_send_op->reqs[inv_send_op->req_num], op, sizeof(spacetime_inv_t));
	inv_send_op->reqs[inv_send_op->req_num].op_meta.opcode = opcode;
	inv_send_op->reqs[inv_send_op->req_num].op_meta.sender = (uint8_t) machine_id;
	inv_send_op->req_num++;


	if(opcode == ST_OP_MEMBERSHIP_CHANGE)
		cyan_printf("FORGING MEMBERSHIP CHANGE for node: %d\n",inv_send_op->reqs[inv_send_op->req_num-1].value[0]);


	send_inv_sgl[br_i].length = (sizeof(spacetime_inv_packet_t) - (INV_MAX_REQ_COALESCE - inv_send_op->req_num));

	w_stats[worker_lid].issued_invs_per_worker++;


	if(inv_send_op->req_num == 1) {
		send_inv_sgl[br_i].addr = (uint64_t) inv_send_op;
		int br_i_x_remotes = br_i * num_of_alive_remotes;

		if(DISABLE_INV_INLINING)
			send_inv_wr[br_i_x_remotes].send_flags = 0;
		else
			send_inv_wr[br_i_x_remotes].send_flags = IBV_SEND_INLINE; //reset possibly signaled wr from the prev round


		// SET THE PREV SEND_WR TO POINT TO CURR
		if (br_i > 0)
			send_inv_wr[br_i_x_remotes - 1].next = &send_inv_wr[br_i_x_remotes];


		// Do a Signaled Send every INV_SS_GRANULARITY broadcasts (INV_SS_GRANULARITY * REMOTE_MACHINES messages)
		if (*total_inv_bcasts % INV_SS_GRANULARITY == 0) {

		    //if not the first SS poll the previous SS completion
			if(*total_inv_bcasts > 0) {
				hrd_poll_cq(cb->dgram_send_cq[INV_UD_QP_ID], 1, &signal_send_wc);

				w_stats[worker_lid].inv_ss_completions_per_worker++;
				if (ENABLE_SS_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					red_printf( "^^^ Polled SS completion[W%d]: \033[31mINV\033[0m %d "
										"(total ss comp: %d --> reqs comp: %d, curr_invs: %d)\n",
								worker_lid, 1, w_stats[worker_lid].inv_ss_completions_per_worker,
								(*total_inv_bcasts - INV_SS_GRANULARITY) * REMOTE_MACHINES + 1,
								*total_inv_bcasts * REMOTE_MACHINES);
			}

			if (ENABLE_SS_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("vvv Send SS[W%d]: \033[31mINV\033[0m \n", worker_lid);
			send_inv_wr[br_i_x_remotes].send_flags |= IBV_SEND_SIGNALED;
		}
		(*total_inv_bcasts)++;
	}
}

static inline void
batch_invs_2_NIC(struct ibv_send_wr *send_inv_wr, struct ibv_sge *send_inv_sgl,
				 uint8_t num_of_alive_remotes,
				 struct hrd_ctrl_blk *cb, uint16_t br_i,
				 uint16_t total_invs_in_batch, uint16_t worker_lid)
{
	int j = 0, ret, k = 0;
	struct ibv_send_wr *bad_send_wr;

	w_stats[worker_lid].issued_packet_invs_per_worker += br_i;
	send_inv_wr[br_i * num_of_alive_remotes - 1].next = NULL;
	if (ENABLE_ASSERTIONS) {
		int sgl_index = 0;
		for (j = 0; j < br_i * num_of_alive_remotes - 1; j++) {
			sgl_index = j / num_of_alive_remotes;
			assert(send_inv_wr[j].num_sge == 1);
			assert(send_inv_wr[j].next == &send_inv_wr[j + 1]);
			assert(DISABLE_INV_INLINING || send_inv_wr[j].send_flags == IBV_SEND_INLINE ||
				   send_inv_wr[j].send_flags == (IBV_SEND_INLINE | IBV_SEND_SIGNALED));
			assert(send_inv_wr[j].sg_list == &send_inv_sgl[sgl_index]);
			assert(((spacetime_inv_packet_t *) send_inv_sgl[sgl_index].addr)->req_num > 0);
			for(k = 0; k < ((spacetime_inv_packet_t *) send_inv_sgl[sgl_index].addr)->req_num; k ++){
				assert(((spacetime_inv_packet_t *) send_inv_sgl[sgl_index].addr)->reqs[k].op_meta.opcode == ST_OP_INV ||
				       ((spacetime_inv_packet_t *) send_inv_sgl[sgl_index].addr)->reqs[k].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
				assert(((spacetime_inv_packet_t *) send_inv_sgl[sgl_index].addr)->reqs[k].op_meta.sender == machine_id);
				assert(num_of_alive_remotes != REMOTE_MACHINES ||
					   ((spacetime_inv_packet_t *) send_inv_sgl[sgl_index].addr)->reqs[k].op_meta.ts.tie_breaker_id == machine_id ||
					   (ENABLE_VIRTUAL_NODE_IDS && ((spacetime_inv_packet_t *) send_inv_sgl[sgl_index].addr)
														   ->reqs[k].op_meta.ts.tie_breaker_id % MACHINE_NUM == machine_id));
			}
//			green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//						 j, ((uint64_t *) &((spacetime_inv_packet_t*) send_inv_sgl[sgl_index].addr)->reqs[0].key)[1]);
		}
//        green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//                     j, ((uint64_t *) &((spacetime_inv_packet_t*) send_inv_sgl[sgl_index].addr)->reqs[0].key)[1]);
		assert(send_inv_wr[j].next == NULL);
	}

	if (ENABLE_SEND_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf(">>> Send[W%d]: %d bcast %d packets \033[31mINVs\033[0m (%d) (Total bcasts: %d, packets %d, invs: %d)\n",
					worker_lid, total_invs_in_batch, br_i, total_invs_in_batch * REMOTE_MACHINES,
					w_stats[worker_lid].issued_invs_per_worker, w_stats[worker_lid].issued_packet_invs_per_worker,
					w_stats[worker_lid].issued_invs_per_worker * REMOTE_MACHINES);
	ret = ibv_post_send(cb->dgram_qp[INV_UD_QP_ID], &send_inv_wr[0], &bad_send_wr);
	CPE(ret, "INVs ibv_post_send error", ret);
}

static inline void
broadcast_invs(spacetime_op_t *ops, spacetime_inv_packet_t *inv_send_packet_ops,
			   int *inv_push_ptr, struct ibv_send_wr *send_inv_wr,
			   struct ibv_sge *send_inv_sgl, uint8_t credits[][MACHINE_NUM],
			   struct hrd_ctrl_blk *cb, long long *total_inv_bcasts,
			   uint16_t worker_lid, spacetime_group_membership last_g_membership,
			   uint32_t* credits_missing, uint16_t *rolling_index)
{
	uint8_t missing_credits = 0;
	uint16_t i = 0, br_i = 0, j = 0, total_invs_in_batch = 0, index = 0;

	if(ENABLE_ASSERTIONS)
		assert(inv_send_packet_ops[*inv_push_ptr].req_num == 0);

	// traverse all of the ops to find invs
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		index = (uint16_t) ((i + *rolling_index) % MAX_BATCH_OPS_SIZE);
		if (ops[index].op_meta.state != ST_PUT_SUCCESS &&
		    ops[index].op_meta.state != ST_REPLAY_SUCCESS &&
			ops[index].op_meta.state != ST_OP_MEMBERSHIP_CHANGE)
			continue;

		//Check for credits
		for (j = 0; j < MACHINE_NUM; j++) {
			if (j == machine_id) continue; // skip the local machine
            if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
			if (credits[INV_UD_QP_ID][j] == 0) {
				missing_credits = 1;
				*rolling_index = index;
				credits_missing[j]++;
//                if(unlikely(credits_missing[j] > M_1))
//					*node_missing_credits = j;
				break;
			}else
				credits_missing[j] = 0;
		}

		// if not enough credits for a Broadcast
		if (missing_credits == 1) break;

		for (j = 0; j < MACHINE_NUM; j++){
			if (j == machine_id) continue; // skip the local machine
			if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
			credits[INV_UD_QP_ID][j]--;
            if (ENABLE_CREDIT_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[31mINVs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					   worker_lid, credits[INV_UD_QP_ID][j], j);
		}

        // Create the broadcast messages
		forge_bcast_inv_wrs(&ops[index], &inv_send_packet_ops[*inv_push_ptr],
							send_inv_wr, send_inv_sgl, last_g_membership.num_of_alive_remotes,
							cb, total_inv_bcasts, br_i, worker_lid,
							(uint8_t) (ops[index].op_meta.state == ST_OP_MEMBERSHIP_CHANGE ?
									   ST_OP_MEMBERSHIP_CHANGE : ST_OP_INV));

		//Change state of op
		if(ops[index].op_meta.state == ST_PUT_SUCCESS)
			ops[index].op_meta.state = ST_IN_PROGRESS_PUT;
		else if(ops[index].op_meta.state == ST_REPLAY_SUCCESS)
			ops[index].op_meta.state = ST_IN_PROGRESS_REPLAY;
		else if(ops[index].op_meta.state == ST_OP_MEMBERSHIP_CHANGE)
			ops[index].op_meta.state = ST_OP_MEMBERSHIP_COMPLETE;
		else{
		    printf("state: %d\n", ops[index].op_meta.state);
			printf("state: %s\n", code_to_str(ops[index].op_meta.state));
		  assert(0);
		}

		total_invs_in_batch++;

		// if packet is full
		if(inv_send_packet_ops[*inv_push_ptr].req_num == INV_MAX_REQ_COALESCE) {
			br_i++;

			// if last batch to the NIC
			if (br_i == MAX_PCIE_BCAST_BATCH) { //check if we should batch it to NIC
				batch_invs_2_NIC(send_inv_wr, send_inv_sgl, last_g_membership.num_of_alive_remotes,
								 cb, br_i, total_invs_in_batch, worker_lid);
				br_i = 0;
				total_invs_in_batch = 0;
			}

			HRD_MOD_ADD(*inv_push_ptr, INV_SEND_OPS_SIZE / REMOTE_MACHINES *
					     last_g_membership.num_of_alive_remotes); //got to the next "packet" + dealing with failutes

///			// Reset data left from previous bcasts after ibv_post_send to avoid sending resetted data
			inv_send_packet_ops[*inv_push_ptr].req_num = 0;
			for(j = 0; j < INV_MAX_REQ_COALESCE; j++)
				inv_send_packet_ops[*inv_push_ptr].reqs[j].op_meta.opcode = ST_EMPTY;
		}
	}


	if(inv_send_packet_ops[*inv_push_ptr].req_num > 0 &&
	   inv_send_packet_ops[*inv_push_ptr].req_num < INV_MAX_REQ_COALESCE)
		br_i++;

	if(br_i > 0)
		batch_invs_2_NIC(send_inv_wr, send_inv_sgl, last_g_membership.num_of_alive_remotes,
						 cb, br_i, total_invs_in_batch, worker_lid);

	//Move to next packet and reset data left from previous bcasts
	if(inv_send_packet_ops[*inv_push_ptr].req_num > 0 &&
	   inv_send_packet_ops[*inv_push_ptr].req_num < INV_MAX_REQ_COALESCE)
	{
		HRD_MOD_ADD(*inv_push_ptr, INV_SEND_OPS_SIZE / REMOTE_MACHINES *
				     last_g_membership.num_of_alive_remotes); //got to the next "packet" + dealing with failutes

		 ///
		inv_send_packet_ops[*inv_push_ptr].req_num = 0;
		for(j = 0; j < INV_MAX_REQ_COALESCE; j++)
			inv_send_packet_ops[*inv_push_ptr].reqs[j].op_meta.opcode = ST_NEW;
	}

	if(ENABLE_ASSERTIONS)
		for(i = 0; i < MAX_BATCH_OPS_SIZE; i++)
			assert(ops[i].op_meta.opcode == ST_OP_GET              ||
				   ops[i].op_meta.state == ST_MISS                 ||
				   ops[i].op_meta.state == ST_PUT_STALL            ||
				   ops[i].op_meta.state == ST_PUT_SUCCESS          ||
				   ops[i].op_meta.state == ST_IN_PROGRESS_PUT      ||
				   ops[i].op_meta.state == ST_REPLAY_COMPLETE      ||
				   ops[i].op_meta.state == ST_IN_PROGRESS_REPLAY   ||
				   ops[i].op_meta.state == ST_OP_MEMBERSHIP_COMPLETE  ||
				   ops[i].op_meta.state == ST_OP_MEMBERSHIP_CHANGE ||
				   ops[i].op_meta.state == ST_PUT_COMPLETE_SEND_VALS);

}

/* ---------------------------------------------------------------------------
------------------------------------ACKS--------------------------------------
---------------------------------------------------------------------------*/

static inline void
forge_ack_wr(spacetime_inv_t* inv_recv_op, spacetime_ack_packet_t* ack_send_op,
			 struct ibv_send_wr* send_ack_wr, struct ibv_sge* send_ack_sgl,
			 struct hrd_ctrl_blk* cb, long long* send_ack_tx,
			 uint16_t send_ack_packets, uint16_t worker_lid, uint8_t opcode)
{
	struct ibv_wc signal_send_wc;
	uint16_t dst_worker_gid = (uint16_t) (inv_recv_op->op_meta.sender * WORKERS_PER_MACHINE + worker_lid);
	if(ENABLE_ASSERTIONS)
		assert(REMOTE_MACHINES != 1 || inv_recv_op->op_meta.sender == REMOTE_MACHINES - machine_id);

	memcpy(&ack_send_op->reqs[ack_send_op->req_num], inv_recv_op, sizeof(spacetime_ack_t));
	ack_send_op->reqs[ack_send_op->req_num].opcode = opcode;
	ack_send_op->reqs[ack_send_op->req_num].sender = (uint8_t) machine_id;
	ack_send_op->req_num++;

	if(ENABLE_ASSERTIONS)
		assert(ack_send_op->req_num <= ACK_MAX_REQ_COALESCE);

	send_ack_sgl[send_ack_packets].length = (sizeof(spacetime_ack_packet_t) - (ACK_MAX_REQ_COALESCE - ack_send_op->req_num));

	w_stats[worker_lid].issued_acks_per_worker++;

	if(ack_send_op->req_num == 1) {
		send_ack_sgl[send_ack_packets].addr = (uint64_t) ack_send_op;
		send_ack_wr[send_ack_packets].wr.ud.ah = remote_worker_qps[dst_worker_gid][ACK_UD_QP_ID].ah;
		send_ack_wr[send_ack_packets].wr.ud.remote_qpn = (uint32) remote_worker_qps[dst_worker_gid][ACK_UD_QP_ID].qpn;
		if(DISABLE_ACK_INLINING)
			send_ack_wr[send_ack_packets].send_flags = 0;
		else
			send_ack_wr[send_ack_packets].send_flags = IBV_SEND_INLINE;

		if (send_ack_packets > 0)
			send_ack_wr[send_ack_packets - 1].next = &send_ack_wr[send_ack_packets];

		// Do a Signaled Send every ACK_SS_GRANULARITY msgs
		if (*send_ack_tx % ACK_SS_GRANULARITY == 0) {
			if(*send_ack_tx > 0){ //if not the first SS poll the previous SS completion
				hrd_poll_cq(cb->dgram_send_cq[ACK_UD_QP_ID], 1, &signal_send_wc);
				w_stats[worker_lid].ack_ss_completions_per_worker++;
				if (ENABLE_SS_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					red_printf("^^^ Polled SS completion[W%d]: \033[33mACK\033[0m %d (total %d)\n", worker_lid, 1,
							   w_stats[worker_lid].ack_ss_completions_per_worker);
			}
			send_ack_wr[send_ack_packets].send_flags |= IBV_SEND_SIGNALED;
			if (ENABLE_SS_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("vvv Send SS[W%d]: \033[33mACK\033[0m\n", worker_lid);
		}
		(*send_ack_tx)++;
	}
}

static inline void
batch_acks_2_NIC(struct ibv_send_wr *send_ack_wr, struct ibv_sge *send_ack_sgl,
				 struct hrd_ctrl_blk *cb, uint16_t send_ack_packets,
				 uint16_t total_acks_in_batch, uint16_t worker_lid)
{
	int j = 0, ret;
	struct ibv_send_wr *bad_send_wr;

	w_stats[worker_lid].issued_packet_acks_per_worker += send_ack_packets;
	send_ack_wr[send_ack_packets - 1].next = NULL;
	if(ENABLE_ASSERTIONS){
		for(j = 0; j < send_ack_packets - 1; j++){
			assert(send_ack_wr[j].next == &send_ack_wr[j+1]);
			assert(send_ack_wr[j].opcode == IBV_WR_SEND);
			assert(send_ack_wr[j].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(send_ack_wr[j].sg_list == &send_ack_sgl[j]);
			assert(send_ack_wr[j].num_sge == 1);
			assert(DISABLE_ACK_INLINING || send_ack_wr[j].send_flags == IBV_SEND_INLINE ||
				   send_ack_wr[j].send_flags == (IBV_SEND_INLINE | IBV_SEND_SIGNALED));
			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->req_num > 0);
			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->reqs[0].opcode == ST_OP_ACK);
			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->reqs[0].sender == machine_id);
		}
		assert(send_ack_wr[j].next == NULL);
	}

	if (ENABLE_SEND_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf(">>> Send[W%d]: %d packets \033[33mACKs\033[0m %d (Total packets: %d, acks: %d)\n",
					worker_lid, send_ack_packets, total_acks_in_batch,
					w_stats[worker_lid].issued_packet_acks_per_worker,
					w_stats[worker_lid].issued_acks_per_worker);
	ret = ibv_post_send(cb->dgram_qp[ACK_UD_QP_ID], &send_ack_wr[0], &bad_send_wr);
	CPE(ret, "ibv_post_send error while sending ACKs", ret);
}

static inline void
issue_acks(spacetime_inv_t *inv_recv_ops, spacetime_ack_packet_t* ack_send_packet_ops,
		   int* ack_push_ptr, long long int* send_ack_tx, struct ibv_send_wr *send_ack_wr,
		   struct ibv_sge *send_ack_sgl, uint8_t credits[][MACHINE_NUM],
		   struct hrd_ctrl_blk *cb,  uint16_t worker_lid)
{
	uint16_t i = 0, total_acks_in_batch = 0, j = 0, send_ack_packets = 0;
	uint8_t last_ack_dst = 255;

	if(ENABLE_ASSERTIONS)
		assert(ack_send_packet_ops[*ack_push_ptr].req_num == 0);

	for (i = 0; i < INV_RECV_OPS_SIZE; i++) {
		if (inv_recv_ops[i].op_meta.opcode == ST_EMPTY)
			break;

		if(ENABLE_ASSERTIONS){
			assert(inv_recv_ops[i].op_meta.opcode == ST_INV_SUCCESS || inv_recv_ops[i].op_meta.opcode == ST_OP_INV ||
				   inv_recv_ops[i].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
			assert(inv_recv_ops[i].op_meta.val_len == ST_VALUE_SIZE);
			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].op_meta.ts.tie_breaker_id == REMOTE_MACHINES - machine_id);
//			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].value[0] == (uint8_t) 'x' + (REMOTE_MACHINES - machine_id));
			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].op_meta.sender == REMOTE_MACHINES - machine_id);
		}

		if (credits[ACK_UD_QP_ID][inv_recv_ops[i].op_meta.sender] == 0)
			assert(0); // we should always have credits for acks

		//reduce credits
		credits[ACK_UD_QP_ID][inv_recv_ops[i].op_meta.sender]--;
		if (ENABLE_CREDIT_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			printf("$$$ Credits[W%d]: \033[33mACKs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
				   worker_lid, credits[ACK_UD_QP_ID][inv_recv_ops[i].op_meta.sender], inv_recv_ops[i].op_meta.sender);

		if(ack_send_packet_ops[*ack_push_ptr].req_num != 0 && inv_recv_ops[i].op_meta.sender != last_ack_dst){ //in case that the last ack dst != this ack dst
			send_ack_packets++;
			if (send_ack_packets == MAX_SEND_ACK_WRS) {
				batch_acks_2_NIC(send_ack_wr, send_ack_sgl, cb, send_ack_packets,
								 total_acks_in_batch, worker_lid);
				send_ack_packets = 0;
				total_acks_in_batch = 0;
			}
			HRD_MOD_ADD(*ack_push_ptr, ACK_SEND_OPS_SIZE); //got to the next "packet"
			//Reset data left from previous unicasts
			ack_send_packet_ops[*ack_push_ptr].req_num = 0;
			for(j = 0; j < ACK_MAX_REQ_COALESCE; j++)
				ack_send_packet_ops[*ack_push_ptr].reqs[j].opcode = ST_EMPTY;
		}
		last_ack_dst = inv_recv_ops[i].op_meta.sender;

		// Create the broadcast messages
		forge_ack_wr(&inv_recv_ops[i], &ack_send_packet_ops[*ack_push_ptr], send_ack_wr,
					 send_ack_sgl, cb, send_ack_tx, send_ack_packets, worker_lid,
					 (uint8_t) (inv_recv_ops[i].op_meta.opcode == ST_INV_SUCCESS ? ST_OP_ACK : ST_OP_MEMBERSHIP_CHANGE));

		//empty inv buffer
		if(inv_recv_ops[i].op_meta.opcode == ST_INV_SUCCESS ||
		   inv_recv_ops[i].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE)
			inv_recv_ops[i].op_meta.opcode = ST_EMPTY;
		else assert(0);


		total_acks_in_batch++;

		if(ack_send_packet_ops[*ack_push_ptr].req_num == ACK_MAX_REQ_COALESCE) {
			send_ack_packets++;
			if (send_ack_packets == MAX_SEND_ACK_WRS) {
				batch_acks_2_NIC(send_ack_wr, send_ack_sgl, cb, send_ack_packets,
								 total_acks_in_batch, worker_lid);
				send_ack_packets = 0;
				total_acks_in_batch = 0;
			}
			HRD_MOD_ADD(*ack_push_ptr, ACK_SEND_OPS_SIZE);
			//Reset data left from previous unicasts
			ack_send_packet_ops[*ack_push_ptr].req_num = 0;
			for(j = 0; j < ACK_MAX_REQ_COALESCE; j++)
				ack_send_packet_ops[*ack_push_ptr].reqs[j].opcode = ST_EMPTY;

		}
	}

	if(ack_send_packet_ops[*ack_push_ptr].req_num > 0 &&
	   ack_send_packet_ops[*ack_push_ptr].req_num < ACK_MAX_REQ_COALESCE)
		send_ack_packets++;

	if (send_ack_packets > 0)
		batch_acks_2_NIC(send_ack_wr, send_ack_sgl, cb, send_ack_packets,
						 total_acks_in_batch, worker_lid);

	//Move to next packet and reset data left from previous unicasts
	if(ack_send_packet_ops[*ack_push_ptr].req_num > 0 &&
	   ack_send_packet_ops[*ack_push_ptr].req_num < ACK_MAX_REQ_COALESCE) {
		HRD_MOD_ADD(*ack_push_ptr, ACK_SEND_OPS_SIZE);
		ack_send_packet_ops[*ack_push_ptr].req_num = 0;
		for(j = 0; j < ACK_MAX_REQ_COALESCE; j++)
			ack_send_packet_ops[*ack_push_ptr].reqs[j].opcode = ST_EMPTY;
	}

	if(ENABLE_ASSERTIONS)
		for(i = 0; i < INV_RECV_OPS_SIZE; i++)
			assert(inv_recv_ops[i].op_meta.opcode == ST_EMPTY);
}


/* ---------------------------------------------------------------------------
------------------------------------VALs--------------------------------------
---------------------------------------------------------------------------*/

static inline void
post_credit_recvs(struct hrd_ctrl_blk *cb, struct ibv_recv_wr *credit_recv_wr,
				  uint16_t num_recvs)
{
	uint16_t i;
	int ret;
	struct ibv_recv_wr *bad_recv_wr;
	for (i = 0; i < num_recvs; i++)
		credit_recv_wr[i].next = (i == num_recvs - 1) ? NULL : &credit_recv_wr[i + 1];
	ret = ibv_post_recv(cb->dgram_qp[CRD_UD_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
	CPE(ret, "ibv_post_recv error: posting recvs for credits before val bcast", ret);
}

// Poll and increment credits
static inline void
poll_cq_for_credits(struct ibv_cq *credit_recv_cq, struct ibv_wc *credit_wc,
					struct ibv_recv_wr* credit_recv_wr, struct hrd_ctrl_blk* cb,
					uint8_t credits[][MACHINE_NUM], uint16_t worker_lid)
{
	spacetime_crd_t* crd_ptr;
	int i = 0, credits_found = 0;
	credits_found = ibv_poll_cq(credit_recv_cq, MAX_RECV_CRD_WRS, credit_wc);
	if(credits_found > 0) {
		if(unlikely(credit_wc[credits_found -1].status != 0)) {
			fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
			exit(0);
		}

		w_stats[worker_lid].received_packet_crds_per_worker += credits_found;
		if(ENABLE_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			green_printf("^^^ Polled reqs[W%d]: \033[1m\033[36mCRDs\033[0m %d, (total: %d)!\n",worker_lid, credits_found, w_stats[worker_lid].received_crds_per_worker);
		for (i = 0; i < credits_found; i++){
			crd_ptr = (spacetime_crd_t*) &credit_wc[i].imm_data;
			if(ENABLE_ASSERTIONS){
				assert(crd_ptr->opcode == ST_OP_CRD);
				assert(REMOTE_MACHINES != 1 || crd_ptr->sender == REMOTE_MACHINES - machine_id);
			}
			w_stats[worker_lid].received_crds_per_worker += crd_ptr->val_credits;
			credits[VAL_UD_QP_ID][crd_ptr->sender] += crd_ptr->val_credits;
			if(ENABLE_ASSERTIONS)
				assert(credits[VAL_UD_QP_ID][crd_ptr->sender] <= VAL_CREDITS);
			if(ENABLE_CREDIT_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[1m\033[32mVALs\033[0m \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",
					   worker_lid, credits[VAL_UD_QP_ID][crd_ptr->sender], crd_ptr->sender);
		}
		if (ENABLE_POST_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			yellow_printf("vvv Post Receives[W%d]: \033[1m\033[36mCRDs\033[0m %d\n", worker_lid, credits_found);
		post_credit_recvs(cb, credit_recv_wr, (uint16_t) credits_found);
	} else if(unlikely(credits_found < 0)) {
		printf("ERROR In the credit CQ\n");
		exit(0);
	}
}

static inline bool
check_val_credits(uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
				  struct ibv_wc *credit_wc, struct ibv_recv_wr* credit_recv_wr,
                  spacetime_group_membership last_g_membership,
				  uint16_t worker_lid)
{
	uint16_t poll_for_credits = 0, j;
	for (j = 0; j < MACHINE_NUM; j++) {
		if (j == machine_id) continue; // skip the local machine
		if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
		if (credits[VAL_UD_QP_ID][j] == 0) {
			poll_for_credits = 1;
			break;
		}
	}

	if (poll_for_credits == 1) {
		poll_cq_for_credits(cb->dgram_recv_cq[CRD_UD_QP_ID], credit_wc, credit_recv_wr, cb, credits, worker_lid);
		// We polled for credits, if we did not find enough just break
		for (j = 0; j < MACHINE_NUM; j++) {
			if (j == machine_id) continue; // skip the local machine
			if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
			if (credits[VAL_UD_QP_ID][j] == 0)
				return false;
		}
	}

	return poll_for_credits == 1 ? false : true;
}

static inline void
forge_bcast_val_wrs(void* op, spacetime_val_packet_t* val_packet_send_op,
					struct ibv_send_wr* send_val_wr, struct ibv_sge* send_val_sgl,
					uint8_t num_of_alive_remotes, struct hrd_ctrl_blk* cb,
					long long* total_val_bcasts, uint16_t br_i, uint16_t worker_lid)
{
	struct ibv_wc signal_send_wc;
	if(ENABLE_ASSERTIONS)
		assert(sizeof(spacetime_ack_t) == sizeof(spacetime_val_t));

	//WARNING ack_op is used to forge from both spacetime_ack_t and spacetime_op_t (when failures occur)--> do not change those structs
	memcpy(&val_packet_send_op->reqs[val_packet_send_op->req_num], op, sizeof(spacetime_val_t));
	val_packet_send_op->reqs[val_packet_send_op->req_num].opcode = ST_OP_VAL;
	val_packet_send_op->reqs[val_packet_send_op->req_num].sender = (uint8_t) machine_id;
	val_packet_send_op->req_num++;

	send_val_sgl[br_i].length = (sizeof(spacetime_val_packet_t) - (VAL_MAX_REQ_COALESCE - val_packet_send_op->req_num));

	w_stats[worker_lid].issued_vals_per_worker++;

	if(val_packet_send_op->req_num == 1) {
		send_val_sgl[br_i].addr = (uint64_t) (uintptr_t) val_packet_send_op;
		int br_i_x_remotes = br_i * num_of_alive_remotes;
		if(DISABLE_VAL_INLINING)
			send_val_wr[br_i_x_remotes].send_flags = 0;
		else
			send_val_wr[br_i_x_remotes].send_flags = IBV_SEND_INLINE;

		// SET THE LAST SEND_WR TO POINT TO NULL
		if (br_i > 0)
			send_val_wr[br_i_x_remotes - 1].next = &send_val_wr[br_i_x_remotes];
		// Do a Signaled Send every VAL_SS_GRANULARITY broadcasts (VAL_SS_GRANULARITY * (MACHINE_NUM - 1) messages)
		if (*total_val_bcasts % VAL_SS_GRANULARITY == 0) {
			if(*total_val_bcasts > 0){
				hrd_poll_cq(cb->dgram_send_cq[VAL_UD_QP_ID], 1, &signal_send_wc);
				w_stats[worker_lid].val_ss_completions_per_worker++;
				if (ENABLE_SS_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					red_printf("^^^ Polled SS completion[W%d]: \033[1m\033[32mVAL\033[0m %d (total %d)"
									   "(total ss comp: %d --> reqs comp: %d, curr_val: %d)\n",
							   worker_lid, 1, w_stats[worker_lid].val_ss_completions_per_worker,
							   (*total_val_bcasts - VAL_SS_GRANULARITY) * REMOTE_MACHINES + 1,
							   *total_val_bcasts * REMOTE_MACHINES);
			}
			if (ENABLE_SS_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("vvv Send SS[W%d]: \033[1m\033[32mVAL\033[0m \n", worker_lid);
			send_val_wr[br_i_x_remotes].send_flags |= IBV_SEND_SIGNALED;
		}
		(*total_val_bcasts)++;
	}
}


static inline void
batch_vals_2_NIC(struct ibv_send_wr *send_val_wr, struct ibv_sge *send_val_sgl,
                 uint8_t num_of_alive_remotes,
				 struct hrd_ctrl_blk *cb, uint16_t br_i,
				 uint16_t total_vals_in_batch, uint16_t worker_lid)
{
	int j = 0, ret;
	struct ibv_send_wr *bad_send_wr;

	w_stats[worker_lid].issued_packet_vals_per_worker += br_i;
	send_val_wr[br_i * num_of_alive_remotes - 1].next = NULL;
	if (ENABLE_ASSERTIONS) {
		int sgl_index;
		for (j = 0; j < br_i * num_of_alive_remotes - 1; j++) {
			sgl_index = j / num_of_alive_remotes;
			assert(send_val_wr[j].next == &send_val_wr[j + 1]);
			assert(send_val_wr[j].num_sge == 1);
			assert(DISABLE_VAL_INLINING || send_val_wr[j].send_flags == IBV_SEND_INLINE ||
				   send_val_wr[j].send_flags == (IBV_SEND_INLINE | IBV_SEND_SIGNALED));
			assert(send_val_wr[j].sg_list == &send_val_sgl[sgl_index]);
		}
		assert(send_val_wr[j].next == NULL);
	}

	if (ENABLE_SEND_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf( ">>> Send[W%d]: %d bcast %d packets \033[1m\033[32mVALs\033[0m"
							 " (%d) (Total bcasts: %d, packets: %d, vals: %d)\n",
					 worker_lid, total_vals_in_batch, br_i, total_vals_in_batch * REMOTE_MACHINES,
					 w_stats[worker_lid].issued_vals_per_worker,
					 w_stats[worker_lid].issued_packet_vals_per_worker,
					 w_stats[worker_lid].issued_vals_per_worker * REMOTE_MACHINES);
	ret = ibv_post_send(cb->dgram_qp[VAL_UD_QP_ID], &send_val_wr[0], &bad_send_wr);
	CPE(ret, "Broadcast VALs ibv_post_send error", ret);
}

static inline uint8_t
broadcast_vals(spacetime_ack_t *ack_ops, spacetime_val_packet_t *val_send_packet_ops, int *val_push_ptr,
			   struct ibv_send_wr *send_val_wr, struct ibv_sge *send_val_sgl,
			   uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
			   struct ibv_wc *credit_wc, spacetime_group_membership last_g_membership,
			   long long *br_tx, struct ibv_recv_wr *credit_recv_wr, uint16_t worker_lid)
{
	uint16_t i = 0, br_i = 0, j, total_vals_in_batch = 0;
	uint8_t has_outstanding_vals = 0;

	if(ENABLE_ASSERTIONS)
		assert(val_send_packet_ops[*val_push_ptr].req_num == 0);

	// traverse all of the ops to find bcasts
	for (i = 0; i < ACK_RECV_OPS_SIZE; i++) {
		if (ack_ops[i].opcode == ST_ACK_SUCCESS || ack_ops[i].opcode == ST_OP_MEMBERSHIP_CHANGE) {
			ack_ops[i].opcode = ST_EMPTY;
			continue;
		} else if (ack_ops[i].opcode == ST_EMPTY)
			continue;

		if(ENABLE_ASSERTIONS)
			assert(ack_ops[i].opcode == ST_LAST_ACK_SUCCESS);

		// if not enough credits for a Broadcast
		if (!check_val_credits(credits, cb, credit_wc, credit_recv_wr,
							   last_g_membership, worker_lid))
		{
			has_outstanding_vals = 1;
			break;
		}

		for (j = 0; j < MACHINE_NUM; j++){
			if (j == machine_id) continue; // skip the local machine
			if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
			credits[VAL_UD_QP_ID][j]--;
            if (ENABLE_CREDIT_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[1m\033[32mVALs\033[0m"
						" \033[31mdecremented\033[0m to %d (for machine %d)\n",
					    worker_lid, credits[VAL_UD_QP_ID][j], j);
		}

		ack_ops[i].opcode = ST_EMPTY;

		// Create the broadcast messages
		forge_bcast_val_wrs(&ack_ops[i], &val_send_packet_ops[*val_push_ptr], send_val_wr,
							send_val_sgl, last_g_membership.num_of_alive_remotes, cb,
							br_tx, br_i, worker_lid);
		total_vals_in_batch++;

		if(val_send_packet_ops[*val_push_ptr].req_num == VAL_MAX_REQ_COALESCE) {
			br_i++;
			if (br_i == MAX_PCIE_BCAST_BATCH) {
				batch_vals_2_NIC(send_val_wr, send_val_sgl, last_g_membership.num_of_alive_remotes,
								 cb, br_i, total_vals_in_batch, worker_lid);
				br_i = 0;
				total_vals_in_batch = 0;
			}
			HRD_MOD_ADD(*val_push_ptr, VAL_SEND_OPS_SIZE / REMOTE_MACHINES *
					last_g_membership.num_of_alive_remotes); //got to the next "packet" + dealing with failutes
			//Reset data left from previous bcasts after ibv_post_send to avoid sending reseted data
			val_send_packet_ops[*val_push_ptr].req_num = 0;
			for(j = 0; j < VAL_MAX_REQ_COALESCE; j++)
				val_send_packet_ops[*val_push_ptr].reqs[j].opcode = ST_EMPTY;
		}
	}

	if(val_send_packet_ops[*val_push_ptr].req_num > 0 &&
	   val_send_packet_ops[*val_push_ptr].req_num < VAL_MAX_REQ_COALESCE)
		br_i++;

	if(br_i > 0)
		batch_vals_2_NIC(send_val_wr, send_val_sgl, last_g_membership.num_of_alive_remotes,
						 cb, br_i, total_vals_in_batch, worker_lid);

	//Reset data left from previous bcasts
	if(val_send_packet_ops[*val_push_ptr].req_num > 0 &&
	   val_send_packet_ops[*val_push_ptr].req_num < VAL_MAX_REQ_COALESCE) {
		HRD_MOD_ADD(*val_push_ptr, VAL_SEND_OPS_SIZE / REMOTE_MACHINES *
				last_g_membership.num_of_alive_remotes); //got to the next "packet" + dealing with failutes
		val_send_packet_ops[*val_push_ptr].req_num = 0;
		for(j = 0; j < VAL_MAX_REQ_COALESCE; j++)
			val_send_packet_ops[*val_push_ptr].reqs[j].opcode = ST_EMPTY;
	}

	if (ENABLE_ASSERTIONS && has_outstanding_vals == 0)
		for (i = 0; i < ACK_RECV_OPS_SIZE; i++)
			assert(ack_ops[i].opcode == ST_EMPTY);

	return has_outstanding_vals;
}


static inline uint8_t
broadcast_vals_on_membership_change
		(spacetime_op_t* ops, spacetime_val_packet_t* val_send_packet_ops,
		 int* val_push_ptr, struct ibv_send_wr* send_val_wr, struct ibv_sge* send_val_sgl,
		 uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
		 struct ibv_wc* credit_wc, spacetime_group_membership last_g_membership,
		 long long* br_tx, struct ibv_recv_wr* credit_recv_wr, uint16_t worker_lid)
{
	uint16_t i = 0, br_i = 0, j, total_vals_in_batch = 0;
	uint8_t has_outstanding_vals = 0;

	if(ENABLE_ASSERTIONS)
		assert(val_send_packet_ops[*val_push_ptr].req_num == 0);

	// traverse all of the ops to find bcasts
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if(ops[i].op_meta.state != ST_REPLAY_COMPLETE &&
		   ops[i].op_meta.state != ST_PUT_COMPLETE_SEND_VALS)
			continue;

		// if not enough credits for a Broadcast
		if (!check_val_credits(credits, cb, credit_wc, credit_recv_wr,
							   last_g_membership, worker_lid)){
			has_outstanding_vals = 1;
			break;
		}

		for (j = 0; j < MACHINE_NUM; j++){
			if (j == machine_id) continue; // skip the local machine
			if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
			credits[VAL_UD_QP_ID][j]--;
            if (ENABLE_CREDIT_PRINTS && ENABLE_VAL_PRINTS &&
					worker_lid < MAX_THREADS_TO_PRINT)
			printf("$$$ Credits[W%d]: \033[1m\033[32mVALs\033[0m "
					"\033[31mdecremented\033[0m to %d (for machine %d)\n",
				    worker_lid, credits[VAL_UD_QP_ID][j], j);
		}


		if(ops[i].op_meta.state == ST_PUT_COMPLETE_SEND_VALS)
			ops[i].op_meta.state = ST_PUT_COMPLETE;
		else
			ops[i].op_meta.state = ST_NEW;

		// Create the broadcast messages
		forge_bcast_val_wrs(&ops[i], &val_send_packet_ops[*val_push_ptr], send_val_wr,
							send_val_sgl, last_g_membership.num_of_alive_remotes, cb,
							br_tx, br_i, worker_lid);
		total_vals_in_batch++;

		if(val_send_packet_ops[*val_push_ptr].req_num == VAL_MAX_REQ_COALESCE) {
			br_i++;
			if (br_i == MAX_PCIE_BCAST_BATCH) {
				batch_vals_2_NIC(send_val_wr, send_val_sgl, last_g_membership.num_of_alive_remotes,
								 cb, br_i, total_vals_in_batch, worker_lid);
				br_i = 0;
				total_vals_in_batch = 0;
			}
			HRD_MOD_ADD(*val_push_ptr, VAL_SEND_OPS_SIZE / REMOTE_MACHINES *
					     last_g_membership.num_of_alive_remotes); //got to the next "packet" + dealing with failutes
			//Reset data left from previous bcasts after ibv_post_send to avoid sending reseted data
			val_send_packet_ops[*val_push_ptr].req_num = 0;
			for(j = 0; j < VAL_MAX_REQ_COALESCE; j++)
				val_send_packet_ops[*val_push_ptr].reqs[j].opcode = ST_EMPTY;
		}
	}

	if(val_send_packet_ops[*val_push_ptr].req_num > 0 &&
	   val_send_packet_ops[*val_push_ptr].req_num < VAL_MAX_REQ_COALESCE)
		br_i++;

	if(br_i > 0)
		batch_vals_2_NIC(send_val_wr, send_val_sgl, last_g_membership.num_of_alive_remotes,
						 cb, br_i, total_vals_in_batch, worker_lid);

	//Reset data left from previous bcasts
	if(val_send_packet_ops[*val_push_ptr].req_num > 0 &&
	   val_send_packet_ops[*val_push_ptr].req_num < VAL_MAX_REQ_COALESCE) {
		HRD_MOD_ADD(*val_push_ptr, VAL_SEND_OPS_SIZE / REMOTE_MACHINES *
				     last_g_membership.num_of_alive_remotes); //got to the next "packet" + dealing with failutes
		val_send_packet_ops[*val_push_ptr].req_num = 0;
		for(j = 0; j < VAL_MAX_REQ_COALESCE; j++)
			val_send_packet_ops[*val_push_ptr].reqs[j].opcode = ST_EMPTY;
	}
	return has_outstanding_vals;
}

/* ---------------------------------------------------------------------------
------------------------------------CRDs--------------------------------------
---------------------------------------------------------------------------*/
static inline void
forge_crd_wr(uint16_t dst_node, struct ibv_send_wr* send_crd_wr,
			 struct hrd_ctrl_blk* cb, long long* send_crd_tx,
			 uint16_t send_crd_packets, uint8_t crd_to_send, uint16_t worker_lid)
{
	struct ibv_wc signal_send_wc;
	uint16_t dst_worker_gid = (uint16_t) (dst_node * WORKERS_PER_MACHINE + worker_lid);

	((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits = crd_to_send;

	send_crd_wr[send_crd_packets].wr.ud.ah = remote_worker_qps[dst_worker_gid][CRD_UD_QP_ID].ah;
	send_crd_wr[send_crd_packets].wr.ud.remote_qpn = (uint32) remote_worker_qps[dst_worker_gid][CRD_UD_QP_ID].qpn;
	send_crd_wr[send_crd_packets].send_flags = IBV_SEND_INLINE;

	w_stats[worker_lid].issued_crds_per_worker += crd_to_send;
	if (send_crd_packets > 0)
		send_crd_wr[send_crd_packets - 1].next = &send_crd_wr[send_crd_packets];

	if (*send_crd_tx % CRD_SS_GRANULARITY == 0) {
		if (*send_crd_tx > 0) {
			hrd_poll_cq(cb->dgram_send_cq[CRD_UD_QP_ID], 1, &signal_send_wc);
			w_stats[worker_lid].crd_ss_completions_per_worker++;
			if (ENABLE_SS_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("^^^ Polled SS completion[W%d]: \033[1m\033[36mCRD\033[0m %d (total %d)\n", worker_lid,
						   1, w_stats[worker_lid].crd_ss_completions_per_worker);
		}
		send_crd_wr[send_crd_packets].send_flags |= IBV_SEND_SIGNALED;
		if (ENABLE_SS_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("vvv Send SS[W%d]: \033[1m\033[36mCRD\033[0m\n", worker_lid);
	}
	(*send_crd_tx)++; // Selective signaling
}

static inline void
batch_crds_2_NIC(struct ibv_send_wr *send_crd_wr, struct hrd_ctrl_blk *cb,
				 uint16_t send_crd_packets, uint16_t total_crds_to_send,
				 uint16_t worker_lid)
{
	int ret, j = 0;
	struct ibv_send_wr *bad_send_wr;

	w_stats[worker_lid].issued_packet_crds_per_worker += send_crd_packets;
	send_crd_wr[send_crd_packets - 1].next = NULL;
	if(ENABLE_ASSERTIONS){
		for(j = 0; j < send_crd_packets - 1; j++){
			assert(send_crd_wr[j].next == &send_crd_wr[j+1]);
			assert(send_crd_wr[j].opcode == IBV_WR_SEND_WITH_IMM);
			assert(send_crd_wr[j].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(send_crd_wr[j].sg_list->length == 0);
			assert(send_crd_wr[j].num_sge == 0);
			assert(send_crd_wr[j].send_flags == IBV_SEND_INLINE ||
				   send_crd_wr[j].send_flags == (IBV_SEND_INLINE | IBV_SEND_SIGNALED));
			assert(((spacetime_crd_t*) &(send_crd_wr[j].imm_data))->val_credits > 0);
			assert(((spacetime_crd_t*) &(send_crd_wr[j].imm_data))->opcode == ST_OP_CRD);
			assert(((spacetime_crd_t*) &(send_crd_wr[j].imm_data))->sender == machine_id);
		}
		assert(send_crd_wr[j].next == NULL);
	}

	if(ENABLE_SEND_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf(">>> Send[W%d]: %d packets \033[1m\033[36mCRDs\033[0m %d (Total packets: %d, credits: %d)\n",
					worker_lid, send_crd_packets, total_crds_to_send, w_stats[worker_lid].issued_crds_per_worker,
					w_stats[worker_lid].issued_packet_crds_per_worker);
	ret = ibv_post_send(cb->dgram_qp[CRD_UD_QP_ID], &send_crd_wr[0], &bad_send_wr);
	CPE(ret, "ibv_post_send error while sending CRDs", ret);
}

static inline void
issue_credits(spacetime_val_t *val_recv_ops, long long int* send_crd_tx,
			  struct ibv_send_wr *send_crd_wr, uint8_t credits[][MACHINE_NUM],
			  struct hrd_ctrl_blk *cb, uint16_t worker_lid)
{
	uint16_t i = 0, send_crd_packets = 0;
	uint8_t credits_per_machine[MACHINE_NUM] = {0}, total_credits_to_send = 0;
	for (i = 0; i < VAL_RECV_OPS_SIZE; i++) {
		if (val_recv_ops[i].opcode == ST_EMPTY)
			break;

		if(credits_per_machine[val_recv_ops[i].sender] == 0 &&
		   credits[CRD_UD_QP_ID][val_recv_ops[i].sender] == 0)
			assert(0); //we should always have credits for CRDs

//		*credit_debug_cnt = 0;

		if(ENABLE_ASSERTIONS)
			assert(credits_per_machine[val_recv_ops[i].sender] < 255);

		credits[CRD_UD_QP_ID][val_recv_ops[i].sender]--;

		if (ENABLE_CREDIT_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			printf("$$$ Credits[W%d]: \033[1m\033[36mCRDs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
				   worker_lid, credits[CRD_UD_QP_ID][val_recv_ops[i].sender], val_recv_ops[i].sender);


		credits_per_machine[val_recv_ops[i].sender]++;
		//empty inv buffer
		if(val_recv_ops[i].opcode == ST_VAL_SUCCESS)
			val_recv_ops[i].opcode = ST_EMPTY;
		else assert(0);
	}
	for(i = 0; i < MACHINE_NUM; i ++){
		if(i == machine_id) continue;
		if(credits_per_machine[i] > 0) {
			forge_crd_wr(i, send_crd_wr, cb, send_crd_tx, send_crd_packets, credits_per_machine[i], worker_lid);
			send_crd_packets++;
			total_credits_to_send += credits_per_machine[i];
			if (send_crd_packets == MAX_SEND_CRD_WRS) {
				batch_crds_2_NIC(send_crd_wr, cb, send_crd_packets, total_credits_to_send, worker_lid);
				send_crd_packets = 0;
				total_credits_to_send = 0;
			}
		}
	}

	if (send_crd_packets > 0)
		batch_crds_2_NIC(send_crd_wr, cb, send_crd_packets, total_credits_to_send, worker_lid);

	if(ENABLE_ASSERTIONS )
		for(i = 0; i < VAL_RECV_OPS_SIZE; i++)
			assert(val_recv_ops[i].opcode == ST_EMPTY);
}

/* ---------------------------------------------------------------------------
----------------------------------- MEMBERSHIP -------------------------------
---------------------------------------------------------------------------*/
static inline void
follower_removal(int suspected_node_id)
{
	red_printf("Suspected node %d!\n", suspected_node_id);

	seqlock_lock(&group_membership.lock);
    if(ENABLE_ASSERTIONS == 1){
		assert(suspected_node_id != machine_id);
		assert(suspected_node_id < MACHINE_NUM);
		assert(node_is_in_membership(*((spacetime_group_membership*) &group_membership), suspected_node_id));
	}
    group_membership.num_of_alive_remotes--;

	bv_bit_reset((bit_vector_t*) &group_membership.g_membership, (uint8_t) suspected_node_id);
	bv_copy((bit_vector_t*) &group_membership.w_ack_init, group_membership.g_membership);
	bv_reverse((bit_vector_t*) &group_membership.w_ack_init);
	bv_bit_set((bit_vector_t*) &group_membership.w_ack_init, (uint8_t) machine_id);

	green_printf("Group Membership: ");
	bv_print(group_membership.g_membership);
	printf("\n");

	seqlock_unlock(&group_membership.lock);

	red_printf("Group membership changed --> Removed follower %d!\n", suspected_node_id);

	if(group_membership.num_of_alive_remotes < (MACHINE_NUM / 2)){
		red_printf("Majority is down!\n");
		exit(-1);
	}
}

static inline uint8_t
group_membership_has_changed(spacetime_group_membership* last_group_membership, uint16_t worker_lid)
{
	uint32_t debug_lock_free_membership_read_cntr = 0;
	spacetime_group_membership lock_free_read_group_membership;

	do { //Lock free read of group membership
		if (ENABLE_ASSERTIONS) {
			debug_lock_free_membership_read_cntr++;
			if (debug_lock_free_membership_read_cntr == M_4) {
				printf("Worker %u stuck on a lock-free read (for group membership)\n", worker_lid);
				debug_lock_free_membership_read_cntr = 0;
			}
		}
		lock_free_read_group_membership = *((spacetime_group_membership*) &group_membership);
	} while (!(seqlock_version_is_same_and_valid(&group_membership.lock,
												 &lock_free_read_group_membership.lock)));
	for(int i = 0; i < GROUP_MEMBERSHIP_ARRAY_SIZE; i++)
	    if(!bv_are_equal(lock_free_read_group_membership.g_membership,
	    		last_group_membership->g_membership))
		{
			*last_group_membership = lock_free_read_group_membership;
			return 1;
		}
	return 0;
}

static inline uint8_t
node_is_in_membership(spacetime_group_membership last_group_membership, int node_id)
{
	return (uint8_t) (bv_bit_get(last_group_membership.g_membership, (uint8_t) node_id) == 1 ? 1 : 0);
}

/* ---------------------------------------------------------------------------
----------------------------------- LATENCY -------------------------------
---------------------------------------------------------------------------*/
//Add latency to histogram (in microseconds)
static inline void
bookkeep_latency(int useconds, uint8_t op)
{
	uint32_t* latency_array;
	int* max_latency_ptr;
	switch (op){
		case ST_OP_PUT:
			latency_array = latency_count.write_reqs;
			max_latency_ptr = &latency_count.max_write_latency;
			break;
		case ST_OP_GET:
			latency_array = latency_count.read_reqs;
			max_latency_ptr = &latency_count.max_read_latency;
			break;
		default: assert(0);
	}
	latency_count.total_measurements++;
	if (useconds > MAX_LATENCY)
		latency_array[LATENCY_BUCKETS]++;
	else
		latency_array[useconds / LATENCY_PRECISION]++;

	if(*max_latency_ptr < useconds)
		*max_latency_ptr = useconds;
}


// Necessary bookkeeping to initiate the latency measurement
static inline void
start_latency_measurement(struct timespec *start)
{
	clock_gettime(CLOCK_MONOTONIC, start);
}

static inline void
stop_latency_measurment(uint8_t req_opcode, struct timespec *start)
{
	struct timespec end;
	clock_gettime(CLOCK_MONOTONIC, &end);
	int useconds = (int) (((end.tv_sec - start->tv_sec) * 1000000) +
				   ((end.tv_nsec - start->tv_nsec) / 1000));
	if (ENABLE_ASSERTIONS) assert(useconds >= 0);
//	printf("Latency of %s %u us\n", code_to_str(req_opcode), useconds);
	bookkeep_latency(useconds, req_opcode);
}


/* ---------------------------------------------------------------------------
------------------------------------OTHERS------------------------------------
---------------------------------------------------------------------------*/
static inline void
stop_latency_of_completed_writes(spacetime_op_t *ops, uint16_t worker_lid,
								 struct timespec *stopwatch)
{
	if(MEASURE_LATENCY && machine_id == 0 && worker_lid == THREAD_MEASURING_LATENCY)
		if(ops[0].op_meta.opcode == ST_OP_PUT &&
		   (ops[0].op_meta.state == ST_MISS || ops[0].op_meta.state == ST_PUT_COMPLETE))
				stop_latency_measurment(ops[0].op_meta.opcode, stopwatch);
}

static inline void
stop_latency_of_completed_reads(spacetime_op_t *ops, uint16_t worker_lid,
								struct timespec *stopwatch)
{
	if(MEASURE_LATENCY && machine_id == 0 && worker_lid == THREAD_MEASURING_LATENCY)
		if(ops[0].op_meta.opcode == ST_OP_GET &&
		   (ops[0].op_meta.state == ST_MISS || ops[0].op_meta.state == ST_GET_COMPLETE))
				stop_latency_measurment(ops[0].op_meta.opcode, stopwatch);
}

static inline int
refill_ops_n_suspect_failed_nodes(uint32_t *trace_iter, uint16_t worker_lid,
								  struct spacetime_trace_command *trace, spacetime_op_t *ops,
								  uint32_t *refilled_per_ops_debug_cnt,
								  spacetime_group_membership *last_group_membership,
								  struct timespec *start,
								  spacetime_op_t **n_hottest_keys_in_ops_get,
								  spacetime_op_t **n_hottest_keys_in_ops_put)
{
	static uint8_t first_iter_has_passed[WORKERS_PER_MACHINE] = { 0 };
//	static struct timespec start;
	int i = 0, refilled_ops = 0, node_suspected = -1;
	for(i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if(ENABLE_ASSERTIONS)
			if(first_iter_has_passed[worker_lid] == 1){
				assert(ops[i].op_meta.opcode == ST_OP_PUT || ops[i].op_meta.opcode == ST_OP_GET);
				assert(ops[i].op_meta.state == ST_PUT_COMPLETE ||
					   ops[i].op_meta.state == ST_GET_COMPLETE ||
					   ops[i].op_meta.state == ST_PUT_SUCCESS ||
					   ops[i].op_meta.state == ST_REPLAY_SUCCESS ||
					   ops[i].op_meta.state == ST_NEW ||
					   ops[i].op_meta.state == ST_MISS ||
					   ops[i].op_meta.state == ST_PUT_STALL ||
					   ops[i].op_meta.state == ST_REPLAY_COMPLETE ||
					   ops[i].op_meta.state == ST_IN_PROGRESS_PUT ||
					   ops[i].op_meta.state == ST_IN_PROGRESS_GET ||
					   ops[i].op_meta.state == ST_IN_PROGRESS_REPLAY ||
					   ops[i].op_meta.state == ST_OP_MEMBERSHIP_CHANGE || ///TODO check this
					   ops[i].op_meta.state == ST_OP_MEMBERSHIP_COMPLETE || ///TODO check this
					   ops[i].op_meta.state == ST_PUT_COMPLETE_SEND_VALS ||
					   ops[i].op_meta.state == ST_GET_STALL);
			}

		if (first_iter_has_passed[worker_lid] == 0 ||
			ops[i].op_meta.state == ST_MISS ||
			ops[i].op_meta.state == ST_PUT_COMPLETE ||
			ops[i].op_meta.state == ST_OP_MEMBERSHIP_COMPLETE ||
			ops[i].op_meta.state == ST_GET_COMPLETE) {
			if(first_iter_has_passed[worker_lid] != 0) {
				if (ENABLE_REQ_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					green_printf("W%d--> Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, value(len-%d): %c\n",
								 worker_lid, ((uint64_t *) &ops[i].op_meta.key)[0],
								 code_to_str(ops[i].op_meta.state), ops[i].op_meta.ts.version,
								 ops[i].op_meta.ts.tie_breaker_id, ops[i].op_meta.val_len, ops[i].value[0]);

                if(ops[i].op_meta.state != ST_MISS)
					w_stats[worker_lid].completed_ops_per_worker += ENABLE_COALESCE_OF_HOT_REQS ? ops[i].no_coales : 1;
                else
					w_stats[worker_lid].reqs_missed_in_kvs++;

				ops[i].no_coales = 1;
				ops[i].op_meta.state = ST_EMPTY;
				ops[i].op_meta.opcode = ST_EMPTY;
				refilled_per_ops_debug_cnt[i] = 0;
				refilled_ops++;
			}

			if(ENABLE_ASSERTIONS)
				assert(trace[*trace_iter].opcode == ST_OP_PUT || trace[*trace_iter].opcode == ST_OP_GET);

            if(MEASURE_LATENCY && machine_id == 0 && worker_lid == THREAD_MEASURING_LATENCY && i == 0)
				start_latency_measurement(start);

           /// INSERT new req(s) to ops
			uint8_t key_id;
			if(ENABLE_COALESCE_OF_HOT_REQS){
			    // see if you could coalesce any requests
			    spacetime_op_t** n_hottest_keys_in_ops;
				do{
					key_id = trace[*trace_iter].key_id;
					n_hottest_keys_in_ops = trace[*trace_iter].opcode == ST_OP_GET ?
											n_hottest_keys_in_ops_get : n_hottest_keys_in_ops_put;
					// if we can coalesce (a hot) req
					if(key_id < COALESCE_N_HOTTEST_KEYS && // is a hot key
					   n_hottest_keys_in_ops[key_id] != NULL && // exists in the ops array
					   n_hottest_keys_in_ops[key_id]->op_meta.opcode == trace[*trace_iter].opcode) // has the same code with the last inserted
					{
						n_hottest_keys_in_ops[key_id]->no_coales++;
						*trace_iter = trace[*trace_iter + 1].opcode != NOP ? *trace_iter + 1 : 0;
					}else
						break;
				} while(1);

				if(key_id < COALESCE_N_HOTTEST_KEYS)
					n_hottest_keys_in_ops[key_id] = &ops[i];
			}

			ops[i].op_meta.state = ST_NEW;
			ops[i].op_meta.opcode = (uint8_t) (CR_ENABLE_ALL_NODES_GETS_EXCEPT_HEAD && machine_id != 0 ?
											   ST_OP_GET : trace[*trace_iter].opcode);
			memcpy(&ops[i].op_meta.key, &trace[*trace_iter].key_hash, sizeof(spacetime_key_t));

			if (ops[i].op_meta.opcode == ST_OP_PUT)
				memset(ops[i].value, ((uint8_t) 'a' + machine_id), ST_VALUE_SIZE);
			else if(ENABLE_READ_COMPLETE_AFTER_VAL_RECV_OF_HOT_REQS){
				//if its a read reset the timestamp
				ops[i].op_meta.ts.version = 0;
				ops[i].op_meta.ts.tie_breaker_id = 0;
			}

			ops[i].op_meta.val_len = (uint8) (ops[i].op_meta.opcode == ST_OP_PUT ? ST_VALUE_SIZE : 0);

			// instead of MOD add
			*trace_iter = trace[*trace_iter + 1].opcode != NOP ? *trace_iter + 1 : 0;


			if(ENABLE_REQ_PRINTS &&  worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("W%d--> Op: %s, hash(1st 8B):%" PRIu64 "\n",
						   worker_lid, code_to_str(ops[i].op_meta.opcode), ((uint64_t *) &ops[i].op_meta.key)[0]);

		}else if(ops[i].op_meta.state == ST_IN_PROGRESS_PUT){
			refilled_per_ops_debug_cnt[i]++;
			///Failure suspicion
			if(CR_IS_RUNNING == 0)
				if(unlikely(refilled_per_ops_debug_cnt[i] > NUM_OF_IDLE_ITERS_FOR_SUSPICION)){
					if(machine_id < NODES_WITH_FAILURE_DETECTOR && worker_lid == WORKER_EMULATING_FAILURE_DETECTOR){
						node_suspected = find_suspected_node(&ops[i], worker_lid, last_group_membership);
						cyan_printf("Worker: %d SUSPECTS node: %d (req %d)\n", worker_lid, node_suspected, i);
						ops[i].op_meta.state = ST_OP_MEMBERSHIP_CHANGE;
						ops[i].value[0] = (uint8_t) node_suspected;
						//reset counter for failure suspicion
						memset(refilled_per_ops_debug_cnt, 0, sizeof(uint32_t) * MAX_BATCH_OPS_SIZE);
					}
				}
		}
	}

	if(refilled_ops == 0)
		w_stats[worker_lid].wasted_loops++;

	if(first_iter_has_passed[worker_lid] == 0)
		first_iter_has_passed[worker_lid] = 1;

	if(ENABLE_ASSERTIONS)
		for(i = 0; i < MAX_BATCH_OPS_SIZE; i++)
			assert(ops[i].op_meta.opcode == ST_OP_PUT || ops[i].op_meta.opcode == ST_OP_GET);

	return node_suspected;
}
#endif //HERMES_INLINE_UTIL_H
