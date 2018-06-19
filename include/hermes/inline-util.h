//
// Created by akatsarakis on 23/05/18.
//

#ifndef HERMES_INLINE_UTIL_H
#define HERMES_INLINE_UTIL_H

#include <infiniband/verbs.h>
#include "spacetime.h"
#include "config.h"
#include "util.h"


static inline void poll_cq_for_credits(struct ibv_cq *credit_recv_cq,
									   struct ibv_wc *credit_wc,
									   uint8_t credits[][MACHINE_NUM], uint16_t worker_lid);
/* ---------------------------------------------------------------------------
------------------------------------GENERIC-----------------------------------
---------------------------------------------------------------------------*/
///TODO ADD Batching to the NIC for post RECEIVES
static inline void post_receives(struct hrd_ctrl_blk *cb, uint16_t num_of_receives,
								 uint8_t buff_type, void *recv_buff, int *push_ptr)
{
    int i,qp_id, req_size, recv_q_depth;
    void* next_buff_addr;
    switch(buff_type){
        case ST_INV_BUFF:
            req_size = sizeof(ud_req_inv_t);
            recv_q_depth = RECV_INV_Q_DEPTH;
            qp_id = INV_UD_QP_ID;
            break;
        case ST_ACK_BUFF:
            req_size = sizeof(ud_req_ack_t);
            recv_q_depth = RECV_ACK_Q_DEPTH;
            qp_id = ACK_UD_QP_ID;
            break;
        case ST_VAL_BUFF:
            req_size = sizeof(ud_req_val_t);
            recv_q_depth = RECV_VAL_Q_DEPTH;
            qp_id = VAL_UD_QP_ID;
            break;
        default: assert(0);
    }

    for(i = 0; i < num_of_receives; i++) {
        next_buff_addr = ((uint8_t*) recv_buff) + (*push_ptr * req_size);
        memset(next_buff_addr, 0, (size_t) req_size); //reset the buffer before posting the receive
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
//    static long long int poll_buff_called_dbg_count[WORKERS_PER_MACHINE][3] = {0};
//	static long long int prev_packets[WORKERS_PER_MACHINE][3] = {0};
//	static long long int prev_msgs[WORKERS_PER_MACHINE][3] = {0};
    void* next_packet_reqs, *recv_op_ptr, *next_req;
	uint8_t *next_packet_req_num_ptr;
	int index = 0, recv_q_depth = 0, max_credits = 0, i = 0, j = 0, packets_polled = 0, reqs_polled = 0;
	uint8_t qp_credits_to_inc = 0, sender = 0;
	size_t req_size = 0;
	switch(buff_type){
		case ST_INV_BUFF:
			qp_credits_to_inc = ACK_UD_QP_ID;
			recv_q_depth = RECV_INV_Q_DEPTH;
			req_size = sizeof(spacetime_inv_t);
			if(ENABLE_ASSERTIONS)
				max_credits = ACK_CREDITS;
			break;
		case ST_ACK_BUFF:
			qp_credits_to_inc = INV_UD_QP_ID;
			recv_q_depth = RECV_ACK_Q_DEPTH;
			req_size = sizeof(spacetime_ack_t);
			if(ENABLE_ASSERTIONS)
				max_credits = INV_CREDITS;
			break;
		case ST_VAL_BUFF:
			qp_credits_to_inc = CRD_UD_QP_ID;
			recv_q_depth = RECV_VAL_Q_DEPTH;
			req_size = sizeof(spacetime_val_t);
			if(ENABLE_ASSERTIONS)
				max_credits = CRD_CREDITS;
			break;
		default: assert(0);
	}

	//poll completion q
	packets_polled = ibv_poll_cq(completion_q, MAX_BATCH_OPS_SIZE - outstanding_ops - *ops_push_ptr, work_completions);

	for(i = 0; i < packets_polled; i++){
		index = (*buf_pull_ptr + 1) % recv_q_depth;
		switch (buff_type) {
			case ST_INV_BUFF:
				sender = ((ud_req_inv_t *) incoming_buff)[index].packet.reqs[0].sender;
				next_packet_reqs = &((ud_req_inv_t *) incoming_buff)[index].packet.reqs;
                next_packet_req_num_ptr = &((ud_req_inv_t *) incoming_buff)[index].packet.req_num;
				if(ENABLE_ASSERTIONS){
					assert(*next_packet_req_num_ptr > 0 && *next_packet_req_num_ptr <= INV_MAX_REQ_COALESCE);
					for(j = 0; j < *next_packet_req_num_ptr; j++){
						assert(((spacetime_inv_t*) next_packet_reqs)[j].version % 2 == 0);
						assert(((spacetime_inv_t*) next_packet_reqs)[j].opcode == ST_OP_INV);
						assert(((spacetime_inv_t*) next_packet_reqs)[j].val_len == ST_VALUE_SIZE);
						assert(REMOTE_MACHINES != 1 || ((spacetime_inv_t*) next_packet_reqs)[j].tie_breaker_id == REMOTE_MACHINES - machine_id);
						assert(REMOTE_MACHINES != 1 || ((spacetime_inv_t*) next_packet_reqs)[j].sender == REMOTE_MACHINES - machine_id);
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
						assert(((spacetime_ack_t*) next_packet_reqs)[j].version % 2 == 0);
						assert(((spacetime_ack_t*) next_packet_reqs)[j].opcode == ST_OP_ACK);
						assert(((spacetime_ack_t*) next_packet_reqs)[j].tie_breaker_id == machine_id);
						assert(REMOTE_MACHINES != 1 || ((spacetime_ack_t*) next_packet_reqs)[j].sender == REMOTE_MACHINES - machine_id);
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
						assert(((spacetime_val_t*) next_packet_reqs)[j].version % 2 == 0);
						assert(((spacetime_val_t*) next_packet_reqs)[j].opcode == ST_OP_VAL);
						assert(REMOTE_MACHINES != 1 || ((spacetime_val_t*) next_packet_reqs)[j].tie_breaker_id == REMOTE_MACHINES - machine_id);
						assert(REMOTE_MACHINES != 1 || ((spacetime_val_t*) next_packet_reqs)[j].sender == REMOTE_MACHINES - machine_id);
					}
				}
				break;
			default:
				assert(0);
		}
		for(j = 0; j < *next_packet_req_num_ptr; j++){
			recv_op_ptr = ((uint8_t *) recv_ops) + (*ops_push_ptr * req_size);
			next_req = &((uint8_t*) next_packet_reqs)[j * req_size];
			memcpy(recv_op_ptr, next_req, req_size);
			reqs_polled++;
			(*ops_push_ptr)++;
		}
		credits[qp_credits_to_inc][sender]++; //increment packet credits

		*next_packet_req_num_ptr = 0; //TODO can be removed since we already reset on posting receives
		HRD_MOD_ADD(*buf_pull_ptr, recv_q_depth);

		if(ENABLE_ASSERTIONS){
			assert(credits[qp_credits_to_inc][sender] <= max_credits);
            assert(reqs_polled == *ops_push_ptr);
		}
	}

	if (ENABLE_RECV_PRINTS || ENABLE_CREDIT_PRINTS || ENABLE_STAT_COUNTING)
		if(packets_polled > 0){
			switch(buff_type){
				case ST_INV_BUFF:
//					poll_buff_called_dbg_count[worker_lid][ST_INV_BUFF % 3]++;
//					if(poll_buff_called_dbg_count[worker_lid][ST_INV_BUFF % 3] % K_256 == 0){
//						printf("[W%d]Packets polled: %llu, msgs found: %llu avg msg / packet: %llu\n", worker_lid,
//							   w_stats[worker_lid].received_packet_invs_per_worker - prev_packets[worker_lid][ST_INV_BUFF % 3],
//							   w_stats[worker_lid].received_invs_per_worker - prev_msgs[worker_lid][ST_INV_BUFF % 3],
//							   (w_stats[worker_lid].received_invs_per_worker - prev_msgs[worker_lid][ST_INV_BUFF % 3]) /
//							   (w_stats[worker_lid].received_packet_invs_per_worker - prev_packets[worker_lid][ST_INV_BUFF % 3]) );
//						prev_packets[worker_lid][ST_INV_BUFF % 3] = w_stats[worker_lid].received_packet_invs_per_worker;
//						prev_msgs[worker_lid][ST_INV_BUFF % 3] = w_stats[worker_lid].received_invs_per_worker;
//					}
					w_stats[worker_lid].received_invs_per_worker += reqs_polled;
					w_stats[worker_lid].received_packet_invs_per_worker += packets_polled;
					if(ENABLE_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						green_printf("^^^ Polled reqs[W%d]: %d packets \033[31mINVs\033[0m %d, (total packets: %d, reqs %d)!\n",
									 worker_lid, packets_polled, reqs_polled,
									 w_stats[worker_lid].received_packet_invs_per_worker, w_stats[worker_lid].received_invs_per_worker);
					if(ENABLE_CREDIT_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
						printf("$$$ Credits[W%d]: \033[33mACKs\033[0m \033[1m\033[32mincremented\033[0m "
									   "to %d (for machine %d)\n", worker_lid, credits[qp_credits_to_inc][sender], sender);
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
static inline void forge_bcast_inv_wrs(spacetime_op_t* op, spacetime_inv_packet_t* inv_send_op,
									   struct ibv_send_wr* send_inv_wr, struct ibv_sge* send_inv_sgl,
									   struct hrd_ctrl_blk* cb, long long* total_inv_bcasts,
									   uint16_t br_i, uint16_t worker_lid)
{
	static int total_completions[WORKERS_PER_MACHINE] = { 0 };
	struct ibv_wc signal_send_wc;

	if(ENABLE_ASSERTIONS)
		assert(sizeof(spacetime_inv_t) == sizeof(spacetime_op_t));

    memcpy(&inv_send_op->reqs[inv_send_op->req_num], op, sizeof(spacetime_inv_t));
	inv_send_op->reqs[inv_send_op->req_num].opcode = ST_OP_INV;
	inv_send_op->reqs[inv_send_op->req_num].sender = (uint8_t) machine_id;
	inv_send_op->req_num++;

	///TODO add code for non-inlining
	w_stats[worker_lid].issued_invs_per_worker += REMOTE_MACHINES;

	if(inv_send_op->req_num == 1) {
		send_inv_sgl[br_i].addr = (uint64_t) (uintptr_t) inv_send_op;
		send_inv_wr[br_i * REMOTE_MACHINES].send_flags = IBV_SEND_INLINE; //reset possibly signaled wr from the prev round
		// SET THE PREV SEND_WR TO POINT TO CURR
		if (br_i > 0)
			send_inv_wr[br_i * REMOTE_MACHINES - 1].next = &send_inv_wr[br_i * REMOTE_MACHINES];
		// Do a Signaled Send every INV_SS_GRANULARITY broadcasts (INV_SS_GRANULARITY * REMOTE_MACHINES messages)
		if (*total_inv_bcasts % INV_SS_GRANULARITY == 0) {
            if(*total_inv_bcasts > 0) { //if not the first SS poll the previous SS completion
				hrd_poll_cq(cb->dgram_send_cq[INV_UD_QP_ID], 1, &signal_send_wc);
				if (ENABLE_SS_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					red_printf( "^^^ Polled SS completion[W%d]: \033[31mINV\033[0m %d "
								"(total ss comp: %d --> reqs comp: %d, curr_invs: %d)\n",
								worker_lid, 1, ++total_completions[worker_lid],
								(*total_inv_bcasts - INV_SS_GRANULARITY) * REMOTE_MACHINES + 1,
								*total_inv_bcasts * REMOTE_MACHINES);
			}
			if (ENABLE_SS_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("vvv Send SS[W%d]: \033[31mINV\033[0m \n", worker_lid);
			send_inv_wr[br_i * REMOTE_MACHINES].send_flags |= IBV_SEND_SIGNALED;
		}
		(*total_inv_bcasts)++;
	}
}

static inline void post_ack_recvs_and_batch_invs_2_NIC(struct ibv_send_wr* send_inv_wr, struct ibv_sge* send_inv_sgl,
													   struct hrd_ctrl_blk* cb, ud_req_ack_t* incoming_acks, int* ack_push_ptr,
													   uint16_t br_i, uint16_t total_invs_in_batch, uint16_t worker_lid)
{
	static int total_invs_issued[WORKERS_PER_MACHINE] = { 0 };///TODO just for debugging
	static int total_inv_packets_issued[WORKERS_PER_MACHINE] = { 0 };///TODO just for debugging
    int j = 0, ret;
	struct ibv_send_wr *bad_send_wr;

	send_inv_wr[br_i * REMOTE_MACHINES - 1].next = NULL;
	if (ENABLE_ASSERTIONS) {
		for (j = 0; j < br_i * REMOTE_MACHINES - 1; j++) {
			assert(send_inv_wr[j].num_sge == 1);
			assert(send_inv_wr[j].next == &send_inv_wr[j + 1]);
			assert(send_inv_wr[j].opcode == IBV_SEND_INLINE | IBV_SEND_SIGNALED);
			assert(send_inv_wr[j].sg_list == &send_inv_sgl[j / REMOTE_MACHINES]);
			assert(send_inv_sgl[j / REMOTE_MACHINES].length == sizeof(spacetime_inv_packet_t));
			assert(((spacetime_inv_packet_t *) send_inv_sgl[j / REMOTE_MACHINES].addr)->req_num > 0);
			assert(((spacetime_inv_packet_t *) send_inv_sgl[j / REMOTE_MACHINES].addr)->reqs[0].opcode == ST_OP_INV);
			assert(((spacetime_inv_packet_t *) send_inv_sgl[j / REMOTE_MACHINES].addr)->reqs[0].sender == machine_id);
//			green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//						 j, ((uint64_t *) &((spacetime_inv_packet_t*) send_inv_sgl[j / REMOTE_MACHINES].addr)->reqs[0].key)[1]);
		}
//        green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//                     j, ((uint64_t *) &((spacetime_inv_packet_t*) send_inv_sgl[j / REMOTE_MACHINES].addr)->reqs[0].key)[1]);
		assert(send_inv_wr[j].next == NULL);
	}

	if (ENABLE_POST_RECV_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		yellow_printf("vvv Post Receives[W%d]: \033[33mACKs\033[0m %d\n", worker_lid, br_i * REMOTE_MACHINES);
	post_receives(cb, (uint16_t) (br_i * REMOTE_MACHINES), ST_ACK_BUFF, incoming_acks, ack_push_ptr);
	total_invs_issued[worker_lid] += total_invs_in_batch;
	total_inv_packets_issued[worker_lid] += br_i;
	if (ENABLE_SEND_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf(">>> Send[W%d]: %d bcast %d packets \033[31mINVs\033[0m (%d) (Total bcasts: %d, packets %d, invs: %d)\n",
					worker_lid, total_invs_in_batch, br_i, total_invs_in_batch * REMOTE_MACHINES,
					total_invs_issued[worker_lid], total_inv_packets_issued[worker_lid],
					total_invs_issued[worker_lid] * REMOTE_MACHINES);
	ret = ibv_post_send(cb->dgram_qp[INV_UD_QP_ID], &send_inv_wr[0], &bad_send_wr);
	if (ret)
		printf("Total invs issued:%d\n", total_invs_issued[worker_lid] * REMOTE_MACHINES);
	CPE(ret, "INVs ibv_post_send error", ret);

	if(ENABLE_ASSERTIONS)
		assert(total_invs_issued[worker_lid] == w_stats[worker_lid].issued_invs_per_worker);
}

static inline void broadcasts_invs(spacetime_op_t* ops, spacetime_inv_packet_t* inv_send_packet_ops, int* inv_push_ptr,
								   struct ibv_send_wr* send_inv_wr, struct ibv_sge* send_inv_sgl,
								   uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
                                   long long* total_inv_bcasts, ud_req_ack_t* incoming_acks, int* ack_push_ptr,
								   uint16_t worker_lid, uint32_t* credit_debug_cnt, uint16_t* rolling_index)
{
	uint8_t missing_credits = 0;
	uint16_t i = 0, br_i = 0, j = 0, total_invs_in_batch = 0, index = 0;

	if(ENABLE_ASSERTIONS){
		if(inv_send_packet_ops[*inv_push_ptr].req_num != 0){
			printf("Inv ptr: %d, req_num: %d\n", *inv_push_ptr, inv_send_packet_ops[*inv_push_ptr].req_num);
			printf("Total invs issued: %llu, MAX_BATCH_OP: %d\n", w_stats[worker_lid].issued_invs_per_worker, MAX_BATCH_OPS_SIZE);
		}
		assert(inv_send_packet_ops[*inv_push_ptr].req_num == 0);
	}

	// traverse all of the ops to find invs
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
        index = (uint16_t) ((i + *rolling_index) % MAX_BATCH_OPS_SIZE);
		if (ops[index].state != ST_PUT_SUCCESS && ops[index].state != ST_BUFFERED_REPLAY)
			continue;

		//Check for credits
		if(inv_send_packet_ops[*inv_push_ptr].req_num == 0) {
			for (j = 0; j < MACHINE_NUM; j++) {
				if (j == machine_id) continue; // skip the local machine
				if (credits[INV_UD_QP_ID][j] == 0) {
					missing_credits = 1;
					*rolling_index = index;
					(*credit_debug_cnt)++;
					break;
				}
			}
			// if not enough credits for a Broadcast
			if (missing_credits == 1) break;
			*credit_debug_cnt = 0;

			for (j = 0; j < MACHINE_NUM; j++)
				credits[INV_UD_QP_ID][j]--;

			if (ENABLE_CREDIT_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[31mINVs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					   worker_lid, credits[INV_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   (machine_id + 1) % MACHINE_NUM);
		}
		//Change state of op
		if(ops[index].state == ST_PUT_SUCCESS)
			ops[index].state = ST_IN_PROGRESS_WRITE;
		else if(ops[index].state == ST_BUFFERED_REPLAY)
			ops[index].state = ST_BUFFERED_IN_PROGRESS_REPLAY;
		else assert(0);

		// Create the broadcast messages
		forge_bcast_inv_wrs(&ops[index], &inv_send_packet_ops[*inv_push_ptr],
							send_inv_wr, send_inv_sgl, cb, total_inv_bcasts, br_i, worker_lid);
		total_invs_in_batch++;
		// if packet is full
		if(inv_send_packet_ops[*inv_push_ptr].req_num == INV_MAX_REQ_COALESCE) {
			br_i++;
			if (br_i == MAX_PCIE_BCAST_BATCH) { //check if we should batch it to NIC
                post_ack_recvs_and_batch_invs_2_NIC(send_inv_wr, send_inv_sgl, cb, incoming_acks,
													ack_push_ptr, br_i, total_invs_in_batch, worker_lid);
				br_i = 0;
				total_invs_in_batch = 0;
			}
			HRD_MOD_ADD(*inv_push_ptr, MAX_BATCH_OPS_SIZE); //got to the next "packet"
			//Reset data left from previous bcasts after ibv_post_send to avoid sending reseted data
			inv_send_packet_ops[*inv_push_ptr].req_num = 0;
			for(j = 0; j < INV_MAX_REQ_COALESCE; j++)
				inv_send_packet_ops[*inv_push_ptr].reqs[j].opcode = ST_EMPTY;
		}
	}


	if(inv_send_packet_ops[*inv_push_ptr].req_num > 0 &&
	   inv_send_packet_ops[*inv_push_ptr].req_num < INV_MAX_REQ_COALESCE)
        br_i++;

   	if(br_i > 0)
		post_ack_recvs_and_batch_invs_2_NIC(send_inv_wr, send_inv_sgl, cb, incoming_acks,
											ack_push_ptr, br_i, total_invs_in_batch, worker_lid);

	//Move to next packet and reset data left from previous bcasts
	if(inv_send_packet_ops[*inv_push_ptr].req_num > 0 &&
	   inv_send_packet_ops[*inv_push_ptr].req_num < INV_MAX_REQ_COALESCE) {
		HRD_MOD_ADD(*inv_push_ptr, MAX_BATCH_OPS_SIZE);
		inv_send_packet_ops[*inv_push_ptr].req_num = 0;
		for(j = 0; j < INV_MAX_REQ_COALESCE; j++)
			inv_send_packet_ops[*inv_push_ptr].reqs[j].opcode = ST_EMPTY;
	}

}

/* ---------------------------------------------------------------------------
------------------------------------ACKS--------------------------------------
---------------------------------------------------------------------------*/

static inline void forge_ack_wr(spacetime_inv_t* inv_recv_op, spacetime_ack_packet_t* ack_send_ops,
								struct ibv_send_wr* send_ack_wr, struct ibv_sge* send_ack_sgl,
								struct hrd_ctrl_blk* cb, long long* send_ack_tx,
								uint16_t send_ack_packets, uint16_t worker_lid)
{
	static int total_completions = 0;
	struct ibv_wc signal_send_wc;
	uint16_t dst_worker_gid = (uint16_t) (inv_recv_op->sender * WORKERS_PER_MACHINE + worker_lid);
    if(ENABLE_ASSERTIONS)
		assert(REMOTE_MACHINES != 1 || inv_recv_op->sender == REMOTE_MACHINES - machine_id);

	memcpy(&ack_send_ops->reqs[ack_send_ops->req_num], inv_recv_op, sizeof(spacetime_ack_t));
	ack_send_ops->reqs[ack_send_ops->req_num].opcode = ST_OP_ACK;
	ack_send_ops->reqs[ack_send_ops->req_num].sender = (uint8_t) machine_id;
	ack_send_ops->req_num++;
    if(ENABLE_ASSERTIONS)
		assert(ack_send_ops->req_num <= ACK_MAX_REQ_COALESCE);

	///TODO add code for non-inlining
	w_stats[worker_lid].issued_acks_per_worker++;

	if(ack_send_ops->req_num == 1) {
		send_ack_sgl[send_ack_packets].addr = (uint64_t) ack_send_ops;
		send_ack_wr[send_ack_packets].wr.ud.ah = remote_worker_qps[dst_worker_gid][ACK_UD_QP_ID].ah;
		send_ack_wr[send_ack_packets].wr.ud.remote_qpn = (uint32) remote_worker_qps[dst_worker_gid][ACK_UD_QP_ID].qpn;
		send_ack_wr[send_ack_packets].send_flags = IBV_SEND_INLINE;

		if (send_ack_packets > 0)
			send_ack_wr[send_ack_packets - 1].next = &send_ack_wr[send_ack_packets];

		// Do a Signaled Send every ACK_SS_GRANULARITY msgs
		if (*send_ack_tx % ACK_SS_GRANULARITY == 0) {
			if(*send_ack_tx > 0){ //if not the first SS poll the previous SS completion
				hrd_poll_cq(cb->dgram_send_cq[ACK_UD_QP_ID], 1, &signal_send_wc);
				if (ENABLE_SS_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					red_printf("^^^ Polled SS completion[W%d]: \033[33mACK\033[0m %d (total %d)\n", worker_lid, 1,
							   ++total_completions);
			}
			send_ack_wr[send_ack_packets].send_flags |= IBV_SEND_SIGNALED;
			if (ENABLE_SS_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("vvv Send SS[W%d]: \033[33mACK\033[0m\n", worker_lid);
		}
		(*send_ack_tx)++;
	}
}

static inline void post_inv_recvs_and_batch_acks_2_NIC(struct ibv_send_wr *send_ack_wr, struct ibv_sge *send_ack_sgl,
													   ud_req_inv_t* incoming_invs, int* inv_push_ptr, struct hrd_ctrl_blk* cb,
													   uint16_t send_ack_packets, uint16_t total_acks_in_batch, uint16_t worker_lid)
{
	static int total_acks_issued[WORKERS_PER_MACHINE]= { 0 }; ///TODO just for debugging
	int j = 0, ret;
	struct ibv_send_wr *bad_send_wr;

	send_ack_wr[send_ack_packets - 1].next = NULL;
	if(ENABLE_ASSERTIONS){
		for(j = 0; j < send_ack_packets - 1; j++){
			assert(send_ack_wr[j].next == &send_ack_wr[j+1]);
			assert(send_ack_wr[j].opcode == IBV_WR_SEND);
			assert(send_ack_wr[j].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(send_ack_wr[j].sg_list == &send_ack_sgl[j]);
			assert(send_ack_wr[j].sg_list->length == sizeof(spacetime_ack_packet_t));
			assert(send_ack_wr[j].num_sge == 1);
			assert(send_ack_wr[j].send_flags == IBV_SEND_INLINE ||
				   send_ack_wr[j].send_flags == IBV_SEND_INLINE | IBV_SEND_SIGNALED);
			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->req_num > 0);
			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->reqs[0].opcode == ST_OP_ACK);
			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->reqs[0].sender == machine_id);
//			green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//						 j, ((uint64_t *) &((spacetime_ack_packet_t*) send_ack_sgl[j / REMOTE_MACHINES].addr)->reqs[0].key)[1]);
		}
//		green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//					 j, ((uint64_t *) &((spacetime_ack_packet_t*) send_ack_sgl[j / REMOTE_MACHINES].addr)->reqs[0].key)[1]);
		assert(send_ack_wr[j].next == NULL);
	}
	//before sending acks post receives for INVs
	if (ENABLE_POST_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		yellow_printf("vvv Post Receives[W%d]: \033[31mINVs\033[0m %d\n", worker_lid, send_ack_packets);
	post_receives(cb, send_ack_packets, ST_INV_BUFF, incoming_invs, inv_push_ptr);
	total_acks_issued[worker_lid] += total_acks_in_batch;
	if (ENABLE_SEND_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf(">>> Send[W%d]: \033[33mACKs\033[0m %d (Total %d)\n", worker_lid,
					total_acks_in_batch, total_acks_issued[worker_lid]);
	ret = ibv_post_send(cb->dgram_qp[ACK_UD_QP_ID], &send_ack_wr[0], &bad_send_wr);
	CPE(ret, "ibv_post_send error while sending ACKs", ret);

	if(ENABLE_ASSERTIONS)
		assert(total_acks_issued[worker_lid] == w_stats[worker_lid].issued_acks_per_worker);
}

static inline void issue_acks(spacetime_inv_t *inv_recv_ops, spacetime_ack_packet_t* ack_send_packet_ops,
							  long long int* send_ack_tx, struct ibv_send_wr *send_ack_wr,
							  struct ibv_sge *send_ack_sgl, uint8_t credits[][MACHINE_NUM],
							  struct hrd_ctrl_blk *cb, ud_req_inv_t* incoming_invs,
							  int* inv_push_ptr, uint16_t worker_lid, uint32_t* credit_debug_cnt)
{
	uint16_t i = 0, send_ack_packets = 0, total_acks_in_batch = 0, j = 0;

    //Reset data left from previous unicasts
	ack_send_packet_ops[send_ack_packets].req_num = 0;
	for(j = 0; j < ACK_MAX_REQ_COALESCE; j++)
		ack_send_packet_ops[send_ack_packets].reqs[j].opcode = ST_EMPTY;

	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (inv_recv_ops[i].opcode == ST_EMPTY)
//		    break; ////TODO test this!! --> I don't think we need to traverse the whole array (MAX_BATCH_OPS_SIZE)
			continue;

		if(ENABLE_ASSERTIONS){
            assert(inv_recv_ops[i].opcode == ST_INV_SUCCESS || inv_recv_ops[i].opcode == ST_OP_INV);
			assert(inv_recv_ops[i].val_len == ST_VALUE_SIZE);
			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].tie_breaker_id == REMOTE_MACHINES - machine_id);
			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].value[0] == (uint8_t) 'x' + (REMOTE_MACHINES - machine_id));
			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].sender == REMOTE_MACHINES - machine_id);
		}

        //TODO WARNING if sender is different change pck aswell!!
		if(ack_send_packet_ops[send_ack_packets].req_num == 0) {
			if (credits[ACK_UD_QP_ID][inv_recv_ops[i].sender] == 0) {
				assert(0); // we should always have credits for acks
				(*credit_debug_cnt)++;
//            red_printf("Not enough credits: %d (to send to machine %d)\n",credits[ACK_UD_QP_ID][inv_recv_ops[i].sender], inv_recv_ops[i].sender);
				continue; //TODO we may need to push this to the top of the stack + (seems ok to batch this again to the KVS)
			}
			*credit_debug_cnt = 0;
			//reduce credits
			credits[ACK_UD_QP_ID][inv_recv_ops[i].sender]--;
			if (ENABLE_CREDIT_PRINTS && ENABLE_ACK_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[33mACKs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					   worker_lid, credits[ACK_UD_QP_ID][inv_recv_ops[i].sender], inv_recv_ops[i].sender);
		}

        //empty inv buffer
		if(inv_recv_ops[i].opcode == ST_INV_SUCCESS)
			inv_recv_ops[i].opcode = ST_EMPTY;
		else assert(0);

		// Create the broadcast messages
		forge_ack_wr(&inv_recv_ops[i], &ack_send_packet_ops[send_ack_packets], send_ack_wr,
					 send_ack_sgl, cb, send_ack_tx, send_ack_packets, worker_lid);
		total_acks_in_batch++;

		if(ack_send_packet_ops[send_ack_packets].req_num == ACK_MAX_REQ_COALESCE) {
			send_ack_packets++;

			if (send_ack_packets == MAX_SEND_ACK_WRS) {
                post_inv_recvs_and_batch_acks_2_NIC(send_ack_wr, send_ack_sgl, incoming_invs, inv_push_ptr,
													cb, send_ack_packets, total_acks_in_batch, worker_lid);
				send_ack_packets = 0;
                total_acks_in_batch = 0;
			}

			//Reset data left from previous unicasts
            ack_send_packet_ops[send_ack_packets].req_num = 0;
			for(j = 0; j < ACK_MAX_REQ_COALESCE; j++)
				ack_send_packet_ops[send_ack_packets].reqs[j].opcode = ST_EMPTY;

		}
	}

	if(ack_send_packet_ops[send_ack_packets].req_num > 0 &&
	   ack_send_packet_ops[send_ack_packets].req_num < ACK_MAX_REQ_COALESCE)
		send_ack_packets++;

	if (send_ack_packets > 0)
		post_inv_recvs_and_batch_acks_2_NIC(send_ack_wr, send_ack_sgl, incoming_invs, inv_push_ptr,
											cb, send_ack_packets, total_acks_in_batch, worker_lid);
}


/* ---------------------------------------------------------------------------
------------------------------------VALs--------------------------------------
---------------------------------------------------------------------------*/
static inline void post_credit_recvs(struct hrd_ctrl_blk *cb,
									 struct ibv_recv_wr *credit_recv_wr,
									 uint16_t *credit_recv_counter)
{
	uint16_t i;
	int ret;
	struct ibv_recv_wr *bad_recv_wr;
	if (*credit_recv_counter > 0) { // Must post receives for credits
		for (i = 0; i < REMOTE_MACHINES * (*credit_recv_counter); i++)
			credit_recv_wr[i].next = (i == (REMOTE_MACHINES * (*credit_recv_counter)) - 1) ? NULL : &credit_recv_wr[i + 1];
		ret = ibv_post_recv(cb->dgram_qp[CRD_UD_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs for credits before val bcast", ret);
		*credit_recv_counter = 0;
	}
}

static inline bool check_val_credits(uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk *cb,
									 struct ibv_wc *credit_wc, uint32_t *credit_debug_cnt,
									 uint16_t worker_lid)
{
	uint16_t poll_for_credits = 0, j;
	for (j = 0; j < MACHINE_NUM; j++) {
		if (j == machine_id) continue; // skip the local machine
		if (credits[VAL_UD_QP_ID][j] == 0) {
			poll_for_credits = 1;
			break;
		}
	}
	if (poll_for_credits == 1) {
		poll_cq_for_credits(cb->dgram_recv_cq[CRD_UD_QP_ID], credit_wc, credits, worker_lid);
		// We polled for credits, if we did not find enough just break
		for (j = 0; j < MACHINE_NUM; j++) {
			if (j == machine_id) continue; // skip the local machine
			if (credits[VAL_UD_QP_ID][j] == 0) {
				(*credit_debug_cnt)++;
				return false;
			}
		}
	}

	if(poll_for_credits == 1)
		return false;
	*credit_debug_cnt = 0;
	return true;
}

static inline void forge_bcast_val_wrs(spacetime_ack_t* ack_op, spacetime_val_packet_t* val_packet_send_op,
									   struct ibv_send_wr* send_val_wr,
									   struct ibv_sge* send_val_sgl, struct hrd_ctrl_blk* cb,
									   long long* total_val_bcasts, uint16_t br_i, uint16_t worker_lid)
{
	static int total_completions[WORKERS_PER_MACHINE] = { 0 };
	struct ibv_wc signal_send_wc;
    if(ENABLE_ASSERTIONS)
		assert(sizeof(spacetime_ack_t) == sizeof(spacetime_val_t));

	memcpy(&val_packet_send_op->reqs[val_packet_send_op->req_num], ack_op, sizeof(spacetime_val_t));
	val_packet_send_op->reqs[val_packet_send_op->req_num].opcode = ST_OP_VAL;
	val_packet_send_op->reqs[val_packet_send_op->req_num].sender = (uint8_t) machine_id;
	val_packet_send_op->req_num++;

	///TODO add code for non-inlining
	w_stats[worker_lid].issued_vals_per_worker += REMOTE_MACHINES;

	if(val_packet_send_op->req_num == 1) {
		send_val_sgl[br_i].addr = (uint64_t) (uintptr_t) val_packet_send_op;
		send_val_wr[br_i * REMOTE_MACHINES].send_flags = IBV_SEND_INLINE;

		// SET THE LAST SEND_WR TO POINT TO NULL
		if (br_i > 0)
			send_val_wr[br_i * REMOTE_MACHINES - 1].next = &send_val_wr[br_i * REMOTE_MACHINES];
		// Do a Signaled Send every VAL_SS_GRANULARITY broadcasts (VAL_SS_GRANULARITY * (MACHINE_NUM - 1) messages)
		if (*total_val_bcasts % VAL_SS_GRANULARITY == 0) {
			if(*total_val_bcasts > 0){
				hrd_poll_cq(cb->dgram_send_cq[VAL_UD_QP_ID], 1, &signal_send_wc);
				if (ENABLE_SS_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					red_printf("^^^ Polled SS completion[W%d]: \033[1m\033[32mVAL\033[0m %d (total %d)"
								"(total ss comp: %d --> reqs comp: %d, curr_val: %d)\n",
								worker_lid, 1, ++total_completions[worker_lid],
							   (*total_val_bcasts - VAL_SS_GRANULARITY) * REMOTE_MACHINES + 1,
							   *total_val_bcasts * REMOTE_MACHINES);
			}
			if (ENABLE_SS_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("vvv Send SS[W%d]: \033[1m\033[32mVAL\033[0m \n", worker_lid);
			send_val_wr[br_i * REMOTE_MACHINES].send_flags |= IBV_SEND_SIGNALED;
		}
		(*total_val_bcasts)++;
	}
}


static inline void post_crd_recvs_and_batch_vals_2_NIC(struct ibv_send_wr *send_val_wr, struct ibv_sge *send_val_sgl,
													   struct ibv_recv_wr* credit_recv_wr, uint16_t* credit_recv_counter,
													   struct hrd_ctrl_blk* cb, uint16_t br_i,
													   uint16_t total_vals_in_batch, uint16_t worker_lid)
{
	static int total_vals_issued[WORKERS_PER_MACHINE]= { 0 }; ///TODO just for debuggina and prints
	static int total_val_packets_issued[WORKERS_PER_MACHINE]= { 0 }; ///TODO just for debuggina and prints
	int j = 0, ret, k = 0;
	struct ibv_send_wr *bad_send_wr;

	send_val_wr[br_i * REMOTE_MACHINES - 1].next = NULL;
	if (ENABLE_ASSERTIONS) {
		for (j = 0; j < br_i * REMOTE_MACHINES - 1; j++) {
			assert(send_val_wr[j].next == &send_val_wr[j + 1]);
			assert(send_val_wr[j].num_sge == 1);
			assert(send_val_wr[j].opcode == IBV_SEND_INLINE | IBV_SEND_SIGNALED);
			assert(send_val_wr[j].sg_list == &send_val_sgl[j / REMOTE_MACHINES]);
			assert(send_val_sgl[j / REMOTE_MACHINES].length == sizeof(spacetime_val_packet_t));
//            for(k = 0; k < ((spacetime_val_packet_t*) send_val_sgl[j / REMOTE_MACHINES].addr)->req_num; k ++)
//				green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//							 j, ((uint64_t *) &((spacetime_val_packet_t*) send_val_sgl[j / REMOTE_MACHINES].addr)->reqs[k].key)[1]);
		}
//		for(k = 0; k < ((spacetime_val_packet_t*) send_val_sgl[j / REMOTE_MACHINES].addr)->req_num; k ++)
//			green_printf("Ops[%d]--> hash(1st 8B):%" PRIu64 "\n",
//					 j, ((uint64_t *) &((spacetime_val_packet_t*) send_val_sgl[j / REMOTE_MACHINES].addr)->reqs[k].key)[1]);
		assert(send_val_wr[j].next == NULL);
	}

	if (ENABLE_POST_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		yellow_printf("vvv Post Receives[W%d]: \033[1m\033[36mCRDs\033[0m %d\n", worker_lid, *credit_recv_counter);
	post_credit_recvs(cb, credit_recv_wr, credit_recv_counter);
	total_vals_issued[worker_lid] += total_vals_in_batch;
	total_val_packets_issued[worker_lid] += br_i;
	if (ENABLE_SEND_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf( ">>> Send[W%d]: %d bcast %d packets \033[1m\033[32mVALs\033[0m (%d) (Total bcasts: %d, packets: %d, vals: %d)\n",
					 worker_lid, total_vals_in_batch, br_i, total_vals_in_batch * REMOTE_MACHINES,
					 total_vals_issued[worker_lid], total_val_packets_issued[worker_lid], total_vals_issued[worker_lid] * REMOTE_MACHINES);
	ret = ibv_post_send(cb->dgram_qp[VAL_UD_QP_ID], &send_val_wr[0], &bad_send_wr);
	if (ret)
		printf("Total vals issued:%d\n", total_vals_issued[worker_lid] * REMOTE_MACHINES);
	CPE(ret, "1st: Broadcast VALs ibv_post_send error", ret);

	if(ENABLE_ASSERTIONS)
		assert(total_vals_issued[worker_lid] * REMOTE_MACHINES == w_stats[worker_lid].issued_vals_per_worker);
}

static inline void broadcasts_vals(spacetime_ack_t* ack_ops, spacetime_val_packet_t* val_send_packet_ops, int* val_push_ptr,
								   struct ibv_send_wr* send_val_wr, struct ibv_sge* send_val_sgl,
								   uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb,
								   struct ibv_wc* credit_wc, uint32_t* credit_debug_cnt,
								   long long* br_tx, struct ibv_recv_wr* credit_recv_wr, uint16_t worker_lid)
{
	uint16_t i = 0, br_i = 0, j, credit_recv_counter = 0, total_vals_in_batch = 0;

	if(ENABLE_ASSERTIONS)
		assert(val_send_packet_ops[*val_push_ptr].req_num == 0);

	// traverse all of the ops to find bcasts
	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (ack_ops[i].opcode != ST_LAST_ACK_SUCCESS)
			continue;
//		else if (ack_ops[i].opcode == ST_EMPTY) ////TODO test this!! --> I don't think we need to traverse the whole array (MAX_BATCH_OPS_SIZE)
//			break;

		if(val_send_packet_ops[*val_push_ptr].req_num == 0) {
			// if not enough credits for a Broadcast
			if (!check_val_credits(credits, cb, credit_wc, credit_debug_cnt, worker_lid))
				break;

			for (j = 0; j < MACHINE_NUM; j++)
				credits[VAL_UD_QP_ID][j]--;

			if (ENABLE_CREDIT_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[1m\033[32mVALs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					   worker_lid, credits[VAL_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   (machine_id + 1) % MACHINE_NUM);

//			if ((*br_tx) % MAX_CRDS_IN_MESSAGE == 0) credit_recv_counter++;
			credit_recv_counter++;
		}

		ack_ops[i].opcode = ST_EMPTY;

		// Create the broadcast messages
		forge_bcast_val_wrs(&ack_ops[i], &val_send_packet_ops[*val_push_ptr], send_val_wr,
									   send_val_sgl, cb, br_tx, br_i, worker_lid);
		total_vals_in_batch++;

		if(val_send_packet_ops[*val_push_ptr].req_num == VAL_MAX_REQ_COALESCE) {
			br_i++;
			if (br_i == MAX_PCIE_BCAST_BATCH) {
				post_crd_recvs_and_batch_vals_2_NIC(send_val_wr, send_val_sgl, credit_recv_wr, &credit_recv_counter,
												    cb, br_i, total_vals_in_batch, worker_lid);
				br_i = 0;
                total_vals_in_batch = 0;
			}
			HRD_MOD_ADD(*val_push_ptr, MAX_BATCH_OPS_SIZE);
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
		post_crd_recvs_and_batch_vals_2_NIC(send_val_wr, send_val_sgl, credit_recv_wr, &credit_recv_counter,
											cb, br_i, total_vals_in_batch, worker_lid);

    //Reset data left from previous bcasts
	if(val_send_packet_ops[*val_push_ptr].req_num > 0 &&
	   val_send_packet_ops[*val_push_ptr].req_num < VAL_MAX_REQ_COALESCE) {
		HRD_MOD_ADD(*val_push_ptr, MAX_BATCH_OPS_SIZE);
		val_send_packet_ops[*val_push_ptr].req_num = 0;
		for(j = 0; j < VAL_MAX_REQ_COALESCE; j++)
			val_send_packet_ops[*val_push_ptr].reqs[j].opcode = ST_EMPTY;
	}

}

/* ---------------------------------------------------------------------------
------------------------------------CRDs--------------------------------------
---------------------------------------------------------------------------*/
// Poll and increment credits
///TODO I don't need to pass the whole credit array just the VAL_UD_QP_ID
static inline void poll_cq_for_credits(struct ibv_cq *credit_recv_cq,
									   struct ibv_wc *credit_wc,
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

		w_stats[worker_lid].received_crds_per_worker += credits_found;
		if(ENABLE_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
			green_printf("^^^ Polled reqs[W%d]: \033[1m\033[36mCRDs\033[0m %d, (total: %d)!\n",worker_lid, credits_found, w_stats[worker_lid].received_crds_per_worker);
		for (i = 0; i < credits_found; i++){
			crd_ptr = (spacetime_crd_t*) &credit_wc[i].imm_data;
            if(ENABLE_ASSERTIONS){
				assert(crd_ptr->opcode == ST_OP_CRD);
				assert(crd_ptr->val_credits <= MAX_CRDS_IN_MESSAGE);
                assert(REMOTE_MACHINES != 1 || crd_ptr->sender == REMOTE_MACHINES - machine_id);
			}
			credits[VAL_UD_QP_ID][crd_ptr->sender] += crd_ptr->val_credits;
			if(ENABLE_ASSERTIONS)
				assert(credits[VAL_UD_QP_ID][crd_ptr->sender] <= VAL_CREDITS);
			if(ENABLE_CREDIT_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[1m\033[32mVALs\033[0m \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",
					   worker_lid, credits[VAL_UD_QP_ID][crd_ptr->sender], crd_ptr->sender);
		}
	} else if(unlikely(credits_found < 0)) {
		printf("ERROR In the credit CQ\n");
		exit(0);
	}
}

static inline void forge_crd_wr(spacetime_val_t* val_recv_op, struct ibv_send_wr* send_crd_wr,
								struct hrd_ctrl_blk* cb, long long* send_crd_tx,
								uint16_t send_crd_packets, uint16_t worker_lid)
{
	static int total_completions = 0;
	struct ibv_wc signal_send_wc;
	uint16_t dst_worker_gid = (uint16_t) (val_recv_op->sender * WORKERS_PER_MACHINE + worker_lid);

    ///TODO may want to change / have variable CRDS in msg
//	spacetime_crd_t crd_tmp;
//	crd_tmp.opcode = ST_OP_CRD;
//	crd_tmp.sender = (uint8_t) machine_id;
//	crd_tmp.val_credits = MAX_CRDS_IN_MESSAGE;
//	memcpy(&send_crd_wr[send_crd_packets].imm_data, &crd_tmp, sizeof(spacetime_crd_t));
	((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits++;
	if(ENABLE_ASSERTIONS)
		assert(((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits <= MAX_CRDS_IN_MESSAGE);


	///TODO add code for non-inlining

	if(((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits == 1) {
		send_crd_wr[send_crd_packets].wr.ud.ah = remote_worker_qps[dst_worker_gid][CRD_UD_QP_ID].ah;
		send_crd_wr[send_crd_packets].wr.ud.remote_qpn = (uint32) remote_worker_qps[dst_worker_gid][CRD_UD_QP_ID].qpn;
		send_crd_wr[send_crd_packets].send_flags = IBV_SEND_INLINE;

		w_stats[worker_lid].issued_crds_per_worker++;

		if (send_crd_packets > 0)
			send_crd_wr[send_crd_packets - 1].next = &send_crd_wr[send_crd_packets];

		if (*send_crd_tx % CRD_SS_GRANULARITY == 0) {
			if (*send_crd_tx > 0) {
				hrd_poll_cq(cb->dgram_send_cq[CRD_UD_QP_ID], 1, &signal_send_wc);
				if (ENABLE_SS_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					red_printf("^^^ Polled SS completion[W%d]: \033[1m\033[36mCRD\033[0m %d (total %d)\n", worker_lid,
							   1, ++total_completions);
			}
			send_crd_wr[send_crd_packets].send_flags |= IBV_SEND_SIGNALED;
			if (ENABLE_SS_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				red_printf("vvv Send SS[W%d]: \033[1m\033[36mCRD\033[0m\n", worker_lid);
		}
		(*send_crd_tx)++; // Selective signaling
	}
}

static inline void post_val_recvs_and_batch_crds_2_NIC(struct ibv_send_wr *send_crd_wr, struct hrd_ctrl_blk* cb,
													   ud_req_val_t* incoming_vals, int* val_push_ptr,
													   uint16_t send_crd_packets, uint16_t worker_lid)
{
	static int total_crds_issued[WORKERS_PER_MACHINE]= { 0 }; ///TODO just for debugging
	int ret, j = 0;
	struct ibv_send_wr *bad_send_wr;

	send_crd_wr[send_crd_packets - 1].next = NULL;
	if(ENABLE_ASSERTIONS){
        for(j = 0; j < send_crd_packets - 1; j++){
			assert(send_crd_wr[j].next == &send_crd_wr[j+1]);
			assert(send_crd_wr[j].opcode == IBV_WR_SEND_WITH_IMM);
			assert(send_crd_wr[j].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(send_crd_wr[j].sg_list->length == 0);
			assert(send_crd_wr[j].num_sge == 0);
			assert(send_crd_wr[j].send_flags == IBV_SEND_INLINE ||
				   send_crd_wr[j].send_flags == IBV_SEND_INLINE | IBV_SEND_SIGNALED);
			assert(((spacetime_crd_t*) &(send_crd_wr[j].imm_data))->val_credits > 0);
			assert(((spacetime_crd_t*) &(send_crd_wr[j].imm_data))->opcode == ST_OP_CRD);
			assert(((spacetime_crd_t*) &(send_crd_wr[j].imm_data))->sender == machine_id);
			assert(((spacetime_crd_t*) &(send_crd_wr[j].imm_data))->val_credits <= MAX_CRDS_IN_MESSAGE);
		}
		assert(send_crd_wr[j].next == NULL);
	}

    //before sending acks post receives for INVs
	if(ENABLE_POST_RECV_PRINTS && ENABLE_VAL_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		yellow_printf("vvv Post Receives[W%d]: \033[1m\033[32mVALs\033[0m %d\n", worker_lid, send_crd_packets);
	post_receives(cb, send_crd_packets, ST_VAL_BUFF, incoming_vals, val_push_ptr);

	total_crds_issued[worker_lid] += send_crd_packets;
	if(ENABLE_SEND_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
		cyan_printf(">>> Send[W%d]: \033[1m\033[36mCRDs\033[0m %d (Total %d)\n", worker_lid, send_crd_packets, total_crds_issued[worker_lid]);
	ret = ibv_post_send(cb->dgram_qp[CRD_UD_QP_ID], &send_crd_wr[0], &bad_send_wr);
	CPE(ret, "ibv_post_send error while sending CRDs", ret);

	if(ENABLE_ASSERTIONS)
		assert(total_crds_issued[worker_lid] == w_stats[worker_lid].issued_crds_per_worker);
}

static inline void issue_credits(spacetime_val_t *val_recv_ops, long long int* send_crd_tx,
								 struct ibv_send_wr *send_crd_wr, uint8_t credits[][MACHINE_NUM],
								 struct hrd_ctrl_blk *cb, ud_req_val_t* incoming_vals,
								 int* val_push_ptr, uint16_t worker_lid, uint32_t* credit_debug_cnt)
{
	uint16_t i = 0, send_crd_packets = 0;


	//Reset data left from previous unicasts
	((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits = 0;

	for (i = 0; i < MAX_BATCH_OPS_SIZE * VAL_MAX_REQ_COALESCE; i++) {
		if (val_recv_ops[i].opcode == ST_EMPTY)
//		    break; ////TODO test this!! --> I don't think we need to traverse the whole array (MAX_BATCH_OPS_SIZE)
			continue;

		if(((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits == 0) {
			if (credits[CRD_UD_QP_ID][val_recv_ops[i].sender] == 0) {
				*(credit_debug_cnt)++;
				continue; //TODO we may need to push this to the top of the stack + (seems ok to batch this again to the KVS)
			}
			*credit_debug_cnt = 0;
			//reduce credits
			credits[CRD_UD_QP_ID][val_recv_ops[i].sender]--;
			if (ENABLE_CREDIT_PRINTS && ENABLE_CRD_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
				printf("$$$ Credits[W%d]: \033[1m\033[36mCRDs\033[0m \033[31mdecremented\033[0m to %d (for machine %d)\n",
					   worker_lid, credits[CRD_UD_QP_ID][val_recv_ops[i].sender],
					   val_recv_ops[i].sender);
		}

        //empty inv buffer
		if(val_recv_ops[i].opcode == ST_VAL_SUCCESS)
			val_recv_ops[i].opcode = ST_EMPTY;
		else{
            printf("Code: %s\n", code_to_str(val_recv_ops[i].opcode));
			assert(0);
		}

		// Create the broadcast messages
		forge_crd_wr(&val_recv_ops[i], send_crd_wr, cb,
					 send_crd_tx, send_crd_packets, worker_lid);

		if(((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits == MAX_CRDS_IN_MESSAGE){
			((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits = 1;
			send_crd_packets++;
			if (send_crd_packets == MAX_SEND_CRD_WRS) {
				post_val_recvs_and_batch_crds_2_NIC(send_crd_wr, cb, incoming_vals,
													val_push_ptr, send_crd_packets, worker_lid);
				send_crd_packets = 0;
			}
			//Reset data left from previous unicasts
			((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits = 0;
		}
	}

	if(((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits > 0 &&
		((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits < MAX_CRDS_IN_MESSAGE){
		((spacetime_crd_t*) &send_crd_wr[send_crd_packets].imm_data)->val_credits = 1;
		send_crd_packets++;
	}


	if (send_crd_packets > 0)
		post_val_recvs_and_batch_crds_2_NIC(send_crd_wr, cb, incoming_vals,
											val_push_ptr, send_crd_packets, worker_lid);

}


/* ---------------------------------------------------------------------------
------------------------------------OTHERS------------------------------------
---------------------------------------------------------------------------*/
static inline uint32_t refill_ops(uint32_t* trace_iter, uint16_t worker_lid,
								  struct spacetime_trace_command *trace,
								  spacetime_op_t *ops, uint32_t* refill_ops_debug_cnt, uint32_t* refilled_per_ops_debug_cnt)
{
    static uint8_t skip_reset_on_first_iter[WORKERS_PER_MACHINE] = { 0 };
    int i = 0, refilled_ops = 0;
    int first_empty_bucket = -1;
	if(skip_reset_on_first_iter[worker_lid] == 0)
		skip_reset_on_first_iter[worker_lid] = 1;
	else
		for(i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
			///TODO Add reordering / moving of uncompleted requests to first buckets of ops
			if(ENABLE_ASSERTIONS){
				assert(ops[i].opcode == ST_OP_PUT || ops[i].opcode == ST_OP_GET);
				assert(ops[i].state == ST_PUT_COMPLETE ||
					   ops[i].state == ST_GET_SUCCESS ||
					   ops[i].state == ST_PUT_SUCCESS ||
					   ops[i].state == ST_IN_PROGRESS_WRITE ||
					   ops[i].state == ST_PUT_STALL ||
					   ops[i].state == ST_GET_STALL);
			}

			if (ops[i].state == ST_PUT_COMPLETE || ops[i].state == ST_GET_SUCCESS) {
				if(ENABLE_REQ_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
					green_printf("W%d--> Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, value(len-%d): %c\n",
								 worker_lid, ((uint64_t *) &ops[i].key)[1],
								 code_to_str(ops[i].state), ops[i].version,
								 ops[i].tie_breaker_id, ops[i].val_len, ops[i].value[0]);
				ops[i].state = ST_EMPTY;
				ops[i].opcode = ST_EMPTY;
				w_stats[worker_lid].cache_hits_per_worker++;
				if(first_empty_bucket == -1)
					first_empty_bucket = i;
			}
		}

	for (i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		if (ops[i].opcode != ST_EMPTY){
			refilled_per_ops_debug_cnt[i]++;
			continue;
		}
		refilled_per_ops_debug_cnt[i] = 0;
		if(ENABLE_ASSERTIONS)
			assert(trace[*trace_iter].opcode == ST_OP_PUT || trace[*trace_iter].opcode == ST_OP_GET);

		refilled_ops++;
		ops[i].state = ST_NEW;
		ops[i].opcode = trace[*trace_iter].opcode;
		memcpy(&ops[i].key, &trace[*trace_iter].key_hash, sizeof(spacetime_key_t));

		if (ops[i].opcode == ST_OP_PUT)
			memset(ops[i].value, ((uint8_t) 'x' + machine_id), ST_VALUE_SIZE);
		ops[i].val_len = (uint8) (ops[i].opcode == ST_OP_PUT ? ST_VALUE_SIZE : 0);
		if(ENABLE_REQ_PRINTS &&  worker_lid < MAX_THREADS_TO_PRINT)
			red_printf("W%d--> Op: %s, hash(1st 8B):%" PRIu64 "\n",
					   worker_lid, code_to_str(ops[i].opcode), ((uint64_t *) &ops[i].key)[1]);
		HRD_MOD_ADD(*trace_iter, TRACE_SIZE);
	}

	if(refilled_ops == 0)
		(*refill_ops_debug_cnt)++;
	else
		*refill_ops_debug_cnt = 0;
}
#endif //HERMES_INLINE_UTIL_H
