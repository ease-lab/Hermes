#include <spacetime.h>
#include "util.h"
#include "inline-util.h"

void *run_worker(void *arg){
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t worker_lid = (uint16_t) params.id;	/* Local ID of this worker thread*/
	uint16_t worker_gid = (uint16_t) (machine_id * WORKERS_PER_MACHINE + params.id);	/* Global ID of this worker thread*/

	int *recv_q_depths, *send_q_depths;
	setup_q_depths(&recv_q_depths, &send_q_depths);
	struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(worker_gid,	/* local_hid */
												0, -1, /* port_index, numa_node_id */
												0, 0,	/* #conn qps, uc */
												NULL, 0, -1,	/* prealloc conn buf, buf size, key */
												TOTAL_WORKER_UD_QPs, DGRAM_BUFF_SIZE,	/* num_dgram_qps, dgram_buf_size */
												BASE_SHM_KEY + worker_lid, /* key */
												recv_q_depths, send_q_depths); /* Depth of the dgram RECV, SEND Q*/

	/* -----------------------------------------------------
	--------------DECLARATIONS------------------------------
	---------------------------------------------------------*/
	///Buffs where reqs arrive
	ud_req_inv_t *incoming_invs = (ud_req_inv_t *) cb->dgram_buf;
	ud_req_ack_t *incoming_acks = (ud_req_ack_t *) &cb->dgram_buf[INV_RECV_REQ_SIZE * RECV_INV_Q_DEPTH];
	ud_req_val_t *incoming_vals = (ud_req_val_t *) &cb->dgram_buf[INV_RECV_REQ_SIZE * RECV_INV_Q_DEPTH +
																  ACK_RECV_REQ_SIZE * RECV_ACK_Q_DEPTH];
	///Send declarations
	struct ibv_send_wr inv_send_wr[MAX_SEND_INV_WRS],
			           ack_send_wr[MAX_SEND_ACK_WRS],
			           val_send_wr[MAX_SEND_VAL_WRS],
			           crd_send_wr[MAX_SEND_CRD_WRS];

	struct ibv_sge     inv_send_sgl[MAX_PCIE_BCAST_BATCH],
					   ack_send_sgl[MAX_SEND_ACK_WRS],
			           val_send_sgl[MAX_PCIE_BCAST_BATCH], send_crd_sgl;

	uint8_t credits[TOTAL_WORKER_UD_QPs][MACHINE_NUM];

	///Receive declarations
	//Used only to batch post recvs to the NIC
	struct ibv_recv_wr inv_recv_wr[MAX_RECV_INV_WRS],
			           ack_recv_wr[MAX_RECV_ACK_WRS],
			           val_recv_wr[MAX_RECV_VAL_WRS],
			           crd_recv_wr[MAX_RECV_CRD_WRS];

    //Used only to batch post recvs to the NIC
	struct ibv_sge 	   inv_recv_sgl[MAX_RECV_INV_WRS],
			           ack_recv_sgl[MAX_RECV_ACK_WRS],
			           val_recv_sgl[MAX_RECV_VAL_WRS], recv_crd_sgl;

	//Used on polling recv req cq (only for immediates)
	struct ibv_wc      inv_recv_wc[MAX_RECV_INV_WRS],
			           ack_recv_wc[MAX_RECV_ACK_WRS],
			           val_recv_wc[MAX_RECV_VAL_WRS],
			           crd_recv_wc[MAX_RECV_CRD_WRS];

	//Intermediate buffs where reqs are copied from incoming_* buffs in order to get passed to the KVS
	spacetime_op_t *ops;
	spacetime_inv_t *inv_recv_ops;
	spacetime_ack_t *ack_recv_ops;
	spacetime_val_t *val_recv_ops;

	//Intermediate buffs where reqs are copied in order to get send to the remote side
	spacetime_inv_packet_t *inv_send_packet_ops;
	spacetime_ack_packet_t *ack_send_packet_ops;
	spacetime_val_packet_t *val_send_packet_ops;

	setup_ops(&ops, &inv_recv_ops, &ack_recv_ops,
			  &val_recv_ops, &inv_send_packet_ops,
			  &ack_send_packet_ops, &val_send_packet_ops);

	//Used to register the intermidiate send_packet_buffs to the NIC when inlining is disabled
	struct ibv_mr *inv_mr, *ack_mr, *val_mr;
	if(DISABLE_INV_INLINING) inv_mr = register_buffer(cb->pd, inv_send_packet_ops, INV_SEND_OPS_SIZE * sizeof(spacetime_inv_packet_t));
	if(DISABLE_ACK_INLINING) ack_mr = register_buffer(cb->pd, ack_send_packet_ops, ACK_SEND_OPS_SIZE * sizeof(spacetime_ack_packet_t));
	if(DISABLE_VAL_INLINING) val_mr = register_buffer(cb->pd, val_send_packet_ops, VAL_SEND_OPS_SIZE * sizeof(spacetime_val_packet_t));

	int inv_push_recv_ptr = 0, inv_pull_recv_ptr = -1,
		ack_push_recv_ptr = 0, ack_pull_recv_ptr = -1,
		val_push_recv_ptr = 0, val_pull_recv_ptr = -1;
	int inv_push_send_ptr = 0, ack_push_send_ptr =  0, val_push_send_ptr = 0;

	setup_recv_WRs(inv_recv_wr,inv_recv_sgl,ack_recv_wr,ack_recv_sgl,val_recv_wr,val_recv_sgl,cb);
    /* Post receives, we need to do this early */
    if(WRITE_RATIO > 0)
		setup_incoming_buffs_and_post_initial_recvs(incoming_invs, incoming_acks, incoming_vals, &inv_push_recv_ptr,
													&ack_push_recv_ptr, &val_push_recv_ptr, inv_recv_wr,
													ack_recv_wr, val_recv_wr, crd_recv_wr, cb, worker_lid);

	setup_qps(worker_gid, cb);
	setup_credits(credits, cb, crd_send_wr, &send_crd_sgl, crd_recv_wr, &recv_crd_sgl);
    setup_send_WRs(inv_send_wr, inv_send_sgl, ack_send_wr, ack_send_sgl, val_send_wr,
				   val_send_sgl, inv_mr, ack_mr, val_mr, worker_lid);

	int i = 0, inv_ops_i = 0, ack_ops_i = 0, val_ops_i = 0;
	uint32_t refilled_ops_debug_cnt[MAX_BATCH_OPS_SIZE] = {0};///TODO only for debug
	uint32_t trace_iter = 0, credit_debug_cnt = 0, refill_ops_debug_cnt = 0;
	long long int rolling_iter = 0; /* For throughput measurement */
	long long int inv_br_tx = 0, val_br_tx = 0, send_ack_tx = 0, send_crd_tx = 0;
	uint16_t rolling_inv_index = 0;
	uint8_t has_outstanding_vals = 0;
	struct spacetime_trace_command *trace;
	trace_init(&trace, worker_gid);

	/* -----------------------------------------------------
	--------------Start the main Loop-----------------------
	---------------------------------------------------------*/
	while (true) {
		if (unlikely(credit_debug_cnt > M_1)) {
			red_printf("Worker %d misses credits \n", worker_lid);
			red_printf("Inv Credits %d, Ack credits %d, Val credits %d, CRD credits %d\n",
					   credits[INV_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   credits[ACK_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   credits[VAL_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   credits[CRD_UD_QP_ID][(machine_id + 1) % MACHINE_NUM]);
			credit_debug_cnt = 0;
		}
		if (unlikely(refill_ops_debug_cnt > M_4)) {
			red_printf("Worker %d is stacked \n", worker_lid);
			if(w_stats[worker_lid].issued_invs_per_worker != w_stats[worker_lid].received_acks_per_worker)
				red_printf("\tCoordinator: issued_invs: %d received acks: %d\n",
						   w_stats[worker_lid].issued_invs_per_worker, w_stats[worker_lid].received_acks_per_worker);
			if(w_stats[worker_lid].received_invs_per_worker != w_stats[worker_lid].issued_acks_per_worker)
				red_printf("\tFollower:    received invs: %d issued acks: %d\n",
						   w_stats[worker_lid].received_invs_per_worker, w_stats[worker_lid].issued_acks_per_worker);
			refill_ops_debug_cnt = 0;
		}

		refill_ops(&trace_iter, worker_lid, trace, ops, &refill_ops_debug_cnt, refilled_ops_debug_cnt);

		if(ENABLE_ASSERTIONS)
			for(i = 0; i < MAX_BATCH_OPS_SIZE; i++)
				assert(ops[i].opcode == ST_OP_PUT || ops[i].opcode == ST_OP_GET);

		batch_ops_to_KVS(MAX_BATCH_OPS_SIZE, &ops, worker_lid, refill_ops_debug_cnt, refilled_ops_debug_cnt);


		if (WRITE_RATIO > 0) {
			///~~~~~~~~~~~~~~~~~~~~~~INVS~~~~~~~~~~~~~~~~~~~~~~~~~~~
			broadcasts_invs(ops, inv_send_packet_ops, &inv_push_send_ptr, inv_send_wr,
							inv_send_sgl, credits, cb, &inv_br_tx,
							worker_lid, &credit_debug_cnt, &rolling_inv_index);

			if(ENABLE_ASSERTIONS)
				for(i = 0; i < MAX_BATCH_OPS_SIZE; i++)
					assert(ops[i].state == ST_BUFFERED_IN_PROGRESS_REPLAY ||
						   ops[i].state == ST_IN_PROGRESS_WRITE ||
						   ops[i].state == ST_PUT_SUCCESS ||
						   ops[i].state == ST_PUT_STALL ||
						   ops[i].opcode == ST_OP_GET);

			///Poll for INVs
			poll_buff_and_post_recvs(incoming_invs, ST_INV_BUFF, &inv_pull_recv_ptr, inv_recv_ops,
									 &inv_ops_i, cb->dgram_recv_cq[INV_UD_QP_ID], inv_recv_wc, cb,
									 &inv_push_recv_ptr, inv_recv_wr, credits, worker_lid);

			if(inv_ops_i > 0) {
				batch_invs_to_KVS(inv_ops_i, &inv_recv_ops, worker_lid);

				///~~~~~~~~~~~~~~~~~~~~~~ACKS~~~~~~~~~~~~~~~~~~~~~~~~~~~
				issue_acks(inv_recv_ops, ack_send_packet_ops, &ack_push_send_ptr, &send_ack_tx, ack_send_wr,
						   ack_send_sgl, credits, cb, worker_lid, &credit_debug_cnt);
				if(ENABLE_ASSERTIONS)
					for(i = 0; i < INV_RECV_OPS_SIZE; i++)
						assert(inv_recv_ops[i].opcode == ST_EMPTY);
				inv_ops_i = 0;
			}

			if(has_outstanding_vals == 0)
				///Poll for Acks
				poll_buff_and_post_recvs(incoming_acks, ST_ACK_BUFF, &ack_pull_recv_ptr, ack_recv_ops,
										 &ack_ops_i, cb->dgram_recv_cq[ACK_UD_QP_ID], ack_recv_wc, cb,
										 &ack_push_recv_ptr, ack_recv_wr, credits, worker_lid);
			else
				has_outstanding_vals = broadcasts_vals(ack_recv_ops, val_send_packet_ops, &val_push_send_ptr,
													   val_send_wr, val_send_sgl, credits, cb, crd_recv_wc,
													   &credit_debug_cnt, &val_br_tx, crd_recv_wr, worker_lid);

			if(ack_ops_i > 0){
				batch_acks_to_KVS(ack_ops_i, &ack_recv_ops, ops, worker_lid);
				if(ENABLE_ASSERTIONS)
					for(i = 0; i < MAX_BATCH_OPS_SIZE; i++)
						assert(ops[i].state == ST_BUFFERED_IN_PROGRESS_REPLAY ||
							   ops[i].state == ST_IN_PROGRESS_WRITE ||
							   ops[i].state == ST_PUT_SUCCESS ||
							   ops[i].state == ST_PUT_COMPLETE ||
							   ops[i].state == ST_PUT_STALL ||
							   ops[i].opcode == ST_OP_GET);

				///~~~~~~~~~~~~~~~~~~~~~~VALS~~~~~~~~~~~~~~~~~~~~~~~~~~~
				if(!DISABLE_VALS_FOR_DEBUGGING)
					has_outstanding_vals = broadcasts_vals(ack_recv_ops, val_send_packet_ops, &val_push_send_ptr,
														   val_send_wr, val_send_sgl, credits, cb, crd_recv_wc,
														   &credit_debug_cnt, &val_br_tx, crd_recv_wr, worker_lid);
                if(ENABLE_ASSERTIONS && has_outstanding_vals == 0)
					for(i = 0; i < ACK_RECV_OPS_SIZE; i++)
						assert(ack_recv_ops[i].opcode == ST_EMPTY);
				ack_ops_i = 0;
			}
			if(!DISABLE_VALS_FOR_DEBUGGING) {
				///Poll for Vals
				poll_buff_and_post_recvs(incoming_vals, ST_VAL_BUFF, &val_pull_recv_ptr, val_recv_ops,
										 &val_ops_i,  cb->dgram_recv_cq[VAL_UD_QP_ID], val_recv_wc, cb,
										 &val_push_recv_ptr, val_recv_wr, credits, worker_lid);

				if (val_ops_i > 0) {
					batch_vals_to_KVS(val_ops_i, &val_recv_ops, worker_lid);

					///~~~~~~~~~~~~~~~~~~~~~~CREDITS~~~~~~~~~~~~~~~~~~~~~~~~~~~
					issue_credits(val_recv_ops, &send_crd_tx, crd_send_wr,
								  credits, cb, worker_lid, &credit_debug_cnt);
					if(ENABLE_ASSERTIONS )
						for(i = 0; i < VAL_RECV_OPS_SIZE; i++)
							assert(val_recv_ops[i].opcode == ST_EMPTY);
					val_ops_i = 0;
				}
			}
		}
		rolling_iter++;
	}
	return NULL;
}

