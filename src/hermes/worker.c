#include <spacetime.h>
#include <optik_mod.h>
#include <time.h>
#include "util.h"
#include "inline-util.h"

///
#include "time_rdtsc.h"
///

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

	//Used to register the intermidiate send_packet_buffs to the NIC iff inlining is disabled
	struct ibv_mr *inv_mr = NULL;
	struct ibv_mr *ack_mr = NULL;
	struct ibv_mr *val_mr = NULL;
	if(DISABLE_INV_INLINING) inv_mr = register_buffer(cb->pd, inv_send_packet_ops, INV_SEND_OPS_SIZE * sizeof(spacetime_inv_packet_t));
	if(DISABLE_ACK_INLINING) ack_mr = register_buffer(cb->pd, ack_send_packet_ops, ACK_SEND_OPS_SIZE * sizeof(spacetime_ack_packet_t));
	if(DISABLE_VAL_INLINING) val_mr = register_buffer(cb->pd, val_send_packet_ops, VAL_SEND_OPS_SIZE * sizeof(spacetime_val_packet_t));

	int inv_push_recv_ptr = 0, inv_pull_recv_ptr = -1,
		ack_push_recv_ptr = 0, ack_pull_recv_ptr = -1,
		val_push_recv_ptr = 0, val_pull_recv_ptr = -1;
	int inv_push_send_ptr = 0, ack_push_send_ptr =  0, val_push_send_ptr = 0;
	spacetime_group_membership last_group_membership = *((spacetime_group_membership*) &group_membership);
	struct spacetime_trace_command *trace;
	trace_init(&trace, worker_gid);
	setup_recv_WRs(inv_recv_wr,inv_recv_sgl,ack_recv_wr,ack_recv_sgl,val_recv_wr,val_recv_sgl,cb);

    /* Post receives, we need to do th is early */
    if(WRITE_RATIO > 0)
		setup_incoming_buffs_and_post_initial_recvs(incoming_invs, incoming_acks, incoming_vals, &inv_push_recv_ptr,
													&ack_push_recv_ptr, &val_push_recv_ptr, inv_recv_wr,
													ack_recv_wr, val_recv_wr, crd_recv_wr, cb, worker_lid);
	setup_qps(worker_gid, cb);
	setup_credits(credits, cb, crd_send_wr, &send_crd_sgl, crd_recv_wr, &recv_crd_sgl);
    setup_send_WRs(inv_send_wr, inv_send_sgl, ack_send_wr, ack_send_sgl, val_send_wr,
				   val_send_sgl, inv_mr, ack_mr, val_mr, worker_lid);

	int invs_polled = 0, acks_polled = 0, vals_polled = 0;
	int node_suspected = -1;
	uint32_t credits_missing[MACHINE_NUM];
	uint32_t num_of_iters_serving_op[MAX_BATCH_OPS_SIZE] = {0};
	uint32_t trace_iter = 0;
	long long int inv_br_tx = 0, val_br_tx = 0, send_ack_tx = 0, send_crd_tx = 0;
	uint16_t rolling_inv_index = 0;
	uint8_t has_outstanding_vals = 0, has_outstanding_vals_from_memb_change = 0;

//	if(INCREASE_TAIL_LATENCY) {
		//Latency enhancement
		init_rdtsc();
		struct timespec time_received_msg;
		get_rdtsc_timespec(&time_received_msg);
//		long long time_received_msg = hrd_get_cycles();
		uint8_t has_msg_stalled = 0;
		spacetime_ack_t stalled_ack;
		uint64_t total_acks_polled = 0;
//	}
	/* -----------------------------------------------------
       ------------------------Main Loop--------------------
	   ----------------------------------------------------- */
	while (true) {

//		if(unlikely(w_stats[worker_lid].total_loops % M_4 == M_4 - 1)){
//			for(int k = 0; k < MAX_BATCH_OPS_SIZE; k++)
//				printf("Op[%d]:%s(%" PRIu64 ")--> state: %s\n", k, code_to_str(ops[k].opcode), ((uint64_t *) &ops[k].key)[0], code_to_str(ops[k].state));
//		}
		node_suspected = refill_ops_n_suspect_failed_nodes(&trace_iter, worker_lid, trace, ops,
														   num_of_iters_serving_op, last_group_membership);

		batch_ops_to_KVS(MAX_BATCH_OPS_SIZE, &ops, worker_lid, last_group_membership);

		if (WRITE_RATIO > 0) {
			///~~~~~~~~~~~~~~~~~~~~~~INVS~~~~~~~~~~~~~~~~~~~~~~~~~~~
			broadcast_invs(ops, inv_send_packet_ops, &inv_push_send_ptr, inv_send_wr,
						   inv_send_sgl, credits, cb, &inv_br_tx, worker_lid,
						   last_group_membership, credits_missing, &rolling_inv_index);

			///Poll for INVs
			poll_buff_and_post_recvs(incoming_invs, ST_INV_BUFF, &inv_pull_recv_ptr, inv_recv_ops,
									 &invs_polled, cb->dgram_recv_cq[INV_UD_QP_ID], inv_recv_wc, cb,
									 &inv_push_recv_ptr, inv_recv_wr, last_group_membership, credits, worker_lid);

			if(invs_polled > 0) {
				batch_invs_to_KVS(invs_polled, &inv_recv_ops, ops, worker_lid,
								  &node_suspected, num_of_iters_serving_op);

				///~~~~~~~~~~~~~~~~~~~~~~ACKS~~~~~~~~~~~~~~~~~~~~~~~~~~~
				issue_acks(inv_recv_ops, ack_send_packet_ops, &ack_push_send_ptr, &send_ack_tx,
						   ack_send_wr, ack_send_sgl, credits, cb, worker_lid);
				invs_polled = 0;
			}

			if(has_outstanding_vals == 0 && has_outstanding_vals_from_memb_change == 0) {
				///Poll for Acks
				poll_buff_and_post_recvs(incoming_acks, ST_ACK_BUFF, &ack_pull_recv_ptr, ack_recv_ops,
										 &acks_polled, cb->dgram_recv_cq[ACK_UD_QP_ID], ack_recv_wc, cb,
										 &ack_push_recv_ptr, ack_recv_wr, last_group_membership, credits, worker_lid);

				if (acks_polled > 0) {
					if(worker_lid < NUM_OF_CORES_TO_INCREASE_TAIL && INCREASE_TAIL_LATENCY){
						if(has_msg_stalled == 0) {
							total_acks_polled += acks_polled; //might need to move this before this if
							if (acks_polled > 1 && total_acks_polled > INCREASE_TAIL_EVERY_X_ACKS) {
								total_acks_polled = 0;
								has_msg_stalled = 1;
								get_rdtsc_timespec(&time_received_msg);
//								yellow_printf("ACK Stalled\n");
								memcpy(&stalled_ack, &ack_recv_ops[acks_polled - 1], sizeof(spacetime_ack_t));
								ack_recv_ops[acks_polled - 1].opcode = ST_EMPTY;
								acks_polled--;
								///Might also need to increase / decrease inv credits
							}
                        }else if(time_elapsed_in_ms(time_received_msg) > INCREASE_TAIL_BY_MS && acks_polled < MAX_BATCH_OPS_SIZE - 1){
								memcpy(&ack_recv_ops[acks_polled], &stalled_ack, sizeof(spacetime_ack_t));
//								green_printf("ACK Unstalled after ms: %2.f\n", time_elapsed_in_ms(time_received_msg));
								has_msg_stalled = 0;
								acks_polled++;
						}
					}
                    if(acks_polled > 0)
						batch_acks_to_KVS(acks_polled, &ack_recv_ops, ops, last_group_membership, worker_lid);

					acks_polled = 0;
				}
			}

			if(!DISABLE_VALS_FOR_DEBUGGING) {
				///~~~~~~~~~~~~~~~~~~~~~~ VALs ~~~~~~~~~~~~~~~~~~~~~~~~~~~
				if(has_outstanding_vals_from_memb_change > 0)
					has_outstanding_vals_from_memb_change = broadcast_vals_on_membership_change(ops, val_send_packet_ops, &val_push_send_ptr,
																									  val_send_wr, val_send_sgl, credits, cb, crd_recv_wc,
																									  last_group_membership, &val_br_tx, crd_recv_wr, worker_lid);
				else
					has_outstanding_vals = broadcast_vals(ack_recv_ops, val_send_packet_ops, &val_push_send_ptr,
														  val_send_wr, val_send_sgl, credits, cb, crd_recv_wc,
														  last_group_membership, &val_br_tx, crd_recv_wr, worker_lid);

				///Poll for Vals
				poll_buff_and_post_recvs(incoming_vals, ST_VAL_BUFF, &val_pull_recv_ptr, val_recv_ops,
										 &vals_polled,  cb->dgram_recv_cq[VAL_UD_QP_ID], val_recv_wc, cb,
										 &val_push_recv_ptr, val_recv_wr, last_group_membership, credits, worker_lid);

				if (vals_polled > 0) {
					batch_vals_to_KVS(vals_polled, &val_recv_ops, ops, worker_lid);

					///~~~~~~~~~~~~~~~~~~~~~~CREDITS~~~~~~~~~~~~~~~~~~~~~~~~~~~
					issue_credits(val_recv_ops, &send_crd_tx, crd_send_wr, credits, cb, worker_lid);
					vals_polled = 0;
				}
			}

            ///Emulating a perfect failure detector via a group membership
			if (unlikely(node_suspected >= 0 && worker_lid == WORKER_EMULATING_FAILURE_DETECTOR))
				follower_removal(node_suspected);


			if(group_membership_has_changed(&last_group_membership, worker_lid) == 1){
			    printf("Reconfiguring group membership\n");
				reconfigure_wrs(inv_send_wr, inv_send_sgl, val_send_wr, val_send_sgl,
								last_group_membership, worker_lid);
				reset_bcast_send_buffers(inv_send_packet_ops, &inv_push_send_ptr,
										 val_send_packet_ops, &val_push_send_ptr);
				complete_writes_and_replays_on_follower_removal(MAX_BATCH_OPS_SIZE, &ops,
																last_group_membership, worker_lid);
				has_outstanding_vals_from_memb_change = broadcast_vals_on_membership_change(ops, val_send_packet_ops,
																							&val_push_send_ptr, val_send_wr,
																							val_send_sgl, credits, cb,
																							crd_recv_wc, last_group_membership,
																							&val_br_tx, crd_recv_wr, worker_lid);
				//reset counter for failure suspicion
				memset(num_of_iters_serving_op, 0, sizeof(uint32_t) * MAX_BATCH_OPS_SIZE);
			}
		}
		w_stats[worker_lid].total_loops++;
	}
	return NULL;
}


