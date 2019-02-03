#include <spacetime.h>
#include <concur_ctrl.h>
#include <time.h>
#include "util.h"
#include "inline-util.h"

///
#include "time_rdtsc.h"
#include "../aether/ud-wrapper.h"
///

int
inv_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
		assert(is_input_code(op_req->op_meta.opcode));
	}

	if(op_req->op_meta.state != ST_PUT_SUCCESS &&
	   op_req->op_meta.state != ST_REPLAY_SUCCESS &&
	   op_req->op_meta.state != ST_OP_MEMBERSHIP_CHANGE)
		return -1;
	return 0; // since inv is a bcast we can return any int other than -1
}

void
inv_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(op_req->op_meta.state == ST_PUT_SUCCESS)
		op_req->op_meta.state = ST_IN_PROGRESS_PUT;
	else if(op_req->op_meta.state == ST_REPLAY_SUCCESS)
	    op_req->op_meta.state = ST_IN_PROGRESS_REPLAY;
	else if(op_req->op_meta.state == ST_OP_MEMBERSHIP_CHANGE)
	    op_req->op_meta.state = ST_OP_MEMBERSHIP_COMPLETE;
	else{
		printf("state: %d\n", op_req->op_meta.state);
		printf("state: %s\n", code_to_str(op_req->op_meta.state));
		assert(0);
	}
}

void
inv_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(inv_to_send, op, sizeof(spacetime_inv_t));
	inv_to_send->op_meta.sender = (uint8_t) machine_id;
	inv_to_send->op_meta.opcode = ST_OP_INV;
//	//TODO change to include membership change
//	inv_to_send->op_meta.opcode = (uint8_t) (op->op_meta.state == ST_OP_MEMBERSHIP_CHANGE ?
//											 ST_OP_MEMBERSHIP_CHANGE : ST_OP_INV);
}


int
ack_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;
	if (inv_req->op_meta.opcode == ST_EMPTY)
		return -1;
	return inv_req->op_meta.sender;
}

void
ack_modify_elem_after_send(uint8_t* req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;

	//empty inv buffer
	if(inv_req->op_meta.opcode == ST_INV_SUCCESS ||
	   inv_req->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE)
		inv_req->op_meta.opcode = ST_EMPTY;
	else assert(0);
}


void
ack_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_ack_t* ack_to_send = (spacetime_ack_t *) msg_to_send;
	memcpy(ack_to_send, triggering_req, sizeof(spacetime_ack_t)); // copy req to next_req_ptr
	ack_to_send->sender = (uint8_t) machine_id;
	ack_to_send->opcode = ST_OP_ACK;
}


int
val_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_ack_t* ack_req = (spacetime_ack_t *) req;
	if (ack_req->opcode == ST_ACK_SUCCESS || ack_req->opcode == ST_OP_MEMBERSHIP_CHANGE) {
		ack_req->opcode = ST_EMPTY;
		return -1;
	} else if (ack_req->opcode == ST_EMPTY)
		return -1;

	if(ENABLE_ASSERTIONS)
		assert(ack_req->opcode == ST_LAST_ACK_SUCCESS);

	return ack_req->sender;
}

void val_modify_elem_after_send(uint8_t* req) {}

void
val_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_val_t* val_to_send = (spacetime_val_t *) msg_to_send;

	memcpy(val_to_send, triggering_req, sizeof(spacetime_val_t)); // copy req to next_req_ptr
	val_to_send->opcode = ST_OP_VAL;
	val_to_send->sender = (uint8_t) machine_id;
}


void*
run_worker(void *arg)
{
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
	struct ibv_send_wr crd_send_wr[MAX_SEND_CRD_WRS];

	struct ibv_sge     send_crd_sgl;

	uint8_t credits[TOTAL_WORKER_UD_QPs][MACHINE_NUM];

	///Receive declarations
	//Used only to batch post recvs to the NIC
	struct ibv_recv_wr crd_recv_wr[MAX_RECV_CRD_WRS];

    //Used only to batch post recvs to the NIC
	struct ibv_sge recv_crd_sgl;

	//Used on polling recv req cq (only for immediates)
	struct ibv_wc crd_recv_wc[MAX_RECV_CRD_WRS];

	//Intermediate buffs where reqs are copied from incoming_* buffs in order to get passed to the KVS
	spacetime_op_t  *ops;
	spacetime_inv_t *inv_recv_ops;
	spacetime_ack_t *ack_recv_ops;
	spacetime_val_t *val_recv_ops;

	aether_setup_ops(&ops, &inv_recv_ops, &ack_recv_ops, &val_recv_ops);



	spacetime_group_membership last_group_membership = group_membership;

	struct spacetime_trace_command *trace;
	trace_init(&trace, worker_gid);

	setup_qps(worker_gid, cb);
	setup_credits(credits, cb, crd_send_wr, &send_crd_sgl, crd_recv_wr, &recv_crd_sgl);

	uint16_t invs_polled = 0, acks_polled = 0, vals_polled = 0;
	int node_suspected = -1;
	uint32_t num_of_iters_serving_op[MAX_BATCH_OPS_SIZE] = {0};
	uint32_t trace_iter = 0;
	long long int send_crd_tx = 0;
	uint16_t rolling_inv_index = 0, ignore_rolling_index = 0;
	uint8_t has_outstanding_vals = 0, has_outstanding_vals_from_memb_change = 0;

	////
	spacetime_op_t* n_hottest_keys_in_ops_get[COALESCE_N_HOTTEST_KEYS];
	spacetime_op_t* n_hottest_keys_in_ops_put[COALESCE_N_HOTTEST_KEYS];
	for(int i = 0; i < COALESCE_N_HOTTEST_KEYS; ++i){
		n_hottest_keys_in_ops_get[i] = NULL;
		n_hottest_keys_in_ops_put[i] = NULL;
	}
	////


////// <AETHER>

    qp_info_t inv_remote_qps[MACHINE_NUM], ack_remote_qps[MACHINE_NUM],
			  val_remote_qps[MACHINE_NUM]; //    , crd_remote_qps[MACHINE_NUM];

    for(uint16_t i = 0; i < MACHINE_NUM; ++i){
    	uint16_t idx = (uint16_t) (i * WORKERS_PER_MACHINE + worker_lid);
		inv_remote_qps[i].ah = remote_worker_qps[idx][INV_UD_QP_ID].ah;
		ack_remote_qps[i].ah = remote_worker_qps[idx][ACK_UD_QP_ID].ah;
		val_remote_qps[i].ah = remote_worker_qps[idx][VAL_UD_QP_ID].ah;
//		crd_remote_qps[i].ah = remote_worker_qps[idx][CRD_UD_QP_ID].ah;
		inv_remote_qps[i].qpn = (uint32_t) remote_worker_qps[idx][INV_UD_QP_ID].qpn;
		ack_remote_qps[i].qpn = (uint32_t) remote_worker_qps[idx][ACK_UD_QP_ID].qpn;
		val_remote_qps[i].qpn = (uint32_t) remote_worker_qps[idx][VAL_UD_QP_ID].qpn;
//		crd_remote_qps[i].qpn = (uint32_t) remote_worker_qps[idx][CRD_UD_QP_ID].qpn;
    }

	ud_channel_t ack_ud_c, inv_ud_c, val_ud_c, crd_ud_c;

	aether_ud_channel_init(cb, &inv_ud_c, INV_UD_QP_ID, "\033[31mINV\033[0m", REQ, INV_MAX_REQ_COALESCE,
						   sizeof(spacetime_inv_t), (uint8_t *) incoming_invs, DISABLE_INV_INLINING == 0 ? 1 : 0,
						   1, inv_remote_qps, 0, &ack_ud_c, INV_CREDITS, MACHINE_NUM, 1, 1);
	aether_ud_channel_init(cb, &ack_ud_c, ACK_UD_QP_ID, "\033[33mACK\033[0m", RESP, ACK_MAX_REQ_COALESCE,
						   sizeof(spacetime_ack_t), (uint8_t *) incoming_acks, DISABLE_ACK_INLINING == 0 ? 1 : 0,
						   0, ack_remote_qps, 0, &inv_ud_c, ACK_CREDITS, MACHINE_NUM, 1, 1);
	aether_ud_channel_init(cb, &val_ud_c, VAL_UD_QP_ID, "\033[1m\033[32mVAL\033[0m", REQ, VAL_MAX_REQ_COALESCE,
						   sizeof(spacetime_val_t), (uint8_t *) incoming_vals, DISABLE_VAL_INLINING == 0 ? 1 : 0,
						   1, val_remote_qps, 1, &crd_ud_c, VAL_CREDITS, MACHINE_NUM, 1, 1);
//	aether_ud_channel_init(cb, &crd_ud_c, ACK_UD_QP_ID, "\033[1m\033[36mCRD\033[0m", SEND_RECV, ACK_MAX_REQ_COALESCE,
//						   sizeof(spacetime_crd_t), (uint8_t *) incoming_crd, DISABLE_ACK_INLINING == 0 ? 1 : 0,
//						   0, ack_remote_qps, 0, &inv_ud_c, ACK_CREDITS, REMOTE_MACHINES, 1, 1);

	///WARNING: we need to post initial receives early to avoid races between them and the sends of other nodes
	aether_setup_incoming_buff_and_post_initial_recvs(&inv_ud_c, cb);
	aether_setup_incoming_buff_and_post_initial_recvs(&ack_ud_c, cb);
	aether_setup_incoming_buff_and_post_initial_recvs(&val_ud_c, cb);
//	aether_setup_incoming_buff_and_post_initial_recvs(&crd_ud_c, cb);

	///WARNING: give some leeway to post receives (before start bcasting)
	sleep(1);

////// </AETHER>
	print_ud_c_overview(&inv_ud_c);

	/* -----------------------------------------------------
       ------------------------Main Loop--------------------
	   ----------------------------------------------------- */
	while (true) {

	    if(unlikely(w_stats[worker_lid].total_loops % M_16 == 0)){
	        //Check something periodically
//			printf("Send: invs: %lu, acks: %lu\n", inv_ud_c.stats.send_total_msgs, ack_ud_c.stats.send_total_msgs);
//			printf("Recv: invs: %lu, acks: %lu\n", inv_ud_c.stats.recv_total_msgs, ack_ud_c.stats.recv_total_msgs);
	    }

		node_suspected = refill_ops_n_suspect_failed_nodes(&trace_iter, worker_lid, trace, ops,
														   num_of_iters_serving_op, last_group_membership,
														   n_hottest_keys_in_ops_get, n_hottest_keys_in_ops_put);
		batch_ops_to_KVS(MAX_BATCH_OPS_SIZE, &ops, worker_lid, last_group_membership);

		if (WRITE_RATIO > 0) {
			///~~~~~~~~~~~~~~~~~~~~~~INVS~~~~~~~~~~~~~~~~~~~~~~~~~~~
			aether_issue_pkts(&inv_ud_c, cb, (uint8_t *) ops,
							  MAX_BATCH_OPS_SIZE, sizeof(spacetime_op_t), &rolling_inv_index,
							  inv_skip_or_get_sender_id, inv_modify_elem_after_send, inv_copy_and_modify_elem);

			///Poll for INVs
			invs_polled = aether_poll_buff_and_post_recvs(&inv_ud_c, INV_RECV_OPS_SIZE,
														  (uint8_t *) inv_recv_ops, cb);

			if(invs_polled > 0) {
				batch_invs_to_KVS(invs_polled, &inv_recv_ops, ops, worker_lid,
								  &node_suspected, num_of_iters_serving_op);

				///~~~~~~~~~~~~~~~~~~~~~~ACKS~~~~~~~~~~~~~~~~~~~~~~~~~~~
				aether_issue_pkts(&ack_ud_c, cb, (uint8_t *) inv_recv_ops,
								  invs_polled, sizeof(spacetime_inv_t), &ignore_rolling_index,
								  ack_skip_or_get_sender_id, ack_modify_elem_after_send, ack_copy_and_modify_elem);
				invs_polled = 0;
				ignore_rolling_index = 0;
			}

			if(has_outstanding_vals == 0 && has_outstanding_vals_from_memb_change == 0) {
				///Poll for Acks
				acks_polled = aether_poll_buff_and_post_recvs(&ack_ud_c, ACK_RECV_OPS_SIZE,
															  (uint8_t *) ack_recv_ops, cb);

				if (acks_polled > 0) {
                    if(acks_polled > 0)
						batch_acks_to_KVS(acks_polled, &ack_recv_ops, ops, last_group_membership, worker_lid);

					acks_polled = 0;
				}
			}

			if(!DISABLE_VALS_FOR_DEBUGGING) {
				for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i)
					assert(ack_recv_ops->opcode == ST_EMPTY);
				///~~~~~~~~~~~~~~~~~~~~~~ VALs ~~~~~~~~~~~~~~~~~~~~~~~~~~~
				has_outstanding_vals = aether_issue_pkts(&val_ud_c, cb, (uint8_t *) ack_recv_ops,
														 ack_ud_c.max_recv_wrs, sizeof(spacetime_ack_t),
														 &ignore_rolling_index, val_skip_or_get_sender_id,
														 val_modify_elem_after_send, val_copy_and_modify_elem);
				ignore_rolling_index = 0;

				///Poll for Vals
				vals_polled = aether_poll_buff_and_post_recvs(&inv_ud_c, VAL_RECV_OPS_SIZE, (uint8_t *) inv_recv_ops, cb);

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


			if(group_membership_has_changed(&last_group_membership, worker_lid)) {
			    printf("Reconfiguring group membership\n");
				assert(0);
			}
		}
		w_stats[worker_lid].total_loops++;
	}
	return NULL;
}


