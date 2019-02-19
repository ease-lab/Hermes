#include <spacetime.h>
#include <concur_ctrl.h>
#include <time.h>
#include "util.h"
#include "inline-util.h"

///
#include "time_rdtsc.h"
#include "../../include/aether/aether.h"
///

static inline uint8_t
head_id(void)
{
	return (uint8_t) 0;
}

static inline uint8_t
tail_id(void)
{
	return MACHINE_NUM - 1;
}


static inline uint8_t
next_node_in_chain(void)
{
	return (uint8_t) ((machine_id + 1) % MACHINE_NUM);
}

static inline uint8_t
prev_node_in_chain(void)
{
	return (uint8_t) (machine_id == 0 ? tail_id() : machine_id - 1);
}


int
fwd_to_next_node(uint8_t *req)
{
	return next_node_in_chain(); // since invs should only be fwded to next node
}




void
inv_fwd_modify_elem_after_send(uint8_t* req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;

	//empty inv buffer
	if(inv_req->op_meta.opcode == ST_INV_SUCCESS ||
	   inv_req->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE)
		inv_req->op_meta.opcode = ST_EMPTY;

	else assert(0);
}

void
inv_fwd_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_inv_t* inv_recv = (spacetime_inv_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv and set opcode
	memcpy(inv_to_send, inv_recv, sizeof(spacetime_inv_t));
	inv_to_send->op_meta.opcode = ST_OP_INV;
}




int
inv_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(is_input_code(op_req->op_meta.opcode));
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
	}

	return op_req->op_meta.state == ST_PUT_SUCCESS ? next_node_in_chain() : -1; // since invs should only be fwded to next node
}

void
inv_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(op_req->op_meta.state == ST_PUT_SUCCESS)
		op_req->op_meta.state = ST_IN_PROGRESS_PUT;
	else{
		printf("state: %s\n", code_to_str(op_req->op_meta.state));
		assert(0);
	}
}

void
inv_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	if(ENABLE_ASSERTIONS)
		assert(machine_id == head_id());

	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(inv_to_send, op, sizeof(spacetime_inv_t));

	inv_to_send->op_meta.opcode = ST_OP_INV;
	inv_to_send->op_meta.initiator = (uint8_t) machine_id;
}


int
remote_write_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(is_input_code(op_req->op_meta.opcode));
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
	}

	return op_req->op_meta.state == ST_PUT_SUCCESS ? head_id() : -1; // send remote writes to head
}

void
remote_write_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	if(ENABLE_ASSERTIONS)
		assert(machine_id != head_id());

	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(inv_to_send, op, sizeof(spacetime_inv_t));

	inv_to_send->op_meta.state = ST_NEW;
	inv_to_send->op_meta.opcode = ST_OP_PUT;
	inv_to_send->initiator = (uint8_t) machine_id;
	inv_to_send->op_meta.initiator = (uint8_t) machine_id;
}



int
remote_write_head_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(machine_id == head_id());
		assert(is_input_code(op_req->op_meta.opcode) || op_req->op_meta.opcode == ST_EMPTY);
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
	}

	return op_req->op_meta.state == ST_PUT_SUCCESS ? next_node_in_chain() : -1; // remote writes must always be fwded to head
}

void
remote_write_head_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(inv_to_send, op, sizeof(spacetime_inv_t));

	inv_to_send->op_meta.opcode = ST_OP_INV;
	inv_to_send->op_meta.initiator = op->initiator;
}

void
remote_write_head_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(op_req->op_meta.state == ST_PUT_SUCCESS)
		op_req->op_meta.state = ST_SEND_CRD;
	else assert(0);
}



void
ack_fwd_modify_elem_after_send(uint8_t* req)
{
	spacetime_ack_t* ack_req = (spacetime_ack_t *) req;

	if(ENABLE_ASSERTIONS)
		assert(ack_req->opcode == ST_LAST_ACK_SUCCESS);

	ack_req->opcode = ST_EMPTY;
}

int
ack_fwd_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_ack_t* ack_req = (spacetime_ack_t *) req;
	if (ack_req->opcode == ST_ACK_SUCCESS)
	{
		ack_req->opcode = ST_EMPTY;
		return -1;
	} else if (ack_req->opcode == ST_EMPTY)
		return -1;

	if(ENABLE_ASSERTIONS)
		assert(ack_req->opcode == ST_LAST_ACK_SUCCESS);

	return prev_node_in_chain();
}

void
ack_fwd_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_ack_t* ack_to_send = (spacetime_ack_t *) msg_to_send;
	memcpy(ack_to_send, triggering_req, sizeof(spacetime_ack_t)); // copy req to next_req_ptr

	ack_to_send->opcode = ST_OP_ACK;
}




int
ack_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;

	if(ENABLE_ASSERTIONS)
		assert(inv_req->op_meta.opcode == ST_INV_SUCCESS || inv_req->op_meta.opcode == ST_EMPTY);

	return prev_node_in_chain();
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
	spacetime_inv_t* inv_ptr  = (spacetime_inv_t *) triggering_req;

	memcpy(ack_to_send, inv_ptr, sizeof(spacetime_ack_t)); // copy req to next_req_ptr

	ack_to_send->opcode = ST_OP_ACK;
	ack_to_send->buff_idx = inv_ptr->buff_idx;
}


int
crd_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_ptr = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS)
		assert(op_ptr->op_meta.state == ST_EMPTY ||
		       op_ptr->op_meta.state == ST_SEND_CRD ||
		       op_ptr->op_meta.state == ST_PUT_STALL ||
		       op_ptr->op_meta.state == ST_PUT_SUCCESS);

	return op_ptr->op_meta.state == ST_SEND_CRD ? op_ptr->initiator : -1;
}

void
crd_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op = (spacetime_op_t *) req;

	//empty inv buffer
	if(op->op_meta.state == ST_SEND_CRD)
		op->op_meta.state = ST_EMPTY;
	else assert(0);
}


void
print_total_send_recv_msgs(ud_channel_t *inv_ud_c, ud_channel_t *ack_ud_c,
						   ud_channel_t *val_ud_c, ud_channel_t *crd_ud_c)
{
	green_printf ("Total Send: invs %d, acks %d, rem_writes %d, crds %d\n",
				  inv_ud_c->stats.send_total_msgs, ack_ud_c->stats.send_total_msgs,
				  val_ud_c->stats.send_total_msgs, crd_ud_c->stats.send_total_msgs);
	green_printf ("Total Recv: invs %d, acks %d, rem_writes %d, crds %d\n",
				  inv_ud_c->stats.recv_total_msgs, ack_ud_c->stats.recv_total_msgs,
				  val_ud_c->stats.recv_total_msgs, crd_ud_c->stats.recv_total_msgs);
}


#define REMOTE_WRITES_UD_QP_ID VAL_UD_QP_ID
#define REMOTE_WRITES_MAX_REQ_COALESCE INV_MAX_REQ_COALESCE
#define DISABLE_REMOTE_WRITES_INLINING DISABLE_INV_INLINING
#define REMOTE_WRITES_CREDITS (CREDITS_PER_REMOTE_WORKER / MACHINE_NUM)

void*
run_worker(void *arg)
{
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t worker_lid = (uint16_t) params.id;	// Local ID of this worker thread
	uint16_t worker_gid = (uint16_t) (machine_id * WORKERS_PER_MACHINE + params.id);	// Global ID of this worker thread




	/* --------------------------------------------------------
	------------------- RDMA AETHER DECLARATIONS---------------
	---------------------------------------------------------*/
	ud_channel_t ud_channels[TOTAL_WORKER_UD_QPs];
	ud_channel_t* ud_channel_ptrs[TOTAL_WORKER_UD_QPs];
	ud_channel_t* inv_ud_c = &ud_channels[INV_UD_QP_ID];
	ud_channel_t* ack_ud_c = &ud_channels[ACK_UD_QP_ID];
	ud_channel_t* crd_ud_c = &ud_channels[CRD_UD_QP_ID];
	ud_channel_t* rem_writes_ud_c = &ud_channels[REMOTE_WRITES_UD_QP_ID];

	for(int i = 0; i < TOTAL_WORKER_UD_QPs; ++i)
		ud_channel_ptrs[i] = &ud_channels[i];

	char inv_qp_name[200], ack_qp_name[200], rem_writes_qp_name[200];
	sprintf(inv_qp_name, "%s[%d]", "\033[31mINV\033[0m", worker_lid);
	sprintf(ack_qp_name, "%s[%d]", "\033[33mACK\033[0m", worker_lid);
	sprintf(rem_writes_qp_name, "%s[%d]", "\033[1m\033[32mREMOTE_WRITES\033[0m", worker_lid);

	aether_ud_channel_init(inv_ud_c, inv_qp_name, REQ, INV_MAX_REQ_COALESCE, sizeof(spacetime_inv_t),
						   DISABLE_INV_INLINING == 0 ? 1 : 0, 0, 0, ack_ud_c, INV_CREDITS, MACHINE_NUM,
						   (uint8_t) machine_id, 1, 1);
	aether_ud_channel_init(ack_ud_c, ack_qp_name, RESP, ACK_MAX_REQ_COALESCE, sizeof(spacetime_ack_t),
						   DISABLE_ACK_INLINING == 0 ? 1 : 0, 0, 0, inv_ud_c, ACK_CREDITS, MACHINE_NUM,
						   (uint8_t) machine_id, 1, 1);

	aether_ud_channel_init(rem_writes_ud_c, rem_writes_qp_name, REQ, REMOTE_WRITES_MAX_REQ_COALESCE,
						   sizeof(spacetime_op_t), DISABLE_REMOTE_WRITES_INLINING == 0 ? 1 : 0, 0, 1,
						   crd_ud_c, REMOTE_WRITES_CREDITS, MACHINE_NUM, (uint8_t) machine_id, 1, 1);

	aether_setup_channel_qps_and_recvs(ud_channel_ptrs, TOTAL_WORKER_UD_QPs, g_share_qs_barrier, worker_lid);

	/* -------------------------------------------------------
	------------------- OTHER DECLARATIONS--------------------
	---------------------------------------------------------*/
	//Intermediate buffs where reqs are copied from incoming_* buffs in order to get passed to the KVS
	spacetime_op_t  *ops;
	spacetime_inv_t *inv_recv_ops;
	spacetime_ack_t *ack_recv_ops;
	spacetime_val_t *val_recv_ops; // UNUSED!

	setup_kvs_buffs(&ops, &inv_recv_ops, &ack_recv_ops, &val_recv_ops);

	//Remote writes init
	spacetime_op_t *remote_writes = memalign(4096, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));
	memset(remote_writes, 0, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));
	for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i){
		remote_writes[i].op_meta.state = ST_EMPTY;
		remote_writes[i].op_meta.opcode = ST_EMPTY;
	}


	struct spacetime_trace_command *trace;
	trace_init(&trace, worker_gid);

	//// <UNUSED>
	spacetime_group_membership last_group_membership = group_membership;
	spacetime_op_t* n_hottest_keys_in_ops_get[COALESCE_N_HOTTEST_KEYS];
	spacetime_op_t* n_hottest_keys_in_ops_put[COALESCE_N_HOTTEST_KEYS];
	for(int i = 0; i < COALESCE_N_HOTTEST_KEYS; ++i){
		n_hottest_keys_in_ops_get[i] = NULL;
		n_hottest_keys_in_ops_put[i] = NULL;
	}
	////</UNUSED>

	uint8_t has_outstanding_rem_writes = 0;
	uint32_t trace_iter = 0;
	uint16_t rolling_idx = 0, remote_writes_rolling_idx = 0;
	uint16_t invs_polled = 0, acks_polled = 0, remote_writes_polled = 0;
	uint32_t num_of_iters_serving_op[MAX_BATCH_OPS_SIZE] = {0};

	/// Spawn stats thread
	if (worker_lid == 0)
		if (spawn_stats_thread() != 0)
			red_printf("Stats thread was not successfully spawned \n");

	/* -----------------------------------------------------
       ------------------------Main Loop--------------------
	   ----------------------------------------------------- */
	while (true) {

	    if(unlikely(w_stats[worker_lid].total_loops % M_16 == 0)){
	        //Check something periodically
//			print_total_send_recv_msgs(inv_ud_c, ack_ud_c, rem_writes_ud_c, crd_ud_c);
//	        uint8_t remote_node = (uint8_t) (machine_id == head_id() ? next_node_in_chain() : head_id());
//	        printf("Inv credits: %d, ack credits: %d, remote_write_crds: %d\n",
//	        		inv_ud_c->credits_per_channels[remote_node],
//	        		ack_ud_c->credits_per_channels[remote_node],
//				    rem_writes_ud_c->credits_per_channels[head_id()]);
//	        printf("Has outstanding: %s\n", has_outstanding_rem_writes == 0 ? "NO" : "YES");
//			for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i)
//				if(machine_id == head_id())
//					printf("remotes[%d]: state-> %s [initiator %d] , key-> %lu\n", i,
//						   code_to_str(remote_writes[i].op_meta.state), remote_writes[i].initiator,
//						   *((uint64_t*) &remote_writes[i].op_meta.key));
//			for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i)
//				printf("ops[%d]: state-> %s, key-> %lu \n", i,
//					   code_to_str(ops[i].op_meta.state), *((uint64_t*) &ops[i].op_meta.key));
	    }

	    //TODO only for dbg
	    // 1st stage: head only initiate requests 						[DONE]
	    // 2nd stage: + rest nodes initiate (local) reads   			[DONE]
	    // 3rd stage: + rest nodes initiate (remote) writes via head    []
	    // 				- block polling new reqs until all of the previous remotes are completed
		// 4th stage: + rest nodes initiate remote reads when invalid   []
	    if(!ENABLE_ONLY_HEAD_REQS || machine_id == head_id()){
			refill_ops_n_suspect_failed_nodes(&trace_iter, worker_lid, trace, ops,
											  num_of_iters_serving_op, last_group_membership,
											  n_hottest_keys_in_ops_get, n_hottest_keys_in_ops_put);

			cr_batch_ops_to_KVS(Local_ops, (uint8_t *) ops, MAX_BATCH_OPS_SIZE, sizeof(spacetime_op_t), NULL);
		}

		if (WRITE_RATIO > 0) {

			if(machine_id == head_id())
			{
				///////////////
			    ///<3rd stage>
				if(has_outstanding_rem_writes == 0) {
					if(ENABLE_ASSERTIONS)
						for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i)
							assert(remote_writes[i].op_meta.state == ST_EMPTY);
					//TODO: this is very conservative it would poll for new remotes
					//      only after completing every remote write
					//      Note: writes will be stalled if already int INVALID state
					remote_writes_polled = aether_poll_buff_and_post_recvs(rem_writes_ud_c, MAX_BATCH_OPS_SIZE,
																		   (uint8_t *) remote_writes);
				}
				cr_batch_ops_to_KVS(Remote_writes, (uint8_t *) remote_writes,
									remote_writes_polled, sizeof(spacetime_op_t), NULL);

				/// Initiate INVs for remotes
				has_outstanding_rem_writes = aether_issue_pkts(inv_ud_c, (uint8_t *) remote_writes,
															   remote_writes_polled, sizeof(spacetime_op_t), NULL,
															   remote_write_head_skip_or_get_sender_id,
															   remote_write_head_modify_elem_after_send,
															   remote_write_head_copy_and_modify_elem);

//				assert(has_outstanding_rem_writes == 0 || inv_ud_c->credits_per_channels[next_node_in_chain()] == 0);

				/// Issue credits for remotes writes
				aether_issue_credits(crd_ud_c, (uint8_t *) remote_writes, remote_writes_polled, sizeof(spacetime_op_t),
									 crd_skip_or_get_sender_id, crd_modify_elem_after_send);

				//TODO make this more efficient
				for(int i = 0; i < remote_writes_polled; ++i)
					if(remote_writes[i].op_meta.state != ST_EMPTY)
						has_outstanding_rem_writes = 1;

				///</3rd stage>
				///////////////

				/// Initiate INVs
				aether_issue_pkts(inv_ud_c, (uint8_t *) ops,
								  MAX_BATCH_OPS_SIZE, sizeof(spacetime_op_t), &rolling_idx,
								  inv_skip_or_get_sender_id, inv_modify_elem_after_send,
								  inv_copy_and_modify_elem);
			}
			///////////////
			///</3rd stage>
			else {
				/// Initiate Remote writes
				aether_issue_pkts(rem_writes_ud_c, (uint8_t *) ops,
								  MAX_BATCH_OPS_SIZE, sizeof(spacetime_op_t), &rolling_idx,
								  remote_write_skip_or_get_sender_id, inv_modify_elem_after_send,
								  remote_write_copy_and_modify_elem);

			}
			///</3rd stage>
			///////////////

			if(machine_id != head_id()) {
				///Poll for INVs
				invs_polled = aether_poll_buff_and_post_recvs(inv_ud_c, INV_RECV_OPS_SIZE, (uint8_t *) inv_recv_ops);

				if (invs_polled > 0) {
					/// Batch INVs to KVS
					cr_batch_ops_to_KVS(Invs, (uint8_t *) inv_recv_ops, invs_polled, sizeof(spacetime_inv_t), ops);

					if (machine_id != tail_id() && machine_id != head_id())
						/// Forward INVS to next node in chain
						aether_issue_pkts(inv_ud_c, (uint8_t *) inv_recv_ops, invs_polled,
										  sizeof(spacetime_inv_t), NULL, fwd_to_next_node,
										  inv_fwd_modify_elem_after_send, inv_fwd_copy_and_modify_elem);

					else if (machine_id == tail_id())
						/// Initiate ACKS (forward to Head)
						aether_issue_pkts(ack_ud_c, (uint8_t *) inv_recv_ops, invs_polled,
										  sizeof(spacetime_inv_t), NULL, ack_skip_or_get_sender_id,
										  ack_modify_elem_after_send, ack_copy_and_modify_elem);
				}
			}

			if(machine_id != tail_id()){
				///Poll for Acks
				acks_polled = aether_poll_buff_and_post_recvs(ack_ud_c, ACK_RECV_OPS_SIZE, (uint8_t *) ack_recv_ops);

				if (acks_polled > 0)
					/// Batch ACKs to KVS
					cr_batch_ops_to_KVS(Acks, (uint8_t *) ack_recv_ops, acks_polled, sizeof(spacetime_ack_t), ops);

				if(machine_id != head_id())
					/// FWD ACKs if not the node before tail
					aether_issue_pkts(ack_ud_c, (uint8_t *) ack_recv_ops,
									  ack_ud_c->recv_pkt_buff_len, sizeof(spacetime_ack_t),
									  NULL, ack_fwd_skip_or_get_sender_id,
									  ack_fwd_modify_elem_after_send, ack_fwd_copy_and_modify_elem);

				else
					///empty ack_rcv_ops in head node
					for(int i = 0; i < ack_ud_c->recv_pkt_buff_len; ++i)
						ack_recv_ops[i].opcode = ST_EMPTY;
			}
		}
		w_stats[worker_lid].total_loops++;
	}
	return NULL;
}


