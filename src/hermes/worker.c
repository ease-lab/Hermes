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
	///printf("Worker(%d): Connection is up\n", worker_lid);

	/* -----------------------------------------------------
	--------------DECLARATIONS------------------------------
	---------------------------------------------------------*/
	ud_req_inv_t *incoming_invs = (ud_req_inv_t *) cb->dgram_buf;
	ud_req_ack_t *incoming_acks = (ud_req_ack_t *) &cb->dgram_buf[INV_RECV_REQ_SIZE * RECV_INV_Q_DEPTH];
	ud_req_val_t *incoming_vals = (ud_req_val_t *) &cb->dgram_buf[INV_RECV_REQ_SIZE * RECV_INV_Q_DEPTH +
																  ACK_RECV_REQ_SIZE * RECV_ACK_Q_DEPTH];
//	ud_req_crd_t *incoming_crds = (ud_req_crd_t *) &cb->dgram_buf[ sizeof(ud_req_inv_t) * RECV_INV_Q_DEPTH
//																   + sizeof(ud_req_ack_t) * RECV_ACK_Q_DEPTH
//																   + sizeof(ud_req_val_t) * RECV_VAL_Q_DEPTH];

	///Send declarations
	struct ibv_send_wr send_inv_wr[MAX_SEND_INV_WRS],
					   send_ack_wr[MAX_SEND_ACK_WRS],
					   send_val_wr[MAX_SEND_VAL_WRS],
			  		   send_crd_wr[MAX_SEND_CRD_WRS]; //, *bad_send_wr;

	struct ibv_sge     send_inv_sgl[MAX_PCIE_BCAST_BATCH],
			           send_ack_sgl[MAX_SEND_ACK_WRS],
			           send_val_sgl[MAX_PCIE_BCAST_BATCH], send_crd_sgl;

	uint8_t credits[TOTAL_WORKER_UD_QPs][MACHINE_NUM];

	///Receive declarations
	struct ibv_recv_wr recv_inv_wr[MAX_RECV_INV_WRS],
					   recv_ack_wr[MAX_RECV_ACK_WRS],
					   recv_val_wr[MAX_RECV_VAL_WRS],
			           recv_crd_wr[MAX_RECV_CRD_WRS]; // *bad_recv_wr;

	struct ibv_sge 	   recv_inv_sgl[MAX_RECV_INV_WRS],
			 		   recv_ack_sgl[MAX_RECV_ACK_WRS],
			           recv_val_sgl[MAX_RECV_VAL_WRS], recv_crd_sgl;

	//Only for immediates
	struct ibv_wc      recv_inv_wc[MAX_RECV_INV_WRS],
				       recv_ack_wc[MAX_RECV_ACK_WRS],
			 	       recv_val_wc[MAX_RECV_VAL_WRS],
			           recv_crd_wc[MAX_RECV_CRD_WRS];



	int inv_push_recv_ptr = 0, inv_pull_recv_ptr = -1,
		ack_push_recv_ptr = 0, ack_pull_recv_ptr = -1,
		val_push_recv_ptr = 0, val_pull_recv_ptr = -1,
		crd_push_recv_ptr = 0, crd_pull_recv_ptr = -1;
	int inv_push_send_ptr = 0, ack_push_send_ptr = 0, val_push_send_ptr = 0;
   	int i;
	//init receiv buffs as empty (not need for CRD since CRD msgs are (immediate) header-only
	for(i = 0; i < RECV_INV_Q_DEPTH; i ++)
        incoming_invs[i].req.opcode = ST_EMPTY;
	for(i = 0; i < RECV_ACK_Q_DEPTH; i ++)
		incoming_acks[i].req.opcode = ST_EMPTY;
	for(i = 0; i < RECV_VAL_Q_DEPTH; i ++)
		incoming_vals[i].req.opcode = ST_EMPTY;

	/* Post receives, we need to do this early */
	if (WRITE_RATIO > 0)
		for(i = 0; i < REMOTE_MACHINES; i++) {
			post_receives(cb, INV_CREDITS * REMOTE_MACHINES, ST_INV_BUFF, incoming_invs, &inv_push_recv_ptr);
//			post_receives(cb, ACK_CREDITS, ST_ACK_BUFF, incoming_acks, &ack_push_recv_ptr);
			post_receives(cb, VAL_CREDITS * REMOTE_MACHINES, ST_VAL_BUFF, incoming_vals, &val_push_recv_ptr);
		}
	setup_qps(worker_gid, cb);

	int inv_ops_i = 0, ack_ops_i = 0, val_ops_i = 0;
	uint16_t outstanding_invs = 0, outstanding_acks = 0, outstanding_vals = 0;

	spacetime_op_t *ops;
	spacetime_op_resp_t *ops_resp;
	spacetime_inv_t *inv_recv_ops, *inv_send_ops;
	spacetime_ack_t *ack_recv_ops, *ack_send_ops;
	spacetime_val_t *val_recv_ops, *val_send_ops;

	setup_ops(&ops, &ops_resp, &inv_recv_ops, &ack_recv_ops,
			  &val_recv_ops, &inv_send_ops, &ack_send_ops, &val_send_ops);

	///if no inlinig declare & set_up_mrs()
	//struct ibv_mr *inv_mr, *ack_mr, *val_mr, *crd_mr;

	setup_credits(credits, cb, send_crd_wr, &send_crd_sgl, recv_crd_wr, &recv_crd_sgl);
	setup_WRs(send_inv_wr, send_inv_sgl, recv_inv_wr, recv_inv_sgl,
			  send_ack_wr, send_ack_sgl, recv_ack_wr, recv_ack_sgl,
			  send_val_wr, send_val_sgl, recv_val_wr, recv_val_sgl, cb, worker_lid);

	int j, is_update;
	long long rolling_iter = 0; /* For throughput measurement */
	uint32_t credit_debug_cnt = 0;
	long long int inv_br_tx = 0, val_br_tx = 0, send_ack_tx = 0;

	/* -----------------------------------------------------
	--------------Start the main Loop-----------------------
	---------------------------------------------------------*/
	int tmp_only_for_dbg = 0, tmp = 0;
	while (true) {
		if (unlikely(credit_debug_cnt > M_1)) {
			red_printf("Worker %d misses credits \n", worker_gid);
			red_printf("Inv Credits %d, Ack credits %d, Val credits %d, CRD credits %d\n",
					   credits[INV_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   credits[ACK_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   credits[VAL_UD_QP_ID][(machine_id + 1) % MACHINE_NUM],
					   credits[CRD_UD_QP_ID][(machine_id + 1) % MACHINE_NUM]);
			credit_debug_cnt = 0;
		}
		for (j = 0; j < MAX_BATCH_OPS_SIZE; j++) {
			///uint32 key_i = hrd_fastrand(&seed) % NUM_KEYS; /* Choose a key */
			///uint32 key_i = rand() % NUM_KEYS; /* Choose a key */
			if (ops[j].state != ST_EMPTY) continue;
			ops[j].state = ST_NEW;
			uint32_t key_i = (uint32_t) worker_lid * MAX_BATCH_OPS_SIZE + j;
//				uint32_t key_i = (uint32_t) worker_gid * MAX_BATCH_OPS_SIZE + j;
//				uint32_t key_i = (uint32_t) j;
			uint128 key_hash = CityHash128((char *) &key_i, 4);

			memcpy(&ops[j].key, &key_hash, sizeof(spacetime_key_t));
			///is_update = (hrd_fastrand(&seed) % 100 < WRITE_RATIO) ? 1 : 0;
//			is_update = (rand() % 100 < WRITE_RATIO) ? 1 : 0;
//			is_update = rolling_iter == 0 ? 1 : 0 ;
			if(tmp == MAX_BATCH_OPS_SIZE * 300) exit(0);
			is_update = (tmp++ / MAX_BATCH_OPS_SIZE) % 2 == 0 ? 1 : 0;
//				is_update =  1; //write-only
			ops[j].opcode = (uint8) (is_update == 1 ? ST_OP_PUT : ST_OP_GET);
			if (is_update == 1)
				memset(ops[j].value, ((uint8_t) machine_id % 2 == 0 ? 'x' : 'y'), ST_VALUE_SIZE);
			ops[j].val_len = (uint8) (is_update == 1 ? ST_VALUE_SIZE : -1);
				red_printf("Key id: %d, op: %s, hash:%" PRIu64 "\n", key_i,
						   code_to_str(ops[j].opcode), ((uint64_t *) &ops[j].key)[1]);
		}

		spacetime_batch_ops(MAX_BATCH_OPS_SIZE, &ops, ops_resp, worker_lid);

		if (WRITE_RATIO > 0) {
			///~~~~~~~~~~~~~~~~~~~~~~INVS~~~~~~~~~~~~~~~~~~~~~~~~~~~
			///TODO remove credits recv etc from bcst_invs
			broadcasts_invs(ops, ops_resp, inv_send_ops, &inv_push_send_ptr,
							send_inv_wr, send_inv_sgl, credits, cb, &inv_br_tx,
							incoming_acks, &ack_push_recv_ptr ,worker_lid);
			///Poll for INVs
			poll_buff(incoming_invs, ST_INV_BUFF, &inv_pull_recv_ptr, inv_recv_ops,
					  &inv_ops_i, outstanding_invs, cb->dgram_recv_cq[INV_UD_QP_ID],
					  recv_inv_wc, credits, worker_lid);
			//printf("inv_ops_i: %d, outstanding_invs: %d, inv_pull_ptr: %d\n", inv_ops_i, outstanding_invs, inv_pull_ptr);

			if(inv_ops_i > 0) {
				///TODO fix outstanding_invs
				spacetime_batch_invs(inv_ops_i, &inv_recv_ops, worker_lid);
				///INVS_bookkeeping
				outstanding_invs = 0; //TODO this is only for testing

				///~~~~~~~~~~~~~~~~~~~~~~ACKS~~~~~~~~~~~~~~~~~~~~~~~~~~~
				issue_acks(inv_recv_ops, ack_send_ops, &ack_push_send_ptr,
						   &send_ack_tx, send_ack_wr, send_ack_sgl, credits,
						   cb, incoming_invs, &inv_push_recv_ptr, worker_lid);
				inv_ops_i = 0;
			}

			///Poll for Acks
			poll_buff(incoming_acks, ST_ACK_BUFF, &ack_pull_recv_ptr, ack_recv_ops,
					  &ack_ops_i, outstanding_acks, cb->dgram_recv_cq[ACK_UD_QP_ID],
					  recv_ack_wc, credits, worker_lid);
			if(ack_ops_i > 0){
				spacetime_batch_acks(ack_ops_i, &ack_recv_ops, ops, worker_lid);
				/*
				///~~~~~~~~~~~~~~~~~~~~~~VALS~~~~~~~~~~~~~~~~~~~~~~~~~~~
				broadcasts_vals(ack_recv_ops, val_send_ops, &val_push_send_ptr,
								send_val_wr, send_val_sgl, credits, cb, send_crd_wc,
								&credit_debug_cnt, &val_br_tx, recv_crd_wr, worker_lid);
								*/
				ack_ops_i = 0;
			}
			/*
            ///Poll for Vals
            ///TODO outstandig_vals are not really required
            poll_buff(incoming_vals, ST_VAL_BUFF, &val_pull_recv_ptr, val_recv_ops,
                      &val_ops_i, outstanding_vals, credits, worker_lid);
            if(val_ops_i > 0){

                spacetime_batch_vals(val_ops_i, &val_recv_ops, &ops, worker_lid);

                ///~~~~~~~~~~~~~~~~~~~~~~CREDITS~~~~~~~~~~~~~~~~~~~~~~~~~~~
//                send_credits(crd_o)
                val_ops_i = 0;
            }

            ///poll_credits
             */
		}
		for(j = 0; j < MAX_BATCH_OPS_SIZE; j++) {
			if (val_recv_ops[j].opcode == ST_VAL_SUCCESS)
				val_recv_ops[j].opcode = ST_EMPTY;
			if(!(ops[j].state == ST_COMPLETE ||
				   ops[j].state == ST_IN_PROGRESS_WRITE ||
				   ops[j].state == ST_BUFFERED))
				printf("Op[%d].state: %s\n", j, code_to_str(ops[j].state));
			assert(ops[j].state == ST_COMPLETE ||
				   ops[j].state == ST_IN_PROGRESS_WRITE ||
				   ops[j].state == ST_BUFFERED);

			assert(ops[j].opcode == ST_OP_PUT || ops[j].opcode == ST_OP_GET);
			if (ops[j].state == ST_COMPLETE) {
					green_printf("Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, value(len-%d): %s\n",
								 ((uint64_t *) &ops[j].key)[1],
								 code_to_str(ops_resp[j].resp_opcode), ops_resp[j].version,
								 ops_resp[j].tie_breaker_id, ops_resp[j].val_len, ops_resp[j].val_ptr);
				ops[j].state = ST_EMPTY;
				tmp_only_for_dbg++;
				w_stats[worker_lid].cache_hits_per_worker++;
			}
		}

		///w_stats[worker_lid].cache_hits_per_client += MAX_BATCH_OPS_SIZE;
		rolling_iter++;
	}
	return NULL;
}

