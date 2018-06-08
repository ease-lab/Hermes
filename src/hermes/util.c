//
// Created by akatsarakis on 15/03/18.
//
#define _GNU_SOURCE

#include "spacetime.h"
#include "inline-util.h"
#include "hrd.h"
#include "util.h"

int spawn_stats_thread() {
    pthread_t *thread_arr = malloc(sizeof(pthread_t));
    pthread_attr_t attr;
    cpu_set_t cpus_stats;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpus_stats);
    if(WORKERS_PER_MACHINE > 17)
        CPU_SET(39, &cpus_stats);
    else
        CPU_SET(2 * WORKERS_PER_MACHINE + 2, &cpus_stats);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
    return pthread_create(&thread_arr[0], &attr, print_stats, NULL);
}

void setup_q_depths(int** recv_q_depths, int** send_q_depths){
    assert(SEND_INV_Q_DEPTH != 0 && RECV_INV_Q_DEPTH != 0);
    assert(SEND_ACK_Q_DEPTH != 0 && RECV_ACK_Q_DEPTH != 0);
    assert(SEND_VAL_Q_DEPTH != 0 && RECV_VAL_Q_DEPTH != 0);
    assert(SEND_CRD_Q_DEPTH != 0 && RECV_CRD_Q_DEPTH != 0);

    *send_q_depths = malloc(TOTAL_WORKER_UD_QPs * sizeof(int));
    *recv_q_depths = malloc(TOTAL_WORKER_UD_QPs * sizeof(int));

    (*recv_q_depths)[INV_UD_QP_ID] = RECV_INV_Q_DEPTH;
    (*recv_q_depths)[ACK_UD_QP_ID] = RECV_ACK_Q_DEPTH;
    (*recv_q_depths)[VAL_UD_QP_ID] = RECV_VAL_Q_DEPTH;
    (*recv_q_depths)[CRD_UD_QP_ID] = RECV_CRD_Q_DEPTH;

    (*send_q_depths)[INV_UD_QP_ID] = SEND_INV_Q_DEPTH;
    (*send_q_depths)[ACK_UD_QP_ID] = SEND_ACK_Q_DEPTH;
    (*send_q_depths)[VAL_UD_QP_ID] = SEND_VAL_Q_DEPTH;
    (*send_q_depths)[CRD_UD_QP_ID] = SEND_CRD_Q_DEPTH;
}


void setup_qps(int worker_gid, struct hrd_ctrl_blk *cb){
    int i;
    int worker_lid = worker_gid % WORKERS_PER_MACHINE;
    for(i = 0; i < TOTAL_WORKER_UD_QPs; i++){
        char worker_dgram_qp_name[HRD_QP_NAME_SIZE];
        sprintf(worker_dgram_qp_name, "worker-dgram-%d-%d", worker_gid, i);
        hrd_publish_dgram_qp(cb, i, worker_dgram_qp_name, WORKER_SL);
//        printf("Worker %d (gid %d) published conn qp %s \n", worker_lid, worker_gid, worker_dgram_qp_name);
    }

    if (worker_lid == 0) {
        create_AHs(cb);
        assert(worker_needed_ah_ready == 0);
        // Spawn a threat for stats
        if (spawn_stats_thread() != 0)
            red_printf("Stats thread was not successfully spawned \n");
        worker_needed_ah_ready = 1;
    }else
        while (worker_needed_ah_ready == 0) usleep(20000);

    assert(worker_needed_ah_ready == 1);
}


void create_AHs(struct hrd_ctrl_blk *cb){
    int i, qp_i;
    int ib_port_index = 0;
    struct ibv_ah *worker_ah[WORKER_NUM][TOTAL_WORKER_UD_QPs];
    struct hrd_qp_attr *worker_qp[WORKER_NUM][TOTAL_WORKER_UD_QPs];
    int local_port_i = ib_port_index;
    char worker_name[HRD_QP_NAME_SIZE];

    // -- GET REMOTE WORKERS QPs
    for(i = 0; i < WORKER_NUM; i++) {
        if (i / WORKERS_PER_MACHINE == machine_id) continue; // skip the local machine
        for (qp_i = 0; qp_i < TOTAL_WORKER_UD_QPs; qp_i++) {
            sprintf(worker_name, "worker-dgram-%d-%d", i, qp_i);
//            printf("Trying to get qp %s \n", worker_name);
            /* Get the UD queue pair for the ith machine */
            worker_qp[i][qp_i] = NULL;
            // printf("CLIENT %d is Looking for client %s\n", clt_gid, clt_name );
            while(worker_qp[i][qp_i] == NULL) {
                worker_qp[i][qp_i] = hrd_get_published_qp(worker_name);
                if(worker_qp[i][qp_i] == NULL)
                    usleep(200000);
            }
            //printf("main:Client %d found clt %d. Client LID: %d\n", clt_gid, i, clt_qp[i][qp_i]->lid);
            struct ibv_ah_attr ah_attr = {
                    //-----INFINIBAND----------
                    .is_global = 0,
                    .dlid = (uint16_t) worker_qp[i][qp_i]->lid,
                    .sl = (uint8_t) worker_qp[i][qp_i]->sl,
                    .src_path_bits = 0,
                    /* port_num (> 1): device-local port for responses to this worker */
                    .port_num = (uint8_t) (local_port_i + 1),
            };

            if (is_roce == 1) {
                //-----RoCE----------
                ah_attr.is_global = 1;
                ah_attr.dlid = 0;
                ah_attr.grh.dgid.global.interface_id =  worker_qp[i][qp_i]->gid_global_interface_id;
                ah_attr.grh.dgid.global.subnet_prefix = worker_qp[i][qp_i]->gid_global_subnet_prefix;
                ah_attr.grh.sgid_index = 0;
                ah_attr.grh.hop_limit = 1;
            }
            worker_ah[i][qp_i]= ibv_create_ah(cb->pd, &ah_attr);
            assert(worker_ah[i][qp_i] != NULL);
            remote_worker_qps[i][qp_i].ah = worker_ah[i][qp_i];
            remote_worker_qps[i][qp_i].qpn = worker_qp[i][qp_i]->qpn;
        }
    }
}

char* code_to_str(uint8_t code){
	switch (code){
        //Object States
        case VALID_STATE:
            return "VALID_STATE";
        case INVALID_STATE:
            return "INVALID_STATE";
        case INVALID_WRITE_STATE:
            return "INVALID_WRITE_STATE";
        case INVALID_BUFF_STATE:
            return "INVALID_BUFF_STATE";
        case INVALID_WRITE_BUFF_STATE:
            return "INVALID_WRITE_BUFF_STATE";
        case WRITE_STATE:
            return "WRITE_STATE";
        case WRITE_BUFF_STATE:
            return "WRITE_BUFF_STATE";
        case REPLAY_STATE:
            return "REPLAY_STATE";
        case REPLAY_BUFF_STATE:
            return "REPLAY_BUFF_STATE";
        case REPLAY_WRITE_STATE:
            return "REPLAY_WRITE_STATE";
        case REPLAY_WRITE_BUFF_STATE:
            return "REPLAY_WRITE_BUFF_STATE";
        //Input opcodes
        case ST_OP_GET:
            return "ST_OP_GET";
        case ST_OP_PUT:
            return "ST_OP_PUT";
        case ST_OP_INV:
            return "ST_OP_INV";
        case ST_OP_ACK:
            return "ST_OP_ACK";
        case ST_OP_VAL:
            return "ST_OP_VAL";
        case ST_OP_CRD:
            return "ST_OP_CRD";
        //Response opcodes
        case ST_GET_SUCCESS:
            return "ST_GET_SUCCESS";
        case ST_PUT_SUCCESS:
            return "ST_PUT_SUCCESS";
        case ST_BUFFERED_REPLAY:
            return "ST_BUFFERED_REPLAY";
        case ST_INV_SUCCESS:
            return "ST_INV_SUCCESS";
        case ST_ACK_SUCCESS:
            return "ST_ACK_SUCCESS";
        case ST_VAL_SUCCESS:
            return "ST_VAL_SUCCESS";
        case ST_LAST_ACK_SUCCESS:
            return "ST_LAST_ACK_SUCCESS";
        case ST_LAST_ACK_PREV_WRITE_SUCCESS:
            return "ST_LAST_ACK_PREV_WRITE_SUCCESS";
        case ST_MISS:
            return "\033[31mST_MISS\033[0m";
        case ST_GET_STALL:
            return "ST_GET_STALL";
        case ST_PUT_STALL:
            return "ST_PUT_STALL";
        case ST_INV_FAIL:
            return "ST_INV_FAIL";
        case ST_ACK_FAIL:
            return "ST_ACK_FAIL";
        case ST_VAL_FAIL:
            return "ST_VAL_FAIL";
        case ST_GET_FAIL:
            return "ST_GET_FAIL";
        case ST_PUT_FAIL:
            return "ST_PUT_FAIL";
        case ST_INV_OUT_OF_GROUP:
            return "ST_INV_OUT_OF_GROUP";
        //Ops bucket states
        case ST_EMPTY:
            return "ST_EMPTY";
        case ST_NEW:
            return "ST_NEW";
        case ST_IN_PROGRESS_WRITE:
            return "ST_IN_PROGRESS_WRITE";
        case ST_BUFFERED:
            return "ST_BUFFERED";
        case ST_BUFFERED_IN_PROGRESS_REPLAY:
            return "ST_BUFFERED_IN_PROGRESS_REPLAY";
        case ST_COMPLETE:
            return "ST_COMPLETE";
        case ST_PROCESSED:
            return "ST_PROCESSED";
        case ST_INV_BUFF:
            return "ST_INV_BUFF";
        case ST_ACK_BUFF:
            return "ST_ACK_BUFF";
        case ST_VAL_BUFF:
            return "ST_VAL_BUFF";
        case ST_CRD_BUFF:
            return "ST_CRD_BUFF";
        case NOP:
            return "NOP";
//        case BUFFERED_READ:
//            return "BUFFERED_READ";
//        case BUFFERED_WRITE:
//            return "BUFFERED_WRITE";
//        case BUFFERED_READ_REPLAYED_WRITE:
//            return "BUFFERED_READ_REPLAYED_WRITE";
//        case BUFFERED_WRITE_REPLAYED_WRITE:
//            return "BUFFERED_WRITE_REPLAYED_WRITE";
//        case BUFFERED_READ_REPLAYED_IN_PROGRESS_WRITE:
//            return "BUFFERED_READ_REPLAYED_IN_PROGRESS_WRITE";
//        case BUFFERED_WRITE_REPLAYED_IN_PROGRESS_WRITE:
//            return "BUFFERED_WRITE_REPLAYED_IN_PROGRESS_WRITE";
        //Failure related
        case ST_OP_HEARTBEAT:
            return "ST_OP_HEARTBEAT";
        case ST_OP_SUSPICION:
            return "ST_OP_SUSPICION";
        default: {
            printf("Wrong code (%d)\n", code);
            assert(0);
		}
	}
}


// Manufactures a trace with a uniform distrbution without a backing file
void manufacture_trace(struct spacetime_trace_command **cmds, int worker_gid)
{
    srand(time(NULL) + worker_gid);
    *cmds = (struct spacetime_trace_command *)malloc((TRACE_SIZE + 1) * sizeof(struct spacetime_trace_command));
    uint32_t i, writes = 0;
    //parse file line by line and insert trace to cmd.
    for (i = 0; i < TRACE_SIZE; i++) {
        //Before reading the request deside if it's gone be read or write
        (*cmds)[i].opcode = (uint8_t) ((rand() % 1000 < WRITE_RATIO) ? ST_OP_PUT :  ST_OP_GET);

        //--- KEY ID----------
        uint32 key_id = (uint32) rand() % SPACETIME_NUM_KEYS;
        if(USE_A_SINGLE_KEY == 1) key_id =  0;
        uint128 key_hash = CityHash128((char *) &(key_id), 4);
        memcpy(&(*cmds)[i].key_hash, &key_hash, 16);
        if ((*cmds)[i].opcode == ST_OP_PUT) writes++;
    }

    if (worker_gid % WORKERS_PER_MACHINE == 0)
        printf("Write Ratio: %.2f%% \nTrace w_size %d \n", (double) (writes * 100) / TRACE_SIZE, TRACE_SIZE);
    (*cmds)[TRACE_SIZE].opcode = NOP;
    // printf("CLient %d Trace w_size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
    //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
}

void trace_init(struct spacetime_trace_command** trace, uint16_t worker_gid)
{
    //create the trace path path
    if (FEED_FROM_TRACE == 1) {
        char local_client_id[3];
        char machine_num[4];
        //get / creat path for the trace
        sprintf(local_client_id, "%d", worker_gid % WORKERS_PER_MACHINE);
        sprintf(machine_num, "%d", machine_id);
        char path[2048];
        char cwd[1024];
        char *was_successful = getcwd(cwd, sizeof(cwd));

        if (!was_successful) {
            printf("ERROR: getcwd failed!\n");
            exit(EXIT_FAILURE);
        }

        snprintf(path, sizeof(path), "%s%s%s%s%s%s", cwd,
                 "/../../traces/current-splited-traces/s_",
                 machine_num, "_c_", local_client_id, ".txt");
        //TODO need to implement "parse_trace"
        //initialize the command array from the trace file
//        parse_trace(path, (struct trace_command **)cmds, g_id % LEADERS_PER_MACHINE);
        assert(0);
    }else
        manufacture_trace(trace, worker_gid);
}

void setup_credits(uint8_t credits[][MACHINE_NUM],     struct hrd_ctrl_blk *cb,
                   struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                   struct ibv_recv_wr* credit_recv_wr, struct ibv_sge* credit_recv_sgl){
    int i = 0;
    spacetime_crd_t crd_tmp;

    // Credit counters
    for (i = 0; i < MACHINE_NUM; i++) {
		credits[INV_UD_QP_ID][i] = INV_CREDITS;
        credits[ACK_UD_QP_ID][i] = 0; //ACK_CREDITS
        credits[VAL_UD_QP_ID][i] = VAL_CREDITS;
        credits[CRD_UD_QP_ID][i] = 0; //CRD_CREDITS
    }

    crd_tmp.opcode = ST_OP_CRD;
    crd_tmp.sender = (uint8_t) machine_id;
    crd_tmp.val_credits = 1;
//    crd_tmp.val_credits = CRDS_IN_MESSAGE;
    // Credit WRs
    for (i = 0; i < MAX_SEND_CRD_WRS; i++) {
        credit_send_sgl->length = 0;
        credit_send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
        credit_send_wr[i].num_sge = 0;
        credit_send_wr[i].sg_list = credit_send_sgl;
        credit_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        credit_send_wr[i].next = NULL;
        credit_send_wr[i].send_flags = IBV_SEND_INLINE;
        credit_send_wr[i].imm_data = 0;
        memcpy(&credit_send_wr[i].imm_data, &crd_tmp, sizeof(spacetime_crd_t));
    }
    //Credit Receives
    credit_recv_sgl->length = 64;
    credit_recv_sgl->lkey = cb->dgram_buf_mr->lkey;
    credit_recv_sgl->addr = (uintptr_t) &cb->dgram_buf[INV_RECV_REQ_SIZE * RECV_INV_Q_DEPTH +
                                                       ACK_RECV_REQ_SIZE * RECV_ACK_Q_DEPTH +
                                                       VAL_RECV_REQ_SIZE * RECV_VAL_Q_DEPTH];
    for (i = 0; i < MAX_RECV_CRD_WRS; i++) {
        credit_recv_wr[i].sg_list = credit_recv_sgl;
        credit_recv_wr[i].num_sge = 1;
    }
}


// set up the OPS buffers
void setup_ops(spacetime_op_t **ops, spacetime_op_resp_t **resp,
			   spacetime_inv_t **inv_recv_ops, spacetime_ack_t **ack_recv_ops,
			   spacetime_val_t **val_recv_ops, spacetime_inv_t **inv_send_ops,
			   spacetime_ack_t **ack_send_ops, spacetime_val_t **val_send_ops)
{
    int i;
    *ops = memalign(4096, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));
    memset(*ops, 0, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));

    *resp = memalign(4096, MAX_BATCH_OPS_SIZE * sizeof(spacetime_op_resp_t));
    assert(ops != NULL && resp != NULL);

    ///Network ops
	///TODO should we memalign aswell?
	*inv_recv_ops = (spacetime_inv_t*) malloc(MAX_BATCH_OPS_SIZE * sizeof(spacetime_inv_t));
	*ack_recv_ops = (spacetime_ack_t*) malloc(MAX_BATCH_OPS_SIZE * sizeof(spacetime_ack_t));
    *val_recv_ops = (spacetime_val_t*) malloc(MAX_BATCH_OPS_SIZE * sizeof(spacetime_val_t)); /* Batch of incoming broadcasts for the Cache*/
	assert(*inv_recv_ops!= NULL && *ack_recv_ops!= NULL && *val_recv_ops!= NULL);

	*inv_send_ops = (spacetime_inv_t*) malloc(MAX_BATCH_OPS_SIZE * sizeof(spacetime_inv_t));
	*ack_send_ops = (spacetime_ack_t*) malloc(MAX_BATCH_OPS_SIZE * sizeof(spacetime_ack_t));
	*val_send_ops = (spacetime_val_t*) malloc(MAX_BATCH_OPS_SIZE * sizeof(spacetime_val_t)); /* Batch of incoming broadcasts for the Cache*/
	assert(*inv_send_ops!= NULL && *ack_send_ops!= NULL && *val_send_ops!= NULL);

	memset(*resp, 0, MAX_BATCH_OPS_SIZE * sizeof(spacetime_op_resp_t));
	memset(*inv_recv_ops, 0, MAX_BATCH_OPS_SIZE * sizeof(spacetime_inv_t));
	memset(*ack_recv_ops, 0, MAX_BATCH_OPS_SIZE * sizeof(spacetime_ack_t));
	memset(*val_recv_ops, 0, MAX_BATCH_OPS_SIZE * sizeof(spacetime_val_t));
	memset(*inv_send_ops, 0, MAX_BATCH_OPS_SIZE * sizeof(spacetime_inv_t));
	memset(*ack_send_ops, 0, MAX_BATCH_OPS_SIZE * sizeof(spacetime_ack_t));
	memset(*val_send_ops, 0, MAX_BATCH_OPS_SIZE * sizeof(spacetime_val_t));

	for(i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		(*ops)[i].opcode = ST_EMPTY;
		(*ops)[i].state = ST_EMPTY;
		(*resp)[i].resp_opcode = ST_EMPTY;
		(*inv_recv_ops)[i].opcode = ST_EMPTY;
		(*ack_recv_ops)[i].opcode = ST_EMPTY;
		(*val_recv_ops)[i].opcode = ST_EMPTY;
		(*inv_send_ops)[i].opcode = ST_EMPTY;
		(*ack_send_ops)[i].opcode = ST_EMPTY;
		(*val_send_ops)[i].opcode = ST_EMPTY;
	}
}

// Set up all coherence WRs
void setup_WRs(struct ibv_send_wr *inv_send_wr, struct ibv_sge *inv_send_sgl,
                    struct ibv_recv_wr *inv_recv_wr, struct ibv_sge *inv_recv_sgl,
					struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                    struct ibv_recv_wr *ack_recv_wr, struct ibv_sge *ack_recv_sgl,
                    struct ibv_send_wr *val_send_wr, struct ibv_sge *val_send_sgl,
                    struct ibv_recv_wr *val_recv_wr, struct ibv_sge *val_recv_sgl,
					struct hrd_ctrl_blk *cb, uint16_t local_worker_id)
{
    int i;

    //Broadcast (INV / VAL) sgls
    for (i = 0; i < MAX_PCIE_BCAST_BATCH; i++) {
        inv_send_sgl[i].length = sizeof(spacetime_inv_t);
        val_send_sgl[i].length = sizeof(spacetime_val_t);
    }

    //Broadcast (INV / VAL) WRs
    assert(MAX_MSGS_IN_PCIE_BCAST_BATCH == MAX_SEND_INV_WRS);
    assert(MAX_MSGS_IN_PCIE_BCAST_BATCH == MAX_SEND_VAL_WRS);
    for (i = 0; i < MAX_MSGS_IN_PCIE_BCAST_BATCH; i++) {
        int i_mod_bcast = i % MAX_MESSAGES_IN_BCAST;
        int sgl_index = i / MAX_MESSAGES_IN_BCAST;
        uint16_t rm_id;
        if (i_mod_bcast < machine_id) rm_id = (uint16_t) i_mod_bcast;
        else rm_id = (uint16_t) ((i_mod_bcast + 1) % MACHINE_NUM);
        uint16_t rm_worker_gid = (uint16_t) (rm_id * WORKERS_PER_MACHINE + local_worker_id);
//        printf("index: %d, rm machine: %d, rm_worker_gid %d, sgl_index %d\n", i, rm_id, rm_worker_gid, sgl_index);

        inv_send_wr[i].wr.ud.ah = remote_worker_qps[rm_worker_gid][INV_UD_QP_ID].ah;
        val_send_wr[i].wr.ud.ah = remote_worker_qps[rm_worker_gid][VAL_UD_QP_ID].ah;

        inv_send_wr[i].wr.ud.remote_qpn = (uint32) remote_worker_qps[rm_worker_gid][INV_UD_QP_ID].qpn;
        val_send_wr[i].wr.ud.remote_qpn = (uint32) remote_worker_qps[rm_worker_gid][VAL_UD_QP_ID].qpn;

        inv_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        val_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;


        inv_send_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no immediate here
        val_send_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no immediate here

        inv_send_wr[i].num_sge = 1;
        val_send_wr[i].num_sge = 1;

        inv_send_wr[i].sg_list = &inv_send_sgl[sgl_index];
        val_send_wr[i].sg_list = &val_send_sgl[sgl_index];

        ///if (CLIENT_ENABLE_INLINING == 1) coh_send_wr[index].send_flags = IBV_SEND_INLINE;
        inv_send_wr[i].send_flags = IBV_SEND_INLINE;
        val_send_wr[i].send_flags = IBV_SEND_INLINE;

        inv_send_wr[i].next = (i_mod_bcast == MAX_MESSAGES_IN_BCAST - 1) ? NULL : &inv_send_wr[i + 1];
        val_send_wr[i].next = (i_mod_bcast == MAX_MESSAGES_IN_BCAST - 1) ? NULL : &val_send_wr[i + 1];
    }

    //Send ACK  WRs
    for(i = 0; i < MAX_SEND_ACK_WRS; i++){
		ack_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
		ack_send_sgl[i].length = sizeof(spacetime_ack_t);
		ack_send_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no immediate here, cids do the job!
        ack_send_wr[i].num_sge = 1;
		///change inline
		ack_send_wr[i].sg_list = &ack_send_sgl[i];
        ack_send_wr[i].send_flags = IBV_SEND_INLINE;
	}

    // Receives
	for (i = 0; i < MAX_RECV_INV_WRS; i++) {
		inv_recv_sgl[i].length = INV_RECV_REQ_SIZE;
        inv_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        inv_recv_wr[i].sg_list = &inv_recv_sgl[i];
        inv_recv_wr[i].num_sge = 1;
	}
    for (i = 0; i < MAX_RECV_ACK_WRS; i++) {
		ack_recv_sgl[i].length = ACK_RECV_REQ_SIZE;
        ack_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        ack_recv_wr[i].sg_list = &ack_recv_sgl[i];
        ack_recv_wr[i].num_sge = 1;
	}
    for (i = 0; i < MAX_RECV_VAL_WRS; i++) {
		val_recv_sgl[i].length = VAL_RECV_REQ_SIZE;
        val_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        val_recv_wr[i].sg_list = &val_recv_sgl[i];
        val_recv_wr[i].num_sge = 1;
	}
}
