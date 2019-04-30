//
// Created by akatsarakis on 15/03/18.
//
#define _GNU_SOURCE

#include "spacetime.h"
#include "inline-util.h"
#include "hrd.h"
#include "util.h"

int spawn_stats_thread(void)
{
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

void setup_q_depths(int** recv_q_depths, int** send_q_depths)
{
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


void share_qp_info_via_memcached(int worker_gid, struct hrd_ctrl_blk *cb)
{
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


void create_AHs(struct hrd_ctrl_blk *cb)
{
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
//            printf("main:Client %d found clt %d. Client LID: %d\n", clt_gid, i, clt_qp[i][qp_i]->lid);
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

uint8_t
is_state_code(uint8_t code)
{
    switch (code) {
        //Object States
        case VALID_STATE:
        case WRITE_STATE:
        case REPLAY_STATE:
        case INVALID_STATE:
        case INVALID_WRITE_STATE:
            return 1;
        default:
            return 0;
    }
}

uint8_t
is_input_code(uint8_t code)
{
    switch (code) {
         //Input opcodes
        case ST_OP_GET:
        case ST_OP_PUT:
        case ST_OP_INV:
        case ST_OP_ACK:
        case ST_OP_VAL:
        case ST_OP_CRD:
        case ST_OP_MEMBERSHIP_CHANGE:
        case ST_OP_MEMBERSHIP_COMPLETE:
            return 1;
        default:
            return 0;
    }
}

uint8_t
is_response_code(uint8_t code)
{
    switch (code) {
        case ST_GET_COMPLETE:
        case ST_PUT_SUCCESS:
        case ST_PUT_COMPLETE:
        case ST_REPLAY_SUCCESS:
        case ST_REPLAY_COMPLETE:
        case ST_INV_SUCCESS:
        case ST_ACK_SUCCESS:
        case ST_VAL_SUCCESS:
        case ST_LAST_ACK_SUCCESS:
        case ST_LAST_ACK_NO_BCAST_SUCCESS:
        case ST_MISS:
        case ST_GET_STALL:
        case ST_PUT_STALL:
        case ST_PUT_COMPLETE_SEND_VALS:
        case ST_INV_OUT_OF_GROUP:
            return 1;
        default:
            return 0;
    }
}

uint8_t
is_bucket_state_code(uint8_t code)
{
    switch (code) {
        case ST_NEW:
        case ST_EMPTY:
        case ST_COMPLETE:
        case ST_IN_PROGRESS_GET:
        case ST_IN_PROGRESS_PUT:
        case ST_IN_PROGRESS_REPLAY:
            return 1;
        default:
            return 0;
    }
}


char* code_to_str(uint8_t code)
{
	switch (code){
        //Object States
        case VALID_STATE:
            return "VALID_STATE";
        case INVALID_STATE:
            return "INVALID_STATE";
        case INVALID_WRITE_STATE:
            return "INVALID_WRITE_STATE";
        case WRITE_STATE:
            return "WRITE_STATE";
        case REPLAY_STATE:
            return "REPLAY_STATE";
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
        case ST_OP_MEMBERSHIP_CHANGE:
            return "ST_OP_MEMBERSHIP_CHANGE";
        case ST_OP_MEMBERSHIP_COMPLETE:
            return "ST_OP_MEMBERSHIP_COMPLETE";
        //Response opcodes
        case ST_GET_COMPLETE:
            return "ST_GET_COMPLETE";
        case ST_PUT_SUCCESS:
            return "ST_PUT_SUCCESS";
        case ST_PUT_COMPLETE:
            return "ST_PUT_COMPLETE";
        case ST_REPLAY_SUCCESS:
            return "ST_REPLAY_SUCCESS";
        case ST_REPLAY_COMPLETE:
            return "ST_REPLAY_COMPLETE";
        case ST_INV_SUCCESS:
            return "ST_INV_SUCCESS";
        case ST_ACK_SUCCESS:
            return "ST_ACK_SUCCESS";
        case ST_VAL_SUCCESS:
            return "ST_VAL_SUCCESS";
        case ST_LAST_ACK_SUCCESS:
            return "ST_LAST_ACK_SUCCESS";
        case ST_LAST_ACK_NO_BCAST_SUCCESS:
            return "ST_LAST_ACK_NO_BCAST_SUCCESS";
        case ST_MISS:
            return "\033[31mST_MISS\033[0m";
        case ST_GET_STALL:
            return "ST_GET_STALL";
        case ST_PUT_STALL:
            return "ST_PUT_STALL";
        case ST_PUT_COMPLETE_SEND_VALS:
            return "ST_PUT_COMPLETE_SEND_VALS";
        case ST_INV_OUT_OF_GROUP:
            return "ST_INV_OUT_OF_GROUP";
        case ST_SEND_CRD:
            return "ST_SEND_CRD";
        //Ops bucket states
        case ST_EMPTY:
            return "ST_EMPTY";
        case ST_NEW:
            return "ST_NEW";
        case ST_IN_PROGRESS_PUT:
            return "ST_IN_PROGRESS_PUT";
        case ST_IN_PROGRESS_REPLAY:
            return "ST_IN_PROGRESS_REPLAY";
        case ST_COMPLETE:
            return "ST_COMPLETE";
        //Buffer Types
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
    srand(time(NULL) + worker_gid * 7);
    *cmds = malloc((NUM_OF_REP_REQS + 1) * sizeof(struct spacetime_trace_command));

    uint32_t i, writes = 0;
    //parse file line by line and insert trace to cmd.
    for (i = 0; i < NUM_OF_REP_REQS; i++) {
        //Before reading the request deside if it's gone be read or write
//        (*cmds)[i].opcode = (uint8_t) (WRITE_RATIO == 1000 || ((rand() % 1000 < WRITE_RATIO)) ? ST_OP_PUT :  ST_OP_GET);
        (*cmds)[i].opcode = (uint8_t) (write_ratio == 1000 || ((rand() % 1000 < write_ratio)) ? ST_OP_PUT :  ST_OP_GET);

        //--- KEY ID----------
        uint32 key_id = KEY_NUM != 0 ? (uint32) rand() % KEY_NUM : (uint32) rand() % SPACETIME_NUM_KEYS;
        if(USE_A_SINGLE_KEY == 1) key_id = 0;
        uint128 key_hash = CityHash128((char *) &(key_id), 4);
//        memcpy(&(*cmds)[i].key_hash, &key_hash, 16); // this is for 16B keys
        memcpy(&(*cmds)[i].key_hash, &((uint64_t*)&key_hash)[1], 8);
        if ((*cmds)[i].opcode == ST_OP_PUT) writes++;
        (*cmds)[i].key_id = (uint8_t) (key_id < 255 ? key_id : ST_KEY_ID_255_OR_HIGHER);
    }

    if (worker_gid % num_workers == 0)
        printf("Write Ratio: %.2f%% \nTrace w_size %d \n", (double) (writes * 100) / NUM_OF_REP_REQS, NUM_OF_REP_REQS);
    (*cmds)[NUM_OF_REP_REQS].opcode = NOP;
    // printf("CLient %d Trace w_size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
    //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
}

// Parse a trace, use this for skewed workloads as uniform trace can be manufactured easilly
int
parse_trace(char* path, struct spacetime_trace_command **cmds, int worker_gid)
{
    FILE * fp;
    ssize_t read;
    size_t len = 0;
    char* ptr;
    char* word;
    char *saveptr;
    char* line = NULL;
    int cmd_count = 0;
    int writes = 0;
    uint32_t hottest_key_counter = 0;
    uint32_t ten_hottest_keys_counter = 0;
    uint32_t twenty_hottest_keys_counter = 0;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }

    while ((read = getline(&line, &len, fp)) != -1)
        cmd_count++;

//    printf("File %s has %d lines \n", path, cmd_count);

    fclose(fp);
    if (line)
        free(line);

    len = 0;
    line = NULL;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }

    (*cmds) = malloc((cmd_count + 1) * sizeof(struct spacetime_trace_command));

    // Initialize random with a seed based on local time and a worker / machine id
    srand((unsigned int) (time(NULL) + worker_gid * 7));

    int debug_cnt = 0;
    //parse file line by line and insert trace to cmd.
    for (int i = 0; i < cmd_count; i++) {
        if ((read = getline(&line, &len, fp)) == -1)
            die("ERROR: Problem while reading the trace\n");
        int word_count = 0;
        assert(word_count == 0);
        word = strtok_r(line, " ", &saveptr);

        //Before reading the request deside if it's gone be read or write
        (*cmds)[i].opcode = (uint8_t) (write_ratio == 1000 || ((rand() % 1000 < write_ratio)) ? ST_OP_PUT :  ST_OP_GET);

        if ((*cmds)[i].opcode == ST_OP_PUT) writes++;

        while (word != NULL) {
            if (word[strlen(word) - 1] == '\n')
                word[strlen(word) - 1] = 0;

            if(word_count == 0) {
                uint32_t key_id = (uint32_t) strtoul(word, &ptr, 10);
                if(key_id == 0)
                    hottest_key_counter++;
                if(key_id < 10)
                    ten_hottest_keys_counter++;
                if(key_id < 20)
                    twenty_hottest_keys_counter++;
                uint128 key_hash = CityHash128((char *) &(key_id), 4);
//              memcpy(&(*cmds)[i].key_hash, &key_hash, 16); // this is for 16B keys
                memcpy(&(*cmds)[i].key_hash, &((uint64_t*)&key_hash)[1], 8); // this is for 8B keys
                (*cmds)[i].key_id = (uint8_t) (key_id < 255 ? key_id : ST_KEY_ID_255_OR_HIGHER);
                debug_cnt++;
            }

            word_count++;
            word = strtok_r(NULL, " ", &saveptr);
            if (word == NULL && word_count != 1) {
                printf("Client %d Error: Reached word %d in line %d : %s \n",
                        worker_gid, word_count, i, line);
                assert(0);
            }
        }

    }

    if (worker_gid % num_workers == 0)
        printf("Trace size: %d | Hottest key (10 | 20 keys): %.2f%% (%.2f | %.2f %%)|"
               " Write Ratio: %.2f%% \n", cmd_count,
               (100 * hottest_key_counter / (double) cmd_count),
               (100 * ten_hottest_keys_counter / (double) cmd_count),
               (100 * twenty_hottest_keys_counter / (double) cmd_count),
               (double) (writes * 100) / cmd_count);

    (*cmds)[cmd_count].opcode = NOP;
    // printf("Thread %d Trace w_size: %d, debug counter %d hot keys %d, cold keys %d \n",l_id, cmd_count, debug_cnt,
    //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace );
    assert(cmd_count == debug_cnt);
    fclose(fp);
    if (line)
        free(line);
    return cmd_count;
}

void trace_init(struct spacetime_trace_command** trace, uint16_t worker_gid)
{
    //create the trace path path
    if (FEED_FROM_TRACE == 1) {
        char local_client_id[6];
        char machine_num[4];
        //get / create path for the trace
        sprintf(local_client_id, "%d", worker_gid % num_workers);
        sprintf(machine_num, "%d", machine_id);
        char path[2048];
        char cwd[1024];
        char *was_successful = getcwd(cwd, sizeof(cwd));

        if (!was_successful) {
            printf("ERROR: getcwd failed!\n");
            exit(EXIT_FAILURE);
        }

        double zipf_exponent = ZIPF_EXPONENT_OF_TRACE / 100.0;

        snprintf(path, sizeof(path), "%s%s%04d%s%.2f%s", cwd,
                "/../../traces/current-splited-traces/t_",
                worker_gid, "_a_", zipf_exponent, ".txt");

        //initialize the command array from the trace file
        parse_trace(path, trace, worker_gid);
    }else
        manufacture_trace(trace, worker_gid);
}

void setup_credits(uint8_t credits[][MACHINE_NUM],     struct hrd_ctrl_blk *cb,
                   struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                   struct ibv_recv_wr* credit_recv_wr, struct ibv_sge* credit_recv_sgl)
{
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
    crd_tmp.val_credits = 0;
//    crd_tmp.val_credits = MAX_CRDS_IN_MESSAGE;
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
void setup_kvs_buffs(spacetime_op_t **ops,
                     spacetime_inv_t **inv_recv_ops,
                     spacetime_ack_t **ack_recv_ops,
                     spacetime_val_t **val_recv_ops)
{
//    *ops = memalign(4096, max_batch_size* (sizeof(spacetime_op_t)));
//    memset(*ops, 0, max_batch_size* (sizeof(spacetime_op_t)));
    *ops = memalign(4096, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));
    memset(*ops, 0, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));
    assert(ops != NULL);

    ///Network ops
	///TODO should we memalign aswell?
//    *inv_recv_ops = (spacetime_inv_t*) malloc(INV_RECV_OPS_SIZE * sizeof(spacetime_inv_t));
//    *ack_recv_ops = (spacetime_ack_t*) malloc(ACK_RECV_OPS_SIZE * sizeof(spacetime_ack_t));
//    *val_recv_ops = (spacetime_val_t*) malloc(VAL_RECV_OPS_SIZE * sizeof(spacetime_val_t)); /* Batch of incoming broadcasts for the Cache*/

//    memset(*inv_recv_ops, 0, INV_RECV_OPS_SIZE * sizeof(spacetime_inv_t));
//    memset(*ack_recv_ops, 0, ACK_RECV_OPS_SIZE * sizeof(spacetime_ack_t));
//    memset(*val_recv_ops, 0, VAL_RECV_OPS_SIZE * sizeof(spacetime_val_t));

//    for(int i = 0; i < INV_RECV_OPS_SIZE; ++i)
//        (*inv_recv_ops)[i].op_meta.opcode = ST_EMPTY;
//
//    for(int i = 0; i < ACK_RECV_OPS_SIZE; ++i)
//        (*ack_recv_ops)[i].opcode = ST_EMPTY;
//
//    for(int i = 0; i < VAL_RECV_OPS_SIZE; ++i)
//        (*val_recv_ops)[i].opcode = ST_EMPTY;

    uint32_t no_ops = (uint32_t) (credits_num * REMOTE_MACHINES * max_coalesce); //credits * remote_machines * max_req_coalesce
	*inv_recv_ops = (spacetime_inv_t*) malloc(no_ops * sizeof(spacetime_inv_t));
	*ack_recv_ops = (spacetime_ack_t*) malloc(no_ops * sizeof(spacetime_ack_t));
    *val_recv_ops = (spacetime_val_t*) malloc(no_ops * sizeof(spacetime_val_t)); /* Batch of incoming broadcasts for the Cache*/
	assert(*inv_recv_ops!= NULL && *ack_recv_ops!= NULL && *val_recv_ops!= NULL);

    memset(*inv_recv_ops, 0, no_ops * sizeof(spacetime_inv_t));
    memset(*ack_recv_ops, 0, no_ops * sizeof(spacetime_ack_t));
    memset(*val_recv_ops, 0, no_ops * sizeof(spacetime_val_t));

    for(int i = 0; i < no_ops; ++i){
        (*ack_recv_ops)[i].opcode = ST_EMPTY;
        (*val_recv_ops)[i].opcode = ST_EMPTY;
        (*inv_recv_ops)[i].op_meta.opcode = ST_EMPTY;
    }

	for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i){
		(*ops)[i].op_meta.opcode = ST_EMPTY;
		(*ops)[i].op_meta.state = ST_EMPTY;
	}
}

// set up the OPS buffers
void setup_ops(spacetime_op_t** ops,
			   spacetime_inv_t** inv_recv_ops,
			   spacetime_ack_t** ack_recv_ops,
			   spacetime_val_t** val_recv_ops,
			   spacetime_inv_packet_t** inv_send_packet_ops,
			   spacetime_ack_packet_t** ack_send_packet_ops,
			   spacetime_val_packet_t** val_send_packet_ops)
{
    int i,j;
    *ops = memalign(4096, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));
    memset(*ops, 0, MAX_BATCH_OPS_SIZE * (sizeof(spacetime_op_t)));
    assert(ops != NULL);

    ///Network ops
	///TODO should we memalign aswell?
	*inv_recv_ops = (spacetime_inv_t*) malloc(INV_RECV_OPS_SIZE * sizeof(spacetime_inv_t));
	*ack_recv_ops = (spacetime_ack_t*) malloc(ACK_RECV_OPS_SIZE * sizeof(spacetime_ack_t));
    *val_recv_ops = (spacetime_val_t*) malloc(VAL_RECV_OPS_SIZE * sizeof(spacetime_val_t)); /* Batch of incoming broadcasts for the Cache*/
	assert(*inv_recv_ops!= NULL && *ack_recv_ops!= NULL && *val_recv_ops!= NULL);

    *inv_send_packet_ops = (spacetime_inv_packet_t*) malloc(INV_SEND_OPS_SIZE * sizeof(spacetime_inv_packet_t));
    *ack_send_packet_ops = (spacetime_ack_packet_t*) malloc(ACK_SEND_OPS_SIZE * sizeof(spacetime_ack_packet_t));
	*val_send_packet_ops = (spacetime_val_packet_t*) malloc(VAL_SEND_OPS_SIZE * sizeof(spacetime_val_packet_t)); /* Batch of incoming broadcasts for the Cache*/
	assert(*inv_send_packet_ops != NULL && *ack_send_packet_ops != NULL && *val_send_packet_ops!= NULL);

    memset(*inv_recv_ops, 0, INV_RECV_OPS_SIZE * sizeof(spacetime_inv_t));
    memset(*ack_recv_ops, 0, ACK_RECV_OPS_SIZE * sizeof(spacetime_ack_t));
    memset(*val_recv_ops, 0, VAL_RECV_OPS_SIZE * sizeof(spacetime_val_t));

	memset(*inv_send_packet_ops, 0, INV_SEND_OPS_SIZE * sizeof(spacetime_inv_packet_t));
	memset(*ack_send_packet_ops, 0, ACK_SEND_OPS_SIZE * sizeof(spacetime_ack_packet_t));
	memset(*val_send_packet_ops, 0, VAL_SEND_OPS_SIZE * sizeof(spacetime_val_packet_t));

    for(i = 0; i < INV_RECV_OPS_SIZE; i++)
        (*inv_recv_ops)[i].op_meta.opcode = ST_EMPTY;

    for(i = 0; i < ACK_RECV_OPS_SIZE; i++)
        (*ack_recv_ops)[i].opcode = ST_EMPTY;

    for(i = 0; i < VAL_RECV_OPS_SIZE; i++)
        (*val_recv_ops)[i].opcode = ST_EMPTY;

	for(i = 0; i < MAX_BATCH_OPS_SIZE; i++) {
		(*ops)[i].op_meta.opcode = ST_EMPTY;
		(*ops)[i].op_meta.state = ST_EMPTY;
	}

    for(i = 0; i < INV_SEND_OPS_SIZE; i++) {
        (*inv_send_packet_ops)[i].req_num = 0;
        for (j = 0; j < INV_MAX_REQ_COALESCE; j++)
            (*inv_send_packet_ops)[i].reqs[j].op_meta.opcode = ST_EMPTY;
    }
    for(i = 0; i < ACK_SEND_OPS_SIZE; i++) {
        (*ack_send_packet_ops)[i].req_num = 0;
        for (j = 0; j < ACK_MAX_REQ_COALESCE; j++)
            (*ack_send_packet_ops)[i].reqs[j].opcode = ST_EMPTY;
    }
    for(i = 0; i < VAL_SEND_OPS_SIZE; i++) {
        (*val_send_packet_ops)[i].req_num = 0;
        for (j = 0; j < VAL_MAX_REQ_COALESCE; j++)
            (*val_send_packet_ops)[i].reqs[j].opcode = ST_EMPTY;
    }
}

// Set up all coherence WRs
void setup_send_WRs(struct ibv_send_wr *inv_send_wr, struct ibv_sge *inv_send_sgl,
					struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                    struct ibv_send_wr *val_send_wr, struct ibv_sge *val_send_sgl,
                    struct ibv_mr *inv_mr, struct ibv_mr *ack_mr,
                    struct ibv_mr *val_mr, uint16_t local_worker_id)
{
    int i, i_mod_bcast, sgl_index;
    uint16_t rm_id;

    //Broadcast (INV / VAL) sgls
    for (i = 0; i < MAX_PCIE_BCAST_BATCH; i++) {
        inv_send_sgl[i].length = sizeof(spacetime_inv_packet_t);
        val_send_sgl[i].length = sizeof(spacetime_val_packet_t);
    }

    //Broadcast (INV / VAL) WRs
    assert(MAX_MSGS_IN_PCIE_BCAST_BATCH == MAX_SEND_INV_WRS);
    assert(MAX_MSGS_IN_PCIE_BCAST_BATCH == MAX_SEND_VAL_WRS);
    for (i = 0; i < MAX_MSGS_IN_PCIE_BCAST_BATCH; i++) {
        i_mod_bcast = i % REMOTE_MACHINES;
        sgl_index = i / REMOTE_MACHINES;
        if (i_mod_bcast < machine_id) rm_id = (uint16_t) i_mod_bcast;
        else rm_id = (uint16_t) ((i_mod_bcast + 1) % MACHINE_NUM);
        uint16_t rm_worker_gid = (uint16_t) (rm_id * WORKERS_PER_MACHINE + local_worker_id);
//        printf("index: %d, rm machine: %d, rm_worker_gid %d, sgl_index %d\n", i, rm_id, rm_worker_gid, sgl_index);
//        printf("Conf: Remotes: %d\n", rm_worker_gid);
        inv_send_wr[i].wr.ud.ah = remote_worker_qps[rm_worker_gid][INV_UD_QP_ID].ah;
        val_send_wr[i].wr.ud.ah = remote_worker_qps[rm_worker_gid][VAL_UD_QP_ID].ah;

        inv_send_wr[i].wr.ud.remote_qpn = (uint32) remote_worker_qps[rm_worker_gid][INV_UD_QP_ID].qpn;
        val_send_wr[i].wr.ud.remote_qpn = (uint32) remote_worker_qps[rm_worker_gid][VAL_UD_QP_ID].qpn;

        inv_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        val_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;


        inv_send_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no inlined_payload here
        val_send_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no inlined_payload here

        inv_send_wr[i].num_sge = 1;
        val_send_wr[i].num_sge = 1;

        inv_send_wr[i].sg_list = &inv_send_sgl[sgl_index];
        val_send_wr[i].sg_list = &val_send_sgl[sgl_index];

        if (DISABLE_INV_INLINING) {
            inv_send_sgl[sgl_index].lkey = inv_mr->lkey;
            inv_send_wr[i].send_flags = 0;
        } else
            inv_send_wr[i].send_flags = IBV_SEND_INLINE;

        if (DISABLE_VAL_INLINING) {
            val_send_sgl[sgl_index].lkey = val_mr->lkey;
            val_send_wr[i].send_flags = 0;
        } else
            val_send_wr[i].send_flags = IBV_SEND_INLINE;

        inv_send_wr[i].next = (i_mod_bcast == REMOTE_MACHINES - 1) ? NULL : &inv_send_wr[i + 1];
        val_send_wr[i].next = (i_mod_bcast == REMOTE_MACHINES - 1) ? NULL : &val_send_wr[i + 1];
    }

    //Send ACK  WRs
    for (i = 0; i < MAX_SEND_ACK_WRS; i++) {
        ack_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        ack_send_sgl[i].length = sizeof(spacetime_ack_packet_t);
        ack_send_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no inlined_payload here, cids do the job!
        ack_send_wr[i].num_sge = 1;
        ///change inline
        ack_send_wr[i].sg_list = &ack_send_sgl[i];

        if (!DISABLE_ACK_INLINING)
            ack_send_wr[i].send_flags = IBV_SEND_INLINE;
        else {
            ack_send_sgl[i].lkey = ack_mr->lkey;
            ack_send_wr[i].send_flags = 0;
        }
    }
}

void setup_recv_WRs(struct ibv_recv_wr *inv_recv_wr, struct ibv_sge *inv_recv_sgl,
                    struct ibv_recv_wr *ack_recv_wr, struct ibv_sge *ack_recv_sgl,
                    struct ibv_recv_wr *val_recv_wr, struct ibv_sge *val_recv_sgl,
					struct hrd_ctrl_blk *cb)
{
    int i;
    // Receives
	for (i = 0; i < MAX_RECV_INV_WRS; i++) {
		inv_recv_sgl[i].length = INV_RECV_REQ_SIZE;
        inv_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        inv_recv_wr[i].sg_list = &inv_recv_sgl[i];
        inv_recv_wr[i].num_sge = 1;
        inv_recv_wr[i].next = (i == MAX_RECV_INV_WRS - 1) ? NULL : &inv_recv_wr[i + 1];
	}

    for (i = 0; i < MAX_RECV_ACK_WRS; i++) {
		ack_recv_sgl[i].length = ACK_RECV_REQ_SIZE;
        ack_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        ack_recv_wr[i].sg_list = &ack_recv_sgl[i];
        ack_recv_wr[i].num_sge = 1;
        ack_recv_wr[i].next = (i == MAX_RECV_ACK_WRS - 1) ? NULL : &ack_recv_wr[i + 1];
	}

    for (i = 0; i < MAX_RECV_VAL_WRS; i++) {
		val_recv_sgl[i].length = VAL_RECV_REQ_SIZE;
        val_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        val_recv_wr[i].sg_list = &val_recv_sgl[i];
        val_recv_wr[i].num_sge = 1;
        val_recv_wr[i].next = (i == MAX_RECV_VAL_WRS - 1) ? NULL : &val_recv_wr[i + 1];
	}
}

void setup_incoming_buffs_and_post_initial_recvs(ud_req_inv_t *incoming_invs, ud_req_ack_t *incoming_acks,
                                                 ud_req_val_t *incoming_vals, int *inv_push_recv_ptr,
                                                 int *ack_push_recv_ptr, int *val_push_recv_ptr,
                                                 struct ibv_recv_wr *inv_recv_wr, struct ibv_recv_wr *ack_recv_wr,
                                                 struct ibv_recv_wr *val_recv_wr, struct ibv_recv_wr *crd_recv_wr,
                                                 struct hrd_ctrl_blk *cb, uint16_t worker_lid)
{
    int i;
    //init recv buffs as empty (not need for CRD since CRD msgs are (inlined_payload) header-only
	for(i = 0; i < RECV_INV_Q_DEPTH; i++)
		incoming_invs[i].packet.req_num = 0;
	for(i = 0; i < RECV_ACK_Q_DEPTH; i++)
		incoming_acks[i].packet.req_num = 0;
	for(i = 0; i < RECV_VAL_Q_DEPTH; i++)
		incoming_vals[i].packet.req_num = 0;

    if(ENABLE_POST_RECV_PRINTS && ENABLE_INV_PRINTS && worker_lid == 0)
        yellow_printf("vvv Post Initial Receives: \033[31mINVs\033[0m %d\n", MAX_RECV_INV_WRS);
    post_receives(cb, MAX_RECV_INV_WRS, ST_INV_BUFF, incoming_invs, inv_push_recv_ptr, inv_recv_wr);
    if(ENABLE_POST_RECV_PRINTS && ENABLE_VAL_PRINTS && worker_lid == 0)
        yellow_printf("vvv Post Initial Receives: \033[1m\033[32mVALs\033[0m %d\n", MAX_RECV_VAL_WRS);
    post_receives(cb, MAX_RECV_VAL_WRS, ST_VAL_BUFF, incoming_vals, val_push_recv_ptr, ack_recv_wr);
    //TODO initial receives for ACKS are not posted currently
    if(ENABLE_POST_RECV_PRINTS && ENABLE_ACK_PRINTS && worker_lid == 0)
        yellow_printf("vvv Post Initial Receives: \033[33mACKs\033[0m %d\n", MAX_RECV_ACK_WRS);
    post_receives(cb, MAX_RECV_ACK_WRS, ST_ACK_BUFF, incoming_acks, ack_push_recv_ptr, val_recv_wr);
    if(ENABLE_POST_RECV_PRINTS && ENABLE_CRD_PRINTS && worker_lid == 0)
        yellow_printf("vvv Post Initial Receives: \033[1m\033[36mCRDs\033[0m %d\n", MAX_RECV_CRD_WRS);
    post_credit_recvs(cb, crd_recv_wr, MAX_RECV_CRD_WRS);
}

