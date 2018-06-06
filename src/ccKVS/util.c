#include "util.h"
#include "city.h"

// Client creates AHs for all remote worker QPs and for the coherence client QPs
void createAHs(uint16_t clt_gid, struct hrd_ctrl_blk *cb)
{
    int i, qp_i;
    int ib_port_index = 0;
    struct ibv_ah *clt_ah[CLIENT_NUM][CLIENT_UD_QPS];
    ///struct ibv_ah *wrkr_ah[WORKER_NUM_UD_QPS][WORKER_NUM];
    struct hrd_qp_attr *clt_qp[CLIENT_NUM][CLIENT_UD_QPS];
    ///struct hrd_qp_attr *wrkr_qp[WORKER_NUM_UD_QPS][WORKER_NUM];
    // -- CONNECT WITH WORKERS
//    for (qp_i = 0; qp_i < WORKER_NUM_UD_QPS; qp_i++) {
//        for(i = 0; i < WORKER_NUM; i++) {
//            if (i / WORKERS_PER_MACHINE == machine_id) continue; // skip the local workers
//            /* Compute the control block and physical port index for client @i */
//            // int cb_i = i % num_server_ports;
//            int local_port_i = ib_port_index;// + cb_i;
//
//            char wrkr_name[HRD_QP_NAME_SIZE];
//            sprintf(wrkr_name, "worker-dgram-%d-%d-%d", i / WORKERS_PER_MACHINE, i % WORKERS_PER_MACHINE, qp_i);
//
//            /* Get the UD queue pair for the ith worker */
//            wrkr_qp[qp_i][i] = NULL;
//            // printf("CLIENT %d Looking for worker %s\n", clt_gid, wrkr_name );
//            while(wrkr_qp[qp_i][i] == NULL) {
//                wrkr_qp[qp_i][i] = hrd_get_published_qp(wrkr_name);
//                if(wrkr_qp[qp_i][i] == NULL)
//                    usleep(200000);
//            }
//            //  printf("main:Client %d found qp %d worker %d of %d workers. Worker LID: %d\n",
//            // clt_gid, qp_i, i, WORKER_NUM, wrkr_qp[qp_i][i]->lid);
//            struct ibv_ah_attr ah_attr = {
//                    //-----INFINIBAND----------
//                    .is_global = 0,
//                    .dlid = (uint16_t) wrkr_qp[qp_i][i]->lid,
//                    .sl = (uint8_t) wrkr_qp[qp_i][i]->sl,
//                    .src_path_bits = 0,
//                    /* port_num (> 1): device-local port for responses to this worker */
//                    .port_num = (uint8_t) (local_port_i + 1),
//            };
//
//            // // <Vasilis>  ---ROCE----------
//            if (is_roce == 1) {
//                ah_attr.is_global = 1;
//                ah_attr.dlid = 0;
//                ah_attr.grh.dgid.global.interface_id =  wrkr_qp[qp_i][i]->gid_global_interface_id;
//                ah_attr.grh.dgid.global.subnet_prefix = wrkr_qp[qp_i][i]->gid_global_subnet_prefix;
//                ah_attr.grh.sgid_index = 0;
//                ah_attr.grh.hop_limit = 1;
//            }
//            // </vasilis>
//            wrkr_ah[qp_i][i]= ibv_create_ah(cb->pd, &ah_attr);
//            assert(wrkr_ah[qp_i][i] != NULL);
//            remote_wrkr_qp[qp_i][i].ah = wrkr_ah[qp_i][i];
//            remote_wrkr_qp[qp_i][i].qpn = wrkr_qp[qp_i][i]->qpn;
//        }
//    }

    // -- CONNECT WITH CLIENTS

    for(i = 0; i < CLIENT_NUM; i++) {
        if (i / CLIENTS_PER_MACHINE == machine_id) continue; // skip the local machine
        for (qp_i = 1; qp_i < CLIENT_UD_QPS; qp_i++) {
            /* Compute the control block and physical port index for client @i */
            // int cb_i = i % num_server_ports;
            int local_port_i = ib_port_index;// + cb_i;

            char clt_name[HRD_QP_NAME_SIZE];
            sprintf(clt_name, "client-dgram-%d-%d", i, qp_i);

            /* Get the UD queue pair for the ith machine */
            clt_qp[i][qp_i] = NULL;
            // printf("CLIENT %d is Looking for client %s\n", clt_gid, clt_name );
            while(clt_qp[i][qp_i] == NULL) {
                clt_qp[i][qp_i] = hrd_get_published_qp(clt_name);
                if(clt_qp[i][qp_i] == NULL)
                    usleep(200000);
            }
            //printf("main:Client %d found clt %d. Client LID: %d\n",
             //       clt_gid, i, clt_qp[i][qp_i]->lid);
            struct ibv_ah_attr ah_attr = {
                    //-----INFINIBAND----------
                    .is_global = 0,
                    .dlid = (uint16_t) clt_qp[i][qp_i]->lid,
                    .sl = (uint8_t) clt_qp[i][qp_i]->sl,
                    .src_path_bits = 0,
                    /* port_num (> 1): device-local port for responses to this worker */
                    .port_num = (uint8_t) (local_port_i + 1),
            };

            // // <Vasilis>  ---ROCE----------
            if (is_roce == 1) {
                ah_attr.is_global = 1;
                ah_attr.dlid = 0;
                ah_attr.grh.dgid.global.interface_id =  clt_qp[i][qp_i]->gid_global_interface_id;
                ah_attr.grh.dgid.global.subnet_prefix = clt_qp[i][qp_i]->gid_global_subnet_prefix;
                ah_attr.grh.sgid_index = 0;
                ah_attr.grh.hop_limit = 1;
            }
            // </vasilis>
            clt_ah[i][qp_i]= ibv_create_ah(cb->pd, &ah_attr);
            assert(clt_ah[i][qp_i] != NULL);
            remote_clt_qp[i][qp_i].ah = clt_ah[i][qp_i];
            remote_clt_qp[i][qp_i].qpn = clt_qp[i][qp_i]->qpn;
        }
    }
}
//
//// Worker creates Ahs for the Client Qps that are used for Remote requests
//void createAHs_for_worker(uint16_t wrkr_lid, struct hrd_ctrl_blk *cb) {
//    int i, qp_i;
//    struct ibv_ah *clt_ah[CLIENT_NUM][CLIENT_UD_QPS];
//    struct hrd_qp_attr *clt_qp[CLIENT_NUM][CLIENT_UD_QPS];
//
//    for(i = 0; i < CLIENT_NUM; i++) {
//        if (i / CLIENTS_PER_MACHINE == machine_id) continue; // skip the local clients
//        /* Compute the control block and physical port index for client @i */
//        int local_port_i = 0;
//
//        char clt_name[HRD_QP_NAME_SIZE];
//        sprintf(clt_name, "client-dgram-%d-%d", i, REMOTE_UD_QP_ID);
//        /* Get the UD queue pair for the ith client */
//        clt_qp[i][REMOTE_UD_QP_ID] = NULL;
//
//        while(clt_qp[i][REMOTE_UD_QP_ID] == NULL) {
//            clt_qp[i][REMOTE_UD_QP_ID] = hrd_get_published_qp(clt_name);
//            //printf("Worker %d is expecting client %s\n" , wrkr_lid, clt_name);
//            if(clt_qp[i][REMOTE_UD_QP_ID] == NULL) {
//                usleep(200000);
//            }
//        }
//        //  printf("main: Worker %d found client %d. Client LID: %d\n",
//        //  	wrkr_lid, i, clt_qp[i][REMOTE_UD_QP_ID]->lid);
//
//        struct ibv_ah_attr ah_attr = {
//                //-----INFINIBAND----------
//                .is_global = 0,
//                .dlid = (uint16_t) clt_qp[i][REMOTE_UD_QP_ID]->lid,
//                .sl = clt_qp[i][REMOTE_UD_QP_ID]->sl,
//                .src_path_bits = 0,
//                /* port_num (> 1): device-local port for responses to this client */
//                .port_num = (uint8) (local_port_i + 1),
//        };
//
//        //  ---ROCE----------
//        if (is_roce == 1) {
//            ah_attr.is_global = 1;
//            ah_attr.dlid = 0;
//            ah_attr.grh.dgid.global.interface_id =  clt_qp[i][REMOTE_UD_QP_ID]->gid_global_interface_id;
//            ah_attr.grh.dgid.global.subnet_prefix = clt_qp[i][REMOTE_UD_QP_ID]->gid_global_subnet_prefix;
//            ah_attr.grh.sgid_index = 0;
//            ah_attr.grh.hop_limit = 1;
//        }
//        clt_ah[i][REMOTE_UD_QP_ID] = ibv_create_ah(cb->pd, &ah_attr);
//        assert(clt_ah[i][REMOTE_UD_QP_ID] != NULL);
//        remote_clt_qp[i][REMOTE_UD_QP_ID].ah = clt_ah[i][REMOTE_UD_QP_ID];
//        remote_clt_qp[i][REMOTE_UD_QP_ID].qpn = clt_qp[i][REMOTE_UD_QP_ID]->qpn;
//    }
//}
//
///* Generate a random permutation of [0, n - 1] for client @clt_gid */
//int* get_random_permutation(int n, int clt_gid, uint64_t *seed) {
//    int i, j, temp;
//    assert(n > 0);
//
//    /* Each client uses a different range in the cycle space of fastrand */
//    for(i = 0; i < clt_gid * CACHE_NUM_KEYS; i++) {
//        hrd_fastrand(seed);
//    }
//
//    printf("client %d: creating a permutation of 0--%d. This takes time..\n",
//           clt_gid, n - 1);
//
//    int *log = (int *) malloc(n * sizeof(int));
//    assert(log != NULL);
//    for(i = 0; i < n; i++) {
//        log[i] = i;
//    }
//
//    printf("\tclient %d: shuffling..\n", clt_gid);
//    for(i = n - 1; i >= 1; i--) {
//        j = hrd_fastrand(seed) % (i + 1);
//        temp = log[i];
//        log[i] = log[j];
//        log[j] = temp;
//    }
//    printf("\tclient %d: done creating random permutation\n", clt_gid);
//
//    return log;
//}
//
//// Set up the buffer space of the worker for multiple qps: With M QPs per worker, Client X sends its reqs to QP: X mod M
//void set_up_the_buffer_space(uint16_t clts_per_qp[], uint32_t per_qp_buf_slots[], uint32_t qp_buf_base[]) {
//    int i, clt_i,qp = 0;
//    // decide how many clients go to each QP
////    for (i = 0; i < CLIENTS_PER_MACHINE; i++) {
////        assert(qp < WORKER_NUM_UD_QPS);
////        clts_per_qp[qp]++;
////        HRD_MOD_ADD(qp, WORKER_NUM_UD_QPS);
////    }
//    for (i = 0; i < MACHINE_NUM; i++) {
//        if (i == machine_id) continue;
//        for (clt_i = 0; clt_i < CLIENTS_PER_MACHINE; clt_i++) {
//            clts_per_qp[(clt_i + i)% WORKER_NUM_UD_QPS]++;
//        }
//    }
//    qp_buf_base[0] = 0;
//    for (i = 0; i < WORKER_NUM_UD_QPS; i++) {
//        per_qp_buf_slots[i] = clts_per_qp[i]  * WS_PER_WORKER;
////        cyan_printf("per_qp_buf_slots for qp %d : %d\n", i, per_qp_buf_slots[i]);
//        if (i < WORKER_NUM_UD_QPS - 1)
//            qp_buf_base[i + 1] =  qp_buf_base[i] + per_qp_buf_slots[i];
//    }
//}

int parse_trace(char* path, struct trace_command **cmds, int clt_gid){
    FILE * fp;
    ssize_t read;
    size_t len = 0;
    char* ptr;
    char* word;
    char *saveptr;
    char* line = NULL;
    int i = 0;
    int cmd_count = 0;
    int word_count = 0;
    int letter_count = 0;
    int writes = 0;
    uint32_t hottest_key_counter = 0;

    fp = fopen(path, "r");
    if (fp == NULL){
        printf ("ERROR: Cannot open file: %s\n", path);
        exit(EXIT_FAILURE);
    }

    while ((read = getline(&line, &len, fp)) != -1)
        cmd_count++;

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
    // printf("File %s has %d lines \n", path, cmd_count);
    (*cmds) = (struct trace_command *)malloc((cmd_count + 1) * sizeof(struct trace_command));
    int debug_cnt = 0;
    //parse file line by line and insert trace to cmd.
    for (i = 0; i < cmd_count; i++) {
        if ((read = getline(&line, &len, fp)) == -1)
            die("ERROR: Problem while reading the trace\n");
        word_count = 0;
        //assert(i < MAX_TRACE_SIZE); //TODO do we need this?
        word = strtok_r (line, " ", &saveptr);
        (*cmds)[i].opcode = 0;

        //Before reading the request deside if it's gone be read or write
        uint8_t is_update = (rand() % 1000 < WRITE_RATIO) ? (uint8_t) 1 : (uint8_t) 0;

        if (is_update) {
            (*cmds)[i].opcode = (uint8_t) 1; // WRITE_OP
            writes++;
        }
        else  (*cmds)[i].opcode = (uint8_t) 0; // READ_OP

        while (word != NULL) {
            letter_count = 0;
            if (word[strlen(word) - 1] == '\n')
                word[strlen(word) - 1] = 0;

            if(word_count == 1) {
                (*cmds)[i].home_machine_id = (uint8_t) (strtoul(word, &ptr, 10) % MACHINE_NUM);
                if (RANDOM_MACHINE == 1) (*cmds)[i].home_machine_id = (uint8_t) (rand() % MACHINE_NUM);
                assert((*cmds)[i].home_machine_id < MACHINE_NUM);
                if (LOAD_BALANCE == 1)
                    (*cmds)[i].home_machine_id = (uint8_t) (rand() % MACHINE_NUM);
//                    printf("random %d \n", (*cmds)[i].home_machine_id);
                ///    while (DISABLE_LOCALS == 1 && (*cmds)[i].home_machine_id == machine_id)
                ///       (*cmds)[i].home_machine_id = (uint8_t) (rand() % MACHINE_NUM);
                ///}else if(DISABLE_LOCALS == 1 && (*cmds)[i].home_machine_id == (uint8_t) machine_id)
                ///   (*cmds)[i].home_machine_id = (uint8_t) (((*cmds)[i].home_machine_id + 1) % MACHINE_NUM);
                if (SEND_ONLY_TO_ONE_MACHINE == 1)
                    ///  if(DISABLE_LOCALS == 1 && machine_id == 0)
                    ///    (*cmds)[i].home_machine_id = (uint8_t) 1;
                    ///else
                    (*cmds)[i].home_machine_id = (uint8_t) 0;
                    ///else if(SEND_ONLY_TO_NEXT_MACHINE == 1)
                    ///(*cmds)[i].home_machine_id = (uint8_t) ((machine_id + 1) % MACHINE_NUM);
                else if(BALANCE_REQS_IN_CHUNKS == 1)
                    (*cmds)[i].home_machine_id = (uint8_t) (CHUNK_NUM == 0? 0 : (i / CHUNK_NUM) % MACHINE_NUM);
                else if (DO_ONLY_LOCALS == 1) (*cmds)[i].home_machine_id = (uint8) machine_id;
                ///assert(DISABLE_LOCALS == 0 || machine_id != (*cmds)[i].home_machine_id);
            } else if(word_count == 2){
//                (*cmds)[i].home_worker_id = (uint8_t) (strtoul(word, &ptr, 10) % WORKERS_PER_MACHINE);
//                if(LOAD_BALANCE == 1 || EMULATING_CREW == 1){
//                    //(*cmds)[i].home_worker_id = (uint8_t) (worker_id % WORKERS_PER_MACHINE);
//                    //HRD_MOD_ADD(worker_id, WORKER_NUM);
//                    (*cmds)[i].home_worker_id = (uint8_t) (rand() % ACTIVE_WORKERS_PER_MACHINE );
//                    //(*cmds)[i].home_worker_id = (uint8_t) (hrd_fastrand(&seed) % WORKERS_PER_MACHINE );
                //}
                //assert((*cmds)[i].home_worker_id < WORKERS_PER_MACHINE);
            } else if(word_count == 3){
                (*cmds)[i].key_id = (uint32_t) strtoul(word, &ptr, 10);
                if (ONLY_CACHE_HITS == 1)
                    (*cmds)[i].key_id = (uint32) rand() % CACHE_NUM_KEYS;
                    // HOT KEYS
                else if ((*cmds)[i].key_id < CACHE_NUM_KEYS) {
                    if ((BALANCE_HOT_WRITES == 1 && is_update) || BALANCE_HOT_REQS == 1) {
                        (*cmds)[i].key_id = (uint32_t) rand() % CACHE_NUM_KEYS;
                        range_assert((*cmds)[i].key_id, 0, CACHE_NUM_KEYS);
                        //(*cmds)[i].key_id = 0;
                    } else if (ENABLE_HOT_REQ_GROUPING == 1) {
                        if ((*cmds)[i].key_id < NUM_OF_KEYS_TO_GROUP) {
                            (*cmds)[i].key_id = ((*cmds)[i].key_id * GROUP_SIZE) + (rand() % GROUP_SIZE);
                        }
                        else if ((*cmds)[i].key_id < NUM_OF_KEYS_TO_GROUP * GROUP_SIZE)
                            (*cmds)[i].key_id += GROUP_SIZE;
                    }
                    if ((*cmds)[i].key_id < HOTTEST_KEYS_TO_TRACK) hottest_key_counter++;
                } else { // COLD KEYS
                    (*cmds)[i].key_id %= HERD_NUM_KEYS;
                    if ((*cmds)[i].key_id < CACHE_NUM_KEYS) (*cmds)[i].key_id+= CACHE_NUM_KEYS;
                }
                if(USE_A_SINGLE_KEY == 1)
                    (*cmds)[i].key_id =  0;

                uint64_t seed = 0xdeadbeaf;

                //if(ONLY_LOAD_BALANCED_HOT_REQS == 1)
                //    (*cmds)[i].key_id = (uint32_t) rand() % CACHE_NUM_KEYS;

                if ((*cmds)[i].key_id < CACHE_NUM_KEYS) { // hot
                    if ((*cmds)[i].opcode == 1) // hot write
                        (*cmds)[i].opcode = (uint8_t) HOT_WRITE;
                    else (*cmds)[i].opcode = (uint8_t) HOT_READ; // hot read
                } else {
                    if ((*cmds)[i].opcode == 1) { // normal write
                        if ((*cmds)[i].home_machine_id == machine_id)
                            (*cmds)[i].opcode = (uint8_t) LOCAL_WRITE;
                        else (*cmds)[i].opcode = (uint8_t) REMOTE_WRITE;
                    }
                    else { // normal read
                        if ((*cmds)[i].home_machine_id == machine_id)
                            (*cmds)[i].opcode = (uint8_t) LOCAL_READ;
                        else (*cmds)[i].opcode = (uint8_t) REMOTE_READ;
                    }
                }
                (*cmds)[i].key_hash = CityHash128((char *) &((*cmds)[i].key_id), 4);


                debug_cnt++;
                //(*cmds)[i].key_id = (*cmds)[i].key_id % 512; //TODO get rid of the %512
            }else if(word_count == 0) {
                while (word[letter_count] != '\0'){
                    switch(word[letter_count]) {
                        // case 'H' :
                        //     (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | HOT_KEY);
                        //     break;
                        // case 'N' :
                        //     (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | NORMAL_KEY);
                        //     break;
                        //     case 'R' :
                        //         (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | READ_OP);
                        //         break;
                        //     case 'W' :
                        //         (*cmds)[i].opcode = (uint8_t) ((*cmds)[i].opcode | WRITE_OP);
                        //         break;
                        default :
                            break;
                            assert(0);
                    }
                    letter_count++;
                }
            }

            word_count++;
            word = strtok_r(NULL, " ", &saveptr);
            if (word == NULL && word_count < 4) {
                printf("Client %d Error: Reached word %d in line %d : %s \n",clt_gid, word_count, i, line);
                assert(false);
            }
        }

        // printf ("Req: %d, opcode %d, home %d, worker %d,key %d\n", i, (*cmds)[i].opcode,
        //         (*cmds)[i].home_machine_id,
        //         (*cmds)[i].home_worker_id, (*cmds)[i].key_id);

    }
    if (clt_gid  == 0) printf("Write Ratio: %.2f%% \n", (double) (writes * 100) / cmd_count);
    if (clt_gid  == 0) printf("Hottest keys percentage of the trace: %.2f%% for %d keys \n",
                              (double) (hottest_key_counter * 100) / cmd_count, HOTTEST_KEYS_TO_TRACK);
    (*cmds)[cmd_count].opcode = NOP;
    // printf("CLient %d Trace size: %d, debug counter %d hot keys %d, cold keys %d \n",clt_gid, cmd_count, debug_cnt,
    //         w_stats[clt_gid].hot_keys_per_trace, w_stats[clt_gid].cold_keys_per_trace );
    assert(cmd_count == debug_cnt);
    fclose(fp);
    if (line)
        free(line);
    return cmd_count;
}

void trace_init(struct trace_command **cmds, int clt_gid) {
    //create the trace path path
    if (FEED_FROM_TRACE == 1) {
        char local_client_id[3];
        char machine_num[4];
        //get / creat path for the trace
        if (CLIENTS_PER_MACHINE <= 23) sprintf(local_client_id, "%d", (clt_gid % CLIENTS_PER_MACHINE));
        else  sprintf(local_client_id, "%d", (clt_gid % 23));// the traces are for 8 clients
        // sprintf(local_client_id, "%d", (clt_gid % 4));
        sprintf(machine_num, "%d", machine_id);
        char path[2048];
        char cwd[1024];
        char *was_successful = getcwd(cwd, sizeof(cwd));

        if (!was_successful) {
            printf("ERROR: getcwd failed!\n");
            exit(EXIT_FAILURE);
        }

//        snprintf(path, sizeof(path), "%s%s%s%s%s%s%d%s", cwd,
//                 "/../../traces/s_",
//                 machine_num, "_c_", local_client_id, "_a_", SKEW_EXPONENT_A, ".txt");

        snprintf(path, sizeof(path), "%s%s", cwd, "/../../traces/s_0_c_0_a_99.txt");
        //initialize the command array from the trace file
        // printf("Client: %d attempts to read the trace: %s\n", clt_gid, path);
        parse_trace(path, cmds, clt_gid % CLIENTS_PER_MACHINE);
        //printf("Trace read by client: %d\n", clt_gid);
    }else
        assert(0);
}

void dump_stats_2_file(struct stats* st){
    uint8_t typeNo = protocol;
    assert(typeNo >=0 && typeNo <=3);
    int i = 0;
    char filename[128];
    FILE *fp;
    double total_MIOPS;
    char* path = "../../results/scattered-results/";
    const char * exectype[] = {
            "BS", //baseline
            "EC", //Eventual Consistency
            "SC", //Strong Consistency (non stalling)
            "SS" //Strong Consistency (stalling)
    };

//    sprintf(filename, "%s/%s_%s_%s_s_%d_a_%d_v_%d_m_%d_c_%d_w_%d_r_%d%s-%d.csv", path,
//            DISABLE_CACHE == 1 ? "BS" : exectype[typeNo],
//            LOAD_BALANCE == 1 ? "UNIF" : "SKEW",
//            (ENABLE_WORKERS_CRCW == 1 ? "CRCW" : (EMULATING_CREW == 1 ? "CREW" : "EREW")),
//            DISABLE_CACHE == 0 && typeNo == 2 && ENABLE_MULTIPLE_SESSIONS != 0 && SESSIONS_PER_CLIENT != 0 ? SESSIONS_PER_CLIENT: 0,
//            SKEW_EXPONENT_A,
//            USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
//            MACHINE_NUM, CLIENTS_PER_MACHINE,
//            WORKERS_PER_MACHINE, WRITE_RATIO,
//            BALANCE_HOT_WRITES == 1  ? "_lbw" : "",
//            machine_id);
    sprintf(filename, "%s/UD_SENDS_n_%d_t_%d_b_%d_%s-%d.csv", path,
            MACHINE_NUM, WRITE_RATIO,
            CACHE_BATCH_SIZE,
            LOAD_BALANCE == 1 ? "UNIF" : "SKEW",
            machine_id);
    printf("%s\n", filename);
    fp = fopen(filename, "w"); // "w" means that we are going to write on this file
    fprintf(fp, "machine_id: %d\n", machine_id);
    fprintf(fp, "comment: worker ID, total MIOPS, local MIOPS, remote MIOPS\n");

//    for(i = 0; i < WORKERS_PER_MACHINE; ++i){
//        total_MIOPS = st->locals_per_worker[i] + st->remotes_per_worker[i];
//        fprintf(fp, "worker: %d, %.2f, %.2f, %.2f\n", i, total_MIOPS,
//                st->locals_per_worker[i], st->remotes_per_worker[i]);
//    }

    fprintf(fp, "comment: client ID, total MIOPS, cache MIOPS, local MIOPS,"
            "remote MIOPS, updates, invalidates, acks, received updates,"
            "received invalidates, received acks\n");
    for(i = 0; i < CLIENTS_PER_MACHINE; ++i){
        total_MIOPS = st->cache_hits_per_client[i] +
                      st->locals_per_client[i] + st->remotes_per_client[i];
        fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
                i, total_MIOPS, st->cache_hits_per_client[i], st->locals_per_client[i],
                st->remotes_per_client[i], st->updates_per_client[i],
                st->invs_per_client[i], st->acks_per_client[i],
                st->received_updates_per_client[i], st->received_invs_per_client[i],
                st->received_acks_per_client[i]);
    }

    /*
    fprintf(fp, "comment: cache MIOPS\n");
    fprintf(fp, "cache: %.2f\n", cache_MIOPS);
    machine_MIOPS += cache_MIOPS;
    fprintf(fp, "comment: machine MIOPS\n");
    fprintf(fp, "machine: %.2f\n", machine_MIOPS);
    */
    fclose(fp);
}

void append_throughput(double throughput)
{
    FILE *throughput_fd;
    throughput_fd = fopen("../../results/throughput.txt", "a");
    fprintf(throughput_fd, "%2.f \n", throughput);
    fclose(throughput_fd);
}

int spawn_stats_thread() {
    pthread_t *thread_arr = malloc(sizeof(pthread_t));
    pthread_attr_t attr;
    cpu_set_t cpus_stats;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpus_stats);
    ///if(WORKERS_PER_MACHINE + CLIENTS_PER_MACHINE > 17)
    if(CLIENTS_PER_MACHINE > 17)
        CPU_SET(39, &cpus_stats);
    else
        CPU_SET(2 *(CLIENTS_PER_MACHINE) + 2, &cpus_stats);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
    return pthread_create(&thread_arr[0], &attr, print_stats, NULL);
}

// pin a worker thread to a core
int pin_client(int c_id) {
    int c_core;
    c_core = PHYSICAL_CORE_DISTANCE * c_id;
    if(c_core > TOTAL_CORES_) { //if you run out of cores in numa node 0
        if (CLIENT_HYPERTHREADING) { //use hyperthreading rather than go to the other socket
            c_core = PHYSICAL_CORE_DISTANCE * (c_id - PHYSICAL_CORES_PER_SOCKET) + 2;
        } else { //spawn clients to numa node 1
            c_core = (c_id - PHYSICAL_CORES_PER_SOCKET) * PHYSICAL_CORE_DISTANCE + 1;
        }
    }
    assert(c_core >= 0 && c_core < TOTAL_CORES);
    return c_core;
}
//
//// pin a client thread to a core
//int pin_client(int c_id) {
//    int c_core;
//    if (!WORKER_HYPERTHREADING || WORKERS_PER_MACHINE < PHYSICAL_CORES_PER_SOCKET) {
//        if (c_id < WORKERS_PER_MACHINE) c_core = PHYSICAL_CORE_DISTANCE * c_id + 2;
//        else c_core = (WORKERS_PER_MACHINE * 2) + (c_id * 2);
//
//        //if (DISABLE_CACHE == 1) c_core = 4 * i + 2; // when bypassing the cache
//        //if (DISABLE_HYPERTHREADING == 1) c_core = (WORKERS_PER_MACHINE * 4) + (c_id * 4);
//        if (c_core > TOTAL_CORES_) { //spawn clients to numa node 1 if you run out of cores in 0
//            c_core -= TOTAL_CORES_;
//        }
//    }
//    else { //we are keeping workers on the same socket
//        c_core = (WORKERS_PER_MACHINE - PHYSICAL_CORES_PER_SOCKET) * 4 + 2 + (4 * c_id);
//        if (c_core > TOTAL_CORES_) c_core = c_core - (TOTAL_CORES_ + 2);
//        if (c_core > TOTAL_CORES_) c_core = c_core - (TOTAL_CORES_ - 1);
//    }
//    assert(c_core >= 0 && c_core < TOTAL_CORES);
//    return c_core;
//}

/* ---------------------------------------------------------------------------
------------------------------CLIENT INITIALIZATION --------------------------
---------------------------------------------------------------------------*/
// Post receives for the coherence traffic in the init phase
void post_coh_recvs(struct hrd_ctrl_blk *cb, int* push_ptr, struct mcast_essentials *mcast, int protocol, void* buf)
{
    check_protocol(protocol);
    int i, j;
    int credits = BROADCAST_CREDITS;
    int max_reqs = SC_CLT_BUF_SLOTS;
    for(i = 0; i < MACHINE_NUM - 1; i++) {
        for(j = 0; j < credits; j++) {
            if (ENABLE_MULTICAST == 1) {
                hrd_post_dgram_recv(mcast->recv_qp,	(void *) (buf + *push_ptr * UD_REQ_SIZE),
                                    UD_REQ_SIZE, mcast->recv_mr->lkey);
            }
            else hrd_post_dgram_recv(cb->dgram_qp[BROADCAST_UD_QP_ID],
                                     (void *) (buf + *push_ptr * UD_REQ_SIZE), UD_REQ_SIZE, cb->dgram_buf_mr->lkey);
            HRD_MOD_ADD(*push_ptr, max_reqs);
            //if (*push_ptr == 0) *push_ptr = 1;
        }
    }
}

// Initialize the mcast_essentials structure that is necessary
void init_multicast(struct mcast_info **mcast_data, struct mcast_essentials **mcast,
                    int local_client_id, struct hrd_ctrl_blk *cb, int protocol)
{
    check_protocol(protocol);
    uint16_t remote_buf_size = UD_REQ_SIZE;
    size_t dgram_buf_size = (size_t) (SC_CLT_BUF_SIZE + remote_buf_size);
    int recv_q_depth = SC_CLIENT_RECV_BR_Q_DEPTH;
    *mcast_data = malloc(sizeof(struct mcast_info));
    (*mcast_data)->clt_id = local_client_id;
    setup_multicast(*mcast_data, recv_q_depth);
    // char buf[40];
    // inet_ntop(AF_INET6, (*mcast_data)->mcast_ud_param.ah_attr.grh.dgid.raw, buf, 40);
    // printf("client: joined dgid: %s mlid 0x%x sl %d\n", buf,	(*mcast_data)->mcast_ud_param.ah_attr.dlid, (*mcast_data)->mcast_ud_param.ah_attr.sl);
    *mcast = malloc(sizeof(struct mcast_essentials));
    (*mcast)->send_ah = ibv_create_ah(cb->pd, &((*mcast_data)->mcast_ud_param.ah_attr));
    (*mcast)->qpn  =  (*mcast_data)->mcast_ud_param.qp_num;
    (*mcast)->qkey  =  (*mcast_data)->mcast_ud_param.qkey;
    (*mcast)->recv_cq = (*mcast_data)->cm_qp[RECV_MCAST_QP].cq;
    (*mcast)->recv_qp = (*mcast_data)->cm_qp[RECV_MCAST_QP].cma_id->qp;
    (*mcast)->recv_mr = ibv_reg_mr((*mcast_data)->cm_qp[RECV_MCAST_QP].pd, (void*) cb->dgram_buf,
                                   dgram_buf_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                                                   IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC);
    free(*mcast_data);
    assert((*mcast)->recv_mr != NULL);
}

// set the different queue depths for client's queue pairs
void set_up_queue_depths(int** recv_q_depths, int** send_q_depths, int protocol)
{
    /* 1st Dgram for communication between Clients and servers
      2nd Dgram for Broadcasting
      3rd Dgram for Flow Control (Credit-based) */
    *send_q_depths = malloc(CLIENT_UD_QPS * sizeof(int));
    *recv_q_depths = malloc(CLIENT_UD_QPS * sizeof(int));
//    if (protocol == EVENTUAL) {
//        (*recv_q_depths)[REMOTE_UD_QP_ID] = CLIENT_RECV_REM_Q_DEPTH;
//        (*recv_q_depths)[BROADCAST_UD_QP_ID] = ENABLE_MULTICAST == 1 ? 1 : EC_CLIENT_RECV_BR_Q_DEPTH;
//        (*recv_q_depths)[FC_UD_QP_ID] = EC_CLIENT_RECV_CR_Q_DEPTH;
//        (*send_q_depths)[REMOTE_UD_QP_ID] = CLIENT_SEND_REM_Q_DEPTH;
//        (*send_q_depths)[BROADCAST_UD_QP_ID] = EC_CLIENT_SEND_BR_Q_DEPTH;
//        (*send_q_depths)[FC_UD_QP_ID] = EC_CLIENT_SEND_CR_Q_DEPTH;
//    }
//    else
    if (protocol == STRONG_CONSISTENCY) {
        ///(*recv_q_depths)[REMOTE_UD_QP_ID] = CLIENT_RECV_REM_Q_DEPTH;
        (*recv_q_depths)[BROADCAST_UD_QP_ID] = SC_CLIENT_RECV_BR_Q_DEPTH;
        (*recv_q_depths)[FC_UD_QP_ID] = SC_CLIENT_RECV_CR_Q_DEPTH;
        ///(*send_q_depths)[REMOTE_UD_QP_ID] = CLIENT_SEND_REM_Q_DEPTH;
        (*send_q_depths)[BROADCAST_UD_QP_ID] = SC_CLIENT_SEND_BR_Q_DEPTH;
        (*send_q_depths)[FC_UD_QP_ID] = SC_CLIENT_SEND_CR_Q_DEPTH;
    }
    else check_protocol(protocol);
}

// Connect with Workers and Clients
void setup_client_conenctions_and_spawn_stats_thread(int clt_gid, struct hrd_ctrl_blk *cb)
{
    int i;
    int local_client_id = clt_gid % CLIENTS_PER_MACHINE;
    for (i = 0; i < CLIENT_UD_QPS; i++) {
        char clt_dgram_qp_name[HRD_QP_NAME_SIZE];
        sprintf(clt_dgram_qp_name, "client-dgram-%d-%d", clt_gid, i);
        hrd_publish_dgram_qp(cb, i, clt_dgram_qp_name, CLIENT_SL);
        // printf("main: Client %d published dgram %s \n", clt_gid, clt_dgram_qp_name);
    }
    if (local_client_id == 0) {
        createAHs(clt_gid, cb);
        assert(clt_needed_ah_ready == 0);
        // Spawn a thread that prints the stats
        if (spawn_stats_thread() != 0)
            red_printf("Stats thread was not successfully spawned \n");
        clt_needed_ah_ready = 1;
    }
    else {
        while (clt_needed_ah_ready == 0);  usleep(200000);
    }
    assert(clt_needed_ah_ready == 1);
    //printf("CLIENT %d has all the needed ahs\n", clt_gid );
}

// set up the OPS buffers
void set_up_ops(struct extended_cache_op **ops, struct extended_cache_op **next_ops, struct extended_cache_op **third_ops,
                struct mica_resp **resp, struct mica_resp **next_resp, struct mica_resp **third_resp,
                struct key_home **key_homes, struct key_home **next_key_homes, struct key_home **third_key_homes)
{
    int i;
    uint32_t extended_ops_size = (OPS_BUFS_NUM * CACHE_BATCH_SIZE * (sizeof(struct extended_cache_op)));
    *ops = memalign(4096, extended_ops_size);
    memset(*ops, 0, extended_ops_size);
    *next_ops = &((*ops)[CACHE_BATCH_SIZE]);
    *third_ops = &((*ops)[2 * CACHE_BATCH_SIZE]); //only used when no Inlining happens


    *resp = memalign(4096, OPS_BUFS_NUM * CACHE_BATCH_SIZE * sizeof(struct mica_resp));
    *next_resp = &((*resp)[CACHE_BATCH_SIZE]);
    *third_resp = &((*resp)[2 * CACHE_BATCH_SIZE]);

    *key_homes = memalign(4096, OPS_BUFS_NUM * CACHE_BATCH_SIZE * sizeof(struct key_home));
    *next_key_homes = &((*key_homes)[CACHE_BATCH_SIZE]);
    *third_key_homes = &((*key_homes)[2 * CACHE_BATCH_SIZE]);

    assert(ops != NULL && next_ops != NULL && third_ops != NULL &&
           resp != NULL && next_resp != NULL && third_resp != NULL &&
           key_homes != NULL && next_key_homes != NULL && third_key_homes != NULL);

    for(i = 0; i <  OPS_BUFS_NUM * CACHE_BATCH_SIZE; i++)
        (*resp)[i].type = EMPTY;
}

// set up the coherence buffers
void set_up_coh_ops(struct cache_op **update_ops, struct cache_op **ack_bcast_ops, struct small_cache_op **inv_ops,
                    struct small_cache_op **inv_to_send_ops, struct mica_resp *update_resp, struct mica_resp *inv_resp,
                    struct mica_op **coh_buf, int protocol)
{
    check_protocol(protocol);
    int i;
    *coh_buf = memalign(4096, COH_BUF_SIZE);
    uint16_t cache_op_size = sizeof(struct cache_op);
    uint16_t small_cache_op_size = sizeof(struct small_cache_op);
    *update_ops = (struct cache_op *)malloc(BCAST_TO_CACHE_BATCH * cache_op_size); /* Batch of incoming broadcasts for the Cache*/
    if (protocol == STRONG_CONSISTENCY) {
        *ack_bcast_ops = (struct cache_op *)malloc(BCAST_TO_CACHE_BATCH * cache_op_size);
        *inv_ops = (struct small_cache_op *)malloc(BCAST_TO_CACHE_BATCH * small_cache_op_size);
        *inv_to_send_ops = (struct small_cache_op *)malloc(BCAST_TO_CACHE_BATCH * small_cache_op_size);
    }
    assert(*update_ops != NULL);
    if (protocol == STRONG_CONSISTENCY)
        assert(*ack_bcast_ops != NULL && *inv_ops != NULL && *inv_to_send_ops != NULL);
    for(i = 0; i < BCAST_TO_CACHE_BATCH; i++) {
        update_resp[i].type = EMPTY;
        if (protocol == STRONG_CONSISTENCY) {
            inv_resp[i].type = EMPTY;
            (*inv_to_send_ops)[i].opcode = EMPTY;
        }
    }

}
// Set up the memory registrations required in the client if there is no Inlining
void set_up_mrs(struct ibv_mr **ops_mr, struct ibv_mr **coh_mr, struct extended_cache_op* ops,
                struct mica_op *coh_buf, struct hrd_ctrl_blk *cb)
{
    if (CLIENT_ENABLE_INLINING == 0) {
        uint32_t extended_ops_size = (OPS_BUFS_NUM * CACHE_BATCH_SIZE * (sizeof(struct extended_cache_op)));
        *ops_mr = register_buffer(cb->pd, (void*)ops, extended_ops_size);
        if (WRITE_RATIO != 0) *coh_mr = register_buffer(cb->pd, (void*)coh_buf, COH_BUF_SIZE);
    }
}
//
//// Set up the remote Requests send and recv WRs
//void set_up_remote_WRs(struct ibv_send_wr* rem_send_wr, struct ibv_sge* rem_send_sgl,
//                       struct ibv_recv_wr* rem_recv_wr, struct ibv_sge* rem_recv_sgl,
//                       struct hrd_ctrl_blk *cb, int clt_gid, struct ibv_mr* ops_mr, int protocol)
//{
//    int i;
//    check_protocol(protocol);
//    ///uint16_t remote_buf_size = ENABLE_WORKER_COALESCING == 1 ?
//    uint16_t remote_buf_size = UD_REQ_SIZE ;
//    // This should be same for both protocols
//    for (i = 0; i < WINDOW_SIZE; i++) {
//        if (CLIENT_ENABLE_INLINING == 0) rem_send_sgl[i].lkey = ops_mr->lkey;
//        else rem_send_wr[i].send_flags = IBV_SEND_INLINE;
//        rem_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
//        rem_send_wr[i].imm_data = (uint32) clt_gid;
//        rem_send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
//        rem_send_wr[i].num_sge = 1;
//        rem_send_wr[i].sg_list = &rem_send_sgl[i];
//        if (USE_ONLY_BIG_MESSAGES == 1)
//            rem_recv_sgl->length = HERD_PUT_REQ_SIZE + sizeof(struct ibv_grh);
//        else
//            rem_recv_sgl->length = remote_buf_size;
//        rem_recv_sgl->lkey = cb->dgram_buf_mr->lkey;
//        rem_recv_sgl->addr = (uintptr_t) &cb->dgram_buf[0];
//        rem_recv_wr[i].sg_list = rem_recv_sgl;
//        rem_recv_wr[i].num_sge = 1;
//    }
//}

// Set up all coherence WRs
void set_up_coh_WRs(struct ibv_send_wr *coh_send_wr, struct ibv_sge *coh_send_sgl,
                    struct ibv_recv_wr *coh_recv_wr, struct ibv_sge *coh_recv_sgl,
                    struct ibv_send_wr *ack_wr, struct ibv_sge *ack_sgl,
                    struct mica_op *coh_buf, uint16_t local_client_id,
                    struct hrd_ctrl_blk *cb, struct ibv_mr *coh_mr, struct mcast_essentials *mcast, int protocol)
{
    int i, j;
    check_protocol(protocol);
    //BROADCAST WRs and credit Receives
    for (j = 0; j < MAX_BCAST_BATCH; j++) {
        ///if (protocol == EVENTUAL) coh_send_sgl[j].length = HERD_PUT_REQ_SIZE;
        //coh_send_sgl[j].addr = (uint64_t) (uintptr_t) (coh_buf + j);
        if (CLIENT_ENABLE_INLINING == 0) coh_send_sgl[j].lkey = coh_mr->lkey;
        for (i = 0; i < MESSAGES_IN_BCAST; i++) {
            uint16_t rm_id;
            if (i < machine_id) rm_id = (uint16_t) i;
            else rm_id = (uint16_t) ((i + 1) % MACHINE_NUM);
            uint16_t clt_i = (uint16_t) (rm_id * CLIENTS_PER_MACHINE + local_client_id);
            uint16_t index = (uint16_t) ((j * MESSAGES_IN_BCAST) + i);
            assert (index < MAX_MSGS_IN_PCIE_BCAST_BATCH);
            if (ENABLE_MULTICAST == 1) {
                coh_send_wr[index].wr.ud.ah = mcast->send_ah;
                coh_send_wr[index].wr.ud.remote_qpn = mcast->qpn;
                coh_send_wr[index].wr.ud.remote_qkey = mcast->qkey;
            } else {
                coh_send_wr[index].wr.ud.ah = remote_clt_qp[clt_i][BROADCAST_UD_QP_ID].ah;
                coh_send_wr[index].wr.ud.remote_qpn = (uint32) remote_clt_qp[clt_i][BROADCAST_UD_QP_ID].qpn;
                coh_send_wr[index].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
            }
            ///if (protocol == EVENTUAL) coh_send_wr[index].opcode = IBV_WR_SEND_WITH_IMM; // TODO we should remove imms from here too
            ///else
            coh_send_wr[index].opcode = IBV_WR_SEND; // Attention!! there is no immediate here, cids do the job!
            coh_send_wr[index].num_sge = 1;
            coh_send_wr[index].sg_list = &coh_send_sgl[j];
            ///if (protocol == EVENTUAL) coh_send_wr[index].imm_data = (uint32) machine_id;
            if (CLIENT_ENABLE_INLINING == 1) coh_send_wr[index].send_flags = IBV_SEND_INLINE;
            coh_send_wr[index].next = (i == MESSAGES_IN_BCAST - 1) ? NULL : &coh_send_wr[index + 1];
        }
    }

    // Coherence Receives
    int max_coh_receives = MAX_COH_RECEIVES;
    ///int max_coh_receives = protocol == EVENTUAL ? EC_MAX_COH_RECEIVES : MAX_COH_RECEIVES;
    for (i = 0; i < max_coh_receives; i++) {
        coh_recv_sgl[i].length = UD_REQ_SIZE;
//        if (protocol == EVENTUAL && ENABLE_MULTICAST == 1)
//            coh_recv_sgl[i].lkey = mcast->recv_mr->lkey;
//        else
        coh_recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        coh_recv_wr[i].sg_list = &coh_recv_sgl[i];
        coh_recv_wr[i].num_sge = 1;
    }

    // Do acknowledgements
    if (protocol == STRONG_CONSISTENCY) {
        // ACK WRs
        for (i = 0; i < BCAST_TO_CACHE_BATCH; i++) {
            ack_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
            //ack_wr[i].imm_data = machine_id;
            ack_sgl[i].length = HERD_GET_REQ_SIZE;
            ack_wr[i].opcode = IBV_WR_SEND; // Attention!! there is no immediate here, cids do the job!
            ack_wr[i].num_sge = 1;
            ack_wr[i].sg_list = &ack_sgl[i];
        }
    }
}

void set_up_credits(uint8_t credits[][MACHINE_NUM], struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                    struct ibv_recv_wr* credit_recv_wr, struct ibv_sge* credit_recv_sgl,
                    struct hrd_ctrl_blk *cb, int protocol)
{
    check_protocol(protocol);
    int i = 0;
    ///int max_credt_wrs = protocol == EVENTUAL ?  EC_MAX_CREDIT_WRS : MAX_CREDIT_WRS;
    ///int max_credit_recvs = protocol == EVENTUAL ? EC_MAX_CREDIT_RECVS : MAX_CREDIT_RECVS;
    int max_credt_wrs = MAX_CREDIT_WRS;
    int max_credit_recvs = MAX_CREDIT_RECVS;
    // Credits
//    if (protocol == EVENTUAL)
//        for (i = 0; i < MACHINE_NUM; i++) credits[EC_UPD_VC][i] = EC_CREDITS;
    ///else {
    for (i = 0; i < MACHINE_NUM; i++) {
        credits[ACK_VC][i] = ACK_CREDITS;
        credits[INV_VC][i] = INV_CREDITS;
        credits[UPD_VC][i] = UPD_CREDITS;
    }
    ///}
    // Credit WRs
    for (i = 0; i < max_credt_wrs; i++) {
        credit_send_sgl->length = 0;
        credit_send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
        credit_send_wr[i].num_sge = 0;
        credit_send_wr[i].sg_list = credit_send_sgl;
        ///    if (protocol == EVENTUAL) credit_send_wr[i].imm_data = (uint32) machine_id;
        credit_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        credit_send_wr[i].next = NULL;
        credit_send_wr[i].send_flags = IBV_SEND_INLINE;
    }
    //Credit Receives
    credit_recv_sgl->length = 64;
    credit_recv_sgl->lkey = cb->dgram_buf_mr->lkey;
    credit_recv_sgl->addr = (uintptr_t) &cb->dgram_buf[0];
    for (i = 0; i < max_credit_recvs; i++) {
        credit_recv_wr[i].sg_list = credit_recv_sgl;
        credit_recv_wr[i].num_sge = 1;
    }
}


/* ---------------------------------------------------------------------------
------------------------------WORKER INITIALIZATION --------------------------
---------------------------------------------------------------------------*/

//void set_up_wrs(struct wrkr_coalesce_mica_op** response_buffer, struct ibv_mr* resp_mr, struct hrd_ctrl_blk *cb, struct ibv_sge* recv_sgl,
//                struct ibv_recv_wr* recv_wr, struct ibv_send_wr* wr, struct ibv_sge* sgl, uint16_t wrkr_lid)
//{
//    uint16_t i;
//    if ((WORKER_ENABLE_INLINING == 0) || (ENABLE_WORKER_COALESCING == 1)) {
//        uint32_t resp_buf_size = ENABLE_WORKER_COALESCING == 1 ? sizeof(struct wrkr_coalesce_mica_op)* WORKER_SS_BATCH :
//                                 sizeof(struct mica_op)* WORKER_SS_BATCH; //the buffer needs to be large enough to deal with NIC asynchronous reads
//        *response_buffer = malloc(resp_buf_size);
//        resp_mr = register_buffer(cb->pd, (void*)(*response_buffer), resp_buf_size);
//    }
//
//    // Initialize the Work requests and the Receive requests
//    for (i = 0; i < WORKER_MAX_BATCH; i++) {
//        if (!ENABLE_COALESCING)
//          recv_sgl[i].length = HERD_PUT_REQ_SIZE + sizeof(struct ibv_grh);//req_size;
//        else recv_sgl[i].length = sizeof(struct wrkr_ud_req);
//        recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
//        recv_wr[i].sg_list = &recv_sgl[i];
//        recv_wr[i].num_sge = 1;
//
//
//        wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
//        wr[i].opcode = IBV_WR_SEND; // Immediate is not used here
//        if (WORKER_ENABLE_INLINING == 0) {
//            sgl[i].lkey = resp_mr->lkey;
//            // sgl[i].addr = (uintptr_t)(*response_buffer)[i].value;
//        }
//        if(ENABLE_MULTI_BATCHES) //) || MEASURE_LATENCY)
//            wr[i].opcode = IBV_WR_SEND_WITH_IMM; // Immediate is not used here
//        wr[i].num_sge = 1;
//        wr[i].sg_list = &sgl[i];
//        if(ENABLE_MULTI_BATCHES || MEASURE_LATENCY)
//            wr[i].imm_data = (uint32) (machine_id * WORKERS_PER_MACHINE) + wrkr_lid;
//    }
//}

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
void check_protocol(int protocol) {
    if (protocol != STRONG_CONSISTENCY) {
        ///if (protocol != EVENTUAL && protocol != STRONG_CONSISTENCY) {
        red_printf("Wrong protocol specified when setting up the queue depths %d \n", protocol);
        assert(false);
    }
}

/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/
// wrapper around getaddrinfo socket function
int get_addr(char *dst, struct sockaddr *addr)
{
    struct addrinfo *res;
    int ret;
    ret = getaddrinfo(dst, NULL, NULL, &res);
    if (ret) {
        printf("getaddrinfo failed - invalid hostname or IP address %s\n", dst);
        return ret;
    }
    memcpy(addr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    return ret;
}

//Handle the addresses
void resolve_addresses(struct mcast_info *mcast_data)
{
    int ret, i, clt_id = mcast_data->clt_id;
    char mcast_addr[40];
    // Source addresses (i.e. local IPs)
    mcast_data->src_addr = (struct sockaddr*)&mcast_data->src_in;
    ret = get_addr(local_IP, ((struct sockaddr *)&mcast_data->src_in)); // to bind
    if (ret) printf("Client: failed to get src address \n");
    for (i = 0; i < MCAST_QPS; i++) {
        ret = rdma_bind_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr);
        if (ret) perror("Client: address bind failed");
    }
    // Destination addresses(i.e. multicast addresses)
    for (i = 0; i < MCAST_GROUPS_PER_CLIENT; i ++) {
        mcast_data->dst_addr[i] = (struct sockaddr*)&mcast_data->dst_in[i];
        int m_cast_group_id = clt_id * MACHINE_NUM + i;
        sprintf(mcast_addr, "224.0.%d.%d", m_cast_group_id / 256, m_cast_group_id % 256);
        // printf("mcast addr %d: %s\n", i, mcast_addr);
        ret = get_addr((char*) &mcast_addr, ((struct sockaddr *)&mcast_data->dst_in[i]));
        if (ret) printf("Client: failed to get dst address \n");
    }
}

// Set up the Send and Receive Qps for the multicast
void set_up_qp(struct cm_qps* qps, int max_recv_q_depth)
{
    int ret, i, recv_q_depth;
    // qps[0].pd = ibv_alloc_pd(qps[0].cma_id->verbs); //new
    for (i = 0; i < MCAST_QPS; i++) {
        qps[i].pd = ibv_alloc_pd(qps[i].cma_id->verbs);
        if (i > 0) qps[i].pd = qps[0].pd;
        recv_q_depth = i == RECV_MCAST_QP ? max_recv_q_depth : 1; // TODO fix this
        qps[i].cq = ibv_create_cq(qps[i].cma_id->verbs, recv_q_depth, &qps[i], NULL, 0);
        struct ibv_qp_init_attr init_qp_attr;
        memset(&init_qp_attr, 0, sizeof init_qp_attr);
        init_qp_attr.cap.max_send_wr = 1;
        init_qp_attr.cap.max_recv_wr = (uint32) recv_q_depth;
        init_qp_attr.cap.max_send_sge = 1;
        init_qp_attr.cap.max_recv_sge = 1;
        init_qp_attr.qp_context = &qps[i];
        init_qp_attr.sq_sig_all = 0;
        init_qp_attr.qp_type = IBV_QPT_UD;
        init_qp_attr.send_cq = qps[i].cq;
        init_qp_attr.recv_cq = qps[i].cq;
        ret = rdma_create_qp(qps[i].cma_id, qps[i].pd, &init_qp_attr);
        if (ret) printf("unable to create QP \n");
    }
}

// Initial function to call to setup multicast, this calls the rest of the relevant functions
void setup_multicast(struct mcast_info *mcast_data, int recv_q_depth) {
    int ret, i, clt_id = mcast_data->clt_id;
    static enum rdma_port_space port_space = RDMA_PS_UDP;
    // Create the channel
    mcast_data->channel = rdma_create_event_channel();
    if (!mcast_data->channel) {
        printf("Client %d :failed to create event channel\n", mcast_data->clt_id);
        exit(1);
    }
    // Set up the cma_ids
    for (i = 0; i < MCAST_QPS; i++) {
        ret = rdma_create_id(mcast_data->channel, &mcast_data->cm_qp[i].cma_id, &mcast_data->cm_qp[i], port_space);
        if (ret) printf("Client %d :failed to create cma_id\n", mcast_data->clt_id);
    }
    // deal with the addresses
    resolve_addresses(mcast_data);
    // set up the 2 qps
    set_up_qp(mcast_data->cm_qp, recv_q_depth);

    struct rdma_cm_event *event;
    for (i = 0; i < MCAST_GROUPS_PER_CLIENT; i++) {
        int qp_i = i == machine_id ? SEND_MCAST_QP : RECV_MCAST_QP;
        ret = rdma_resolve_addr(mcast_data->cm_qp[i].cma_id, mcast_data->src_addr, mcast_data->dst_addr[i], 20000);
        if (ret) printf("Client %d: failed to resolve address: %d, qp_i %d \n", clt_id, i, qp_i);
        if (ret) perror("Reason");
        while (rdma_get_cm_event(mcast_data->channel, &event) == 0) {
            switch (event->event) {
                case RDMA_CM_EVENT_ADDR_RESOLVED:
                    // printf("Client %d: RDMA ADDRESS RESOLVED address: %d \n", clt_id, i);
                    ret = rdma_join_multicast(mcast_data->cm_qp[qp_i].cma_id, mcast_data->dst_addr[i], mcast_data);
                    if (ret) printf("unable to join multicast \n");
                    break;
                case RDMA_CM_EVENT_MULTICAST_JOIN:
                    if (i == machine_id) mcast_data->mcast_ud_param = event->param.ud;
                    // printf("RDMA JOIN MUlTICAST EVENT %d \n", i);
                    break;
                case RDMA_CM_EVENT_MULTICAST_ERROR:
                default:
                    break;
            }
            rdma_ack_cm_event(event);
            if (event->event == RDMA_CM_EVENT_MULTICAST_JOIN) break;
        }
    }
}
// if (i != RECV_MCAST_QP) {
// destroying the QPs works fine but hurts performance...
//  rdma_destroy_qp(mcast_data->cm_qp[i].cma_id);
//  rdma_destroy_id(mcast_data->cm_qp[i].cma_id);
//}
//    }
// rdma_destroy_event_channel(mcast_data->channel);
// if (mcast_data->mcast_ud_param == NULL) mcast_data->mcast_ud_param = event->param.ud;
//}

//
//// call to test the multicast
//void multicast_testing(struct mcast_essentials *mcast, int clt_gid, struct hrd_ctrl_blk *cb)
//{
//
//    struct ibv_wc mcast_wc;
//    printf ("Client: Multicast Qkey %u and qpn %u \n", mcast->qkey, mcast->qpn);
//
//
//    struct ibv_sge mcast_sg;
//    struct ibv_send_wr mcast_wr;
//    struct ibv_send_wr *mcast_bad_wr;
//
//    memset(&mcast_sg, 0, sizeof(mcast_sg));
//    mcast_sg.addr	  = (uintptr_t)cb->dgram_buf;
//    mcast_sg.length = 10;
//    //mcast_sg.lkey	  = cb->dgram_buf_mr->lkey;
//
//    memset(&mcast_wr, 0, sizeof(mcast_wr));
//    mcast_wr.wr_id      = 0;
//    mcast_wr.sg_list    = &mcast_sg;
//    mcast_wr.num_sge    = 1;
//    mcast_wr.opcode     = IBV_WR_SEND_WITH_IMM;
//    mcast_wr.send_flags = IBV_SEND_SIGNALED | IBV_SEND_INLINE;
//    mcast_wr.imm_data   = (uint32) clt_gid + 120 + (machine_id * 10);
//    mcast_wr.next       = NULL;
//
//    mcast_wr.wr.ud.ah          = mcast->send_ah;
//    mcast_wr.wr.ud.remote_qpn  = mcast->qpn;
//    mcast_wr.wr.ud.remote_qkey = mcast->qkey;
//
//    if (ibv_post_send(cb->dgram_qp[0], &mcast_wr, &mcast_bad_wr)) {
//        fprintf(stderr, "Error, ibv_post_send() failed\n");
//        assert(false);
//    }
//
//    printf("THe mcast was sent, I am waiting for confirmation imm data %d\n", mcast_wr.imm_data);
//    hrd_poll_cq(cb->dgram_send_cq[0], 1, &mcast_wc);
//    printf("The mcast was sent \n");
//    hrd_poll_cq(mcast->recv_cq, 1, &mcast_wc);
//    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
//    hrd_poll_cq(mcast->recv_cq, 1, &mcast_wc);
//    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
//    hrd_poll_cq(mcast->recv_cq, 1, &mcast_wc);
//    printf("Client %d imm data recved %d \n", clt_gid, mcast_wc.imm_data);
//
//    exit(0);
//}
