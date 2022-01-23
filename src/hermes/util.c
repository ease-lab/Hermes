//
// Created by akatsarakis on 15/03/18.
//
#define _GNU_SOURCE

#include "util.h"
#include "hrd.h"
#include "inline-util.h"
#include "spacetime.h"

int
spawn_stats_thread(void)
{
  pthread_t* thread_arr = malloc(sizeof(pthread_t));
  pthread_attr_t attr;
  cpu_set_t cpus_stats;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpus_stats);

  if (DEFAULT_THREAD_OF_STAT_THREAD != -1) {
    CPU_SET(DEFAULT_THREAD_OF_STAT_THREAD, &cpus_stats);
  } else {
    if (MAX_WORKERS_PER_MACHINE > 17)
      CPU_SET(39, &cpus_stats);
    else
      CPU_SET(2 * MAX_WORKERS_PER_MACHINE + 2, &cpus_stats);
  }

  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
  return pthread_create(&thread_arr[0], &attr, print_stats_thread, NULL);
}

uint8_t
is_state_code(uint8_t code)
{
  switch (code) {
    // Object States
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
      // Input opcodes
    case ST_OP_GET:
    case ST_OP_PUT:
    case ST_OP_RMW:
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
    // RMW
    case ST_RMW_ABORT:
    case ST_RMW_STALL:
    case ST_RMW_SUCCESS:
    case ST_RMW_COMPLETE:
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
    case ST_IN_PROGRESS_RMW:
    case ST_IN_PROGRESS_REPLAY:
      return 1;
    default:
      return 0;
  }
}

char*
code_to_str(uint8_t code)
{
  switch (code) {
    // Object States
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
    // Input opcodes
    case ST_OP_GET:
      return "ST_OP_GET";
    case ST_OP_PUT:
      return "ST_OP_PUT";
    case ST_OP_RMW:
      return "ST_OP_RMW";
    case ST_OP_INV:
      return "ST_OP_INV";
    case ST_OP_INV_ABORT:
      return "ST_OP_INV_ABORT";
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
    // Response opcodes
    case ST_GET_COMPLETE:
      return "ST_GET_COMPLETE";
    case ST_PUT_SUCCESS:
      return "ST_PUT_SUCCESS";
    case ST_PUT_COMPLETE:
      return "ST_PUT_COMPLETE";
    case ST_RMW_SUCCESS:
      return "ST_RMW_SUCCESS";
    case ST_RMW_COMPLETE:
      return "ST_RMW_COMPLETE";
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
    case ST_RMW_STALL:
      return "ST_RMW_STALL";
    case ST_RMW_ABORT:
      return "ST_RMW_ABORT";
    case ST_PUT_COMPLETE_SEND_VALS:
      return "ST_PUT_COMPLETE_SEND_VALS";
    case ST_RMW_COMPLETE_SEND_VALS:
      return "ST_RMW_COMPLETE_SEND_VALS";
    case ST_REPLAY_COMPLETE_SEND_VALS:
      return "ST_REPLAY_COMPLETE_SEND_VALS";
    case ST_INV_OUT_OF_GROUP:
      return "ST_INV_OUT_OF_GROUP";
    case ST_SEND_CRD:
      return "ST_SEND_CRD";
    // Ops bucket states
    case ST_EMPTY:
      return "ST_EMPTY";
    case ST_NEW:
      return "ST_NEW";
    case ST_IN_PROGRESS_PUT:
      return "ST_IN_PROGRESS_PUT";
    case ST_IN_PROGRESS_RMW:
      return "ST_IN_PROGRESS_RMW";
    case ST_IN_PROGRESS_REPLAY:
      return "ST_IN_PROGRESS_REPLAY";
    case ST_COMPLETE:
      return "ST_COMPLETE";
    // Buffer Types
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
    // Failure related
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

// Creates a trace with a uniform distribution without a backing file
void
create_uni_trace(struct spacetime_trace_command** cmds, int worker_gid)
{
  srand(time(NULL) + worker_gid * 7);
  *cmds =
      malloc((NUM_OF_REP_REQS + 1) * sizeof(struct spacetime_trace_command));
  int rmws = 0;

  uint32_t i, writes = 0;
  // parse file line by line and insert trace to cmd.
  for (i = 0; i < NUM_OF_REP_REQS; i++) {
    // Before reading the request deside if it's gone be read or write
    (*cmds)[i].opcode =
        (uint8_t)(update_ratio == 1000 || ((rand() % 1000 < update_ratio))
                      ? ST_OP_PUT
                      : ST_OP_GET);

    if (ENABLE_RMWs && (*cmds)[i].opcode == ST_OP_PUT)
      (*cmds)[i].opcode =
          (uint8_t)(rmw_ratio == 1000 || ((rand() % 1000 < rmw_ratio))
                        ? ST_OP_RMW
                        : ST_OP_PUT);

    if ((*cmds)[i].opcode == ST_OP_RMW) rmws++;
    if ((*cmds)[i].opcode == ST_OP_PUT) writes++;

    //--- KEY ID----------
    uint32 key_id = KEY_NUM != 0 ? (uint32)rand() % KEY_NUM
                                 : (uint32)rand() % SPACETIME_NUM_KEYS;
    if (USE_A_SINGLE_KEY == 1) key_id = 0;
    uint128 key_hash = CityHash128((char*)&(key_id), 4);
    //        memcpy(&(*cmds)[i].key_hash, &key_hash, 16); // this is for 16B
    //        keys
    memcpy(&(*cmds)[i].key_hash, &((uint64_t*)&key_hash)[1], 8);
    (*cmds)[i].key_id =
        (uint8_t)(key_id < 255 ? key_id : ST_KEY_ID_255_OR_HIGHER);
  }

  if (worker_gid % num_workers == 0)
    printf(
        "Update Ratio: %.2f%% (Writes|RMWs: %.2f%%|%.2f%%)\n"
        "Trace w_size %d \n",
        (double)((writes + rmws) * 100) / NUM_OF_REP_REQS,
        (double)(writes * 100) / NUM_OF_REP_REQS,
        (double)(rmws * 100) / NUM_OF_REP_REQS, NUM_OF_REP_REQS);
  (*cmds)[NUM_OF_REP_REQS].opcode = NOP;
  // printf("CLient %d Trace w_size: %d, debug counter %d hot keys %d, cold keys
  // %d \n",l_id, cmd_count, debug_cnt,
  //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace
  //         );
}

// Parse a trace, use this only for skewed workloads as uniform trace can be
// created (see create_uni_trace)
int
parse_trace(char* path, struct spacetime_trace_command** cmds, int worker_gid)
{
  FILE* fp;
  ssize_t read;
  size_t len = 0;
  char* ptr;
  char* word;
  char* saveptr;
  char* line = NULL;
  int rmws = 0;
  int writes = 0;
  int cmd_count = 0;
  uint32_t hottest_key_counter = 0;
  uint32_t ten_hottest_keys_counter = 0;
  uint32_t twenty_hottest_keys_counter = 0;

  fp = fopen(path, "r");
  if (fp == NULL) {
    printf("ERROR: Cannot open file: %s\n", path);
    exit(EXIT_FAILURE);
  }

  while ((read = getline(&line, &len, fp)) != -1)
    cmd_count++;

  //    printf("File %s has %d lines \n", path, cmd_count);

  fclose(fp);
  if (line) free(line);

  len = 0;
  line = NULL;

  fp = fopen(path, "r");
  if (fp == NULL) {
    printf("ERROR: Cannot open file: %s\n", path);
    exit(EXIT_FAILURE);
  }

  (*cmds) = malloc((cmd_count + 1) * sizeof(struct spacetime_trace_command));

  // Initialize random with a seed based on local time and a worker / machine id
  srand((unsigned int)(time(NULL) + worker_gid * 7));

  int debug_cnt = 0;
  // parse file line by line and insert trace to cmd.
  for (int i = 0; i < cmd_count; i++) {
    if ((read = getline(&line, &len, fp)) == -1) {
      printf("ERROR: Problem while reading the trace!\n");
      exit(1);
    }
    int word_count = 0;
    assert(word_count == 0);
    word = strtok_r(line, " ", &saveptr);

    // Before reading the request deside if it's gone be read or write
    (*cmds)[i].opcode =
        (uint8_t)(update_ratio == 1000 || ((rand() % 1000 < update_ratio))
                      ? ST_OP_PUT
                      : ST_OP_GET);

    if (ENABLE_RMWs && (*cmds)[i].opcode == ST_OP_PUT)
      (*cmds)[i].opcode =
          (uint8_t)(rmw_ratio == 1000 || ((rand() % 1000 < rmw_ratio))
                        ? ST_OP_RMW
                        : ST_OP_PUT);

    if ((*cmds)[i].opcode == ST_OP_PUT) writes++;
    if ((*cmds)[i].opcode == ST_OP_RMW) rmws++;

    while (word != NULL) {
      if (word[strlen(word) - 1] == '\n') word[strlen(word) - 1] = 0;

      if (word_count == 0) {
        uint32_t key_id = (uint32_t)strtoul(word, &ptr, 10);
        if (key_id == 0) hottest_key_counter++;
        if (key_id < 10) ten_hottest_keys_counter++;
        if (key_id < 20) twenty_hottest_keys_counter++;
        uint128 key_hash = CityHash128((char*)&(key_id), 4);
        //              memcpy(&(*cmds)[i].key_hash, &key_hash, 16); // this is
        //              for 16B keys
        memcpy(&(*cmds)[i].key_hash, &((uint64_t*)&key_hash)[1],
               8);  // this is for 8B keys
        (*cmds)[i].key_id =
            (uint8_t)(key_id < 255 ? key_id : ST_KEY_ID_255_OR_HIGHER);
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

  if (worker_gid % num_workers == 0) {
    printf(
        "Trace size: %d | Hottest key (10 | 20 keys): %.2f%% (%.2f | %.2f "
        "%%)\n",
        cmd_count, (100 * hottest_key_counter / (double)cmd_count),
        (100 * ten_hottest_keys_counter / (double)cmd_count),
        (100 * twenty_hottest_keys_counter / (double)cmd_count));
    printf("Update Ratio: %.2f%% (Writes|RMWs: %.2f%%|%.2f%%)\n",
           (double)((writes + rmws) * 100) / cmd_count,
           (double)(writes * 100) / cmd_count,
           (double)(rmws * 100) / cmd_count);
  }
  (*cmds)[cmd_count].opcode = NOP;
  // printf("Thread %d Trace w_size: %d, debug counter %d hot keys %d, cold keys
  // %d \n",l_id, cmd_count, debug_cnt,
  //         t_stats[l_id].hot_keys_per_trace, t_stats[l_id].cold_keys_per_trace
  //         );
  assert(cmd_count == debug_cnt);
  fclose(fp);
  if (line) free(line);
  return cmd_count;
}

void
trace_init(struct spacetime_trace_command** trace, uint16_t worker_gid)
{
  // create the trace path path
  if (FEED_FROM_TRACE == 1) {
    char local_client_id[6];
    char machine_num[4];
    // get / create path for the trace
    sprintf(local_client_id, "%d", worker_gid % num_workers);
    sprintf(machine_num, "%d", machine_id);
    char path[2048];
    char cwd[1024];
    char* was_successful = getcwd(cwd, sizeof(cwd));

    if (!was_successful) {
      printf("ERROR: getcwd failed!\n");
      exit(EXIT_FAILURE);
    }

    double zipf_exponent = ZIPF_EXPONENT_OF_TRACE / 100.0;

    snprintf(path, sizeof(path), "%s%s%04d%s%.2f%s", cwd,
             "/../../traces/current-splitted-traces/t_", worker_gid, "_a_",
             zipf_exponent, ".txt");

    // initialize the command array from the trace file
    parse_trace(path, trace, worker_gid);
  } else
    create_uni_trace(trace, worker_gid);
}

// set up the OPS buffers
void
setup_kvs_buffs(spacetime_op_t** ops, spacetime_inv_t** inv_recv_ops,
                spacetime_ack_t** ack_recv_ops, spacetime_val_t** val_recv_ops)
{
  *ops = memalign(4096, MAX_BATCH_KVS_OPS_SIZE * (sizeof(spacetime_op_t)));
  memset(*ops, 0, MAX_BATCH_KVS_OPS_SIZE * (sizeof(spacetime_op_t)));
  assert(ops != NULL);

  // Dirty way to support ACKs that might be as big as INVs
  uint16_t ack_size =
      ENABLE_RMWs ? sizeof(spacetime_inv_t) : sizeof(spacetime_ack_t);
  spacetime_inv_t** rmw_ack_r_ops = (spacetime_inv_t**)ack_recv_ops;
  /// Network ops
  /// TODO should we memalign aswell?

  uint32_t no_ops =
      (uint32_t)(credits_num * MAX_REMOTE_MACHINES *
                 max_coalesce);  // credits * remote_machines * max_req_coalesce
  //    uint32_t no_ops = (uint32_t) (credits_num * remote_machine_num *
  //    max_coalesce); //credits * remote_machines * max_req_coalesce
  *inv_recv_ops = (spacetime_inv_t*)malloc(no_ops * sizeof(spacetime_inv_t));
  *ack_recv_ops = (spacetime_ack_t*)malloc(no_ops * ack_size);
  *val_recv_ops = (spacetime_val_t*)malloc(
      no_ops *
      sizeof(spacetime_val_t)); /* Batch of incoming broadcasts for the Cache*/
  assert(*inv_recv_ops != NULL && *ack_recv_ops != NULL &&
         *val_recv_ops != NULL);

  memset(*inv_recv_ops, 0, no_ops * sizeof(spacetime_inv_t));
  memset(*ack_recv_ops, 0, no_ops * sizeof(spacetime_ack_t));
  memset(*val_recv_ops, 0, no_ops * sizeof(spacetime_val_t));

  for (int i = 0; i < no_ops; ++i) {
    (*val_recv_ops)[i].opcode = ST_EMPTY;
    (*inv_recv_ops)[i].op_meta.opcode = ST_EMPTY;
    if (ENABLE_RMWs == 0)
      (*ack_recv_ops)[i].opcode = ST_EMPTY;
    else
      (*rmw_ack_r_ops)[i].op_meta.opcode = ST_EMPTY;
  }

  for (int i = 0; i < MAX_BATCH_KVS_OPS_SIZE; ++i) {
    (*ops)[i].op_meta.opcode = ST_EMPTY;
    (*ops)[i].op_meta.state = ST_EMPTY;
  }
}
