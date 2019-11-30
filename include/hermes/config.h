//
// Created by akatsarakis on 15/03/18.
//

#ifndef SPACETIME_CONFIG_H
#define SPACETIME_CONFIG_H
#include <stdint.h>
#include <assert.h>
#include "sizes.h"

#define HERMES_CEILING(x,y) (((x) + (y) - 1) / (y))

#define MACHINE_NUM 3
#define REMOTE_MACHINES (MACHINE_NUM - 1)
#define GROUP_MEMBERSHIP_ARRAY_SIZE HERMES_CEILING(MACHINE_NUM, 8) //assuming uint8_t
#define WORKERS_PER_MACHINE 38
#define KV_SOCKET 0
#define SOCKET_TO_START_SPAWNING_THREADS 0
#define USE_ALL_SOCKETS 1
#define ENABLE_HYPERTHREADING 1
#define UPDATE_RATIO 1000
#define RMW_RATIO 0 //percentage of writes to be RMWs
#define MAX_BATCH_OPS_SIZE 50 // up to 254

#define ENABLE_RMWs 0 //0
static_assert(ENABLE_RMWs == 0 || ENABLE_RMWs == 1,"");

//LATENCY
#define MEASURE_LATENCY 1
#define THREAD_MEASURING_LATENCY 4
#define MAX_LATENCY 1000 //in us
#define LATENCY_BUCKETS 1000
#define LATENCY_PRECISION (MAX_LATENCY / LATENCY_BUCKETS) //latency granularity in us

// Fairness
#define ENABLE_VIRTUAL_NODE_IDS 0 //0
#define VIRTUAL_NODE_IDS_PER_NODE 20
static_assert(!ENABLE_VIRTUAL_NODE_IDS || VIRTUAL_NODE_IDS_PER_NODE > MACHINE_NUM, "");
static_assert(!ENABLE_VIRTUAL_NODE_IDS || MACHINE_NUM * VIRTUAL_NODE_IDS_PER_NODE < 255, "");

// SKEW
#define ENABLE_COALESCE_OF_HOT_REQS 0 //0 //WARNING!!! this must be disabled for cr
#define COALESCE_N_HOTTEST_KEYS 100
#define ENABLE_READ_COMPLETE_AFTER_VAL_RECV_OF_HOT_REQS 0 // 1
#define ENABLE_WRITE_COALESCE_TO_THE_SAME_KEY_IN_SAME_NODE 0

//DEBUG
#define ENABLE_ASSERTIONS 1
#define DISABLE_VALS_FOR_DEBUGGING 0
#define KEY_NUM 0 //use 0 to disable


//REQUESTS
#define FEED_FROM_TRACE 0 //0
#define ZIPF_EXPONENT_OF_TRACE 99 // if FEED_FROM_TRACE == 1 | this is divided by 100 (e.g. use 99 for  a = 0.99)
#define NUM_OF_REP_REQS K_256     // if FEED_FROM_TRACE == 0
#define USE_A_SINGLE_KEY 0        // if FEED_FROM_TRACE == 0
#define ST_KEY_ID_255_OR_HIGHER 255

// FAILURES
#define ENABLE_HADES_FAILURE_DETECTION 0 //0
static_assert(ENABLE_HADES_FAILURE_DETECTION == 0, "WARNING HADES is currently not working");



/*-------------------------------------------------
----------------- REQ INLINING --------------------
--------------------------------------------------*/
#define DISABLE_INLINING 0

/*-------------------------------------------------
----------------- REQ COALESCING -------------------
--------------------------------------------------*/
#define MAX_REQ_COALESCE 15
//#define MAX_REQ_COALESCE MAX_BATCH_OPS_SIZE

/*-------------------------------------------------
-----------------FLOW CONTROL---------------------
--------------------------------------------------*/
#define CREDITS_PER_REMOTE_WORKER (1 * MAX_REQ_COALESCE) // Hermes
//#define CREDITS_PER_REMOTE_WORKER 250 //(MAX_BATCH_OPS_SIZE) // CR

////////////////////////////////
/// NOT TUNABLE
////////////////////////////////
/*-------------------------------------------------
----------------- MAX HERMES OPS SIZE -------------
--------------------------------------------------*/
#define MAX_RECV_WRS (CREDITS_PER_REMOTE_WORKER * REMOTE_MACHINES)
#define MAX_MSG_RECV_OPS_SIZE (MAX_RECV_WRS * MAX_REQ_COALESCE)
#define HERMES_MAX_BATCH_SIZE MAX(MAX_MSG_RECV_OPS_SIZE, MAX_BATCH_OPS_SIZE)
#define CR_MAX_BATCH_SIZE HERMES_MAX_BATCH_SIZE


/*-------------------------------------------------
---------------- QPs Numbers ----------------------
--------------------------------------------------*/
typedef enum {
    INV_UD_QP_ID = 0,
    ACK_UD_QP_ID,
    VAL_UD_QP_ID,
    CRD_UD_QP_ID,
    END_HERMES_QPS_ENUM
} hermes_qps_enum;
//QPs
#define TOTAL_WORKER_UD_QPs END_HERMES_QPS_ENUM
#define TOTAL_WORKER_N_FAILURE_DETECTION_UD_QPs (TOTAL_WORKER_UD_QPs + (ENABLE_HADES_FAILURE_DETECTION ? 2 : 0))



// ////////////////
// <CR configuration>
// ////////////////

//CR DEBUG
#define CR_ENABLE_ONLY_HEAD_REQS 0
#define CR_ENABLE_ALL_NODES_GETS_EXCEPT_HEAD 0
#define CR_ENABLE_BLOCKING_INVALID_WRITES_ON_HEAD 0

#define CR_REMOTE_WRITES_MAX_REQ_COALESCE INV_MAX_REQ_COALESCE
#define CR_REMOTE_WRITES_CREDITS (CREDITS_PER_REMOTE_WORKER / MACHINE_NUM)

#define CR_ENABLE_REMOTE_READS 0
#define CR_REMOTE_READS_CREDITS 20
#define CR_REMOTE_READS_MAX_REQ_COALESCE INV_MAX_REQ_COALESCE

#define CR_ENABLE_EARLY_INV_CRDS 1

#define CR_ACK_CREDITS (255) //(MACHINE_NUM * ACK_CREDITS)
//#define CR_ACK_CREDITS (MACHINE_NUM * 255) //(MACHINE_NUM * ACK_CREDITS)

#define CR_INV_UD_QP_ID 0
#define CR_INV_CRD_UD_QP_ID 1
#define CR_ACK_UD_QP_ID (CR_ENABLE_EARLY_INV_CRDS == 1 ? 2 : 1)
#define CR_REMOTE_WRITES_UD_QP_ID (CR_ACK_UD_QP_ID + 1)
#define CR_REMOTE_WRITE_CRD_UD_QP_ID (CR_REMOTE_WRITES_UD_QP_ID + 1)
#define CR_REMOTE_READS_UD_QP_ID  (CR_REMOTE_WRITE_CRD_UD_QP_ID + 1)
#define CR_REMOTE_READS_RESP_UD_QP_ID (CR_REMOTE_READS_UD_QP_ID + 1)
#define CR_TOTAL_WORKER_UD_QPs (TOTAL_WORKER_UD_QPs + (CR_ENABLE_REMOTE_READS ? 2 : 0) + (CR_ENABLE_EARLY_INV_CRDS ? 1 : 0))
// ////////////////
// </CR configuration>
// ////////////////




/*-------------------------------------------------
-----------------PRINTS (DBG)---------------------
--------------------------------------------------*/
///Warning some prints assume that there are no faults (multiplications with REMOTE_MACHINES)
#define MAX_THREADS_TO_PRINT 1
#define ENABLE_REQ_PRINTS 0
#define ENABLE_BATCH_OP_PRINTS 0
#define ENABLE_INV_PRINTS 0
#define ENABLE_ACK_PRINTS 0
#define ENABLE_VAL_PRINTS 0

//Stats prints
#define PRINT_STATS_EVERY_MSECS 4000 //5000 //10000 //10
#define PRINT_WORKER_STATS 0

//Stats
#define EXIT_ON_STATS_PRINT 1
#define PRINT_NUM_STATS_BEFORE_EXITING 5 //80
#define DUMP_XPUT_STATS_TO_FILE 1

//FAKE NODE FAILURE
#define FAKE_FAILURE 0
#define NODE_TO_FAIL 2
#define ROUNDS_BEFORE_FAILURE 2

//FAILURE DETECTION
#define WORKER_WITH_FAILURE_DETECTOR 0

// Rarely change
#define TOTAL_THREADS_PER_CORE 2
#define TOTAL_CORES_PER_SOCKET 10
#define TOTAL_NUMBER_OF_SOCKETS 2
#define TOTAL_HW_CORES (TOTAL_THREADS_PER_CORE * TOTAL_CORES_PER_SOCKET * TOTAL_NUMBER_OF_SOCKETS)
#define BASE_SHM_KEY 24
#define WORKER_SL 0 // service level for the workers

///Debug
//#define SPACETIME DEBUG 2
#ifndef SPACETIME_DEBUG
# define SPACETIME_DEBUG 0
#endif




struct thread_params {
    int id;
};

struct latency_counters{
    uint32_t read_reqs[LATENCY_BUCKETS + 1];
    uint32_t write_reqs[LATENCY_BUCKETS + 1];
    int max_read_latency;
    int max_write_latency;
    long long total_measurements;
};


extern struct latency_counters latency_count;

// global config (CLI) configurable vars
extern uint8_t is_CR;
extern int update_ratio;
extern int rmw_ratio;
extern int num_workers;
extern int credits_num;
extern int max_coalesce;
extern int max_batch_size; //for batches to KVS

#endif //SPACETIME_CONFIG_H
