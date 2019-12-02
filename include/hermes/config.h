//
// Created by akatsarakis on 15/03/18.
//

#ifndef SPACETIME_CONFIG_H
#define SPACETIME_CONFIG_H
#include <stdint.h>
#include <assert.h>
#include "sizes.h"

// MAX_ defines are treated as DEFAULT_ as well (i.e., if not altered by CLI args)

/*-------------------------------------------------
------------ SETUP & DEFAULT SETTINGS -------------
--------------------------------------------------*/
#define MAX_MACHINE_NUM          5 // maximum nodes
#define MAX_WORKERS_PER_MACHINE 38 // maximum number of threads per node

// Number of sockets (numa nodes), cores and h/w threads per core on each node
#define TOTAL_THREADS_PER_CORE   2
#define TOTAL_CORES_PER_SOCKET  10
#define TOTAL_NUMBER_OF_SOCKETS  2

// Default workload writes / updates accesses (the rest are reads)
#define DEFAULT_UPDATE_RATIO  1000  // both writes and RMWs (RMW_RATIO inderectly provides WRITE_RATIO)
#define DEFAULT_RMW_RATIO        0  // percentage of UPDATE_RATIO to be RMWs
#define ENABLE_RMWs              0  // if RMWs is not enabled then all UPDATE_RATIO == WRITE_RATIO


// Max operations per-thread to batches to the KVS (either received packets or read/write/RMW requests)
#define MAX_BATCH_KVS_OPS_SIZE 250 //50 // up to 254


/*-------------------------------------------------
----------------- RDMA SETTINGS -------------------
--------------------------------------------------*/
// Request coalescing (max --readily available-- messages to batch in a single RDMA packet)
#define MAX_REQ_COALESCE        15
//#define MAX_REQ_COALESCE MAX_BATCH_KVS_OPS_SIZE

// Flow control
#define MAX_CREDITS_PER_REMOTE_WORKER (1 * MAX_REQ_COALESCE) // Hermes

// Request inlining
#define DISABLE_INLINING         0


/*-------------------------------------------------
----------------- SECONDARY SETTINGS --------------
--------------------------------------------------*/
// LATENCY
#define DEFAULT_MEASURE_LATENCY 0
#define DEFAULT_WORKER_MEASURING_LATENCY 0
#define MAX_LATENCY 1000 //in us
#define LATENCY_BUCKETS 1000
#define LATENCY_PRECISION (MAX_LATENCY / LATENCY_BUCKETS) //latency granularity in us

// FAIRNESS
#define ENABLE_VIRTUAL_NODE_IDS 0 //0
#define VIRTUAL_NODE_IDS_PER_NODE 20

// SKEW
#define ENABLE_COALESCE_OF_HOT_REQS 0 //0 //WARNING!!! this must be disabled for cr
#define COALESCE_N_HOTTEST_KEYS 100
#define ENABLE_READ_COMPLETE_AFTER_VAL_RECV_OF_HOT_REQS 0 // 1
#define ENABLE_WRITE_COALESCE_TO_THE_SAME_KEY_IN_SAME_NODE 0

// DEBUG
#define ENABLE_ASSERTIONS 0
#define DISABLE_VALS_FOR_DEBUGGING 0
#define KEY_NUM 0 //use 0 to disable


// REQUESTS
#define FEED_FROM_TRACE 0 //0
#define ZIPF_EXPONENT_OF_TRACE 99 // if FEED_FROM_TRACE == 1 | this is divided by 100 (e.g. use 99 for  a = 0.99)
#define NUM_OF_REP_REQS K_256     // if FEED_FROM_TRACE == 0
#define USE_A_SINGLE_KEY 0        // if FEED_FROM_TRACE == 0
#define ST_KEY_ID_255_OR_HIGHER 255




/*-------------------------------------------------
---------------- Debug and others -----------------
--------------------------------------------------*/
//DBG Prints
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

// FAILURE DETECTION (RM)
#define ENABLE_HADES_FAILURE_DETECTION 0 //0
#define WORKER_WITH_FAILURE_DETECTOR 0
static_assert(ENABLE_HADES_FAILURE_DETECTION == 0, "WARNING HADES is currently not working");

//FAKE NODE FAILURE
#define FAKE_FAILURE 0
#define NODE_TO_FAIL 2
#define ROUNDS_BEFORE_FAILURE 2



// Rarely (or never) change
#define BASE_SHM_KEY 24
#define WORKER_SL 0 // service level for the workers
#define MAX_REMOTE_MACHINES (MAX_MACHINE_NUM - 1)
#define HERMES_CEILING(x,y) (((x) + (y) - 1) / (y))
#define GROUP_MEMBERSHIP_ARRAY_SIZE HERMES_CEILING(MAX_MACHINE_NUM, 8) //assuming uint8_t
#define TOTAL_HW_CORES (TOTAL_THREADS_PER_CORE * TOTAL_CORES_PER_SOCKET * TOTAL_NUMBER_OF_SOCKETS)
static_assert(MAX_WORKERS_PER_MACHINE < TOTAL_HW_CORES - 1, "Leave at least a hw thread free for OS etc..");

#define KV_SOCKET 0 // socket to allocate KVS (huge-)pages
#define USE_ALL_SOCKETS 1
#define ENABLE_HYPERTHREADING 1
#define SOCKET_TO_START_SPAWNING_THREADS 0


///Debug
//#define SPACETIME DEBUG 2
#ifndef SPACETIME_DEBUG
# define SPACETIME_DEBUG 0
#endif


////////////////////////////////
/// NOT TUNABLE
////////////////////////////////
/*-------------------------------------------------
----------------- MAX HERMES OPS SIZE -------------
--------------------------------------------------*/
#define MAX_MSG_RECV_OPS_SIZE (MAX_CREDITS_PER_REMOTE_WORKER * MAX_REMOTE_MACHINES * MAX_REQ_COALESCE)
#define HERMES_MAX_BATCH_SIZE MAX(MAX_MSG_RECV_OPS_SIZE, MAX_BATCH_KVS_OPS_SIZE)



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



/*-------------------------------------------------
----------------- CR CONFIGURATION ----------------
--------------------------------------------------*/
#define CR_ENABLE_REMOTE_READS   0
#define CR_REMOTE_READS_CREDITS 20
//#define CR_REMOTE_READS_MAX_REQ_COALESCE MAX_REQ_COALESCE

#define MAX_CREDITS_PER_REMOTE_WORKER_CR 250 //(MAX_BATCH_KVS_OPS_SIZE) // CR

#define CR_ACK_CREDITS (255) // //(MAX_MACHINE_NUM * 255)

#define CR_ENABLE_EARLY_INV_CRDS 1 // optimization to increase request pipelining


typedef enum {
    CR_INV_UD_QP_ID = 0,
#ifdef CR_ENABLE_EARLY_INV_CRDS
    CR_INV_CRD_UD_QP_ID,
#endif
    CR_ACK_UD_QP_ID,
    CR_REMOTE_WRITES_UD_QP_ID,
    CR_REMOTE_WRITE_CRD_UD_QP_ID,
    CR_REMOTE_READS_UD_QP_ID,
    CR_REMOTE_READS_RESP_UD_QP_ID
} cr_qps_enum;

#define CR_TOTAL_WORKER_UD_QPs (TOTAL_WORKER_UD_QPs + (CR_ENABLE_REMOTE_READS ? 2 : 0) + (CR_ENABLE_EARLY_INV_CRDS ? 1 : 0))

// Max CR batch op size
#define MAX_MSG_RECV_OPS_SIZE_CR (MAX_REQ_COALESCE * MAX_CREDITS_PER_REMOTE_WORKER_CR * MAX_REMOTE_MACHINES)
#define CR_MAX_BATCH_SIZE MAX(MAX_MSG_RECV_OPS_SIZE_CR, MAX_BATCH_KVS_OPS_SIZE)

//CR DEBUG
#define CR_ENABLE_ONLY_HEAD_REQS 0
#define CR_ENABLE_ALL_NODES_GETS_EXCEPT_HEAD 0
#define CR_ENABLE_BLOCKING_INVALID_WRITES_ON_HEAD 0


/*-------------------------------------------------
----------------- Global Vars ---------------------
--------------------------------------------------*/

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


extern int machine_num; // must be smaller or equal to MAX_MACHINE_NUM
extern int remote_machine_num; // must be smaller or equal to MAX_MACHINE_NUM
extern int worker_measuring_latency;


//extern int value_size; // must be smaller or equal to MAX_MACHINE_NUM

#endif //SPACETIME_CONFIG_H
