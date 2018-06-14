//
// Created by akatsarakis on 15/03/18.
//

#ifndef SPACETIME_CONFIG_H
#define SPACETIME_CONFIG_H
#include "hrd.h"

#define ENABLE_ASSERTIONS 0
#define MACHINE_NUM 3
#define REMOTE_MACHINES (MACHINE_NUM - 1)
#define GROUP_MEMBERSHIP_ARRAY_SIZE  CEILING(MACHINE_NUM, 8) //assuming uint8_t
#define WORKERS_PER_MACHINE 10
#define ENABLE_HYPERTHREADING 1
#define KV_SOCKET 0
#define START_SPAWNING_THREADS_FROM_SOCKET 0
#define WRITE_RATIO 1000
#define MAX_BATCH_OPS_SIZE 185 //30 //5

//TRACE
#define FEED_FROM_TRACE 0
#define TRACE_SIZE K_128
#define USE_A_SINGLE_KEY 0



/*-------------------------------------------------
-----------------FLOW CONTROL---------------------
--------------------------------------------------*/
#define CREDITS_PER_REMOTE_WORKER 150 ///MAX_BATCH_OPS_SIZE //3 //60 //30
#define INV_CREDITS CREDITS_PER_REMOTE_WORKER
#define ACK_CREDITS CREDITS_PER_REMOTE_WORKER
#define VAL_CREDITS CREDITS_PER_REMOTE_WORKER
#define CRD_CREDITS CREDITS_PER_REMOTE_WORKER
#define CRDS_IN_MESSAGE 1 /* How many credits exist in a single back-pressure message- seems to be working with / 3*/

/*-------------------------------------------------
-----------------PCIe BATCHING---------------------
--------------------------------------------------*/
#define MAX_PCIE_BCAST_BATCH INV_CREDITS //(128 / (MACHINE_NUM - 1)) // how many broadcasts can fit in a batch
#define MAX_MSGS_IN_PCIE_BCAST_BATCH (MAX_PCIE_BCAST_BATCH * REMOTE_MACHINES) //must be smaller than the q_depth

/**/
#define MAX_SEND_INV_WRS MAX_MSGS_IN_PCIE_BCAST_BATCH
#define MAX_SEND_ACK_WRS (INV_CREDITS * REMOTE_MACHINES)
#define MAX_SEND_VAL_WRS MAX_MSGS_IN_PCIE_BCAST_BATCH
#define MAX_SEND_CRD_WRS (VAL_CREDITS * REMOTE_MACHINES)

#define MAX_RECV_INV_WRS (INV_CREDITS * REMOTE_MACHINES)
#define MAX_RECV_ACK_WRS (ACK_CREDITS * REMOTE_MACHINES)
#define MAX_RECV_VAL_WRS (VAL_CREDITS * REMOTE_MACHINES)
#define MAX_RECV_CRD_WRS (CRD_CREDITS * REMOTE_MACHINES)
/*-------------------------------------------------
-----------------REQUEST SIZES---------------------
--------------------------------------------------*/
#define KEY_SIZE 16
#define INV_RECV_REQ_SIZE (sizeof(ud_req_inv_t)) // Buffer slot size required for a INV request
#define ACK_RECV_REQ_SIZE (sizeof(ud_req_ack_t)) // Buffer slot size required for a ACK request
#define VAL_RECV_REQ_SIZE (sizeof(ud_req_val_t)) // Buffer slot size required for a VAL request
#define CRD_RECV_REQ_SIZE (sizeof(ud_req_crd_t)) // Buffer slot size required for a credits request

/*-------------------------------------------------
-----------------SELECTIVE SIGNALING---------------
-------------------------------------------------*/
#define INV_SS_GRANULARITY (MAX_PCIE_BCAST_BATCH + 1)
#define ACK_SS_GRANULARITY (MAX_SEND_ACK_WRS + 1)
#define VAL_SS_GRANULARITY (MAX_PCIE_BCAST_BATCH + 1)
#define CRD_SS_GRANULARITY (MAX_SEND_CRD_WRS + 1)

//#define MIN_SS_GRANULARITY 127// The minimum ss batch
//#define INV_SS_GRANULARITY MAX((MIN_SS_GRANULARITY / REMOTE_MACHINES), \
//                               (MAX_PCIE_BCAST_BATCH + 1))
//#define ACK_SS_GRANULARITY MAX(MIN_SS_GRANULARITY, (MAX_SEND_ACK_WRS + 1))
//#define VAL_SS_GRANULARITY MAX((MIN_SS_GRANULARITY / REMOTE_MACHINES), \
//                               (MAX_PCIE_BCAST_BATCH + 1))
//#define CRD_SS_GRANULARITY MAX(MIN_SS_GRANULARITY, (MAX_SEND_CRD_WRS + 1))

/*-------------------------------------------------
-----------------QPs & QUEUE DEPTHS----------------
--------------------------------------------------*/
//QPs
#define INV_UD_QP_ID 0
#define ACK_UD_QP_ID 1
#define VAL_UD_QP_ID 2
#define CRD_UD_QP_ID 3
#define TOTAL_WORKER_UD_QPs 4

//RECV Depths
#define RECV_INV_Q_DEPTH (MAX_RECV_INV_WRS + 3) /// it requires an upper bound
#define RECV_ACK_Q_DEPTH (MAX_RECV_ACK_WRS + 3)
#define RECV_VAL_Q_DEPTH (MAX_RECV_VAL_WRS + 3)
#define RECV_CRD_Q_DEPTH (MAX_RECV_CRD_WRS + 3)

//SEND Depths
#define SEND_INV_Q_DEPTH (((INV_SS_GRANULARITY * REMOTE_MACHINES) * 2) + 3)
#define SEND_ACK_Q_DEPTH ((ACK_SS_GRANULARITY * 2) + 3)
#define SEND_VAL_Q_DEPTH (((VAL_SS_GRANULARITY * REMOTE_MACHINES) * 2) + 3)
#define SEND_CRD_Q_DEPTH ((CRD_SS_GRANULARITY * 2) + 3)

#define DGRAM_BUFF_SIZE ((INV_RECV_REQ_SIZE * RECV_INV_Q_DEPTH) + \
                         (ACK_RECV_REQ_SIZE * RECV_ACK_Q_DEPTH) + \
                         (VAL_RECV_REQ_SIZE * RECV_VAL_Q_DEPTH) + \
                         (64))  //CREDITS are header-only (inlined)

/*-------------------------------------------------
----------------- REQ COALESCING -------------------
--------------------------------------------------*/
#define INV_MAX_REQ_COALESCE 1
#define ACK_MAX_REQ_COALESCE 1
#define VAL_MAX_REQ_COALESCE 1

/*-------------------------------------------------
-----------------PRINTS (DBG)---------------------
--------------------------------------------------*/
#define MAX_THREADS_TO_PRINT 0
#define ENABLE_REQ_PRINTS 0
#define ENABLE_BATCH_OP_PRINTS 0
#define ENABLE_CREDIT_PRINTS 0
#define ENABLE_SEND_PRINTS 0
#define ENABLE_POST_RECV_PRINTS 0
#define ENABLE_RECV_PRINTS 0
#define ENABLE_SS_PRINTS 1
#define ENABLE_INV_PRINTS 1
#define ENABLE_ACK_PRINTS 0
#define ENABLE_VAL_PRINTS 1
#define ENABLE_CRD_PRINTS 0

//Stats
#define EXIT_ON_PRINT 1
#define PRINT_NUM 3
#define MEASURE_LATENCY 0
#define DUMP_STATS_2_FILE 0
#define ENABLE_STAT_COUNTING 1

// Rarely change
#define WORKER_NUM (MACHINE_NUM * WORKERS_PER_MACHINE)
#define TOTAL_CORES 40
//#define WINDOW_SIZE_ (WINDOW_SIZE - 1)
//#define MSG_GRAN_OF_SELECTIVE_SIGNALING_ (MSG_GRAN_OF_SELECTIVE_SIGNALING - 1)
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

/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same cache line */
struct remote_qp {
    struct ibv_ah *ah;
    int qpn;
    // no padding needed- false sharing is not an issue, only fragmentation
};

extern volatile struct remote_qp remote_worker_qps[WORKER_NUM][TOTAL_WORKER_UD_QPs];
extern volatile char worker_needed_ah_ready;
#endif //SPACETIME_CONFIG_H
