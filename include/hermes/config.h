//
// Created by akatsarakis on 15/03/18.
//

#ifndef SPACETIME_CONFIG_H
#define SPACETIME_CONFIG_H
#include "hrd.h"

#define ENABLE_ASSERTIONS 1
#define MACHINE_NUM 2
#define GROUP_MEMBERSHIP_ARRAY_SIZE  CEILING(MACHINE_NUM, 8) //assuming uint8_t
#define WORKERS_PER_MACHINE 1
#define ENABLE_HYPERTHREADING 0
#define KV_SOCKET 1
#define START_SPAWNING_THREADS_FROM_SOCKET 1
#define WINDOW_SIZE 128
#define WRITE_RATIO 50
#define MAX_BATCH_OPS_SIZE 3//5
#define WORKER_MAX_BATCH 127
#define MSG_GRAN_OF_SELECTIVE_SIGNALING 8 // use one to disable selective signalling

//----------Protocol flow control-----------------
#define CREDITS_PER_REMOTE_WORKER 3//60 //30
#define INV_CREDITS (CREDITS_PER_REMOTE_WORKER)
#define ACK_CREDITS (CREDITS_PER_REMOTE_WORKER)
#define VAL_CREDITS (CREDITS_PER_REMOTE_WORKER)
#define CRD_CREDITS (CREDITS_PER_REMOTE_WORKER)
#define CREDITS_IN_MESSAGE 1 /* How many credits exist in a single back-pressure message- seems to be working with / 3*/
///TODO change CRD_BUFF_SIZE
///#define TOTAL_CREDITS (VAL_CREDITS + ACK_CREDITS + INV_CREDITS) /* Credits for each machine to issue Broadcasts */
///#define CREDIT_DIVIDER CREDITS_PER_REMOTE_WORKER //2 //1 /// this  has the potential to cause deadlocks //  =take care that this can be a big part of the network traffic
///#define CREDITS_IN_MESSAGE (CREDITS_PER_REMOTE_WORKER / CREDIT_DIVIDER) /* How many credits exist in a single back-pressure message- seems to be working with / 3*/
///#define CRD_BUFF_SIZE ((TOTAL_CREDITS / CREDITS_IN_MESSAGE) * (MACHINE_NUM - 1))

//#define MAX_BROADCAST_MESSAGES ((MACHINE_NUM - 1) * TOTAL_CREDITS)
//#define MAX_BROADCAST_RECEIVES ((MACHINE_NUM - 1) * BROADCAST_CREDITS)

//---------Buffer Space-------------
/* We post receives for credits after sending broadcasts or acks,
	For Broadcasts the maximum number is: (MACHINE_NUM - 1) * (CEILING(MAX_PCIE_BCAST_BATCH, CREDITS_IN_MESSAGE))
	For acks the maximum number is: CEILING(BCAST_TO_CACHE_BATCH, CREDITS_IN_MESSAGE)   */
///#define MAX_CREDIT_RECVS_FOR_BCASTS (MACHINE_NUM - 1) * (CEILING(MAX_BCAST_BATCH, CREDITS_IN_MESSAGE))
///#define MAX_CREDIT_RECVS_FOR_ACKS (CEILING(MAX_BATCH_OPS_SIZE, CREDITS_IN_MESSAGE))
///#define MAX_CREDIT_RECVS (MAX(MAX_CREDIT_RECVS_FOR_BCASTS, MAX_CREDIT_RECVS_FOR_ACKS))

/* Request sizes */
#define KEY_SIZE 16
#define HERD_GET_REQ_SIZE ((KEY_SIZE + 1 )) /* 16 byte key + opcode */
#define HERD_PUT_REQ_SIZE (KEY_SIZE + 1 + 1 + HERD_VALUE_SIZE) /* Key, op, len, val */

#define INV_RECV_REQ_SIZE (sizeof(ud_req_inv_t)) // Buffer slot size required for a INV request
#define ACK_RECV_REQ_SIZE (sizeof(ud_req_ack_t)) // Buffer slot size required for a ACK request
#define VAL_RECV_REQ_SIZE (sizeof(ud_req_val_t)) // Buffer slot size required for a VAL request
#define CRD_RECV_REQ_SIZE (sizeof(ud_req_crd_t)) // Buffer slot size required for a credits request



/*-------------------------------------------------
-----------------QPs & QUEUE DEPTHS----------------
--------------------------------------------------*/
//QPs
#define INV_UD_QP_ID 0
#define ACK_UD_QP_ID 1
#define VAL_UD_QP_ID 2
#define CRD_UD_QP_ID 3
#define TOTAL_WORKER_UD_QPs 4

// ------COMMON-------------------
#define MAX_PCIE_BCAST_BATCH 3 ///4 //8 //(128 / (MACHINE_NUM - 1)) // how many broadcasts can fit in a batch
#define MAX_MESSAGES_IN_BCAST (MACHINE_NUM - 1)
#define MESSAGES_IN_BCAST_BATCH MAX_PCIE_BCAST_BATCH * MAX_MESSAGES_IN_BCAST //must be smaller than the q_depth

/*-------------------------------------------------
-----------------SELECTIVE SIGNALING---------------
--------------------------------------------------*/
#define MIN_SS_BATCH 1 //127// The minimum ss batch
//#define CREDIT_SS_BATCH MAX(MIN_SS_BATCH, (MAX_CREDIT_WRS + 1))
//#define CREDIT_SS_BATCH_ (CREDIT_SS_BATCH - 1)
//#define WORKER_SS_BATCH MAX(MIN_SS_BATCH, (WORKER_MAX_BATCH + 1))
//#define WORKER_SS_BATCH_ (WORKER_SS_BATCH - 1)
//// if this is smaller than MAX_BCAST_BATCH + 2 it will deadlock because the signaling messaged is polled before actually posted
//#define BROADCAST_SS_BATCH MAX((MIN_SS_BATCH / (MACHINE_NUM - 1)), (MAX_PCIE_BCAST_BATCH + 2))
//#define ACK_SS_BATCH MAX(MIN_SS_BATCH, (MAX_BATCH_OPS_SIZE + 1)) //* (MACHINE_NUM - 1)
#define INV_SS_BATCH MAX(MIN_SS_BATCH, MESSAGES_IN_BCAST_BATCH)
#define ACK_SS_BATCH MAX(MIN_SS_BATCH, ACK_CREDITS + 2) ///(SEND_ACK_Q_DEPTH - 1) //MAX(MIN_SS_BATCH, MAX_BATCH_OPS_SIZE + 1) // ACK_CREDITS ///(SEND_ACK_Q_DEPTH - 1) //MIN_SS_BATCH
#define VAL_SS_BATCH MAX(MIN_SS_BATCH, MESSAGES_IN_BCAST_BATCH) //(SEND_VAL_Q_DEPTH - 1) //MIN_SS_BATCH
#define CRD_SS_BATCH (SEND_CRD_Q_DEPTH - 1) //MIN_SS_BATCH

//RECV Depths
#define RECV_INV_Q_DEPTH ((MACHINE_NUM - 1) * INV_CREDITS + 3) /// a reasonable upper bound
#define RECV_ACK_Q_DEPTH ((MACHINE_NUM - 1) * ACK_CREDITS + 3)
#define RECV_VAL_Q_DEPTH ((MACHINE_NUM - 1) * VAL_CREDITS + 3)
#define RECV_CRD_Q_DEPTH ((MACHINE_NUM - 1) * VAL_CREDITS + 3) ///TODO Check this ///(MAX_BROADCAST_MESSAGES  + 8) // a reasonable upper bound

// SEND
//#define SEND_INV_Q_DEPTH ((MACHINE_NUM - 1) * MAX_BATCH_OPS_SIZE) //((MACHINE_NUM - 1) * INV_CREDITS )
//#define SEND_ACK_Q_DEPTH ((MACHINE_NUM - 1) * MAX_BATCH_OPS_SIZE)
//#define SEND_VAL_Q_DEPTH ((MACHINE_NUM - 1) * MAX_BATCH_OPS_SIZE)
//#define SEND_CRD_Q_DEPTH ((MACHINE_NUM - 1) + 4)


#define SEND_INV_Q_DEPTH (INV_SS_BATCH + 8) //(MESSAGES_IN_BCAST_BATCH + MAX_MESSAGES_IN_BCAST * 1) // a reasonable upperbound
#define SEND_ACK_Q_DEPTH ((MACHINE_NUM - 1) * ACK_CREDITS + 8)
//#define SEND_ACK_Q_DEPTH ((MACHINE_NUM - 1) * ACK_CREDITS + 3)
#define SEND_VAL_Q_DEPTH (MESSAGES_IN_BCAST_BATCH + 3)// ((MACHINE_NUM - 1) * VAL_CREDITS)
#define SEND_CRD_Q_DEPTH ((MACHINE_NUM - 1) * CRD_CREDITS)

#define DGRAM_BUFF_SIZE ((INV_RECV_REQ_SIZE * RECV_INV_Q_DEPTH) + \
                         (ACK_RECV_REQ_SIZE * RECV_ACK_Q_DEPTH) + \
                         (VAL_RECV_REQ_SIZE * RECV_VAL_Q_DEPTH) + \
                         (64))  //CREDITS are header-only (inlined) //(CRD_RECV_REQ_SIZE * RECV_CRD_Q_DEPTH))



//Stats
#define EXIT_ON_PRINT 1
#define PRINT_NUM 3
#define MEASURE_LATENCY 0
#define DUMP_STATS_2_FILE 0
#define ENABLE_KV_STATS 0
#define ENABLE_STAT_COUNTING 1
// Rarely change
///#define CLIENT_NUM (MACHINE_NUM * CLIENTS_PER_MACHINE)
#define WORKER_NUM (MACHINE_NUM * WORKERS_PER_MACHINE)
#define TOTAL_CORES 40
//#define WINDOW_SIZE_ (WINDOW_SIZE - 1)
#define MSG_GRAN_OF_SELECTIVE_SIGNALING_ (MSG_GRAN_OF_SELECTIVE_SIGNALING - 1)
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
