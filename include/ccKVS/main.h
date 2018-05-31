#ifndef ARMONIA_MAIN_H
#define ARMONIA_MAIN_H
#include <stdint.h>
#include <pthread.h>
#include "city.h"
#include "hrd.h"

//-------------------------------------------
/* ----------SYSTEM------------------------ */
//-------------------------------------------
#define TOTAL_CORES 40
#define TOTAL_CORES_ (TOTAL_CORES - 1)
#define SOCKET_NUM 2
#define PHYSICAL_CORES_PER_SOCKET 10
#define PHYSICAL_CORE_DISTANCE 4 // distance between two physical cores of the same socket
#define VIRTUAL_CORES_PER_SOCKET 20
///#define WORKER_HYPERTHREADING 1
#define CLIENT_HYPERTHREADING 1
#define MAX_SERVER_PORTS 1


#define WINDOW_SIZE 128

#define CLIENTS_PER_MACHINE 15
#define MACHINE_NUM 2

#define CACHE_SOCKET 0 // socket where the cache is bind

#define CLIENT_NUM (CLIENTS_PER_MACHINE * MACHINE_NUM)

///Warning I changed 1->0 and 2->1
//#define REMOTE_UD_QP_ID 0 /* The id of the UD QP the clients use for remote reqs */
#define BROADCAST_UD_QP_ID 0 /* The id of the UD QP the clients use for braodcasting */
#define FC_UD_QP_ID 1 /* The id of the UD QP the clients use for flow control */
#define CLIENT_UD_QPS 2 /* The number of QPs for the client TODO THIS MUST BE 3*/


#define ENABLE_CACHE_STATS 0
#define EXIT_ON_PRINT 1
#define PRINT_NUM 2
#define APPEND_THROUGHPUT 1
#define DUMP_STATS_2_FILE 0


/*
 * The polling logic in HERD requires the following:
 * 1. 0 < MICA_OP_GET < MICA_OP_PUT < HERD_OP_GET < HERD_OP_PUT
 * 2. HERD_OP_GET = MICA_OP_GET + HERD_MICA_OFFSET
 * 3. HERD_OP_PUT = MICA_OP_PUT + HERD_MICA_OFFSET
 *
 * This allows us to detect HERD requests by checking if the request region
 * opcode is more than MICA_OP_PUT. And then we can convert a HERD opcode to
 * a MICA opcode by subtracting HERD_MICA_OFFSET from it.
 */
#define HERD_MICA_OFFSET 10
#define HERD_OP_GET (MICA_OP_GET + HERD_MICA_OFFSET)
#define HERD_OP_PUT (MICA_OP_PUT + HERD_MICA_OFFSET)
//#define OP_UPDATE 123


/*-------------------------------------------------
	-----------------PROTOCOLS-----------------
--------------------------------------------------*/
#define STRONG_CONSISTENCY 2
#define STRONG_CONSISTENCY_STALLING 3
#define ENABLE_MULTIPLE_SESSIONS 1
#define SESSIONS_PER_CLIENT 6
/* Choose a protocol*/
#define STALLING_PROTOCOL cache_batch_op_sc_stalling
#define NON_STALLING_PROTOCOL cache_batch_op_sc_non_stalling
#define NON_STALLING_PROTOCOL_SESSIONS cache_batch_op_sc_non_stalling_sessions
#define CACHE_BATCH_OP_SC (ENABLE_MULTIPLE_SESSIONS == 1 ? NON_STALLING_PROTOCOL_SESSIONS : NON_STALLING_PROTOCOL)

#ifndef CACHE_BATCH_OP_SC
# define CACHE_BATCH_OP_SC STALLING_PROTOCOL
#endif


/*-------------------------------------------------
	-----------------BATCHING ABILITIES-----------------
--------------------------------------------------*/
//-----CLIENT-------

//#define ENABLE_THREAD_PARTITIONING_C_TO_W_ 1
//#define ENABLE_THREAD_PARTITIONING_C_TO_W (ENABLE_WORKERS_CRCW == 1 ? ENABLE_THREAD_PARTITIONING_C_TO_W_ : 0)
#define BALANCE_REQS_ 0 //
#define BALANCE_REQS  (((ENABLE_WORKERS_CRCW == 1) && (ENABLE_THREAD_PARTITIONING_C_TO_W == 0)) ? BALANCE_REQS_ : 0) //

//#define WINDOW_SIZE 128 /* Maximum remote batch*/
//#define LOCAL_WINDOW  66 //12 // 21 for 200
//#define LOCAL_REGIONS 3 // number of local regions per client
//#define LOCAL_REGION_SIZE (LOCAL_WINDOW / LOCAL_REGIONS)
//#define WS_PER_WORKER (ENABLE_THREAD_PARTITIONING_C_TO_W == 1 ? 6 : 4) //22 /* Number of outstanding requests kept by each client of any given worker*/
//#define MAX_OUTSTANDING_REQS (WS_PER_WORKER * (WORKER_NUM - WORKERS_PER_MACHINE))
//#define ENABLE_MULTI_BATCHES 0 // allow multiple batches
//#define MAX_REMOTE_RECV_WCS (ENABLE_MULTI_BATCHES == 1 ? (MAX(MAX_OUTSTANDING_REQS, WINDOW_SIZE)) : WINDOW_SIZE)
#define MINIMUM_BATCH_ABILITY 16
#define MIN_EMPTY_PERCENTAGE 5
//#define FINISH_BATCH_ON_MISSING_CREDIT 0 // TODO no good reason to even have this





//----- WORKER BUFFER
//#define WORKER_REQ_SIZE (ENABLE_COALESCING == 1 ? (UD_REQ_SIZE + EXTRA_WORKER_REQ_BYTES) : UD_REQ_SIZE)
//#define WORKER_NET_REQ_SIZE (WORKER_REQ_SIZE - GRH_SIZE)
//#define MULTIGET_AVAILABLE_SIZE WORKER_NET_REQ_SIZE
//#define MAX_COALESCE_PER_MACH ((MULTIGET_AVAILABLE_SIZE - 1) / HERD_GET_REQ_SIZE) // -1 because we overload val_len with the number of gets
//#define ENABLE_INLINE_GET_REQS (ENABLE_COALESCING == 1 ? 1 : 1) // Inline get requests even though big objects are used
#define MAXIMUM_INLINE_SIZE 188
//-----WORKER-------
//#define WORKER_MAX_BATCH 127
#define ENABLE_MINIMUM_WORKER_BATCHING 0
#define WORKER_MINIMUM_BATCH 16 // DOES NOT WORK


//#define WORKER_SEND_BUFF_SIZE ( KEY_SIZE + 1 + 1 + WRKR_COALESCING_BUF_SLOT_SIZE)
//#define CLIENT_REMOTE_BUFF_SIZE (GRH_SIZE + WORKER_SEND_BUFF_SIZE)


// INLINING
#define CLIENT_ENABLE_INLINING 1
///#define CLIENT_ENABLE_INLINING (((USE_BIG_OBJECTS == 1) || (MULTIGET_AVAILABLE_SIZE > MAXIMUM_INLINE_SIZE)) ?  0 : 1)
//#define WORKER_RESPONSE_MAX_SIZE (ENABLE_WORKER_COALESCING == 1 ? (MAX_COALESCE_PER_MACH * HERD_VALUE_SIZE) : HERD_VALUE_SIZE)
//#define WORKER_ENABLE_INLINING (((USE_BIG_OBJECTS == 1) || (WORKER_RESPONSE_MAX_SIZE > MAXIMUM_INLINE_SIZE)) ?  0 : 1)

// CACHE
#define ENABLE_HOT_KEY_TRACKING 0
#define HOTTEST_KEYS_TO_TRACK 20



/*-------------------------------------------------
-----------------DEBUGGING-------------------------
--------------------------------------------------*/
#define ENABLE_SS_DEBUGGING 0 // first thing to open in a deadlock
#define ENABLE_ASSERTIONS 1
#define ENABLE_STAT_COUNTING 1
#define MEASURE_LATENCY 1
#define REMOTE_LATENCY_MARK 100 // mark a remote request for measurement by attaching this to the imm_data of the wr
#define ENABLE_WINDOW_STATS 0

#define DO_ONLY_LOCALS 0
#define USE_A_SINGLE_KEY 0
#define ENABLE_WAKE_UP 0
#define USE_ONLY_BIG_MESSAGES 0 // deprecated
#define ONLY_CACHE_HITS 1
#define CLIENT_SL 0 //service level for the clients
#define WORKER_SL 0 // service level for the workers
#define VERBOSE_DEBUG 0
#define STALLING_DEBUG_SC 0 // prints information about the stalled ops, check debug_stalling_SC()
#define DEBUG_COALESCING 0
#define DEBUG_WORKER_RECVS 0


/*-------------------------------------------------
	-----------------TRACE-----------------
--------------------------------------------------*/
#define POISSON_AVG_REQ_ARRIVAL 10//in ns
#define SEND_ONLY_TO_ONE_MACHINE 0 // Dynamically alters trace to send all the requests to one machinr
#define SEND_ONLY_TO_NEXT_MACHINE 0 // Dynamically alters trace so each machine sends its requests to the next one
#define BALANCE_REQS_IN_CHUNKS 0
#define CHUNK_NUM 0
#define BALANCE_HOT_REQS 1 // Use a uniform access pattern among hot requests
#define BALANCE_HOT_WRITES 0// Use a uniform access pattern among hot writes
#define ENABLE_HOT_REQ_GROUPING 0 // Group the hot keys, such that the accesses pof key correspond to a group
#define NUM_OF_KEYS_TO_GROUP 10
#define GROUP_SIZE 50
#define SKEW_EXPONENT_A 99 // representation divided by 100 (i.e. 99 means a = 0.99)
#define EMULATING_CREW 1 // emulate crew, to facilitate running the CREW baseline
#define RANDOM_MACHINE 0 // pick a rnadom machine
#define DISABLE_CACHE 0  // Run Baseline
#define LOAD_BALANCE 1 // Use a uniform access pattern

/*-------------------------------------------------
	-----------------CONSISTENCY-------------------------
--------------------------------------------------*/
//----MULTICAST
#define ENABLE_MULTICAST 0
#define MULTICAST_TESTING_ 0
#define MULTICAST_TESTING (ENABLE_MULTICAST == 1 ? MULTICAST_TESTING_ : 0)
#define SEND_MCAST_QP 0
#define RECV_MCAST_QP 1
#define MCAST_QPS MACHINE_NUM
#define MCAST_GROUPS_PER_CLIENT MACHINE_NUM

// ------COMMON-------------------
#define MAX_BCAST_BATCH (ENABLE_MULTICAST == 1 ? 4 : 4) //8 //(128 / (MACHINE_NUM - 1)) // how many broadcasts can fit in a batch
#define MESSAGES_IN_BCAST (ENABLE_MULTICAST == 1 ? 1 : (MACHINE_NUM - 1))
#define MESSAGES_IN_BCAST_BATCH MAX_BCAST_BATCH * MESSAGES_IN_BCAST //must be smaller than the q_depth
#define BCAST_TO_CACHE_BATCH 10 //90 //100 // helps to keep small //47 for EC

//----------EC flow control-----------------
//#define EC_CREDITS 60 //experiments with 33
//#define EC_CREDIT_DIVIDER 2 /*This is actually useful in high write ratios TODO tweak this*/
//#define EC_CREDITS_IN_MESSAGE (EC_CREDITS / EC_CREDIT_DIVIDER)
//#define EC_MAX_CREDIT_WRS (EC_CREDITS / EC_CREDITS_IN_MESSAGE) * (MACHINE_NUM - 1)
//#define EC_MAX_COH_MESSAGES (EC_CREDITS * (MACHINE_NUM - 1))
//#define EC_MAX_COH_RECEIVES (EC_CREDITS * (MACHINE_NUM - 1))
//#define EC_MAX_CREDIT_RECVS (CEILING(EC_MAX_COH_MESSAGES, EC_CREDITS_IN_MESSAGE))
//#define EC_VIRTUAL_CHANNELS 1
//#define EC_UPD_VC 0


//----------SC flow control-----------------
#define CREDITS_FOR_EACH_CLIENT 60 //30
#define UPD_CREDITS (CREDITS_FOR_EACH_CLIENT)
#define ACK_CREDITS (CREDITS_FOR_EACH_CLIENT)
#define INV_CREDITS (CREDITS_FOR_EACH_CLIENT)
#define BROADCAST_CREDITS (UPD_CREDITS + ACK_CREDITS + INV_CREDITS) /* Credits for each machine to issue Broadcasts */
#define VIRTUAL_CHANNELS 3 // upds acks and invs
#define ACK_VC 0
#define INV_VC 1
#define UPD_VC 2
#define SC_CREDIT_DIVIDER 2 //1 /// this  has the potential to cause deadlocks //  =take care that this can be a big part of the network traffic
#define CREDITS_IN_MESSAGE (CREDITS_FOR_EACH_CLIENT / SC_CREDIT_DIVIDER) /* How many credits exist in a single back-pressure message- seems to be working with / 3*/
#define MAX_CREDIT_WRS (BROADCAST_CREDITS / CREDITS_IN_MESSAGE) * (MACHINE_NUM - 1)
#define MAX_COH_MESSAGES ((MACHINE_NUM - 1) * BROADCAST_CREDITS)
#define MAX_COH_RECEIVES ((MACHINE_NUM - 1) * BROADCAST_CREDITS)

//---------Buffer Space-------------
#define SC_CLT_BUF_SIZE (UD_REQ_SIZE * (MACHINE_NUM - 1) * BROADCAST_CREDITS)
//#define EC_CLT_BUF_SIZE (UD_REQ_SIZE * (MACHINE_NUM - 1) * EC_CREDITS)
#define SC_CLT_BUF_SLOTS ((MACHINE_NUM - 1) * BROADCAST_CREDITS)
//#define EC_CLT_BUF_SLOTS (EC_CLT_BUF_SIZE  / UD_REQ_SIZE)

#define OPS_BUFS_NUM (CLIENT_ENABLE_INLINING == 1 ? 2 : 3) // how many OPS buffers are in use
//#define EXTENDED_OPS_SIZE (OPS_BUFS_NUM * CACHE_BATCH_SIZE * CACHE_OP_SIZE)
#define COH_BUF_SIZE (CLIENT_ENABLE_INLINING == 1 ?	(MAX_BCAST_BATCH * MICA_OP_SIZE) : (BROADCAST_SS_BATCH * MICA_OP_SIZE))
#define COH_BUF_SLOTS (CLIENT_ENABLE_INLINING == 1 ? MAX_BCAST_BATCH : BROADCAST_SS_BATCH)
/* We post receives for credits after sending broadcasts or acks,
	For Broadcasts the maximum number is: (MACHINE_NUM - 1) * (CEILING(MAX_BCAST_BATCH, CREDITS_IN_MESSAGE))
	For acks the maximum number is: CEILING(BCAST_TO_CACHE_BATCH, REDITS_IN_MESSAGE)   */
#define MAX_CREDIT_RECVS_FOR_BCASTS (MACHINE_NUM - 1) * (CEILING(MAX_BCAST_BATCH, CREDITS_IN_MESSAGE))
#define MAX_CREDIT_RECVS_FOR_ACKS (CEILING(BCAST_TO_CACHE_BATCH, CREDITS_IN_MESSAGE))
#define MAX_CREDIT_RECVS (MAX(MAX_CREDIT_RECVS_FOR_BCASTS, MAX_CREDIT_RECVS_FOR_ACKS))

/*-------------------------------------------------
-----------------SELECTIVE SIGNALING-------------------------
--------------------------------------------------*/
#define MIN_SS_BATCH 127// THe minimum ss batch
#define CREDIT_SS_BATCH MAX(MIN_SS_BATCH, (MAX_CREDIT_WRS + 1))
#define CREDIT_SS_BATCH_ (CREDIT_SS_BATCH - 1)
//#define EC_CREDIT_SS_BATCH MAX(MIN_SS_BATCH, (EC_MAX_CREDIT_WRS + 1))
//#define EC_CREDIT_SS_BATCH_ (EC_CREDIT_SS_BATCH - 1)
//#define WORKER_SS_BATCH MAX(MIN_SS_BATCH, (WORKER_MAX_BATCH + 1))
//#define WORKER_SS_BATCH_ (WORKER_SS_BATCH - 1)
#define CLIENT_SS_BATCH MAX(MIN_SS_BATCH, (WINDOW_SIZE + 1))
#define CLIENT_SS_BATCH_ (CLIENT_SS_BATCH - 1)
// if this is smaller than MAX_BCAST_BATCH + 2 it will deadlock because the signaling messaged is polled before actually posted
#define BROADCAST_SS_BATCH MAX((MIN_SS_BATCH / (MACHINE_NUM - 1)), (MAX_BCAST_BATCH + 2))
#define ACK_SS_BATCH MAX(MIN_SS_BATCH, (BCAST_TO_CACHE_BATCH + 1)) //* (MACHINE_NUM - 1)


/*-------------------------------------------------
-----------------QUEUE DEPTHS-------------------------
--------------------------------------------------*/

//RECV
//#define WORKER_RECV_Q_DEPTH  (((MACHINE_NUM - 1) * CEILING(CLIENTS_PER_MACHINE, WORKER_NUM_UD_QPS) * WS_PER_WORKER) + 3) // + 3 for good measre
#define CLIENT_RECV_REM_Q_DEPTH ((ENABLE_MULTI_BATCHES == 1 ? MAX_OUTSTANDING_REQS :  2 * CLIENT_SS_BATCH) + 3)

//#define EC_CLIENT_RECV_BR_Q_DEPTH (EC_MAX_COH_RECEIVES + 3)
#define SC_CLIENT_RECV_BR_Q_DEPTH (MAX_COH_RECEIVES + 3)

//#define EC_CLIENT_RECV_CR_Q_DEPTH (EC_MAX_CREDIT_RECVS + 3) // recv credits EC
#define SC_CLIENT_RECV_CR_Q_DEPTH (MAX_COH_MESSAGES  + 8) // a reasonable upper bound

// SEND
//#define WORKER_SEND_Q_DEPTH  WORKER_MAX_BATCH + 3 // + 3 for good measre
#define CLIENT_SEND_REM_Q_DEPTH  ((ENABLE_MULTI_BATCHES == 1  ? MAX_OUTSTANDING_REQS : CLIENT_SS_BATCH) + 3) // 60)

//#define EC_CLIENT_SEND_BR_Q_DEPTH (MAX((MACHINE_NUM - 1) * BROADCAST_SS_BATCH, EC_MAX_COH_MESSAGES + 14) + 3)
#define SC_CLIENT_SEND_BR_Q_DEPTH (MAX(MAX_COH_MESSAGES, (BROADCAST_SS_BATCH * (MACHINE_NUM - 1) + ACK_SS_BATCH)) + 13)

//#define EC_CLIENT_SEND_CR_Q_DEPTH  (2 * EC_CREDIT_SS_BATCH + 3) // send credits EC
#define SC_CLIENT_SEND_CR_Q_DEPTH (2 * CREDIT_SS_BATCH + 13)

// WORKERS synchronization options
#if ENABLE_WORKERS_CRCW == 1
	extern struct mica_kv kv;
# define KVS_BATCH_OP mica_batch_op_crcw
#else /*ENABLE_WORKERS_CRCW == 0*/
# define KVS_BATCH_OP mica_batch_op
#endif
//LATENCY Measurment
#define MAX_LATENCY 400 //in us
#define LATENCY_BUCKETS 200 //latency accuracy

/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24
#define RR_SIZE (16 * 1024 * 1024)	/* Request region size */

#define OFFSET(wn, cn, ws) ((wn * CLIENTS_PER_MACHINE * LOCAL_WINDOW) + \
	(cn * LOCAL_WINDOW) + ws) // There was a bug here, wehre Instead of Clients per machine, it was CLIENT_NUM

//Defines for parsing the trace
#define _200_K 200000
#define MAX_TRACE_SIZE _200_K
#define FEED_FROM_TRACE 1
#define NOP 0
#define HOT_WRITE 1
#define HOT_READ 2
#define REMOTE_WRITE 3
#define REMOTE_READ 4
#define LOCAL_WRITE 5
#define LOCAL_READ 6


#define IS_READ(X)  ((X) == HOT_READ || (X) == LOCAL_READ || (X) == REMOTE_READ  ? 1 : 0)
#define IS_WRITE(X)  ((X) == HOT_WRITE || (X) == LOCAL_WRITE || (X) == REMOTE_WRITE  ? 1 : 0)
#define IS_HOT(X)  ((X) == HOT_WRITE || (X) == HOT_READ ? 1 : 0)
#define IS_NORMAL(X)  (!IS_HOT((X)))
#define IS_LOCAL(X) ((X) == LOCAL_WRITE || (X) == LOCAL_READ ? 1 : 0)
#define IS_REMOTE(X) ((X) == REMOTE_WRITE || (X) == REMOTE_READ ? 1 : 0)

struct trace_command {
	uint8_t  opcode;
	uint8_t  home_machine_id;
	uint8_t  home_worker_id;
	uint32_t key_id;
	uint128 key_hash;
};

struct coalesce_inf {
	uint16_t wr_i;
	uint16_t slots;
	uint16_t op_i;
	uint16_t wrkr;
};
/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same cache line */
struct remote_qp {
	struct ibv_ah *ah;
	int qpn;
	// no padding needed- false sharing is not an issue, only fragmentation
};


// a client sends to a particular ud qp to all workers, therefore to better utilize its L1 cache
// we store worker AHs by QP instead of by worker id
//extern volatile struct remote_qp remote_wrkr_qp[WORKER_NUM_UD_QPS][WORKER_NUM];
extern volatile struct remote_qp remote_clt_qp[CLIENT_NUM][CLIENT_UD_QPS];
//extern volatile char clt_needed_ah_ready, wrkr_needed_ah_ready;
extern volatile char clt_needed_ah_ready;
struct mica_op;
extern volatile struct mica_op *local_req_region;


struct client_stats { // 2 cache lines
	long long cache_hits_per_client;
	long long remotes_per_client;
	long long locals_per_client;

	long long updates_per_client;
	long long acks_per_client;  //only SC
	long long invs_per_client; //only SC

	long long received_updates_per_client;
	long long received_acks_per_client; //only SC
	long long received_invs_per_client; //only SC

	long long remote_messages_per_client;
	long long cold_keys_per_trace;
	long long batches_per_client;

	long long stalled_time_per_client;

	double empty_reqs_per_trace;
	long long wasted_loops;
	double tot_empty_reqs_per_trace;


	//long long unused[3]; // padding to avoid false sharing
};

//
//struct worker_stats { // 1 cache line
//	long long remotes_per_worker;
//	long long locals_per_worker;
//	long long batches_per_worker;
//	long long empty_polls_per_worker;
//
//	long long unused[4]; // padding to avoid false sharing
//};


///extern volatile char local_recv_flag[WORKERS_PER_MACHINE][CLIENTS_PER_MACHINE][64]; //false sharing problem -- fixed with padding
extern volatile struct client_stats w_stats[CLIENTS_PER_MACHINE];
//extern volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];

struct thread_params {
	int id;
	int base_port_index;
	int num_server_ports;
	int num_client_ports;
	int update_percentage;
	int postlist;
};

struct latency_counters{
	uint32_t* remote_reqs;
	uint32_t* local_reqs;
	uint32_t* hot_reads;
	uint32_t* hot_writes;
	long long total_measurements;
};


struct local_latency {
	int measured_local_region;
	uint8_t local_latency_start_polling;
	char* flag_to_poll;
};



extern uint8_t protocol;
extern optik_lock_t kv_lock;
extern struct latency_counters latency_count;

//void *run_worker(void *arg);
void *run_client(void *arg);
void *print_stats(void*);


#endif
