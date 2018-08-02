#ifndef HRD_H
#define HRD_H

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <numaif.h>

#include <malloc.h>
#include <time.h>
#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include "hrd_sizes.h"


//<vasilis> Multicast
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <netinet/in.h>
#include <netdb.h>
// <vasilis>

#define HRD_Q_DEPTH 128

#define USE_BIG_OBJECTS 0
#define EXTRA_CACHE_LINES 0
#define BASE_VALUE_SIZE 46 //max is --> 46 //40
#define SHIFT_BITS (USE_BIG_OBJECTS == 1 ? 3 : 0) // number of bits to shift left or right to calculate the value length
#define HRD_DEFAULT_PSN 3185	/* PSN for all queues */ // starting Packet Sequence Number
#define HRD_DEFAULT_QKEY 0x11111111
#define HRD_MAX_LID 256

#define HRD_QP_NAME_SIZE 200	/* Size (in bytes) of a queue pair name */
#define HRD_RESERVED_NAME_PREFIX "__HRD_RESERVED_NAME_PREFIX"

#define HRD_CONNECT_IB_ATOMICS 0

#define HERD_NUM_BKTS (USE_BIG_OBJECTS == 1 ? (128 * 1024) : (2 * 1024 * 1024))
#define HERD_LOG_CAP  (1024 * 1024 * 1024)


#define HERD_NUM_KEYS (USE_BIG_OBJECTS == 1 ? (512 * 1024) : (4 * 1024 * 1024))//(8 * 1024 * 1024)


#define KVS_VALUE_SIZE (USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE) : BASE_VALUE_SIZE) //(169 + 64)// 46 + 64 + 64//32 //(46 + 64)
/* Request sizes */
#define KEY_SIZE 16
#define HERD_GET_REQ_SIZE ((KEY_SIZE + 1 )) /* 16 byte key + opcode */
#define HERD_PUT_REQ_SIZE (KEY_SIZE + 1 + 1 + KVS_VALUE_SIZE) /* Key, op, len, val */

#define GRH_SIZE 40 // GLobal Routing Header
#define MICA_OP_SIZE (USE_BIG_OBJECTS == 1 ? (64 + (EXTRA_CACHE_LINES * 64)) : 64)
//#define CACHE_OP_SIZE //MICA_OP_SIZE
#define UD_REQ_SIZE (MICA_OP_SIZE + GRH_SIZE) // Buffer slot size required for a UD request



#define ENABLE_COALESCING 0
//#define ENABLE_WORKER_COALESCING_ (USE_BIG_OBJECTS == 0 ? 1 : 0)
//#define ENABLE_WORKER_COALESCING (ENABLE_COALESCING == 1 ? ENABLE_WORKER_COALESCING_ : 0)


#define DESIRED_COALESCING_FACTOR 32


#define WRKR_COALESCING_BUF_SLOT_SIZE_ (DESIRED_COALESCING_FACTOR * KVS_VALUE_SIZE)
///#define WRKR_COALESCING_BUF_SLOT_SIZE (ENABLE_WORKER_COALESCING == 1 ? WRKR_COALESCING_BUF_SLOT_SIZE_ : HERD_VALUE_SIZE)
#define WRKR_COALESCING_BUF_SLOT_SIZE KVS_VALUE_SIZE

#define MINIMUM_WORKER_REQ_SIZE ((DESIRED_COALESCING_FACTOR * HERD_GET_REQ_SIZE) + 1) + GRH_SIZE
#define EXTRA_WORKER_REQ_BYTES_ (MINIMUM_WORKER_REQ_SIZE <= UD_REQ_SIZE ? 0 : MINIMUM_WORKER_REQ_SIZE - UD_REQ_SIZE)
#define EXTRA_WORKER_REQ_BYTES (ENABLE_COALESCING == 1 ? EXTRA_WORKER_REQ_BYTES_ : 0)


//#define CACHE_OP_SIZE WORKER_NET_REQ_SIZE//MICA_OP_SIZE


#define HUGE_PAGE_SIZE 2097152
#define LEVERAGE_TLB_COALESCING 1

/*
 * Small max_inline_data reduces the QP's max WQE size, which reduces the
 * DMA size in doorbell method of WQE fetch.
 */
#define HRD_MAX_INLINE  188//(USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + 60) : 60) //60 is what kalia had here//
// This is required for ROCE not sure yet why
// <vasilis>
#define IB_PHYS_PORT 1
// </vasilis>
// <akatsarakis>
#define USE_HUGE_PAGES 1
// </akatsarakis>
/* Useful when `x = (x + 1) % N` is done in a loop */
#define HRD_MOD_ADD(x, N) do { \
	x = x + 1; \
	if(x == N) { \
		x = 0; \
	} \
} while(0)

#define MOD_ADD_WITH_BASE(x, N, B) do { \
	x = x + 1; \
	if(x == B + N) { \
		x = B; \
	} \
} while(0)






/* Compile time assert. !!(@condition) converts @condition into a 0/1 bool. */
#define ct_assert(condition) ((void) sizeof(char[-1 + 2 * !!(condition)]))

/* Ensure that x is between a and b, inclusive */
#define range_assert(x, a, b) (assert(x >= a && x <= b))

#ifndef likely
#  define likely(x)      __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#  define unlikely(x)    __builtin_expect(!!(x), 0)
#endif

/* Compare, print, and exit */
#define CPE(val, msg, err_code) \
	if(unlikely(val)) { fprintf(stderr, msg); fprintf(stderr, " Error %d \n", err_code); \
	exit(err_code);}

/* vasilis added a ceiling and a MAX*/
#define CEILING(x,y) (((x) + (y) - 1) / (y))
#define MAX(x,y) (x > y ? x : y)
#define MIN(x,y) (x < y ? x : y)


#define forceinline inline __attribute__((always_inline))
#define _unused(x) ((void)(x))	/* Make production build happy */

/* Is pointer x aligned to A-byte alignment? */
#define is_aligned(x, A) (((uint64_t) x) % A == 0)

// <vasilis>
#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

int is_roce, is_master, is_client;
int machine_id, machines_num;
char *remote_IP, *local_IP;
int remote_id;

// returns the number of remote IP addresses and fills the remoteIPs array with them
int getRemoteIPs(char***);
void die(const char *);

/* Registry info about a QP */
struct hrd_qp_attr {
	char name[HRD_QP_NAME_SIZE];

	// ROCE
	uint64_t gid_global_interface_id;	// Store the gid fields separately because I
	uint64_t gid_global_subnet_prefix; 	// don't like unions. Needed for RoCE only

	/* Info about the RDMA buffer associated with this QP */
	uintptr_t buf_addr;
	uint32_t buf_size;
	uint32_t rkey;

	int lid;
	int qpn;
	uint8_t sl;
};



struct hrd_ctrl_blk {

	int local_hid;	/* Local ID on the machine this process runs on */

	/* Info about the device/port to use for this control block */
	struct ibv_context *ctx;
	int port_index;	/* User-supplied. 0-based across all devices */
	int device_id;	/* Resovled by libhrd from @port_index */
	int dev_port_id;	/* 1-based within dev @device_id. Resolved by libhrd */
	int numa_node_id;	/* NUMA node id */

	struct ibv_pd *pd;	/* A protection domain for this control block */

	/* Connected QPs */
	int use_uc;
	int num_conn_qps;
	struct ibv_qp **conn_qp;
	struct ibv_cq **conn_cq;
	volatile uint8_t *conn_buf;	/* A buffer for RDMA over RC/UC QPs */
	int conn_buf_size;
	int conn_buf_shm_key;
	struct ibv_mr *conn_buf_mr;

	/* Datagram QPs */
	int num_dgram_qps;
	struct ibv_qp **dgram_qp;
	struct ibv_cq **dgram_send_cq, **dgram_recv_cq;
	volatile uint8_t *dgram_buf;	/* A buffer for RECVs on dgram QPs */
	int dgram_buf_size;
	int *recv_q_depth;
	int *send_q_depth;
	int dgram_buf_shm_key;
	struct ibv_mr *dgram_buf_mr;

	//struct ibv_wc *wc;	/* Array of work completions */
};

/* Major initialzation functions */
struct hrd_ctrl_blk* hrd_ctrl_blk_init(int local_hid,
									   int port_index, int numa_node_id,
									   int num_conn_qps, int use_uc,
									   volatile void *prealloc_conn_buf, int conn_buf_size, int conn_buf_shm_key,
									   int num_dgram_qps, int dgram_buf_size, int dgram_buf_shm_key,
									   int *recv_q_depth, int *send_q_depth);

int hrd_ctrl_blk_destroy(struct hrd_ctrl_blk *cb);

/* Debug */
void hrd_ibv_devinfo(void);

/* RDMA resolution functions */
struct ibv_device* hrd_resolve_port_index(struct hrd_ctrl_blk *cb,
										  int port_index);

uint16_t hrd_get_local_lid(struct ibv_context *ctx, int port_id);

void hrd_create_conn_qps(struct hrd_ctrl_blk *cb);
void hrd_create_dgram_qps(struct hrd_ctrl_blk *cb);

void hrd_connect_qp(struct hrd_ctrl_blk *cb,
					int conn_qp_idx, struct hrd_qp_attr *remote_qp_attr);

/* Post 1 RECV for this queue pair for this buffer. Low performance. */
void hrd_post_dgram_recv(struct ibv_qp *qp, void *buf_addr, int len, int lkey);

/* Fill @wc with @num_comps comps from this @cq. Exit on error. */
static inline uint32_t
hrd_poll_cq(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc)
{
	int comps = 0;
	uint32_t debug_cnt = 0;
	while(comps < num_comps) {
		if (debug_cnt > M_256) {
			printf("Someone is stuck waiting for a completion %d / %d  \n", comps, num_comps );
			debug_cnt = 0;
		}
		int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
		if(new_comps != 0) {
			// printf("I see completions %d\n", new_comps);
			/* Ideally, we should check from comps -> new_comps - 1 */
			if(wc[comps].status != 0) {
				fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
				exit(0);
			}
			comps += new_comps;
		}
		debug_cnt++;
	}
	return debug_cnt;
}

static inline struct ibv_mr* register_buffer(struct ibv_pd *pd, void* buf, uint32_t size)
{
	int ib_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
		IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
	struct ibv_mr* mr = ibv_reg_mr(pd,(char *)buf, size, ib_flags);
	assert(mr != NULL);
	return mr;
}
/* Fill @wc with @num_comps comps from this @cq. Return -1 on error, else 0. */
static inline int
hrd_poll_cq_ret(struct ibv_cq *cq, int num_comps, struct ibv_wc *wc)
{
	int comps = 0;

	while(comps < num_comps) {
		int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
		if(new_comps != 0) {
			/* Ideally, we should check from comps -> new_comps - 1 */
			if(wc[comps].status != 0) {
				fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
				return -1;	/* Return an error so the caller can clean up */
			}

			comps += new_comps;
		}
	}

	return 0;	/* Success */
}

/* Registry functions */
void hrd_publish(const char *key, void *value, int len);
int hrd_get_published(const char *key, void **value);

/* Publish the nth connected queue pair from this cb with this name */
void hrd_publish_conn_qp(struct hrd_ctrl_blk *cb, int n, const char *qp_name);

/* Publish the nth datagram queue pair from this cb with this name */
void hrd_publish_dgram_qp(struct hrd_ctrl_blk *cb, int n, const char *qp_name, uint8_t sl);

struct hrd_qp_attr* hrd_get_published_qp(const char *qp_name);

void hrd_publish_ready(const char *qp_name);
void hrd_wait_till_ready(const char *qp_name);

/* Utility functions */
static inline uint32_t hrd_fastrand(uint64_t *seed)
{
	*seed = *seed * 1103515245 + 12345;
	return (uint32_t) (*seed >> 32);
}

static inline long long hrd_get_cycles()
{
	unsigned low, high;
	unsigned long long val;
	asm volatile ("rdtsc" : "=a" (low), "=d" (high));
	val = high;
	val = (val << 32) | low;
	return val;
}

static inline int hrd_is_power_of_2(uint32_t n)
{
	return n && !(n & (n - 1));
}

void *hrd_malloc_socket(int shm_key, int size, int socket_id);
int hrd_free(int shm_key, void *shm_buf);
void red_printf(const char *format, ...);
void yellow_printf(const char *format, ...);
void green_printf(const char *format, ...);
void cyan_printf(const char *format, ...);
void hrd_get_formatted_time(char *timebuf);
void hrd_nano_sleep(int ns);
char *hrd_getenv(const char *name);

#endif /* HRD_H */
