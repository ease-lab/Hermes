#ifndef HRD_H
#define HRD_H

#include <assert.h>
#include <errno.h>
#include <numaif.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#include <infiniband/verbs.h>
#include <libmemcached/memcached.h>
#include <malloc.h>
#include <time.h>
#include "sizes.h"

//<vasilis> Multicast
// TODO we do not use hw multicast because it helps only on master-based
// patterns
//#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
// <vasilis>

#define USE_BIG_OBJECTS 0
#define EXTRA_CACHE_LINES 0
#define BASE_VALUE_SIZE 46  // max is --> 46
#define SHIFT_BITS \
  (USE_BIG_OBJECTS == 1 ? 3 : 0)  // number of bits to shift left or right to
                                  // calculate the value length
#define HRD_DEFAULT_PSN \
  3185 /* PSN for all queues */  // starting Packet Sequence Number
#define HRD_DEFAULT_QKEY 0x11111111

#define HRD_QP_NAME_SIZE 200 /* Size (in bytes) of a queue pair name */
#define HRD_RESERVED_NAME_PREFIX "__HRD_RESERVED_NAME_PREFIX"

#define KVS_VALUE_SIZE                                \
  (USE_BIG_OBJECTS == 1                               \
       ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE) \
       : BASE_VALUE_SIZE)  //(169 + 64)// 46 + 64 + 64//32 //(46 + 64)

#define HUGE_PAGE_SIZE 2097152
#define LEVERAGE_TLB_COALESCING 1

/*
 * Small max_inline_data reduces the QP's max WQE size, which reduces the
 * DMA size in doorbell method of WQE fetch.
 */
#define HRD_MAX_INLINE \
  188  //(USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + 60) : 60) //60 is
       // what kalia had here//

// This is required for ROCE not sure yet why
// <vasilis>
#define IB_PHYS_PORT 1
// </vasilis>
// <akatsarakis>
#define USE_HUGE_PAGES 1
// </akatsarakis>

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#endif

#ifndef unlikely
#define unlikely(x) __builtin_expect(!!(x), 0)
#endif

/* Compare, print, and exit */
#define CPE(val, msg, err_code)                \
  if (unlikely(val)) {                         \
    fprintf(stderr, msg);                      \
    fprintf(stderr, " Error %d \n", err_code); \
    exit(err_code);                            \
  }

/* vasilis added a ceiling and a MAX*/
#define CEILING(x, y) (((x) + (y)-1) / (y))
#define MAX(x, y) (x > y ? x : y)

int is_roce;
int machine_id;
char *remote_IP, *local_IP;

/* Registry info about a QP */
struct hrd_qp_attr {
  char name[HRD_QP_NAME_SIZE];

  // ROCE
  uint64_t
      gid_global_interface_id;  // Store the gid fields separately because I
  uint64_t gid_global_subnet_prefix;  // don't like unions. Needed for RoCE only

  /* Info about the RDMA buffer associated with this QP */
  uintptr_t buf_addr;
  uint32_t buf_size;
  uint32_t rkey;

  int lid;
  int qpn;
  uint8_t sl;
};

struct hrd_ud_ctrl_blk {
  int local_hid; /* Local ID on the machine this process runs on */

  /* Info about the device/port to use for this control block */
  struct ibv_context* ctx;
  int device_id;    /* Resovled by libhrd from @port_index */
  int dev_port_id;  /* 1-based within dev @device_id. Resolved by libhrd */
  int numa_node_id; /* NUMA node id */

  struct ibv_pd* pd; /* A protection domain for this control block */

  /* Datagram QPs */
  int num_dgram_qps;
  struct ibv_qp** dgram_qp;
  struct ibv_cq **dgram_send_cq, **dgram_recv_cq;
  volatile uint8_t* dgram_buf; /* A buffer for RECVs on dgram QPs */
  int* recv_q_depth;
  int* send_q_depth;
  int dgram_buf_shm_key;
  struct ibv_mr* dgram_buf_mr;
};

/* Major initialzation functions */

struct hrd_ud_ctrl_blk* hrd_ud_ctrl_blk_init(
    int local_hid, int port_index,
    int numa_node_id, /* -1 means don't use hugepages */
    int num_dgram_qps, int dgram_buf_size, int dgram_buf_shm_key,
    int* recv_q_depth, int* send_q_depth);

int hrd_ud_ctrl_blk_destroy(struct hrd_ud_ctrl_blk* cb);

/* RDMA resolution functions */
struct ibv_device* hrd_resolve_port_index(struct hrd_ud_ctrl_blk* cb,
                                          int port_index);

uint16_t hrd_get_local_lid(struct ibv_context* ctx, int port_id);

void hrd_create_dgram_qps(struct hrd_ud_ctrl_blk* cb);

/* Fill @wc with @num_comps comps from this @cq. Exit on error. */
static inline uint32_t
hrd_poll_cq(struct ibv_cq* cq, int num_comps, struct ibv_wc* wc)
{
  int comps = 0;
  uint32_t debug_cnt = 0;
  while (comps < num_comps) {
    if (debug_cnt > M_256) {
      printf("Someone is stuck waiting for a completion %d / %d  \n", comps,
             num_comps);
      debug_cnt = 0;
    }
    int new_comps = ibv_poll_cq(cq, num_comps - comps, &wc[comps]);
    if (new_comps != 0) {
      // printf("I see completions %d\n", new_comps);
      /* Ideally, we should check from comps -> new_comps - 1 */
      if (wc[comps].status != 0) {
        fprintf(stderr, "Bad wc status %d\n", wc[comps].status);
        exit(0);
      }
      comps += new_comps;
    }
    debug_cnt++;
  }
  return debug_cnt;
}

static inline struct ibv_mr*
register_buffer(struct ibv_pd* pd, void* buf, uint32_t size)
{
  int ib_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                 IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
  struct ibv_mr* mr = ibv_reg_mr(pd, (char*)buf, size, ib_flags);
  assert(mr != NULL);
  return mr;
}

/* Registry functions */
void hrd_publish(const char* key, void* value, int len);
int hrd_get_published(const char* key, void** value);

///* Publish the nth connected queue pair from this cb with this name */
// void hrd_publish_conn_qp(struct hrd_ud_ctrl_blk *cb, int n, const char
// *qp_name);

/* Publish the nth datagram queue pair from this cb with this name */
void hrd_publish_dgram_qp(struct hrd_ud_ctrl_blk* cb, int n,
                          const char* qp_name, uint8_t sl);

struct hrd_qp_attr* hrd_get_published_qp(const char* qp_name);

/* Utility functions */
static inline uint32_t
hrd_fastrand(uint64_t* seed)
{
  *seed = *seed * 1103515245 + 12345;
  return (uint32_t)(*seed >> 32);
}

void* hrd_malloc_socket(int shm_key, uint64_t size, int socket_id);
int hrd_free(int shm_key, void* shm_buf);
char* hrd_getenv(const char* name);

// Like printf, but colorfur. Limited to 1000 characters.
typedef enum { YELLOW = 0, RED, GREEN, CYAN } color_print_t;
void colored_printf(color_print_t color, const char* format, ...);

extern char dev_name[50];
#endif /* HRD_H */
