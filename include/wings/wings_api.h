//
// Created by akatsarakis on 06/02/19.
//

#ifndef WINGS_API_H
#define WINGS_API_H
#include "../utils/bit_vector.h"
#include "hrd.h"

/// WARNING!!
/// 	Accessible functions not defined below (in wings_api.h but exist only in
/// wings.h) and starting with underscore
///		(i.e. "_wings_*") are internal and should not be called directly
/// by the application

#define WINGS_ENABLE_ASSERTIONS 0
#define WINGS_MAX_SUPPORTED_INLINING 187
#define WINGS_ENABLE_BATCH_POST_RECVS_TO_NIC 1

#define WINGS_ENABLE_STAT_COUNTING 1

#define WINGS_MIN_PCIE_BCAST_BATCH 1
#define WINGS_MIN(x, y) (x < y ? x : y)

#define WINGS_ENABLE_PRINTS 0
#define WINGS_ENABLE_SS_PRINTS (1 && WINGS_ENABLE_PRINTS)
#define WINGS_ENABLE_SEND_PRINTS (1 && WINGS_ENABLE_PRINTS)
#define WINGS_ENABLE_RECV_PRINTS (1 && WINGS_ENABLE_PRINTS)
#define WINGS_ENABLE_CREDIT_PRINTS (1 && WINGS_ENABLE_PRINTS)
#define WINGS_ENABLE_POST_RECV_PRINTS (1 && WINGS_ENABLE_PRINTS)

#define WINGS_IS_ROCE 0
#define MAX_MTU_SIZE 4096

/* Useful when `x = (x + 1) % N` is done in a loop */
#define WINGS_MOD_ADD(x, N) \
  do {                      \
    x = x + 1;              \
    if (x == N) x = 0;      \
  } while (0)

/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same cache line */
typedef struct {
  struct ibv_ah* ah;
  uint32_t qpn;
  // no padding needed- false sharing is not an issue, only fragmentation
} qp_info_t;

typedef struct {
  uint8_t only_small_msgs : 1;  // support for up to 256 unique senders per
                                // instance (e.g. thread)
  uint8_t sender_id : 7;  // support for up to 128 unique senders per instance
                          // (e.g. thread)
  uint8_t req_num;        // <= max_coalescing of a channel
  uint8_t reqs[];         // sizeof(req_num * req_size)
} wings_pkt_t, wings_ud_send_pkt_t;

// Packets with GRH
typedef struct {
  struct ibv_grh grh;
  wings_pkt_t pkt;
} __attribute__((packed))
wings_ud_recv_pkt_t;  // rcved rdma ud pkts come with a grh padding

typedef struct {
  uint8_t sender_id;  // support for up to 256 unique senders per instance (e.g.
                      // thread)
  uint16_t crd_num;   // credit num
} __attribute__((packed)) wings_crd_t;  // always send as inlined_payload

typedef struct {
  uint8_t sender_id;  // support for up to 256 unique senders per instance (e.g.
                      // thread)
  uint8_t inlined_payload[3];  // available space to be used by the application
} __attribute__((packed)) wings_hdr_only_t;  // always send as inlined_payload

static_assert(sizeof(wings_hdr_only_t) == 4 * sizeof(uint8_t), "");

typedef struct {
  uint64_t send_total_msgs;
  uint64_t send_total_pkts;
  uint64_t send_total_pcie_batches;

  uint64_t ss_completions;
  uint64_t recv_total_msgs;
  uint64_t recv_total_pkts;

  uint64_t
      no_stalls_due_to_credits;  // number of stalls due to not enough credits
} ud_channel_stats_t;

enum channel_type { REQ, RESP, CRD };

typedef struct _ud_channel_t {
  struct ibv_qp* qp;

  enum channel_type type;
  uint8_t max_coalescing;
  uint8_t expl_crd_ctrl;
  uint8_t disable_crd_ctrl;
  uint8_t is_header_only;
  uint8_t is_bcast_channel;
  uint8_t is_inlining_enabled;
  struct _ud_channel_t* channel_providing_crds;

  char* qp_name;
  uint16_t qp_id;  // id of qp in cb
  uint16_t max_msg_size;
  uint16_t small_msg_size;

  uint8_t channel_id;     // id of the curr channel (e.g. local node id)
  uint16_t num_channels;  // e.g. remote nodes + local node
  uint16_t num_crds_per_channel;
  uint16_t* credits_per_channels;  // array size of num_channels denoting
                                   // available space on remote sides
  /// Credits refer to msgs irrespective if coalesed or not --> a remote buffer
  /// must be able to handle max_number_of_msgs * max_coalescing

  volatile uint8_t* recv_pkt_buff;  /// Intermediate buffs where reqs are copied
                                    /// when pkts are received
  wings_ud_send_pkt_t* send_pkt_buff;  /// Intermediate buffs where reqs are
                                       /// copied when pkts are send

  uint16_t send_pkt_buff_len;
  uint16_t recv_pkt_buff_len;

  uint16_t max_send_wrs;
  uint16_t max_recv_wrs;

  uint16_t send_q_depth;
  uint16_t recv_q_depth;

  uint16_t ss_granularity;  // selective signaling granularity
  uint16_t max_pcie_bcast_batch;

  uint64_t total_pkts_send;  // used for selective signaling

  int send_push_ptr;
  int recv_push_ptr;
  int recv_pull_ptr;

  struct ibv_send_wr* send_wr;
  struct ibv_recv_wr* recv_wr;  // Used only to batch post recvs to the NIC

  struct ibv_sge* send_sgl;
  struct ibv_sge* recv_sgl;  // Used only to batch post recvs to the NIC

  struct ibv_cq* send_cq;
  struct ibv_cq* recv_cq;
  struct ibv_wc* recv_wc;  // (size of max_recv_wrs) Used on polling recv req cq
                           // (only for immediates)

  /// Send wcs are omitted since they are only used for selective signaling
  /// (within send function calls)

  struct ibv_mr* send_mem_region;  // NULL if inlining is enabled

  struct ibv_pd* pd;  // A protection domain for this ud channel

  // Remote QPs
  qp_info_t* remote_qps;

  // Used only for type == CRD
  uint16_t* no_crds_to_send_per_endpoint;

  // Stats
  ud_channel_stats_t stats;

  uint8_t enable_overflow_msgs;
  uint8_t num_overflow_msgs;   // msgs in overflow_msg_buff always <=
                               // max_coalescing - 1
  uint8_t* overflow_msg_buff;  // use to keep message in case of polling
                               // a pkt and it doesn't fit in the recv array we

  // Toggles
  uint8_t enable_stats;
  uint8_t enable_prints;
} ud_channel_t;

// Define some function pointers used when issuing pkts
typedef void (*modify_input_elem_after_send_t)(uint8_t*);
typedef int (*skip_input_elem_or_get_dst_id_t)(
    uint8_t*);  // Should return -1 to skip otherwise returns the sender id
typedef void (*copy_and_modify_input_elem_t)(uint8_t* msg_to_send,
                                             uint8_t* triggering_req);

static inline void
wings_NOP_modify_elem_after_send(uint8_t* req)
{ /*Do not change anything*/
}

/// Init and Util functions
void wings_print_ud_c_overview(ud_channel_t* ud_c);

void wings_ud_channel_destroy(
    ud_channel_t* ud_c);  // This must be used to destroy all ud_c (both CRD and
                          // typical ud_c)

// This is used to int only non-CRDs channels (CRDs are initialized internally)
void wings_ud_channel_init(ud_channel_t* ud_c, char* qp_name,
                           enum channel_type type, uint8_t max_coalescing,
                           uint16_t max_req_size, uint16_t small_req_size,
                           uint8_t enable_inlining, uint8_t is_header_only,
                           uint8_t is_bcast,
                           // Credits
                           uint8_t disable_crd_ctrl, uint8_t expl_crd_ctrl,
                           ud_channel_t* linked_channel,
                           uint16_t crds_per_channel, uint16_t num_channels,
                           uint8_t channel_id,
                           // Toggles
                           uint8_t stats_on, uint8_t prints_on);

void wings_setup_channel_qps_and_recvs(ud_channel_t** ud_c_array,
                                       uint16_t ud_c_num,
                                       dbit_vector_t* shared_rdy_var,
                                       uint16_t worker_lid);

/// Main functions
static inline uint16_t wings_poll_buff_and_post_recvs(ud_channel_t* ud_c,
                                                      uint16_t max_pkts_to_poll,
                                                      uint8_t* recv_buff_space);

static inline uint8_t wings_issue_pkts(
    ud_channel_t* ud_c, bit_vector_t* membership, uint8_t* input_array_of_elems,
    uint16_t input_array_len, uint16_t size_of_input_elems,
    uint16_t* input_array_rolling_idx,
    skip_input_elem_or_get_dst_id_t skip_or_get_sender_id_func_ptr,
    modify_input_elem_after_send_t modify_elem_after_send,
    copy_and_modify_input_elem_t copy_and_modify_elem);

static inline void wings_issue_credits(
    ud_channel_t* ud_c, bit_vector_t* membership, uint8_t* input_array_of_elems,
    uint16_t input_array_len, uint16_t size_of_input_elems,
    skip_input_elem_or_get_dst_id_t skip_or_get_sender_id_func_ptr,
    modify_input_elem_after_send_t modify_elem_after_send);

#endif  // WINGS_API_H
