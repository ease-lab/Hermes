//
// Created by akatsarakis on 06/02/19.
//

#ifndef AETHER_API_H
#define AETHER_API_H
#include <hrd.h>

/// WARNING!!
/// 	Accessible functions not defined below (in aether_api.h but exist only in aether.h) and starting with underscore
///		(i.e. "_aether_*") are internal and should not be called directly by the application

#define AETHER_ENABLE_ASSERTIONS 1
#define AETHER_MAX_SUPPORTED_INLINING 187
#define AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC 1

#define AETHER_ENABLE_STAT_COUNTING 1

#define AETHER_MIN_PCIE_BCAST_BATCH 1
#define AETHER_MIN(x,y) (x < y ? x : y)

#define AETHER_ENABLE_PRINTS 0
#define AETHER_ENABLE_SS_PRINTS (1 && AETHER_ENABLE_PRINTS)
#define AETHER_ENABLE_SEND_PRINTS (1 && AETHER_ENABLE_PRINTS)
#define AETHER_ENABLE_RECV_PRINTS (1 && AETHER_ENABLE_PRINTS)
#define AETHER_ENABLE_CREDIT_PRINTS (1 && AETHER_ENABLE_PRINTS)
#define AETHER_ENABLE_POST_RECV_PRINTS (1 && AETHER_ENABLE_PRINTS)

/* Useful when `x = (x + 1) % N` is done in a loop */
#define AETHER_MOD_ADD(x, N) do { \
	x = x + 1; \
	if(x == N) x = 0; \
} while(0)


/* ah pointer and qpn are accessed together in the critical path
   so we are putting them in the same cache line */
typedef struct
{
    struct ibv_ah *ah;
    uint32_t qpn;
    // no padding needed- false sharing is not an issue, only fragmentation
}
qp_info_t;


typedef struct
{
    uint8_t sender_id;  // support for up to 256 unique senders per instance (e.g. thread)
    uint8_t req_num;    // <= max_coalescing of a channel
    uint8_t reqs[];     // sizeof(req_num * req_size)
}
aether_pkt_t, aether_ud_send_pkt_t;

// Packets with GRH
typedef struct
{
    struct ibv_grh grh;
    aether_pkt_t pkt;
}__attribute__((packed))
aether_ud_recv_pkt_t; // rcved rdma ud pkts come with a grh padding

typedef struct
{
    uint8_t sender_id;   // support for up to 256 unique senders per instance (e.g. thread)
    uint16_t crd_num;    // credit num
}__attribute__((packed))
aether_crd_t; // always send as immediate

typedef struct {
    uint64_t send_total_msgs;
    uint64_t send_total_pkts;
    uint64_t send_total_pcie_batches;

    uint64_t ss_completions;
    uint64_t recv_total_msgs;
    uint64_t recv_total_pkts;
}
ud_channel_stats_t;

enum channel_type {REQ, RESP, CRD};

typedef struct _ud_channel_t
{
	struct ibv_qp * qp;

    enum channel_type type;
    uint8_t max_coalescing;
    uint8_t is_bcast_channel;
    uint8_t expl_crd_ctrl;
    uint8_t is_inlining_enabled;
    struct _ud_channel_t* channel_providing_crds; // this is NULL if explicit crd_ctrl is 1

    char* qp_name;
    uint16_t qp_id; //id of qp in cb
    uint16_t max_msg_size;

    uint16_t num_channels; // e.g. remote nodes
    uint16_t num_crds_per_channel;
    uint8_t* credits_per_rem_channels; // array size of num_channels denoting available space on remote sides
    /// Credits refer to msgs irrespective if coalesed or not --> a remote buffer must be able to handle max_number_of_msgs * max_coalescing


    volatile uint8_t* recv_pkt_buff; /// Intermediate buffs where reqs are copied when pkts are received
    aether_ud_send_pkt_t* send_pkt_buff; /// Intermediate buffs where reqs are copied when pkts are send

    uint16_t send_pkt_buff_len;
    uint16_t recv_pkt_buff_len;


    uint16_t max_send_wrs;
    uint16_t max_recv_wrs;

    uint16_t send_q_depth;
    uint16_t recv_q_depth;

    uint16_t ss_granularity; //selective signaling granularity
    uint16_t max_pcie_bcast_batch;

    uint64_t total_pkts_send; // used for selective signaling

    int send_push_ptr;
    int recv_push_ptr;
    int recv_pull_ptr;


    struct ibv_send_wr* send_wr;
    struct ibv_recv_wr* recv_wr;  // Used only to batch post recvs to the NIC

    struct ibv_sge*     send_sgl;
    struct ibv_sge*     recv_sgl; // Used only to batch post recvs to the NIC

	struct ibv_cq* send_cq;
    struct ibv_cq* recv_cq;
    struct ibv_wc* recv_wc;        // (size of max_recv_wrs) Used on polling recv req cq (only for immediates)

    /// Send wcs are omitted since they are only used for selective signaling (within send function calls)

    struct ibv_mr* send_mem_region; // NULL if inlining is enabled

    // Remote QPs
    qp_info_t* remote_qps;

    // Used only for type == CRD
    uint16_t *no_crds_to_send_per_endpoint;

    // Stats
    ud_channel_stats_t stats;

    // Toggles
    uint8_t enable_stats;
    uint8_t enable_prints;
}
ud_channel_t;

// Define some function pointers used when issuing pkts
typedef void (*modify_input_elem_after_send_t) (uint8_t*);
typedef int  (*skip_input_elem_or_get_sender_id_t) (uint8_t*); //Should return -1 to skip otherwise returns the sender id
typedef void (*copy_and_modify_input_elem_t) (uint8_t* msg_to_send, uint8_t* triggering_req);


/// Init and Util functions
void print_ud_c_overview(ud_channel_t* ud_c);

void aether_ud_channel_init(ud_channel_t *ud_c, char *qp_name, enum channel_type type,
							uint8_t max_coalescing, uint16_t max_req_size,
							uint8_t enable_inlining, uint8_t is_bcast,
						    // Credits
							uint8_t expl_crd_ctrl, ud_channel_t *linked_channel,
							uint8_t crds_per_channel, uint16_t num_channels,
							// Toggles
							uint8_t stats_on, uint8_t prints_on);

void aether_ud_channel_crd_init(ud_channel_t *ud_c, char *qp_name,
							    // Credits
								ud_channel_t *linked_channel, uint8_t crds_per_channel, uint16_t num_channels,
								// Toggles
								uint8_t enable_stats, uint8_t enable_prints);

void aether_allocate_and_init_all_qps(ud_channel_t** ud_c_array, uint16_t ud_c_num,
									  uint16_t worker_gid, uint16_t worker_lid);


/// Main functions
static inline uint16_t
aether_poll_buff_and_post_recvs(ud_channel_t* ud_c, uint16_t max_pkts_to_poll,
                                uint8_t* recv_buff_space);

static inline uint8_t
aether_issue_pkts(ud_channel_t *ud_c,
                  uint8_t *input_array_of_elems, uint16_t input_array_len,
                  uint16_t size_of_input_elems, uint16_t* input_array_rolling_idx,
                  skip_input_elem_or_get_sender_id_t skip_or_get_sender_id_func_ptr,
                  modify_input_elem_after_send_t modify_elem_after_send,
                  copy_and_modify_input_elem_t copy_and_modify_elem);

static inline void
aether_issue_credits(ud_channel_t *ud_c, uint8_t *input_array_of_elems,
					 uint16_t input_array_len, uint16_t size_of_input_elems,
					 skip_input_elem_or_get_sender_id_t skip_or_get_sender_id_func_ptr,
					 modify_input_elem_after_send_t modify_elem_after_send);


#endif //AETHER_API_H
