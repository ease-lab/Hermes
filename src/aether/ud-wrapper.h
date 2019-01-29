//
// Created by akatsarakis on 22/01/19.
//

#ifndef AETHER_UD_WRAPPER_H
#define AETHER_UD_WRAPPER_H

#include <hrd.h>

#define AETHER_BASE_SHM_KEY 5322
#define AETHER_ENABLE_ASSERTIONS 1
#define AETHER_MAX_SUPPORTED_INLINING 187
#define AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC 1


#define AETHER_MIN_PCIE_BCAST_BATCH 1
#define AETHER_MIN(x,y) (x < y ? x : y)

#define AETHER_ENABLE_SS_PRINTS 0
#define AETHER_ENABLE_REQ_PRINTS 0
#define AETHER_ENABLE_SEND_PRINTS 0
#define AETHER_ENABLE_RECV_PRINTS 0
#define AETHER_ENABLE_CREDIT_PRINTS 0
#define AETHER_ENABLE_POST_RECV_PRINTS 0
#define AETHER_ENABLE_BATCH_OP_PRINTS 0

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
aether_pkt, aether_ud_send_pkt;

// Packets with GRH
typedef struct
{
    struct ibv_grh grh;
    aether_pkt pkt;
}
aether_ud_recv_pkt; // rcved rdma ud pkts come with a grh padding


typedef struct {
    uint64_t send_total_msgs;
    uint64_t send_total_pkts;
    uint64_t send_total_pcie_batches;

    uint64_t ss_completions;
    uint64_t recv_total_msgs;
    uint64_t recv_total_pkts;
}
ud_channel_stats;

enum channel_type {SEND, RECV, SEND_RECV};

typedef struct _ud_channel_t
{
    enum channel_type type;
    uint8_t max_coalescing;
    uint8_t is_bcast_channel;
    uint8_t explicit_crd_ctrl;
    uint8_t is_inlining_enabled;
    struct _ud_channel_t* channel_providing_crds; // this is NULL if explicit crd_ctrl is 1

    char* qp_name;
    uint16_t qp_id; //id of qp in cb
    uint16_t max_req_size;

    uint16_t num_remote_channels; // e.g. remote nodes
    uint16_t max_crds_per_channel;
    uint8_t* credits_per_rem_channels; // array size of num_remote_channels denoting available space on remote sides
    /// Credits refer to msgs irrespective if coalesed or not --> a remote buffer must be able to handle max_number_of_msgs * max_coalescing


    volatile uint8_t* recv_reqs_buff; /// Intermediate buffs where reqs are copied when pkts are received
    aether_ud_send_pkt* send_reqs_buff; /// Intermediate buffs where reqs are copied when pkts are send

    uint16_t send_reqs_size;
    uint16_t recv_reqs_size;


    uint16_t max_send_wrs;
    uint16_t max_recv_wrs;

    uint16_t send_q_depth;
    uint16_t recv_q_depth;

    uint16_t ss_granularity; //selective signaling granularity
    uint16_t max_pcie_bcast_batch;


    int send_reqs_push_ptr;
    int recv_reqs_push_ptr;
    int recv_reqs_pull_ptr;


    struct ibv_send_wr* send_wr;
    struct ibv_recv_wr* recv_wr;  // Used only to batch post recvs to the NIC

    struct ibv_sge*     send_sgl;
    struct ibv_sge*     recv_sgl; // Used only to batch post recvs to the NIC

    struct ibv_cq* recv_cq;
    struct ibv_wc* recv_wc;        // (size of max_recv_wrs) Used on polling recv req cq (only for immediates)

    /// Send wcs are omitted since they are only used for selective signaling (within send function calls)

    struct ibv_mr* send_mem_region; // NULL if inlining is enabled

    // Remote QPs
    qp_info_t* remote_qps;

    // Stats
    ud_channel_stats stats;

    // Toggles
    uint8_t enable_stats;
    uint8_t enable_prints;
}
ud_channel_t;




void aether_unit_test(void);






static inline void
aether_assert_binary(uint8_t var)
{
    assert(var == 0 || var == 1);
}

static void
aether_assertions(ud_channel_t* ud_channel)
{
    aether_assert_binary(ud_channel->is_bcast_channel);
    aether_assert_binary(ud_channel->explicit_crd_ctrl);
    aether_assert_binary(ud_channel->is_inlining_enabled);
    assert(!ud_channel->explicit_crd_ctrl || ud_channel->channel_providing_crds != NULL);

    assert(ud_channel->max_req_size > 0);
    assert(ud_channel->max_coalescing > 0);
    assert(ud_channel->num_remote_channels > 0);
    assert(ud_channel->send_q_depth > 0 || ud_channel->recv_q_depth > 0);
}


static inline uint16_t
aether_ud_recv_max_pkt_size(ud_channel_t *ud_c)
{
    //TODO add assertion that this must be smaller than max_MTU
    assert(ud_c->max_req_size > 0 && ud_c->max_coalescing > 0);
    return sizeof(aether_ud_recv_pkt) + ud_c->max_req_size * ud_c->max_coalescing;
}

static inline uint16_t
aether_ud_send_max_pkt_size(ud_channel_t *ud_c)
{
    //TODO add assertion that this must be smaller than max_MTU
    assert(ud_c->max_req_size > 0 && ud_c->max_coalescing > 0);
    return sizeof(aether_ud_send_pkt) + ud_c->max_req_size * ud_c->max_coalescing;
}

static inline uint8_t*
aether_get_n_msg_ptr_from_send_pkt(ud_channel_t *ud_c, aether_ud_send_pkt *pkt, uint8_t n)
{
    assert(ud_c->max_coalescing > n && pkt->req_num >= n);
    return pkt->reqs + (n * ud_c->max_req_size);
}

static inline uint8_t*
aether_get_n_msg_ptr_from_recv_pkt(ud_channel_t *ud_c, aether_ud_recv_pkt* pkt, uint8_t n)
{
    return aether_get_n_msg_ptr_from_send_pkt(ud_c, &pkt->pkt, n);
}

static inline uint8_t*
aether_get_n_msg_of_buff_ptr(ud_channel_t *ud_c, uint8_t* buff, uint8_t n)
{
    return buff + (n * ud_c->max_req_size);
}

static inline aether_ud_send_pkt*
aether_curr_send_pkt_ptr(ud_channel_t *ud_c)
{
    return &ud_c->send_reqs_buff[ud_c->send_reqs_push_ptr];
}

static inline void
aether_inc_send_push_ptr(ud_channel_t *ud_c)
{
    AETHER_MOD_ADD(ud_c->send_reqs_push_ptr, ud_c->send_reqs_size);
    aether_curr_send_pkt_ptr(ud_c)->req_num = 0; //Reset data left from previous unicasts / bcasts
}

static inline void
aether_inc_recv_push_ptr(ud_channel_t *ud_c)
{
    AETHER_MOD_ADD(ud_c->recv_reqs_push_ptr, ud_c->recv_reqs_size);
}

static inline void
aether_inc_recv_pull_ptr(ud_channel_t *ud_c)
{
    AETHER_MOD_ADD(ud_c->recv_reqs_pull_ptr, ud_c->recv_reqs_size);
}


static inline uint8_t
aether_has_sufficient_crds(ud_channel_t *ud_c, uint8_t endpoint_id)
{
    return (uint8_t) (ud_c->credits_per_rem_channels[endpoint_id] > 0);
}

static inline void
aether_dec_crds(ud_channel_t *ud_c, uint8_t endpoint_id)
{
    assert(aether_has_sufficient_crds(ud_c, endpoint_id));
    ud_c->credits_per_rem_channels[endpoint_id]--;
    if (ENABLE_CREDIT_PRINTS && ud_c->enable_prints)
        printf("$$$ Credits: %s \033[31mdecremented\033[0m to %d (for endpoint %d)\n",
               ud_c->qp_name, ud_c->credits_per_rem_channels[endpoint_id], endpoint_id);

}

#endif //AETHER_UD_WRAPPER_H
