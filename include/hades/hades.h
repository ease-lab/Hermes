//
// Created by akatsarakis on 17/01/19.
//

#ifndef HADES_H
#define HADES_H

#include "../utils/bit_vector.h"
#include "../utils/time_rdtsc.h"
#include "../wings/wings_api.h"
// Send heartbeats
// Recv heartbeats
// Change View
// Update local membership

// (Ostracism)
// arbitration --> a node provides an obolus

// all nodes are able to communicate w/ each other

// fd provides a view as a membership change
// only as long as it differs with the current view
// and agrees with a majority of other node views.

// The update granularity of local view works as a lease
// to membership changes which prevents sequentially
// consistent reads in the presence of network partitions
//       I.E. a node in a minority partition is able to detect
//       that cannot reach the majority of nodes and stops serving
//       local reads, maintaining linearizability (instead of sequential consistency)
//       For this

// Epochs

// Guarantees Nodes in the same EPOCH id have the same group view

#define ENABLE_ARBITRATION 1
#define FAKE_LINK_FAILURE 1
#define FAKE_LINK_FAILURE_AFTER_SEC 15
#define STOP_FAKE_LINK_FAILURE_AFTER_SEC 20
#define FAKE_ONE_WAY_LINK_FAILURE 0
#define FAKE_LINK_FAILURE_NODE_A 2
#define FAKE_LINK_FAILURE_NODE_B 1
static_assert(FAKE_LINK_FAILURE_NODE_A != FAKE_LINK_FAILURE_NODE_B, "");

typedef struct
{
    uint8_t node_id  : 8;
    uint8_t epoch_id : 8;
    uint8_t same_w_local_membership :1;
    uint8_t have_ostracised_for_dst_node :7;
    bit_vector_t view;
}
__attribute__((packed))
hades_view_t;
static_assert(sizeof(hades_view_t) <= 4, "Currently send using a 4B header only field (RDMA immediate)");

typedef struct
{
    hades_view_t last_local_view;
    hades_view_t intermediate_local_view;

    bit_vector_t curr_g_membership;
    uint8_t nodes_in_membership;

    uint8_t max_num_nodes;
    uint8_t* recved_views_flag;
    hades_view_t* remote_recved_views;

    // Polling
    uint16_t max_views_to_poll;
    hades_view_t *poll_buff; // used for polling remote views

    // Timing
    uint32_t send_view_every_us;
    uint32_t update_local_view_every_ms;
    struct timespec *ts_last_send; // issues views to remotes iff have not send a view within the predefined timeout
    struct timespec ts_last_view_change; // update views and possible changes membership iff pre-defined timeout is exceed

    // Ostracism
    uint8_t *have_ostracized_for; //an array storing info whether or not in a view the sender ostracized someone for this node
}
hades_ctx_t;

typedef struct
{
    hades_ctx_t ctx;
    ud_channel_t* hviews_c;
    ud_channel_t* hviews_crd_c;
}
hades_wings_ctx_t;

inline static void
hades_ctx_init(hades_ctx_t* ctx, uint8_t node_id, uint8_t max_nodes,
               uint16_t max_views_to_poll,
               uint32_t send_view_us, uint32_t update_local_view_ms)
{
   assert(max_views_to_poll > 0);

   ctx->intermediate_local_view.epoch_id = 0;
   ctx->intermediate_local_view.node_id = node_id;
   ctx->nodes_in_membership = 1;
   bv_init(&ctx->curr_g_membership);
   bv_bit_set(&ctx->curr_g_membership, node_id);
   bv_init(&ctx->intermediate_local_view.view);
   bv_bit_set(&ctx->intermediate_local_view.view, node_id);
   ctx->last_local_view = ctx->intermediate_local_view;

   ctx->max_num_nodes = max_nodes;
   ctx->recved_views_flag = malloc(sizeof(uint8_t) * max_nodes);
   ctx->remote_recved_views = malloc(sizeof(hades_view_t) * max_nodes);
   for(int i = 0; i < max_nodes; ++i){
      ctx->recved_views_flag[i] = 0;
      bv_init(&ctx->remote_recved_views[i].view);
   }

   ctx->max_views_to_poll = max_views_to_poll;
   ctx->poll_buff = malloc(sizeof(hades_view_t) * max_views_to_poll);

   // Setup timers
   init_rdtsc(1, 0); ///WARNING: this is not thread safe!!
   get_rdtsc_timespec(&ctx->ts_last_view_change);
   ctx->ts_last_send = malloc(sizeof(struct timespec) * max_nodes);
   for(int i = 0; i < max_nodes; ++i)
      get_rdtsc_timespec(&ctx->ts_last_send[i]);

   ctx->send_view_every_us = send_view_us;
   ctx->update_local_view_every_ms = update_local_view_ms;
   assert(2 * 1000 * update_local_view_ms > send_view_us);

   //Ostracism
   ctx->have_ostracized_for = malloc(sizeof(uint8_t) * max_nodes);
   for(int i = 0; i < max_nodes; ++i)
      ctx->have_ostracized_for[i] = 0;

}


// WARNING: hades wings_ctx_init initializes only the first part of the
// required channels wings_setup_channel_qps_and_recvs must be called by
// the application afterwards to finish the initialization of wings.
inline static void
hades_wings_ctx_init(hades_wings_ctx_t* wctx, uint8_t node_id, uint8_t max_nodes,
                     uint16_t max_views_to_poll,
                     uint32_t send_view_us, uint32_t update_local_view_ms,
                     ud_channel_t* hviews_c, ud_channel_t* hviews_crd_c,
                     uint16_t worker_lid)
{
   hades_ctx_init(&wctx->ctx, node_id, max_nodes, max_views_to_poll,
                  send_view_us, update_local_view_ms);

   wctx->hviews_c = hviews_c;
   wctx->hviews_crd_c = hviews_crd_c;

   const uint8_t is_bcast = 0;
   const uint8_t stats_on = 1;
   const uint8_t prints_on = 1;
   const uint8_t is_hdr_only = 1;
   const uint8_t expl_crd_ctrl = 1;
   const uint8_t enable_inlining = 1;
   const uint8_t disable_crd_ctrl = 0;
   const uint8_t credits = (const uint8_t) (2 * update_local_view_ms * 1000 / send_view_us);

   char qp_name[200];
   sprintf(qp_name, "%s%d", "\033[1m\033[32mHades\033[0m", worker_lid);

   wings_ud_channel_init(wctx->hviews_c, qp_name, REQ, 1, sizeof(hades_view_t) - sizeof(uint8_t), 0,
                         enable_inlining, is_hdr_only, is_bcast,
                         disable_crd_ctrl, expl_crd_ctrl, wctx->hviews_crd_c, credits,
                         max_nodes, (uint8_t) machine_id, stats_on, prints_on);
}


// How does somebody joins?
// epoch id 0
// must see at least a majority of views with same epoch id > 0
// || majority of views with epoch id 0
#endif //HADES_H
