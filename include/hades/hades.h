//
// Created by akatsarakis on 17/01/19.
//

#ifndef HADES_H
#define HADES_H

#include "../utils/bit_vector.h"
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

// add leases to membership changes as well to
// prevent sequentially consistent reads

// Epochs

#define HADES_SEND_VIEW_EVERY_US 1000
#define HADES_CHECK_VIEW_CHANGE_EVERY_MS 10
static_assert(2 * 1000 * HADES_CHECK_VIEW_CHANGE_EVERY_MS > HADES_SEND_VIEW_EVERY_US, "");

#define DISABLE_INLINING 0
#define MAX_HBS_TO_POLL 10
#define ENABLE_HADES_INLINING ((DISABLE_INLINING || sizeof(hades_view_t) >= 188) ? 0 : 1)

typedef struct
{
    uint8_t node_id;
    uint8_t epoch_id;
    uint8_t same_w_local_membership;
//    uint16_t epoch_id;
    bit_vector_t view;
}
__attribute__((packed))
hades_view_t;
static_assert(sizeof(hades_view_t) <= 4, "Currently send using a 4B header only field (RDMA immediate)");

typedef struct
{
    hades_view_t local_view;

    bit_vector_t curr_g_membership;
    uint8_t nodes_in_membership;

    uint8_t max_num_nodes;
    uint8_t* recved_views_flag;
    hades_view_t* remote_recved_views;
}
__attribute__((packed))
hades_ctx_t;


inline static void
hades_ctx_init(hades_ctx_t* ctx, uint8_t node_id, uint8_t max_nodes)
{
   ctx->local_view.epoch_id = 0;
   ctx->local_view.node_id = node_id;
   ctx->nodes_in_membership = 1;
   bv_init(&ctx->curr_g_membership);
   bv_bit_set(&ctx->curr_g_membership, node_id);
   bv_init(&ctx->local_view.view);
   bv_bit_set(&ctx->local_view.view, node_id);

   ctx->max_num_nodes = max_nodes;
   ctx->recved_views_flag = malloc(sizeof(uint8_t) * max_nodes);
   ctx->remote_recved_views = malloc(sizeof(hades_view_t) * max_nodes);
   for(int i = 0; i < max_nodes; ++i){
      ctx->recved_views_flag[i] = 0;
      bv_init(&ctx->remote_recved_views[i].view);
   }
}

// Guarantees Nodes in the same EPOCH id must have the same group view

// How does somebody joins?
// epoch id 0
// must see at least a majority of views with same epoch id > 0
// || majority of views with epoch id 0
#endif //HADES_H
