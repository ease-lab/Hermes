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

#define HADES_SEND_VIEW_EVERY_US 10000
#define HADES_CHECK_VIEW_CHANGE_EVERY_MS 10

#define DISABLE_INLINING 0
#define ENABLE_HADES_INLINING ((DISABLE_INLINING || sizeof(hades_membership_t) >= 188) ? 0 : 1)

typedef struct
{
    uint8_t node_id;
    uint16_t epoch_id;
    bit_vector_t curr_view;
}
__attribute__((packed))
hades_membership_t;

typedef struct
{
    uint8_t node_id;
    uint16_t epoch_id;
    bit_vector_t curr_g_membership;
    uint8_t nodes_in_membership;
}
__attribute__((packed))
hades_ctx_t;

inline static void
efficient_copy_of_membership_from_ctx(hades_membership_t* membership, hades_ctx_t* ctx)
{
   ///Warrning: assumes hades_membership_t & hades_ctx_t are aligned, while node_id is not copied.
   memcpy(membership, ctx, sizeof(hades_membership_t) - sizeof(uint8_t));
}

inline static void
hades_ctx_init(hades_ctx_t* ctx, uint8_t node_id)
{
   ctx->epoch_id = 0;
   ctx->node_id = node_id;
   ctx->nodes_in_membership = 1;
   bv_init(&ctx->curr_g_membership);
   bv_bit_set(&ctx->curr_g_membership, node_id);
}

// Guarantees Nodes in the same EPOCH id must have the same group view

// How does somebody joins?
#endif //HADES_H
