//
// Created by akatsarakis on 17/01/19.
//

#ifndef HADES_H
#define HADES_H

#include <bit_vector.h>
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

#define SEND_VIEW_EVERY_US 100
#define CHECK_VIEW_CHANGE_EVERY_MS 10

typedef struct
{
    uint8_t node_id;
    uint16_t epoch_id;
    bit_vector_t curr_view;
}
hades_membership
__attribute__((packed));

typedef struct
{
    uint8_t node_id;
    uint16_t epoch_id;
    uint8_t nodes_in_membership;
    bit_vector_t curr_g_membership;
}
hades_ctx
__attribute__((packed));


// Guarantees Nodes in the same EPOCH id must have the same group view

// How does somebody joins?
#endif //HADES_H
