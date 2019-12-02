//
// Created by akatsarakis on 12/02/19.
//

#include <getopt.h>
#include "../../include/hades/hades.h"



typedef struct
{
    hades_view_t* ctx_last_local_view;
    uint8_t dst_id;
}
hades_view_wrapper_w_dst_id_t;

int
hades_skip_or_get_dst_id(uint8_t *req)
{
	return ((hades_view_wrapper_w_dst_id_t*)req)->dst_id;
}

void
hades_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	hades_view_wrapper_w_dst_id_t* last_local_view =
            (hades_view_wrapper_w_dst_id_t*) triggering_req;
	hades_view_t* send_hbt = (hades_view_t*) (msg_to_send - 1);

    *send_hbt = *last_local_view->ctx_last_local_view;
}

int
hades_crd_skip_or_get_sender_id(uint8_t *req)
{
    hades_view_t* req_hbt = (hades_view_t *) req;
	return req_hbt->node_id; // always send crd
}



static inline void
print_send_hbt(ud_channel_t *hbeat_c, hades_ctx_t *ctx)
{
    colored_printf(YELLOW, "Send view[%lu]: {node %d, epoch_id %d} ", hbeat_c->stats.send_total_msgs,
                  ctx->intermediate_local_view.node_id, ctx->intermediate_local_view.epoch_id);
    bv_print_enhanced(ctx->curr_g_membership);
    printf("\n");
}

static inline void
print_recved_hbts(ud_channel_t *hbeat_c, hades_view_t* hbt_array, uint16_t no_hbts)
{
    for(int i = 0; i < no_hbts; ++i){
        colored_printf(GREEN, "Recved view[%lu]: {node %d, epoch_id %d} ",
                     hbeat_c->stats.recv_total_msgs,
                     hbt_array[i].node_id, hbt_array[i].epoch_id);
        bv_print_enhanced(hbt_array[i].view);
        printf("\n");
    }
}

static inline uint8_t
majority_of_nodes(hades_ctx_t* ctx)
{
    assert(ctx->max_num_nodes > 1);
    return (uint8_t) (ctx->max_num_nodes == 2 ? 2 : (ctx->max_num_nodes / 2) + 1);
}


static inline void
check_if_majority_is_rechable(hades_ctx_t *h_ctx)
{
    if(bv_no_setted_bits(h_ctx->last_local_view.view) >= majority_of_nodes(h_ctx) &&
       bv_no_setted_bits(h_ctx->intermediate_local_view.view) < majority_of_nodes(h_ctx))
    {
        colored_printf(RED, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        colored_printf(RED, "~ [HADES WARNING]: I cannot reach a majority ! ~\n");
        colored_printf(RED, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        colored_printf(YELLOW, "Last membership (epoch %d): ", h_ctx->intermediate_local_view.epoch_id);
        bv_print_enhanced(h_ctx->curr_g_membership);
        colored_printf(YELLOW, "My current view: ");
        bv_print_enhanced(h_ctx->intermediate_local_view.view);
        colored_printf(RED, "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    }
}



static inline uint8_t
skip_to_apply_fake_link_failure(uint8_t node_id)
{
    static uint8_t ts_is_inited = 0;
    static uint8_t link_has_failed = 0;
    static struct timespec ts_fake_link_failure;

    if((machine_id == FAKE_LINK_FAILURE_NODE_A && node_id == FAKE_LINK_FAILURE_NODE_B) ||
       (!FAKE_ONE_WAY_LINK_FAILURE &&
        node_id == FAKE_LINK_FAILURE_NODE_A && machine_id == FAKE_LINK_FAILURE_NODE_B))
    {
        if(ts_is_inited == 0){
            get_rdtsc_timespec(&ts_fake_link_failure);
            ts_is_inited = 1;
        }

        if(time_elapsed_in_sec(ts_fake_link_failure) > FAKE_LINK_FAILURE_AFTER_SEC &&
           time_elapsed_in_sec(ts_fake_link_failure) < STOP_FAKE_LINK_FAILURE_AFTER_SEC)
        {
            if(link_has_failed == 0){
                colored_printf(RED, "%sLink failure between node %d and %d\n",
                           FAKE_ONE_WAY_LINK_FAILURE ? "One-way " : "",
                           FAKE_LINK_FAILURE_NODE_A, FAKE_LINK_FAILURE_NODE_B);
                link_has_failed = 1;
            }
            return 1;
        }
    }
    return 0;
}


static inline uint8_t
is_in_membership(hades_ctx_t *h_ctx, uint8_t node_id)
{
    return bv_bit_get(h_ctx->curr_g_membership, node_id);
}

//Skip iterations for arbitration:
static inline uint8_t
skip_arbitration(hades_ctx_t *h_ctx, uint8_t i)
{
    if(i == machine_id) return 1;               // 1. my local machine id
    if(!h_ctx->recved_views_flag[i]) return 1;  // 2. machine ids that I have not received a view
//    if(!is_in_membership(h_ctx, i)) return 1;   // 3. machine ids that are not currently in the group membership
    if(h_ctx->remote_recved_views[i].have_ostracised_for_dst_node == 1) return 1; // 3. this node has not already ostracise someone for me
    if(!bv_bit_get(h_ctx->remote_recved_views[i].view, // 4. If my node id does not exist in their view
            machine_id))
        return 1;
    return 0;
}

// In case of a link failure (either both or one way) between nodes A and B. Rest of
// nodes would be able to detect such a failure using its received views and resolve
// this deterministically by choosing the one with the highest node id to be expelled
// from the group membership.
// Once a node is voted to be expelled by the majority of nodes it gets removed from the
// membership, this method is inspired by the "ostracism" procedure under the Athenian
// democracy in which any citizen could be expelled from the city of Athens for ten years.

// If a node has ostracised somebody for me I cannot ostracised somebody for him
static inline void
view_arbitration_via_ostracism(hades_ctx_t *h_ctx)
{
    for(uint8_t i = 0; i < h_ctx->max_num_nodes; ++i)
        h_ctx->have_ostracized_for[i] = 0;

    for(uint8_t i = 0; i < h_ctx->max_num_nodes; ++i){
        if(skip_arbitration(h_ctx, i)) continue;

        for(uint8_t j = 0; j < h_ctx->max_num_nodes; ++j){
            if(i >= j) continue; // for efficiency we do not need to check those
            if(skip_arbitration(h_ctx, j)) continue;

            uint8_t i_view_of_j = bv_bit_get(h_ctx->remote_recved_views[i].view, j);
            uint8_t j_view_of_i = bv_bit_get(h_ctx->remote_recved_views[j].view, i);

            if(i_view_of_j == 0 || j_view_of_i == 0){
                // by default always ostracise this to the Max(i, j) --> j is always > i
                // unless it's an one way failure from the opposite side where we have to ostracise i
                uint8_t node_to_ostracise = i_view_of_j == 1 ? i : j;
                uint8_t node_to_ostracised_for = i_view_of_j == 1 ? j : i;

                h_ctx->recved_views_flag[node_to_ostracise] = 0;
                h_ctx->have_ostracized_for[node_to_ostracised_for] = 1;
                bv_bit_reset(&h_ctx->intermediate_local_view.view, node_to_ostracise);

//                yellow_printf("Ostracism: between nodes %d-%d --> %d is ostracized\n", i, j, node_to_ostracise);
//                printf("My view: (epoch %d)\n", h_ctx->intermediate_local_view.epoch_id);
//                bv_print_enhanced(h_ctx->intermediate_local_view.view);
            }

        }
    }
}

static inline uint8_t
get_max_received_epoch_id(hades_ctx_t *h_ctx)
{
    uint8_t max_epoch_id = 0;
   for(int i = 0; i < h_ctx->max_num_nodes; ++i)
       if(h_ctx->recved_views_flag[i] == 1 &&
               h_ctx->remote_recved_views[i].epoch_id > max_epoch_id)
           max_epoch_id = h_ctx->remote_recved_views[i].epoch_id;
    return max_epoch_id;
}

static inline void
update_view_n_membership(hades_ctx_t *h_ctx)
{
    if(time_elapsed_in_ms(h_ctx->ts_last_view_change) > h_ctx->update_local_view_every_ms) {
        get_rdtsc_timespec(&h_ctx->ts_last_view_change); // Reset timer

        uint8_t views_aggreeing = 1; // (always agree with my local view)
        uint8_t same_w_local_membership = 0;
        uint16_t max_epoch_id = h_ctx->intermediate_local_view.epoch_id;

        if(ENABLE_ARBITRATION)
            view_arbitration_via_ostracism(h_ctx);

        //if view has changed update ctx
        if (!bv_are_equal(h_ctx->intermediate_local_view.view, h_ctx->curr_g_membership) ||
            get_max_received_epoch_id(h_ctx) > h_ctx->intermediate_local_view.epoch_id)
        {
            for (int i = 0; i < h_ctx->max_num_nodes; ++i) {
                if (i == machine_id) continue;
                if (h_ctx->recved_views_flag[i] == 0) continue;

                if (bv_are_equal(h_ctx->intermediate_local_view.view, h_ctx->remote_recved_views[i].view)) {
                    views_aggreeing++;
                    if (max_epoch_id < h_ctx->remote_recved_views[i].epoch_id){
                        max_epoch_id = h_ctx->remote_recved_views[i].epoch_id;
                        same_w_local_membership = h_ctx->remote_recved_views[i].same_w_local_membership;
                    }
                }
                h_ctx->recved_views_flag[i] = 0; //reset the received flag
            }

            if (views_aggreeing >= majority_of_nodes(h_ctx)) {
                h_ctx->intermediate_local_view.epoch_id = (uint8_t) (max_epoch_id +
                                                        (same_w_local_membership == 1 ? 0 : 1));
                bv_copy(&h_ctx->curr_g_membership, h_ctx->intermediate_local_view.view);

//                printf("Max epoch id: %d, same_w_local_membership: %d\n",
//                        max_epoch_id, same_w_local_membership);
                colored_printf(YELLOW, "[HADES] MEMBERSHIP CHANGE --> [epoch %d], ", h_ctx->intermediate_local_view.epoch_id);
                bv_print(h_ctx->curr_g_membership);
                printf("\n");
//                bv_print_enhanced(h_ctx->curr_g_membership);
            }
        }

        check_if_majority_is_rechable(h_ctx);

        // update last local view
        h_ctx->last_local_view = h_ctx->intermediate_local_view;
        h_ctx->last_local_view.same_w_local_membership = bv_are_equal(h_ctx->last_local_view.view,
                                                                      h_ctx->curr_g_membership);

        // Reset local view
        bv_reset_all(&h_ctx->intermediate_local_view.view);
        bv_bit_set(&h_ctx->intermediate_local_view.view, (uint8_t) machine_id);
    }
}


static inline void
issue_heartbeats(hades_wings_ctx_t *hw_ctx)
{
    hades_ctx_t* h_ctx = &hw_ctx->ctx;
    hades_view_wrapper_w_dst_id_t last_local_view;

    last_local_view.ctx_last_local_view = &h_ctx->last_local_view;

    for (uint8_t i = 0; i < h_ctx->max_num_nodes; ++i){
        h_ctx->last_local_view.have_ostracised_for_dst_node = h_ctx->have_ostracized_for[i];
        if(i == machine_id) continue;
        if(FAKE_LINK_FAILURE && skip_to_apply_fake_link_failure(i)) continue;

        last_local_view.dst_id = i;
        if(time_elapsed_in_us(h_ctx->ts_last_send[i]) > h_ctx->send_view_every_us) {
            // Reset a tmp timer in case the send fails due to not enough crds
            struct timespec ts_last_send_tmp;
            get_rdtsc_timespec(&ts_last_send_tmp);
            uint8_t send_failed = wings_issue_pkts(hw_ctx->hviews_c, NULL, (uint8_t *) &last_local_view, 1,
                                                   sizeof(hades_view_wrapper_w_dst_id_t), NULL,
                                                   hades_skip_or_get_dst_id, wings_NOP_modify_elem_after_send,
                                                   hades_copy_and_modify_elem);
            if(!send_failed)
                h_ctx->ts_last_send[i] = ts_last_send_tmp;
//                print_send_hbt(hw_ctx->hviews_c, h_ctx);
        }
    }
}

//static inline
void
update_view_and_issue_hbs(hades_wings_ctx_t *hw_ctx)
{
    update_view_n_membership(&hw_ctx->ctx);

    issue_heartbeats(hw_ctx);
}


//static inline
uint16_t
poll_for_remote_views(hades_wings_ctx_t *hw_ctx)
{
    hades_ctx_t *h_ctx = &hw_ctx->ctx;

    // Poll for membership send
    uint16_t views_polled = wings_poll_buff_and_post_recvs(hw_ctx->hviews_c, h_ctx->max_views_to_poll,
                                                           (uint8_t *) h_ctx->poll_buff);

//    print_recved_hbts(hw_ctx->hviews_c, h_ctx->poll_buff, views_polled);

    for(int i = 0; i < views_polled; ++i){
        uint8_t sender_id = h_ctx->poll_buff[i].node_id;
        h_ctx->recved_views_flag[sender_id] = 1;
        h_ctx->remote_recved_views[sender_id] = h_ctx->poll_buff[i];
        bv_bit_set(&h_ctx->intermediate_local_view.view, sender_id);

        // In case somebody tries to rejoin
        if(h_ctx->last_local_view.epoch_id > 1)
            if(h_ctx->poll_buff[i].epoch_id == 0 &&
               hw_ctx->hviews_c->credits_per_channels[sender_id] == 0)
            {
                ///Need to reset its credits and reconfigure the qps to start sending views again
                ///Warning: currently we share qp info via memcache so if node storing memcache (e.g. houston)
                ///         fails we cannot make him re-join (prev qp info are lost)
                printf("Resetting credits and reconfiguring ibv_qps for channel: %d\n", sender_id);
                wings_reset_credits(hw_ctx->hviews_c, sender_id);
                wings_reconfigure_wrs_ah(hw_ctx->hviews_c, sender_id);
            }
    }

    wings_issue_credits(hw_ctx->hviews_crd_c, NULL, (uint8_t *) h_ctx->poll_buff, views_polled, sizeof(hades_view_t),
                        hades_crd_skip_or_get_sender_id, wings_NOP_modify_elem_after_send);

    return views_polled;
}


void*
hades_loop_only_thread(void* hades_wings_ctx)
{
    hades_wings_ctx_t *hw_ctx = hades_wings_ctx;

    uint64_t no_iters = 0;
    while(true){
        /// Print every X iteration (Mainly for dbging)
        no_iters++;
        if(no_iters % M_32 == 0){
//            printf("My view: (epoch %d)\n", hw_ctx->ctx.intermediate_local_view.epoch_id);
//            bv_print_enhanced(hw_ctx->ctx.intermediate_local_view.view);
        }

        /// Main loop
        update_view_and_issue_hbs(hw_ctx);


        poll_for_remote_views(hw_ctx);
    }
}


void*
hades_full_thread(void *node_id)
{
    //////////////////////////////////
    /// failure detector context init
    //////////////////////////////////

    /// Wings (rdma communication) init
    ud_channel_t* ud_c_ptrs[2];
    ud_channel_t ud_channels[2];

    for(int i = 0; i < 2; ++i)
        ud_c_ptrs[i] = &ud_channels[i];

    ud_channel_t* hviews_c = ud_c_ptrs[0];
    ud_channel_t* hviews_crd_c = ud_c_ptrs[1];

    // other Vars
    uint8_t machine_num = 3;
    uint16_t worker_lid = 0;
    uint16_t max_views_to_poll = 10;
    uint32_t send_view_every_us = 100;
    uint32_t update_local_view_ms = 10;

    uint8_t _node_id = *((uint8_t*) node_id);

    hades_wings_ctx_t w_ctx;
    hades_wings_ctx_init(&w_ctx, _node_id, machine_num,
                         max_views_to_poll, send_view_every_us, update_local_view_ms,
                         hviews_c, hviews_crd_c, worker_lid);

    wings_setup_channel_qps_and_recvs(ud_c_ptrs, 2, NULL, 0);

    hades_loop_only_thread(&w_ctx);

    return NULL;
}
