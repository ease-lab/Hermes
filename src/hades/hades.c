//
// Created by akatsarakis on 12/02/19.
//

#include <getopt.h>
#include "../../include/hades/hades.h"
#include "../../include/wings/wings.h"



const uint8_t credits = 10;
const uint8_t machine_num = 3;


int
hades_skip_or_get_dst_id(uint8_t *req)
{
    /// WARNING: this is not thread safe

    ///  Get round robin remote dst_ids based on
    ///  the number of times this function is called
    static uint16_t dst_id;
    WINGS_MOD_ADD(dst_id, machine_num);
    if(dst_id == machine_id)
        WINGS_MOD_ADD(dst_id, machine_num);
	return dst_id;
//    return 0;// always broadcast hbt
}

void
hades_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	hades_view_t* last_local_view = (hades_view_t*) triggering_req;
	hades_view_t* send_hbt = (hades_view_t*) (msg_to_send - 1);

    *send_hbt = *last_local_view;
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
    yellow_printf("Send view[%lu]: {node %d, epoch_id %d} ",
                  hbeat_c->stats.send_total_msgs,
                  ctx->intermediate_local_view.node_id, ctx->intermediate_local_view.epoch_id);
    bv_print_enhanced(ctx->curr_g_membership);
    printf("\n");
}

static inline void
print_recved_hbts(ud_channel_t *hbeat_c, hades_view_t* hbt_array, uint16_t no_hbts)
{
    for(int i = 0; i < no_hbts; ++i){
        green_printf("Recved view[%lu]: {node %d, epoch_id %d} ",
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
        red_printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        red_printf("~ WARNING: I cannot reach a majority of nodes! ~\n");
        red_printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
        yellow_printf("Last membership (epoch %d): ", h_ctx->intermediate_local_view.epoch_id);
        bv_print_enhanced(h_ctx->curr_g_membership);
        yellow_printf("My current view: ");
        bv_print_enhanced(h_ctx->intermediate_local_view.view);
        red_printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
    }
}


static inline void
issue_heartbeats(hades_wings_ctx_t *hw_ctx)
{
    hades_ctx_t* h_ctx = &hw_ctx->ctx;
    if(time_elapsed_in_us(h_ctx->ts_last_send) > h_ctx->send_view_every_us) {
//        struct timespec ts_last_send_tmp; // TODO may Reset a tmp timer in case the send fails
        get_rdtsc_timespec(&h_ctx->ts_last_send);

        /// Send view
        for (int i = 0; i < h_ctx->max_num_nodes; ++i)
            wings_issue_pkts(hw_ctx->hviews_c, (uint8_t *) &h_ctx->last_local_view, 1, sizeof(hades_view_t),
                             NULL, hades_skip_or_get_dst_id, wings_NOP_modify_elem_after_send,
                             hades_copy_and_modify_elem);
//            if(!send_failed){
//                ts_last_send = ts_last_send_tmp;
//                print_send_hbt(hbeat_c, &h_ctx);
//            }
    }
}


static inline void
update_view_n_membership(hades_ctx_t *h_ctx)
{
    if(time_elapsed_in_ms(h_ctx->ts_last_view_change) > h_ctx->update_local_view_every_ms) {
        get_rdtsc_timespec(&h_ctx->ts_last_view_change); // Reset timer

        uint8_t views_aggreeing = 1; // (always agree with my local view)
        uint8_t same_w_local_membership = 0;
        uint16_t max_epoch_id = h_ctx->intermediate_local_view.epoch_id;

        //if view has changed update ctx
        if (!bv_are_equal(h_ctx->intermediate_local_view.view, h_ctx->curr_g_membership)) {
            for (int i = 0; i < h_ctx->max_num_nodes; ++i) {
                if (i == machine_id) continue;
                if (h_ctx->recved_views_flag[i] == 0) continue;

                if (bv_are_equal(h_ctx->intermediate_local_view.view, h_ctx->remote_recved_views[i].view)) {
                    views_aggreeing++;
                    if (max_epoch_id < h_ctx->remote_recved_views[i].epoch_id)
                        max_epoch_id = h_ctx->remote_recved_views[i].epoch_id;
                    if (h_ctx->remote_recved_views[i].same_w_local_membership != 0)
                        same_w_local_membership = 1;
                }
                h_ctx->recved_views_flag[i] = 0; //reset the received flag
            }

            if (views_aggreeing >= majority_of_nodes(h_ctx)) {
                h_ctx->intermediate_local_view.epoch_id = (uint8_t) (max_epoch_id +
                                                        (same_w_local_membership == 1 ? 0 : 1));
                bv_copy(&h_ctx->curr_g_membership, h_ctx->intermediate_local_view.view);

                green_printf("GROUP MEMBERSHIP CHANGE: epoch(%d)\n", h_ctx->intermediate_local_view.epoch_id);
                bv_print_enhanced(h_ctx->curr_g_membership);
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
update_view_and_issue(hades_wings_ctx_t *hw_ctx) //hades_ctx_t *h_ctx) //, ud_channel_t *hbeat_c)
{
    update_view_n_membership(&hw_ctx->ctx);

    issue_heartbeats(hw_ctx);
}


static inline uint16_t
poll_for_remote_views(hades_wings_ctx_t *hw_ctx)
{
    hades_ctx_t *h_ctx = &hw_ctx->ctx;

    // Poll for membership send
    uint16_t views_polled = wings_poll_buff_and_post_recvs(hw_ctx->hviews_c, h_ctx->max_views_to_poll,
                                                           (uint8_t *) h_ctx->poll_buff);

//    print_recved_hbts(hw_ctx->hviews_c, h_ctx->poll_buff, views_polled);

    if(views_polled > 0)
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

    wings_issue_credits(hw_ctx->hviews_crd_c, (uint8_t *) h_ctx->poll_buff, views_polled, sizeof(hades_view_t),
                        hades_crd_skip_or_get_sender_id, wings_NOP_modify_elem_after_send);

    return views_polled;
}


void*
hades_loop_only_thread(void* hades_wings_ctx)
{
    hades_wings_ctx_t *hw_ctx = hades_wings_ctx;

    while(true){
        update_view_and_issue(hw_ctx);

        poll_for_remote_views(hw_ctx);
    }
}


void*
hades_full_thread(void *node_id)
{
    uint8_t machine_id = *((uint8_t*) node_id);
    //////////////////////////////////
    /// failure detector context init
    //////////////////////////////////

    /// Wings (rdma communication) init
    ud_channel_t ud_channels[2];
    ud_channel_t* ud_c_ptrs[2];

    for(int i = 0; i < 2; ++i)
        ud_c_ptrs[i] = &ud_channels[i];

    ud_channel_t* hviews_c = ud_c_ptrs[0];
    ud_channel_t* hviews_crd_c = ud_c_ptrs[1];

    // other Vars
    uint16_t worker_lid = 0;
    uint16_t max_views_to_poll = 10;
    uint32_t send_view_every_us = 1000;
    uint32_t update_local_view_ms = 10;

    hades_wings_ctx_t w_ctx;
    hades_wings_ctx_init(&w_ctx, (uint8_t) machine_id, machine_num,
                         max_views_to_poll, send_view_every_us, update_local_view_ms,
                         hviews_c, hviews_crd_c, worker_lid);

    wings_setup_channel_qps_and_recvs(ud_c_ptrs, 2, NULL, 0);

    hades_loop_only_thread(&w_ctx);

    return NULL;
}

int
main(int argc, char *argv[])
{
    machine_id = -1;

    static struct option opts[] = {
            { .name = "machine-id",			.has_arg = 1, .val = 'm' },
            { .name = "dev-name",			.has_arg = 1, .val = 'd'},
            { 0 }
    };

    /* Parse and check arguments */
    while(1) {
        int c = getopt_long(argc, argv, "m:d:", opts, NULL);
        if(c == -1) {
            break;
        }
        switch (c) {
            case 'm':
                machine_id = atoi(optarg);
                break;
            case 'd':
                memcpy(dev_name, optarg, strlen(optarg));
                break;
            default:
                printf("Invalid argument %d\n", c);
                assert(false);
        }
    }

    hades_full_thread(&machine_id);
}

