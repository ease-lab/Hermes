//
// Created by akatsarakis on 12/02/19.
//

#include <getopt.h>
#include "../../include/utils/time_rdtsc.h"
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


void
hbt_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	hades_ctx_t* ctx = (hades_ctx_t*) triggering_req;
	hades_view_t* send_hbt = (hades_view_t*) (msg_to_send - 1);

    *send_hbt = ctx->local_view;
//    efficient_copy_of_membership_from_ctx(send_hbt, ctx);
//    send_hbt->node_id = (uint8_t) machine_id;
}




int
hbt_crd_skip_or_get_sender_id(uint8_t *req)
{
    hades_view_t* req_hbt = (hades_view_t *) req;
	return req_hbt->node_id; // always send crd
}

void
hbt_crd_modify_elem_after_send(uint8_t *req)
{
    // Do not change anything
}




static inline void
print_send_hbt(ud_channel_t *hbeat_c, hades_ctx_t *ctx)
{
    yellow_printf("Send view[%lu]: {node %d, epoch_id %d} ",
                  hbeat_c->stats.send_total_msgs,
                  ctx->local_view.node_id, ctx->local_view.epoch_id);
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

int
main(int argc, char *argv[])
{
    is_roce = -1; machine_id = -1;

    static struct option opts[] = {
            { .name = "machine-id",			.has_arg = 1, .val = 'm' },
            { .name = "is-roce",			.has_arg = 1, .val = 'r' },
            { .name = "dev-name",			.has_arg = 1, .val = 'd'},
            { 0 }
    };

    /* Parse and check arguments */
    while(1) {
        int c = getopt_long(argc, argv, "m:r:d:", opts, NULL);
        if(c == -1) {
            break;
        }
        switch (c) {
            case 'm':
                machine_id = atoi(optarg);
                break;
            case 'r':
                is_roce = atoi(optarg);
                break;
            case 'd':
                memcpy(dev_name, optarg, strlen(optarg));
                break;
            default:
                printf("Invalid argument %d\n", c);
                assert(false);
        }
    }

    /// Wings (rdma communication) init
    ud_channel_t ud_channels[2];
    ud_channel_t* ud_c_ptrs[2];
    ud_channel_t* hbeat_c = &ud_channels[0];
    ud_channel_t* hbeat_crd_c = &ud_channels[1];

    for(int i = 0; i < 2; ++i)
        ud_c_ptrs[i] = &ud_channels[i];

    uint16_t worker_lid = 0;

    const uint8_t is_bcast = 0;
	const uint8_t stats_on = 1;
	const uint8_t prints_on = 1;
	const uint8_t is_hdr_only = 1;
	const uint8_t expl_crd_ctrl = 1;
	const uint8_t disable_crd_ctrl = 0;

    char qp_name[200];
    sprintf(qp_name, "%s%d", "\033[1m\033[32mHBeat\033[0m", worker_lid);

    wings_ud_channel_init(hbeat_c, qp_name, REQ, 1, sizeof(hades_view_t) - sizeof(uint8_t),
                          ENABLE_HADES_INLINING, is_hdr_only, is_bcast,
                          disable_crd_ctrl, expl_crd_ctrl, hbeat_crd_c, credits,
                          machine_num, (uint8_t) machine_id, stats_on, prints_on);

    wings_setup_channel_qps_and_recvs(ud_c_ptrs, 2, NULL, 0);


    /// failure detector context init
    hades_ctx_t h_ctx;
    hades_ctx_init(&h_ctx, (uint8_t) machine_id, machine_num);

    /// timing init
    struct timespec ts_last_send;
    struct timespec ts_last_view_change;
    init_rdtsc(1, 0);
    get_rdtsc_timespec(&ts_last_send);
    get_rdtsc_timespec(&ts_last_view_change);

    hades_view_t last_local_view;
    last_local_view = h_ctx.local_view;
    hades_view_t polled_hbts[MAX_HBS_TO_POLL];

    while(true){

        if(time_elapsed_in_us(ts_last_send) > HADES_SEND_VIEW_EVERY_US) {
            struct timespec ts_last_send_tmp;
            get_rdtsc_timespec(&ts_last_send); // Reset a tmp timer in case the send fails

            /// Send view
            for (int i = 0; i < h_ctx.max_num_nodes; ++i)
//                wings_issue_pkts(hbeat_c, (uint8_t *) &h_ctx, 1, sizeof(hades_ctx_t), NULL,
                wings_issue_pkts(hbeat_c, (uint8_t *) &last_local_view, 1, sizeof(hades_view_t), NULL,
                                 hades_skip_or_get_dst_id, wings_NOP_modify_elem_after_send,
//                                 hbt_modify_elem_after_send,
                                 hades_copy_and_modify_elem);
//            if(!send_failed){
//                ts_last_send = ts_last_send_tmp;
//                print_send_hbt(hbeat_c, &h_ctx);
//            }
        }

        // Poll for membership send
        uint16_t hbs_polled = wings_poll_buff_and_post_recvs(hbeat_c, MAX_HBS_TO_POLL,
                                                             (uint8_t *) polled_hbts);

//        print_recved_hbts(hbeat_c, polled_hbts, hbs_polled);

        if(hbs_polled > 0){
            for(int i = 0; i < hbs_polled; ++i){
                uint8_t sender_id = polled_hbts[i].node_id;
                h_ctx.recved_views_flag[sender_id] = 1;
                h_ctx.remote_recved_views[sender_id] = polled_hbts[i];
                bv_bit_set(&h_ctx.local_view.view, sender_id);
            }
        }




        if(time_elapsed_in_ms(ts_last_view_change) > HADES_CHECK_VIEW_CHANGE_EVERY_MS){
            get_rdtsc_timespec(&ts_last_view_change); // Reset timer
            // TODO: if view has changed update ctx

            uint8_t views_aggreeing = 1; // (always agree with my local view)
            uint16_t max_epoch_id = h_ctx.local_view.epoch_id;
            uint8_t same_w_local_membership = 0;
            if(!bv_are_equal(h_ctx.local_view.view, h_ctx.curr_g_membership)){
                for(int i = 0; i < h_ctx.max_num_nodes; ++i){
                    if(i == machine_id) continue;
                    if(h_ctx.recved_views_flag[i] == 0) continue;

                    if(bv_are_equal(h_ctx.local_view.view, h_ctx.remote_recved_views[i].view)){
                        views_aggreeing++;
                        if(max_epoch_id < h_ctx.remote_recved_views[i].epoch_id)
                            max_epoch_id = h_ctx.remote_recved_views[i].epoch_id;
                        if(h_ctx.remote_recved_views[i].same_w_local_membership != 0)
                            same_w_local_membership = 1;
                    }
                    h_ctx.recved_views_flag[i] = 0; //reset the received flag
                }

                if(views_aggreeing >= majority_of_nodes(&h_ctx)){
                    h_ctx.local_view.epoch_id = (uint8_t) (max_epoch_id +
                                                           (same_w_local_membership == 1 ? 0 : 1));
                    bv_copy(&h_ctx.curr_g_membership, h_ctx.local_view.view);

                    green_printf("GROUP MEMBERSHIP CHANGE: epoch(%d)\n", h_ctx.local_view.epoch_id);
                    bv_print_enhanced(h_ctx.curr_g_membership);
                }
            }

            if(bv_no_setted_bits(last_local_view.view) >= majority_of_nodes(&h_ctx) &&
               bv_no_setted_bits(h_ctx.local_view.view) < majority_of_nodes(&h_ctx))
            {
                red_printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                red_printf("~ WARNING: I cannot reach a majority of nodes! ~\n");
                red_printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
                yellow_printf("Last membership (epoch %d): ", h_ctx.local_view.epoch_id);
                bv_print_enhanced(h_ctx.curr_g_membership);
                yellow_printf("My current view: ");
                bv_print_enhanced(h_ctx.local_view.view);
                red_printf("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n");
            }

            last_local_view = h_ctx.local_view;
            last_local_view.same_w_local_membership = bv_are_equal(last_local_view.view,
                                                                   h_ctx.curr_g_membership);

            // Reset local view
            bv_reset_all(&h_ctx.local_view.view);
            bv_bit_set(&h_ctx.local_view.view, (uint8_t) machine_id);
        }

        wings_issue_credits(hbeat_crd_c, (uint8_t *) polled_hbts, hbs_polled, sizeof(hades_view_t),
                            hbt_crd_skip_or_get_sender_id, wings_NOP_modify_elem_after_send);
    }
}
