//
// Created by akatsarakis on 12/02/19.
//

#include <getopt.h>
#include "../../include/utils/time_rdtsc.h"
#include "../../include/hades/hades.h"
#include "../../include/wings/wings.h"



#define MAX_HBS_TO_POLL 10
const uint8_t credits = 10;
const uint8_t machine_num = 2;


int
hbt_skip_or_get_sender_id(uint8_t *req)
{
	return 0; // always broadcast hbt
}

void
hbt_modify_elem_after_send(uint8_t* req)
{
    // Do not change anything
}


void
hbt_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	hades_ctx_t* ctx = (hades_ctx_t*) triggering_req;
	hades_membership_t* send_hbt = (hades_membership_t*) msg_to_send;

    efficient_copy_of_membership_from_ctx(send_hbt, ctx);
    send_hbt->node_id = (uint8_t) machine_id;
}




int
hbt_crd_skip_or_get_sender_id(uint8_t *req)
{
    hades_membership_t* req_hbt = (hades_membership_t *) req;
	return req_hbt->node_id; // always send crd
}

void
hbt_crd_modify_elem_after_send(uint8_t *req)
{
    // Do not change anything
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

    green_printf("Sizeof hades_membership_t: %d\n", sizeof(hades_membership_t));

    /// Wings (rdma communication) init
    ud_channel_t ud_channels[2];
    ud_channel_t* ud_c_ptrs[2];
    ud_channel_t* hbeat_c = &ud_channels[0];
    ud_channel_t* hbeat_crd_c = &ud_channels[1];

    for(int i = 0; i < 2; ++i)
        ud_c_ptrs[i] = &ud_channels[i];

    uint16_t worker_lid = 0;

    const uint8_t is_bcast = 1;
	const uint8_t stats_on = 1;
	const uint8_t prints_on = 1;
	const uint8_t is_hdr_only = 1;
	const uint8_t expl_crd_ctrl = 1;
	const uint8_t disable_crd_ctrl = 0;

    char qp_name[200];
    sprintf(qp_name, "%s%d", "\033[1m\033[32mHBeat\033[0m", worker_lid);

    wings_ud_channel_init(hbeat_c, qp_name, REQ, 1, sizeof(hades_membership_t) - sizeof(uint8_t),
                          ENABLE_HADES_INLINING, is_hdr_only, is_bcast,
                          disable_crd_ctrl, expl_crd_ctrl, hbeat_crd_c, credits,
                          machine_num, (uint8_t) machine_id, stats_on, prints_on);

    wings_setup_channel_qps_and_recvs(ud_c_ptrs, 2, NULL, 0);


    /// failure detector context init
    hades_ctx_t h_ctx;
    hades_ctx_init(&h_ctx, (uint8_t) machine_id);

    /// timing init
    struct timespec ts_last_send;
    struct timespec ts_last_view_change;
    init_rdtsc(1, 0);
    get_rdtsc_timespec(&ts_last_send);
    get_rdtsc_timespec(&ts_last_view_change);

    hades_membership_t polled_hbts[MAX_HBS_TO_POLL];

    while(true){

        // Poll for membership send
        uint16_t hbs_polled = wings_poll_buff_and_post_recvs(hbeat_c, MAX_HBS_TO_POLL,
                                                             (uint8_t *) polled_hbts);
//        if(hbs_polled > 0)
//            printf("Recved a view, %lu\n", hbeat_c->stats.recv_total_msgs);

        if(time_elapsed_in_us(ts_last_send) > HADES_SEND_VIEW_EVERY_US){
            get_rdtsc_timespec(&ts_last_send); // Reset timer
            /// Send view
            wings_issue_pkts(hbeat_c, (uint8_t *) &h_ctx, 1, sizeof(hades_ctx_t), NULL,
                             hbt_skip_or_get_sender_id, hbt_modify_elem_after_send,
                             hbt_copy_and_modify_elem);
//            printf("About to send a view, %lu\n", hbeat_c->stats.send_total_msgs);
        }

        if(time_elapsed_in_ms(ts_last_view_change) > HADES_CHECK_VIEW_CHANGE_EVERY_MS){
            get_rdtsc_timespec(&ts_last_view_change); // Reset timer
            // TODO: if view has changed update ctx
        }

        wings_issue_credits(hbeat_crd_c, (uint8_t *) polled_hbts, hbs_polled, sizeof(hades_membership_t),
                            hbt_crd_skip_or_get_sender_id, hbt_crd_modify_elem_after_send);
    }
}
