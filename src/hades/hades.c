//
// Created by akatsarakis on 12/02/19.
//

#include <getopt.h>
#include <time_rdtsc.h>
#include "../../include/hades/hades.h"
#include "../../include/wings/wings_api.h"


#define DISABLE_INLINING 0
#define ENABLE_HADES_INLINING ((DISABLE_INLINING || sizeof(hades_membership_t) >= 188) ? 0 : 1)

const uint8_t credits = 10;
const uint8_t machine_num = 10;

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


    ud_channel_t ud_channels[2];
    ud_channel_t* ud_c_ptrs[2];
    ud_channel_t* hbeat_c = &ud_channels[0];
    ud_channel_t* hbeat_crd_c = &ud_channels[1];

    for(int i = 0; i < 2; ++i)
        ud_c_ptrs[i] = &ud_channels[i];

    uint16_t worker_lid = 0;
    char qp_name[200];
    sprintf(qp_name, "%s%d", "\033[1m\033[32mHBeat\033[0m", worker_lid);

    wings_ud_channel_init(hbeat_c, qp_name, REQ, 1, sizeof(hades_membership_t),
                           ENABLE_HADES_INLINING, 1, 1, hbeat_crd_c, credits,
                           machine_num, (uint8_t) machine_id, 1, 1);

    wings_setup_channel_qps_and_recvs(ud_c_ptrs, 2, NULL, 0);


    struct timespec ts_last_send;
    struct timespec ts_last_view_change;

    init_rdtsc(1, 0);
    get_rdtsc_timespec(&ts_last_send);
    get_rdtsc_timespec(&ts_last_view_change);

    while(true){

        //poll for membership send

        if(time_elapsed_in_ms(ts_last_send) > HADES_SEND_VIEW_EVERY_US){
            //send view
            get_rdtsc_timespec(&ts_last_send);
        }

        if(time_elapsed_in_ms(ts_last_view_change) > HADES_CHECK_VIEW_CHANGE_EVERY_MS){
            //check if view has changed
            get_rdtsc_timespec(&ts_last_view_change);
        }
    }
}