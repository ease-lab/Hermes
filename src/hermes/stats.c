#include "util.h"

void *print_stats(void* no_arg){
    uint16_t i, print_count = 0;
    long long all_worker_xput = 0;
    double total_throughput = 0;
    int sleep_time = 20;
    struct worker_stats curr_w_stats[WORKERS_PER_MACHINE], prev_w_stats[WORKERS_PER_MACHINE];
    struct stats all_stats;
    sleep(4);
    memcpy(prev_w_stats, (void*) w_stats, WORKERS_PER_MACHINE * (sizeof(struct worker_stats)));
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
    while(true) {
        sleep(sleep_time);
        clock_gettime(CLOCK_REALTIME, &end);
        double seconds = (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1000000001;
        start = end;
        memcpy(curr_w_stats, (void*) w_stats, WORKERS_PER_MACHINE * (sizeof(struct worker_stats)));
        all_worker_xput = 0;
        print_count++;
        if (EXIT_ON_PRINT == 1 && print_count == PRINT_NUM) {
          if (MEASURE_LATENCY && machine_id == 0) print_latency_stats();
//            for(i = 0; i < WORKERS_PER_MACHINE; i++)
//                if(w_stats[i].received_invs_per_worker != w_stats[i].issued_acks_per_worker)
//                    red_printf("\tFollower[%d]:    received invs: %d issued acks: %d\n",
//                               i, w_stats[i].received_invs_per_worker,w_stats[i].issued_acks_per_worker);
//                else
//                    green_printf("\tFollower[%d]:    received invs: %d issued acks: %d\n",
//                               i, w_stats[i].received_invs_per_worker,w_stats[i].issued_acks_per_worker);
            printf("---------------------------------------\n");
            printf("------------RUN TERMINATED-------------\n");
            printf("---------------------------------------\n");
            exit(0);
        }
        seconds *= MILLION; // compute only MIOPS
        for (i = 0; i < WORKERS_PER_MACHINE; i++) {
            all_worker_xput += curr_w_stats[i].completed_ops_per_worker - prev_w_stats[i].completed_ops_per_worker;
            all_stats.xput_per_worker[i] = (curr_w_stats[i].completed_ops_per_worker - prev_w_stats[i].completed_ops_per_worker) / seconds;
        }


        memcpy(prev_w_stats, curr_w_stats, WORKERS_PER_MACHINE * (sizeof(struct worker_stats)));
        total_throughput = all_worker_xput / seconds;
        printf("---------------PRINT %d time elapsed %.2f---------------\n", print_count, seconds / MILLION);
        green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
        for (i = 0; i < WORKERS_PER_MACHINE; i++) {
//            yellow_printf("W%d: %.2f MIOPS-Batch %.2f(%.2f) -H %.2f -W %llu -E %.2f -AC %.2f \n", i, all_stats.xput_per_worker[i], all_stats.batch_size_per_worker[i],
//                          all_stats.stalled_time_per_worker[i], trace_ratio, curr_w_stats[i].wasted_loops, all_stats.empty_reqs_per_worker[i],
//                          all_stats.average_coalescing_per_worker[i]);
            all_stats.issued_invs_avg_coalesing[i] = w_stats[i].issued_invs_per_worker / (double) w_stats[i].issued_packet_invs_per_worker;
            all_stats.issued_acks_avg_coalesing[i] = w_stats[i].issued_acks_per_worker / (double) w_stats[i].issued_packet_acks_per_worker;
            all_stats.issued_vals_avg_coalesing[i] = w_stats[i].issued_vals_per_worker / (double) w_stats[i].issued_packet_vals_per_worker;
            all_stats.issued_crds_avg_coalesing[i] = w_stats[i].issued_crds_per_worker / (double) w_stats[i].issued_packet_crds_per_worker;

            all_stats.received_invs_avg_coalesing[i] = w_stats[i].received_invs_per_worker / (double) w_stats[i].received_packet_invs_per_worker;
            all_stats.received_acks_avg_coalesing[i] = w_stats[i].received_acks_per_worker / (double) w_stats[i].received_packet_acks_per_worker;
            all_stats.received_vals_avg_coalesing[i] = w_stats[i].received_vals_per_worker / (double) w_stats[i].received_packet_vals_per_worker;
            all_stats.received_crds_avg_coalesing[i] = w_stats[i].received_crds_per_worker / (double) w_stats[i].received_packet_crds_per_worker;

            all_stats.percentage_of_wasted_loops[i] = w_stats[i].wasted_loops / (double) w_stats[i].total_loops * 100;
            all_stats.completed_reqs_per_loop[i] = curr_w_stats[i].completed_ops_per_worker / (double) w_stats[i].total_loops;
            cyan_printf("W%d: ",i);
            yellow_printf("%.2f MIOPS, Coalescing{Inv: %.2f, Ack: %.2f, Val: %.2f, Crd: %.2f}\n",
                          all_stats.xput_per_worker[i],
                          all_stats.issued_invs_avg_coalesing[i], all_stats.issued_acks_avg_coalesing[i],
                          all_stats.issued_vals_avg_coalesing[i], all_stats.issued_crds_avg_coalesing[i]);
            yellow_printf("\t wasted_loops: %.2f%, reqs per loop: %.2f, total reqs %d\n", all_stats.percentage_of_wasted_loops[i],
                          all_stats.completed_reqs_per_loop[i], curr_w_stats[i].completed_ops_per_worker);
        }
        green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
        printf("---------------------------------------\n");
        ///if(ENABLE_CACHE_STATS == 1) print_cache_stats(start, machine_id);
        if(DUMP_STATS_2_FILE == 1)
            dump_stats_2_file(&all_stats);

    }
}

void init_stats(void)
{
    int i;
    for(i = 0; i < WORKERS_PER_MACHINE; i++){
        w_stats[i].completed_ops_per_worker = 0;

        w_stats[i].issued_invs_per_worker = 0;
        w_stats[i].issued_acks_per_worker = 0;
        w_stats[i].issued_vals_per_worker = 0;
        w_stats[i].issued_crds_per_worker = 0;

        w_stats[i].issued_packet_invs_per_worker = 0;
        w_stats[i].issued_packet_acks_per_worker = 0;
        w_stats[i].issued_packet_vals_per_worker = 0;
        w_stats[i].issued_packet_crds_per_worker = 0;

        w_stats[i].inv_ss_completions_per_worker = 0;
        w_stats[i].ack_ss_completions_per_worker = 0;
        w_stats[i].val_ss_completions_per_worker = 0;
        w_stats[i].crd_ss_completions_per_worker = 0;

        w_stats[i].received_invs_per_worker = 0;
        w_stats[i].received_acks_per_worker = 0;
        w_stats[i].received_vals_per_worker = 0;
        w_stats[i].received_crds_per_worker = 0;

        w_stats[i].received_packet_invs_per_worker = 0;
        w_stats[i].received_packet_acks_per_worker = 0;
        w_stats[i].received_packet_vals_per_worker = 0;
        w_stats[i].received_packet_crds_per_worker = 0;
    }
}
