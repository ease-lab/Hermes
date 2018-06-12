#include "util.h"

void *print_stats(void* no_arg){
    uint16_t i, print_count = 0;
    long long all_worker_cache_hits = 0;
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
        all_worker_cache_hits = 0;
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
            all_worker_cache_hits += curr_w_stats[i].cache_hits_per_worker - prev_w_stats[i].cache_hits_per_worker;
            all_stats.cache_hits_per_worker[i] = (curr_w_stats[i].cache_hits_per_worker - prev_w_stats[i].cache_hits_per_worker) / seconds;
            all_stats.remotes_per_worker[i] = (curr_w_stats[i].remotes_per_worker - prev_w_stats[i].remotes_per_worker) / seconds;
            all_stats.locals_per_worker[i] = (curr_w_stats[i].locals_per_worker - prev_w_stats[i].locals_per_worker) / seconds;
            all_stats.updates_per_worker[i] = (curr_w_stats[i].issued_vals_per_worker - prev_w_stats[i].issued_vals_per_worker) / seconds;
            all_stats.invs_per_worker[i] = (curr_w_stats[i].issued_invs_per_worker - prev_w_stats[i].issued_invs_per_worker) / seconds;
            all_stats.acks_per_worker[i] = (curr_w_stats[i].issued_acks_per_worker - prev_w_stats[i].issued_acks_per_worker) / seconds;
            all_stats.received_updates_per_worker[i] = (curr_w_stats[i].received_vals_per_worker - prev_w_stats[i].received_vals_per_worker) / seconds;
            all_stats.received_invs_per_worker[i] = (curr_w_stats[i].received_invs_per_worker - prev_w_stats[i].received_invs_per_worker) / seconds;
            all_stats.received_acks_per_worker[i] = (curr_w_stats[i].received_acks_per_worker - prev_w_stats[i].received_acks_per_worker) / seconds;
            if (curr_w_stats[i].remote_messages_per_worker - prev_w_stats[i].remote_messages_per_worker > 0) {
              all_stats.average_coalescing_per_worker[i] =  ((curr_w_stats[i].remotes_per_worker - prev_w_stats[i].remotes_per_worker)
                  /(double) (curr_w_stats[i].remote_messages_per_worker - prev_w_stats[i].remote_messages_per_worker));
            }
            if (curr_w_stats[i].batches_per_worker - prev_w_stats[i].batches_per_worker > 0) {
                all_stats.batch_size_per_worker[i] = (curr_w_stats[i].remotes_per_worker - prev_w_stats[i].remotes_per_worker) /
                    (double) (curr_w_stats[i].batches_per_worker - prev_w_stats[i].batches_per_worker);
                all_stats.stalled_time_per_worker[i] = (curr_w_stats[i].stalled_time_per_worker - prev_w_stats[i].stalled_time_per_worker) /
                    (double)(curr_w_stats[i].batches_per_worker - prev_w_stats[i].batches_per_worker);
            }
            else {
                all_stats.batch_size_per_worker[i] = 0;
                all_stats.stalled_time_per_worker[i] = 0;
            }

            uint32_t total_loops = curr_w_stats[i].batches_per_worker - prev_w_stats[i].batches_per_worker +
                                   curr_w_stats[i].wasted_loops - prev_w_stats[i].wasted_loops;
            if (total_loops > 0)
                all_stats.empty_reqs_per_worker[i] =  (curr_w_stats[i].tot_empty_reqs_per_trace - prev_w_stats[i].tot_empty_reqs_per_trace) / total_loops ;
        }


        memcpy(prev_w_stats, curr_w_stats, WORKERS_PER_MACHINE * (sizeof(struct worker_stats)));
        total_throughput = all_worker_cache_hits / seconds;
        printf("---------------PRINT %d time elapsed %.2f---------------\n", print_count, seconds / MILLION);
        green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
        for (i = 0; i < WORKERS_PER_MACHINE; i++) {
            double trace_ratio;
            long long total_reqs = curr_w_stats[i].cache_hits_per_worker + curr_w_stats[i].remotes_per_worker + curr_w_stats[i].locals_per_worker;
            if (total_reqs > 0)
                trace_ratio =  curr_w_stats[i].cache_hits_per_worker / (double)total_reqs;
            yellow_printf("W%d: %.2f MIOPS-Batch %.2f(%.2f) -H %.2f -W %llu -E %.2f -AC %.2f \n", i, all_stats.cache_hits_per_worker[i], all_stats.batch_size_per_worker[i],
                          all_stats.stalled_time_per_worker[i], trace_ratio, curr_w_stats[i].wasted_loops, all_stats.empty_reqs_per_worker[i],
                          all_stats.average_coalescing_per_worker[i]);
            printf("Total reqs: %llu\n", curr_w_stats[i].cache_hits_per_worker);
        }
        green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
        printf("---------------------------------------\n");
        ///if(ENABLE_CACHE_STATS == 1) print_cache_stats(start, machine_id);
        if(DUMP_STATS_2_FILE == 1)
            dump_stats_2_file(&all_stats);

    }
}
