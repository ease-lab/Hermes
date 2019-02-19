#include "util.h"


void *print_stats(void* no_arg){
    uint16_t i, print_count = 0;
    long long all_worker_xput = 0;
    double total_throughput = 0;
//    int sleep_time = 20;
    struct worker_stats curr_w_stats[WORKERS_PER_MACHINE], prev_w_stats[WORKERS_PER_MACHINE];
    struct stats all_stats;
    sleep(4);
    memcpy(prev_w_stats, (void*) w_stats, WORKERS_PER_MACHINE * (sizeof(struct worker_stats)));
    struct timespec start, end;
    clock_gettime(CLOCK_REALTIME, &start);
    while(true) {
        usleep(PRINT_STATS_EVERY_MSECS * 1000);
        clock_gettime(CLOCK_REALTIME, &end);
        double seconds = (end.tv_sec - start.tv_sec) + (double) (end.tv_nsec - start.tv_nsec) / 1000000001;
        start = end;
        memcpy(curr_w_stats, (void*) w_stats, WORKERS_PER_MACHINE * (sizeof(struct worker_stats)));
        all_worker_xput = 0;
        print_count++;
        if(FAKE_FAILURE == 1 && machine_id == NODE_TO_FAIL && print_count == ROUNDS_BEFORE_FAILURE){
            red_printf("---------------------------------------\n");
            red_printf("------------  NODE FAILED  ------------\n");
            red_printf("---------------------------------------\n");
            exit(0);
        }
        if (EXIT_ON_STATS_PRINT == 1 && print_count == PRINT_NUM_STATS_BEFORE_EXITING) {
          if (MEASURE_LATENCY && machine_id == 0) dump_latency_stats();
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
        if(PRINT_WORKER_STATS) {
            for (i = 0; i < WORKERS_PER_MACHINE; i++) {
//            yellow_printf("W%d: %.2f MIOPS-Batch %.2f(%.2f) -H %.2f -W %llu -E %.2f -AC %.2f \n", i, all_stats.xput_per_worker[i], all_stats.batch_size_per_worker[i],
//                          all_stats.stalled_time_per_worker[i], trace_ratio, curr_w_stats[i].wasted_loops, all_stats.empty_reqs_per_worker[i],
//                          all_stats.average_coalescing_per_worker[i]);
                all_stats.issued_invs_avg_coalesing[i] =
                        w_stats[i].issued_invs_per_worker / (double) w_stats[i].issued_packet_invs_per_worker;
                all_stats.issued_acks_avg_coalesing[i] =
                        w_stats[i].issued_acks_per_worker / (double) w_stats[i].issued_packet_acks_per_worker;
                all_stats.issued_vals_avg_coalesing[i] =
                        w_stats[i].issued_vals_per_worker / (double) w_stats[i].issued_packet_vals_per_worker;
                all_stats.issued_crds_avg_coalesing[i] =
                        w_stats[i].issued_crds_per_worker / (double) w_stats[i].issued_packet_crds_per_worker;

                all_stats.received_invs_avg_coalesing[i] =
                        w_stats[i].received_invs_per_worker / (double) w_stats[i].received_packet_invs_per_worker;
                all_stats.received_acks_avg_coalesing[i] =
                        w_stats[i].received_acks_per_worker / (double) w_stats[i].received_packet_acks_per_worker;
                all_stats.received_vals_avg_coalesing[i] =
                        w_stats[i].received_vals_per_worker / (double) w_stats[i].received_packet_vals_per_worker;
                all_stats.received_crds_avg_coalesing[i] =
                        w_stats[i].received_crds_per_worker / (double) w_stats[i].received_packet_crds_per_worker;

                all_stats.percentage_of_wasted_loops[i] =
                        w_stats[i].wasted_loops / (double) w_stats[i].total_loops * 100;
                all_stats.completed_reqs_per_loop[i] =
                        curr_w_stats[i].completed_ops_per_worker / (double) w_stats[i].total_loops;
                cyan_printf("W%d: ", i);
                yellow_printf("%.2f MIOPS, Coalescing{Inv: %.2f, Ack: %.2f, Val: %.2f, Crd: %.2f}\n",
                              all_stats.xput_per_worker[i],
                              all_stats.issued_invs_avg_coalesing[i], all_stats.issued_acks_avg_coalesing[i],
                              all_stats.issued_vals_avg_coalesing[i], all_stats.issued_crds_avg_coalesing[i]);
                yellow_printf("\t wasted_loops: %.2f%, reqs per loop: %.2f, total reqs %d, reqs missed: %d\n",
                              all_stats.percentage_of_wasted_loops[i],
                              all_stats.completed_reqs_per_loop[i], curr_w_stats[i].completed_ops_per_worker,
                              curr_w_stats[i].reqs_missed_in_kvs);
            }
            green_printf("SYSTEM MIOPS: %.2f \n", total_throughput);
            printf("---------------------------------------\n");
        }
        if(DUMP_STATS_2_FILE == 1)
            dump_stats_2_file(&all_stats);

    }
}

void init_stats(void) //TODO this is not required (global vars are always initialized to zero)
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

//assuming microsecond latency
void dump_latency_stats(void){
    FILE *latency_stats_fd;
    int i = 0;
    char filename[128];
    char* path = "../../results/latency";

    sprintf(filename, "%s/latency_stats_v_%d_m_%d_w_%d_b_%d_c_%d_t_%d_ci_%d_ca_%d_cv_%d.csv", path,
            (int) (ST_VALUE_SIZE + 2),
            MACHINE_NUM,
            WRITE_RATIO,
            MAX_BATCH_OPS_SIZE,
            CREDITS_PER_REMOTE_WORKER,
            WORKERS_PER_MACHINE,
            INV_MAX_REQ_COALESCE,
            ACK_MAX_REQ_COALESCE,
            VAL_MAX_REQ_COALESCE);

    latency_stats_fd = fopen(filename, "w");
    fprintf(latency_stats_fd, "#---------------- Read Reqs --------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "reads: %d, %d\n",i * LATENCY_PRECISION, latency_count.read_reqs[i]);
    fprintf(latency_stats_fd, "reads: -1, %d\n", latency_count.read_reqs[LATENCY_BUCKETS]); //print outliers
    fprintf(latency_stats_fd, "reads-hl: %d\n", latency_count.max_read_latency); //print max read latency

    fprintf(latency_stats_fd, "#---------------- Write Reqs ---------------\n");
    for(i = 0; i < LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "writes: %d, %d\n",i * LATENCY_PRECISION, latency_count.write_reqs[i]);
    fprintf(latency_stats_fd, "writes: -1, %d\n", latency_count.write_reqs[LATENCY_BUCKETS]); //print outliers
    fprintf(latency_stats_fd, "writes-hl: %d\n", latency_count.max_write_latency); //print max write latency

    fclose(latency_stats_fd);

    printf("Latency stats saved at %s\n", filename);
}

