//
// Created by akatsarakis on 15/03/18.
//

#ifndef HERMES_UTIL_H
#define HERMES_UTIL_H

#include <stdint.h>
#include <stdio.h>
#include "hrd.h"
#include "config.h"

struct client_stats { // 2 cache lines
    long long cache_hits_per_client;
    long long remotes_per_client;
    long long locals_per_client;

    long long updates_per_client;
    long long acks_per_client;  //only SC
    long long invs_per_client; //only SC

    long long received_updates_per_client;
    long long received_acks_per_client; //only SC
    long long received_invs_per_client; //only SC

    long long remote_messages_per_client;
    long long cold_keys_per_trace;
    long long batches_per_client;

    long long stalled_time_per_client;

    double empty_reqs_per_trace;
    long long wasted_loops;
    double tot_empty_reqs_per_trace;


    //long long unused[3]; // padding to avoid false sharing
};


struct worker_stats { // 1 cache line
    long long remotes_per_worker;
    long long locals_per_worker;
    long long batches_per_worker;
    long long empty_polls_per_worker;

    long long unused[4]; // padding to avoid false sharing
};

struct stats {

    double remotes_per_worker[WORKERS_PER_MACHINE];
    double locals_per_worker[WORKERS_PER_MACHINE];
    double batch_size_per_worker[WORKERS_PER_MACHINE];
    double aver_reqs_polled_per_worker[WORKERS_PER_MACHINE];


    double batch_size_per_client[CLIENTS_PER_MACHINE];
    double stalled_time_per_client[CLIENTS_PER_MACHINE];
    double empty_reqs_per_client[CLIENTS_PER_MACHINE];
    double cache_hits_per_client[CLIENTS_PER_MACHINE];
    double remotes_per_client[CLIENTS_PER_MACHINE];
    double locals_per_client[CLIENTS_PER_MACHINE];
    double average_coalescing_per_client[CLIENTS_PER_MACHINE];

    double updates_per_client[CLIENTS_PER_MACHINE];
    double acks_per_client[CLIENTS_PER_MACHINE];
    double invs_per_client[CLIENTS_PER_MACHINE];

    double received_updates_per_client[CLIENTS_PER_MACHINE];
    double received_acks_per_client[CLIENTS_PER_MACHINE];
    double received_invs_per_client[CLIENTS_PER_MACHINE];

    double write_ratio_per_client[CLIENTS_PER_MACHINE];
};
void dump_stats_2_file(struct stats* st);

void *run_worker(void *arg);
void *print_stats(void* no_arg);
void print_latency_stats(void);
struct hrd_qp_attr** setup_and_return_qps(int worker_lid, struct hrd_ctrl_blk *cb);

extern int qps_published;
extern volatile struct client_stats c_stats[CLIENTS_PER_MACHINE];
extern volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];
#endif //HERMES_UTIL_H
