//
// Created by akatsarakis on 15/03/18.
//

#ifndef HERMES_CONFIG_H
#define HERMES_CONFIG_H
#include "hrd.h"

#define MACHINE_NUM 2
#define WORKERS_PER_MACHINE 1
#define CLIENTS_PER_MACHINE 1
#define QPS_PER_WORKER 1
#define WINDOW_SIZE 128
#define MSG_GRAN_OF_SELECTIVE_SIGNALING 8 // use one to disable selective signalling
#define WRITE_RATIO 10

//Stats
#define EXIT_ON_PRINT 0
#define PRINT_NUM 3
#define MEASURE_LATENCY 0
#define DUMP_STATS_2_FILE 0
#define ENABLE_CACHE_STATS 0
// Rarely change
#define CLIENT_NUM (MACHINE_NUM * CLIENTS_PER_MACHINE)
#define WORKER_NUM (MACHINE_NUM * WORKERS_PER_MACHINE)
#define TOTAL_CORES 40
#define WINDOW_SIZE_ (WINDOW_SIZE - 1)
#define MSG_GRAN_OF_SELECTIVE_SIGNALING_ (MSG_GRAN_OF_SELECTIVE_SIGNALING - 1)

struct thread_params {
    int id;
    int base_port_index;
    int num_server_ports;
    int num_client_ports;
    int update_percentage;
    int postlist;
};
#endif //HERMES_CONFIG_H
