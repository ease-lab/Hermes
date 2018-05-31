//
// Created by akatsarakis on 15/03/18.
//
#define _GNU_SOURCE
#include <hrd.h>
#include <config.h>
#include <sched.h>
#include <pthread.h>
#include "util.h"

int qps_published;

int spawn_stats_thread() {
    pthread_t *thread_arr = malloc(sizeof(pthread_t));
    pthread_attr_t attr;
    cpu_set_t cpus_stats;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpus_stats);
    if(WORKERS_PER_MACHINE + CLIENTS_PER_MACHINE > 17)
        CPU_SET(39, &cpus_stats);
    else
        CPU_SET(2 *(WORKERS_PER_MACHINE + CLIENTS_PER_MACHINE) + 2, &cpus_stats);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_stats);
    return pthread_create(&thread_arr[0], &attr, print_stats, NULL);
}

// Return once remote worker QPs are published
int remote_qps_are_published(void) {
    int i, j;

    for(i = 0; i < MACHINE_NUM; i ++){
       if(i == machine_id) continue; // skip local machine
        for(j = 0; j < WORKERS_PER_MACHINE; j++){
            char name[HRD_QP_NAME_SIZE];
            sprintf(name, "worker-qp-%d-%d-%d", i, j, machine_id);

            /* Get the queue pair for the ith machine */
            // printf("CLIENT %d is Looking for client %s\n", clt_gid, clt_name );
            while(hrd_get_published_qp(name) == NULL)
                usleep(200000);
        }
    }
    return 1;
}

struct hrd_qp_attr** setup_and_return_qps(int worker_lid, struct hrd_ctrl_blk *cb){
    int i, remote_machine_id;
    //Publish worker QPs
    struct hrd_qp_attr** remote_qps = malloc((WORKER_NUM - WORKERS_PER_MACHINE) * sizeof(struct hrd_qp_attr*));
    for (remote_machine_id = 0; remote_machine_id < MACHINE_NUM; remote_machine_id++) {
        if(machine_id == remote_machine_id) continue; //do not create qps for local machine
        char worker_conn_qp_name[HRD_QP_NAME_SIZE];
        sprintf(worker_conn_qp_name, "worker-qp-%d-%d-%d", machine_id, worker_lid, remote_machine_id);
        int qp_id = machine_id < remote_machine_id ? remote_machine_id - 1 : remote_machine_id;
        hrd_publish_conn_qp(cb, qp_id, worker_conn_qp_name);
        printf("Worker %d published conn qp %s \n", worker_lid, worker_conn_qp_name);
    }

    if (worker_lid == 0) {
        assert(qps_published == 0);
        if (spawn_stats_thread() != 0)
            red_printf("Stats thread was not successfully spawned \n");
        qps_published = remote_qps_are_published();
    }else
        while (qps_published == 0) usleep(200000);

    assert(qps_published == 1);

    // Connect to remote qps
    for(i = 0; i < MACHINE_NUM; i++){
        if(i == machine_id) continue;
        char name[HRD_QP_NAME_SIZE];
        sprintf(name, "worker-qp-%d-%d-%d", i, worker_lid, machine_id);
        int qp_id = machine_id < i ? i - 1 : i ;
        remote_qps[qp_id] = hrd_get_published_qp(name);
        hrd_connect_qp(cb, qp_id, remote_qps[qp_id]);
    }
    //printf("Worker %d is connected to all the needed qps\n", worker_gid);
    return remote_qps;
}