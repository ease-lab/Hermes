//
// Created by akatsarakis on 15/03/18.
//

#ifndef HERMES_UTIL_H
#define HERMES_UTIL_H

#include <stdint.h>
#include <stdio.h>
#include "hrd.h"
#include "config.h"
#include "spacetime.h"

struct worker_stats { // 2 cache lines
    long long cache_hits_per_worker;
    long long remotes_per_worker;
    long long locals_per_worker;

    long long issued_acks_per_worker;
    long long issued_invs_per_worker;
    long long issued_vals_per_worker;
	long long issued_crds_per_worker;

    long long received_acks_per_worker;
    long long received_invs_per_worker;
    long long received_vals_per_worker;
	long long received_crds_per_worker;

	long long issued_packet_acks_per_worker;
    long long issued_packet_invs_per_worker;
    long long issued_packet_vals_per_worker;
	long long issued_packet_crds_per_worker;


	long long received_packet_acks_per_worker;
    long long received_packet_invs_per_worker;
    long long received_packet_vals_per_worker;
	long long received_packet_crds_per_worker;

    long long batches_per_worker;
    long long remote_messages_per_worker;

    long long stalled_time_per_worker;

    long long wasted_loops;
    double empty_reqs_per_trace;
    long long cold_keys_per_trace;
    double tot_empty_reqs_per_trace;


    //long long unused[3]; // padding to avoid false sharing
};

struct stats {
    double locals_per_worker[WORKERS_PER_MACHINE];
    double remotes_per_worker[WORKERS_PER_MACHINE];
    double batch_size_per_worker[WORKERS_PER_MACHINE];
    double empty_reqs_per_worker[WORKERS_PER_MACHINE];
    double cache_hits_per_worker[WORKERS_PER_MACHINE];
    double stalled_time_per_worker[WORKERS_PER_MACHINE];
    double average_coalescing_per_worker[WORKERS_PER_MACHINE];

    double acks_per_worker[WORKERS_PER_MACHINE];
    double invs_per_worker[WORKERS_PER_MACHINE];
    double updates_per_worker[WORKERS_PER_MACHINE];

    double received_updates_per_worker[WORKERS_PER_MACHINE];
    double received_acks_per_worker[WORKERS_PER_MACHINE];
    double received_invs_per_worker[WORKERS_PER_MACHINE];

    double write_ratio_per_worker[WORKERS_PER_MACHINE];
};

void dump_stats_2_file(struct stats* st);
void trace_init(struct spacetime_trace_command ** trace, uint16_t worker_lid);
void create_AHs(struct hrd_ctrl_blk *cb);
void *run_worker(void *arg);
void *print_stats(void* no_arg);
void print_latency_stats(void);
void setup_qps(int worker_lid, struct hrd_ctrl_blk *cb);
void setup_q_depths(int** recv_q_depths, int** send_q_depths);
char* code_to_str(uint8_t code);
void setup_ops(spacetime_op_t **ops,
			   spacetime_inv_t **inv_recv_ops, spacetime_ack_t **ack_recv_ops,
			   spacetime_val_t **val_recv_ops, spacetime_inv_packet_t **inv_send_ops,
			   spacetime_ack_packet_t **ack_send_ops, spacetime_val_packet_t **val_send_ops);
void setup_credits(uint8_t credits[][MACHINE_NUM],     struct hrd_ctrl_blk *cb,
				   struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
				   struct ibv_recv_wr* credit_recv_wr, struct ibv_sge* credit_recv_sgl);
void setup_WRs(struct ibv_send_wr *inv_send_wr, struct ibv_sge *inv_send_sgl,
                    struct ibv_recv_wr *inv_recv_wr, struct ibv_sge *inv_recv_sgl,
					struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                    struct ibv_recv_wr *ack_recv_wr, struct ibv_sge *ack_recv_sgl,
                    struct ibv_send_wr *val_send_wr, struct ibv_sge *val_send_sgl,
                    struct ibv_recv_wr *val_recv_wr, struct ibv_sge *val_recv_sgl,
					struct hrd_ctrl_blk *cb, uint16_t local_client_id);
void post_coh_recvs(struct hrd_ctrl_blk *cb,
					int* inv_push_ptr, ud_req_inv_t* inv_buf,
					int* ack_push_ptr, ud_req_ack_t* ack_buf,
					int* val_push_ptr, ud_req_val_t* val_buf);


extern int qps_published;
extern volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];
#endif //HERMES_UTIL_H
