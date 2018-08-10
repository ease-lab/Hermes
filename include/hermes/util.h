//
// Created by akatsarakis on 15/03/18.
//

#ifndef HERMES_UTIL_H
#define HERMES_UTIL_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include "hrd.h"
#include "config.h"
#include "spacetime.h"
//#include "time_rdtsc.h"

struct worker_stats { // 2 cache lines
    long long completed_ops_per_worker;
    long long reqs_missed_in_cache;

	long long issued_invs_per_worker;
    long long issued_acks_per_worker;
    long long issued_vals_per_worker;
	long long issued_crds_per_worker;

	long long issued_packet_invs_per_worker;
	long long issued_packet_acks_per_worker;
    long long issued_packet_vals_per_worker;
	long long issued_packet_crds_per_worker;

    long long inv_ss_completions_per_worker;
	long long ack_ss_completions_per_worker;
	long long val_ss_completions_per_worker;
	long long crd_ss_completions_per_worker;

	long long received_invs_per_worker;
    long long received_acks_per_worker;
    long long received_vals_per_worker;
	long long received_crds_per_worker;

	long long received_packet_invs_per_worker;
	long long received_packet_acks_per_worker;
    long long received_packet_vals_per_worker;
	long long received_packet_crds_per_worker;

	long long received_acks_stalled; //for faking tail-latency

    long long stalled_time_per_worker;

    long long wasted_loops;
	long long total_loops;
    double empty_reqs_per_trace;
    long long cold_keys_per_trace;
    double tot_empty_reqs_per_trace;
};

struct stats {
	double xput_per_worker[WORKERS_PER_MACHINE];

	double issued_invs_avg_coalesing[WORKERS_PER_MACHINE];
	double issued_acks_avg_coalesing[WORKERS_PER_MACHINE];
	double issued_vals_avg_coalesing[WORKERS_PER_MACHINE];
	double issued_crds_avg_coalesing[WORKERS_PER_MACHINE];

	double received_invs_avg_coalesing[WORKERS_PER_MACHINE];
	double received_acks_avg_coalesing[WORKERS_PER_MACHINE];
	double received_vals_avg_coalesing[WORKERS_PER_MACHINE];
	double received_crds_avg_coalesing[WORKERS_PER_MACHINE];

	double percentage_of_wasted_loops[WORKERS_PER_MACHINE];
	double completed_reqs_per_loop[WORKERS_PER_MACHINE];
//	long long issued_packet_acks_per_worker;
    double batch_size_per_worker[WORKERS_PER_MACHINE];
    double empty_reqs_per_worker[WORKERS_PER_MACHINE];
    double stalled_time_per_worker[WORKERS_PER_MACHINE];
    double average_coalescing_per_worker[WORKERS_PER_MACHINE];

    double acks_per_worker[WORKERS_PER_MACHINE];
    double invs_per_worker[WORKERS_PER_MACHINE];
    double updates_per_worker[WORKERS_PER_MACHINE];

    double write_ratio_per_worker[WORKERS_PER_MACHINE];
};

void dump_stats_2_file(struct stats* st);
void trace_init(struct spacetime_trace_command ** trace, uint16_t worker_lid);
void create_AHs(struct hrd_ctrl_blk *cb);
void *run_worker(void *arg);
void *print_stats(void* no_arg);
void dump_latency_stats(void);
void setup_qps(int worker_lid, struct hrd_ctrl_blk *cb);
void setup_q_depths(int** recv_q_depths, int** send_q_depths);
char* code_to_str(uint8_t code);
void init_stats(void);
void setup_ops(spacetime_op_t **ops,
			   spacetime_inv_t **inv_recv_ops, spacetime_ack_t **ack_recv_ops,
			   spacetime_val_t **val_recv_ops, spacetime_inv_packet_t **inv_send_ops,
			   spacetime_ack_packet_t **ack_send_ops, spacetime_val_packet_t **val_send_ops);

void setup_credits(uint8_t credits[][MACHINE_NUM],     struct hrd_ctrl_blk *cb,
				   struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
				   struct ibv_recv_wr* credit_recv_wr, struct ibv_sge* credit_recv_sgl);

void setup_send_WRs(struct ibv_send_wr *inv_send_wr, struct ibv_sge *inv_send_sgl,
					struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                    struct ibv_send_wr *val_send_wr, struct ibv_sge *val_send_sgl,
                    struct ibv_mr *inv_mr, struct ibv_mr *ack_mr,
                    struct ibv_mr *val_mr, uint16_t local_worker_id);

void setup_recv_WRs(struct ibv_recv_wr *inv_recv_wr, struct ibv_sge *inv_recv_sgl,
                    struct ibv_recv_wr *ack_recv_wr, struct ibv_sge *ack_recv_sgl,
                    struct ibv_recv_wr *val_recv_wr, struct ibv_sge *val_recv_sgl,
					struct hrd_ctrl_blk *cb);

void setup_incoming_buffs_and_post_initial_recvs(ud_req_inv_t *incoming_invs, ud_req_ack_t *incoming_acks,
												 ud_req_val_t *incoming_vals,
												 int *inv_push_recv_ptr, int *ack_push_recv_ptr, int *val_push_recv_ptr,
												 struct ibv_recv_wr *inv_recv_wr, struct ibv_recv_wr *ack_recv_wr,
												 struct ibv_recv_wr *val_recv_wr,
												 struct ibv_recv_wr *crd_recv_wr, struct hrd_ctrl_blk *cb,
												 uint16_t worker_lid);


//static inline double time_elapsed_in_ms(long long start)
//{
//    return (hrd_get_cycles() - start) * 2.1 / 1000000;
//}
//
//static inline int time_has_elapsed(long long start, int time_in_ms)
//{
//	int upp = (int) (2.1 * 1000000 * time_in_ms);
//    return hrd_get_cycles() - start > upp;
//}

//static inline double time_elapsed_in_ms(struct timespec start)
//{
//	struct timespec end, *diff;
//	GetRdtscTime(&end);
//	diff = TimeSpecDiff(&end,&start);
//	return  diff->tv_nsec * 1000000;
//}
//
//static inline int time_has_elapsed(struct timespec start, int time_in_ms)
//{
//    return  time_elapsed_in_ms(start) > time_in_ms;
//}
extern int qps_published;
extern volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];
#endif //HERMES_UTIL_H
