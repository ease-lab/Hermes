//
// Created by akatsarakis on 15/03/18.
//

#ifndef HERMES_UTIL_H
#define HERMES_UTIL_H

#include <stdint.h>
#include <stdio.h>
#include <time.h>
#include "config.h"
#include "hrd.h"
#include "spacetime.h"

struct worker_stats {
  long long completed_ops_per_worker;
  long long completed_wrs_per_worker;
  long long completed_rmws_per_worker;
  long long aborted_rmws_per_worker;
  long long reqs_missed_in_kvs;

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

  long long received_acks_stalled;  // for faking tail-latency

  long long stalled_time_per_worker;

  long long wasted_loops;
  long long total_loops;
  double empty_reqs_per_trace;
  long long cold_keys_per_trace;
  double tot_empty_reqs_per_trace;
};

struct stats {
  double xput_per_worker[MAX_WORKERS_PER_MACHINE];
  double rmw_xput_per_worker[MAX_WORKERS_PER_MACHINE];
  double rmw_abort_rate_per_worker[MAX_WORKERS_PER_MACHINE];

  double issued_invs_avg_coalesing[MAX_WORKERS_PER_MACHINE];
  double issued_acks_avg_coalesing[MAX_WORKERS_PER_MACHINE];
  double issued_vals_avg_coalesing[MAX_WORKERS_PER_MACHINE];
  double issued_crds_avg_coalesing[MAX_WORKERS_PER_MACHINE];

  double received_invs_avg_coalesing[MAX_WORKERS_PER_MACHINE];
  double received_acks_avg_coalesing[MAX_WORKERS_PER_MACHINE];
  double received_vals_avg_coalesing[MAX_WORKERS_PER_MACHINE];
  double received_crds_avg_coalesing[MAX_WORKERS_PER_MACHINE];

  double percentage_of_wasted_loops[MAX_WORKERS_PER_MACHINE];
  double completed_reqs_per_loop[MAX_WORKERS_PER_MACHINE];

  //	long long issued_packet_acks_per_worker;
  double batch_size_per_worker[MAX_WORKERS_PER_MACHINE];
  double empty_reqs_per_worker[MAX_WORKERS_PER_MACHINE];
  double stalled_time_per_worker[MAX_WORKERS_PER_MACHINE];
  double average_coalescing_per_worker[MAX_WORKERS_PER_MACHINE];

  double acks_per_worker[MAX_WORKERS_PER_MACHINE];
  double invs_per_worker[MAX_WORKERS_PER_MACHINE];
  double updates_per_worker[MAX_WORKERS_PER_MACHINE];

  double write_ratio_per_worker[MAX_WORKERS_PER_MACHINE];
};

// init all stats to 0
static inline void
init_stats(struct worker_stats* w_stats)
{
  memset(w_stats, 0, sizeof(struct worker_stats) * MAX_WORKERS_PER_MACHINE);
}

void trace_init(struct spacetime_trace_command** trace, uint16_t worker_lid);
void* run_worker(void* arg);
void* print_stats_thread(void* no_arg);
void dump_latency_stats(void);

// Maybe inline these
uint8_t is_state_code(uint8_t code);
uint8_t is_input_code(uint8_t code);
uint8_t is_response_code(uint8_t code);
uint8_t is_bucket_state_code(uint8_t code);

int spawn_stats_thread(void);
char* code_to_str(uint8_t code);

void setup_kvs_buffs(spacetime_op_t** ops, spacetime_inv_t** inv_recv_ops,
                     spacetime_ack_t** ack_recv_ops,
                     spacetime_val_t** val_recv_ops);

extern dbit_vector_t* g_share_qs_barrier;
extern volatile struct worker_stats w_stats[MAX_WORKERS_PER_MACHINE];
#endif  // HERMES_UTIL_H
