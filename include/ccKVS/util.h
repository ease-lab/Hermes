#ifndef ARMONIA_UTILS_H
#define ARMONIA_UTILS_H

#include "cache.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
//<vasilis> Multicast
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <byteswap.h>
#include <netinet/in.h>
#include <netdb.h>
// <vasilis>

#define DGRAM_BUF_SIZE 4096

struct local_req {
    struct mica_op req;
    uint8_t wrkr_id;
};

extern uint64_t seed;

/* ---------------------------------------------------------------------------
------------------------------STATS --------------------------------------
---------------------------------------------------------------------------*/
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
void append_throughput(double);
void window_stats(struct extended_cache_op *op, struct mica_resp *resp);
int spawn_stats_thread();
void print_latency_stats(void);


/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/
// This helps us set up the necessary rdma_cm_ids for the multicast groups
struct cm_qps
{
  int receive_q_depth;
	struct rdma_cm_id* cma_id;
	struct ibv_pd* pd;
	struct ibv_cq* cq;
	struct ibv_mr* mr;
	void *mem;
};

// This helps us set up the multicasts
struct mcast_info
{
	int	clt_id;
	struct rdma_event_channel *channel;
  struct sockaddr_storage dst_in[MCAST_GROUPS_PER_CLIENT];
	struct sockaddr *dst_addr[MCAST_GROUPS_PER_CLIENT];
	struct sockaddr_storage src_in;
	struct sockaddr *src_addr;
  struct cm_qps cm_qp[MCAST_QPS];
  //Send-only stuff
  struct rdma_ud_param mcast_ud_param;

};

// this contains all data we need to perform our mcasts
struct mcast_essentials {
  struct ibv_cq *recv_cq;
  struct ibv_qp *recv_qp;
  struct ibv_mr *recv_mr;
  struct ibv_ah *send_ah;
  uint32_t qpn;
  uint32_t qkey;
};

int get_addr(char*, struct sockaddr*);
int alloc_nodes(void);
void setup_multicast(struct mcast_info*, int);
void resolve_addresses(struct mcast_info*);
void set_up_qp(struct cm_qps*, int);
void multicast_testing(struct mcast_essentials*, int , struct hrd_ctrl_blk*);

/* ---------------------------------------------------------------------------
------------------------------INITIALIZATION --------------------------------------
---------------------------------------------------------------------------*/

void createAHs(uint16_t clt_gid, struct hrd_ctrl_blk *cb);
void createAHs_for_workers(uint16_t, struct hrd_ctrl_blk*);
int* get_random_permutation(int n, int clt_gid, uint64_t *seed);
int parse_trace(char* path, struct trace_command **cmds, int clt_gid);
void set_up_the_buffer_space(uint16_t[], uint32_t[], uint32_t[]);
void trace_init(struct trace_command **cmds, int clt_gid);
void init_multicast(struct mcast_info**, struct mcast_essentials**, int, struct hrd_ctrl_blk*, int);
void set_up_queue_depths(int**, int**, int);
// Connect with Workers and Clients
void setup_client_conenctions_and_spawn_stats_thread(int clt_gid, struct hrd_ctrl_blk *cb);
void set_up_ops(struct extended_cache_op**, struct extended_cache_op**,
                struct extended_cache_op**, struct mica_resp**, struct mica_resp**,
                struct mica_resp**, struct key_home**, struct key_home**, struct key_home**);
void set_up_coh_ops(struct cache_op**, struct cache_op**, struct small_cache_op**,
                    struct small_cache_op**, struct mica_resp*, struct mica_resp*,
                    struct mica_op**, int);
// Post receives for the coherence traffic in the init phase
void post_coh_recvs(struct hrd_ctrl_blk*, int*, struct mcast_essentials*, int, void*);
// Set up the memory registrations required in the client if there is no Inlining
void set_up_mrs(struct ibv_mr**, struct ibv_mr**, struct extended_cache_op*, struct mica_op*,
                struct hrd_ctrl_blk*);
void set_up_credits(uint8_t[][MACHINE_NUM], struct ibv_send_wr*, struct ibv_sge*,
				  struct ibv_recv_wr*, struct ibv_sge*, struct hrd_ctrl_blk*, int);
// Set up the remote Requests send and recv WRs
void set_up_remote_WRs(struct ibv_send_wr*, struct ibv_sge*, struct ibv_recv_wr*,
                       struct ibv_sge*, struct hrd_ctrl_blk* , int, struct ibv_mr*, int);
// Set up all coherence send and recv WRs// Set up all coherence WRs
void set_up_coh_WRs(struct ibv_send_wr*, struct ibv_sge*, struct ibv_recv_wr*, struct ibv_sge*,
         struct ibv_send_wr*, struct ibv_sge*, struct mica_op*, uint16_t,
				 struct hrd_ctrl_blk*, struct ibv_mr*, struct mcast_essentials*, int);


/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
// check if the given protocol is invalid
void check_protocol(int);
// pin a worker thread to a core
int pin_worker(int w_id);
// pin a client thread to a core
int pin_client(int c_id);


#endif /* ARMONIA_UTILS_H */
