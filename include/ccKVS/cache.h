#ifndef ARMONIA_CACHE_H
#define ARMONIA_CACHE_H


// Optik Options
#ifndef CORE_NUM
#define DEFAULT
#define CORE_NUM 8
#endif
#include "optik_mod.h"
#include "hrd.h"
#include "main.h"
#include "mica.h"

#define CACHE_DEBUG 0
#define CACHE_NUM_KEYS (250 * 1000)
#define CACHE_NUM_BKTS (64 * 1024) //TODO 64K buckets seems to be enough to store most of our keys

#define WRITE_RATIO 250 //155// out of a 1000, e.g 10 means 10/1000 i.e. 1%
#define CACHE_BATCH_SIZE 10 //

/*
Baseline 200

 EC
0   400
0.2 400
1   400
2   250
5   150

SC - balanced writes
0     400
0.2   300
1     250
2     200
5      60

SC - skewed writes
0
0.2   300 9-19
1     120  9-19
2
5
*/

//Cache States
#define VALID_STATE 1
#define INVALID_STATE 2
#define INVALID_REPLAY_STATE 3
#define WRITE_STATE 4
#define WRITE_REPLAY_STATE 5

//Cache Opcode
#define CACHE_OP_GET 111
#define CACHE_OP_PUT 112
#define CACHE_OP_UPD 113
#define CACHE_OP_INV 114
#define CACHE_OP_ACK 115
#define CACHE_OP_BRC 116       //Warning although this is cache opcode it's returned by cache to either Broadcast upd or inv

//Cache Response
#define ST_EMPTY 120
#define CACHE_GET_SUCCESS 121
#define CACHE_PUT_SUCCESS 122
#define CACHE_UPD_SUCCESS 123
#define CACHE_INV_SUCCESS 124
#define CACHE_ACK_SUCCESS 125
#define CACHE_LAST_ACK_SUCCESS 126
#define RETRY 127

#define CACHE_MISS 130
#define CACHE_GET_STALL 131
#define CACHE_PUT_STALL 132
#define CACHE_UPD_FAIL 133
#define CACHE_INV_FAIL 134
#define CACHE_ACK_FAIL 135
#define CACHE_GET_FAIL 136
#define CACHE_PUT_FAIL 137

#define UNSERVED_CACHE_MISS 140

char* code_to_str(uint8_t code);
struct key_home{
	uint8_t machine;
	uint8_t worker;
};

/* Fixed-size 16 byte keys */
struct spacetime_key {
	cache_meta meta; // This should be 8B (unused --> in mica)
	unsigned int bkt			:32;
	unsigned int server			:16;
	unsigned int tag			:16;
};


struct spacetime_op {
	struct cache_key key;	/* This must be the 1st field and 16B aligned */
	uint8_t opcode;
	uint8_t val_len;
	uint8_t value[MICA_MAX_VALUE];
};

// this is used to facilitate the coalescing
struct spacetime_op_coalescing {
	struct cache_key key;	/* This must be the 1st field and 16B aligned */
	uint8_t opcode;
	uint8_t val_len;
	uint8_t value[MICA_MAX_VALUE + EXTRA_WORKER_REQ_BYTES];
};

//
struct small_cache_op {
	struct cache_key key;	/* This must be the 1st field and 16B aligned */
	uint8_t opcode;
};

struct cache_meta_stats { //TODO change this name
	/* Stats */
	long long num_get_success;
	long long num_put_success;
	long long num_upd_success;
	long long num_inv_success;
	long long num_ack_success;
	long long num_get_stall;
	long long num_put_stall;
	long long num_upd_fail;
	long long num_inv_fail;
	long long num_ack_fail;
	long long num_get_miss;
	long long num_put_miss;
	long long num_unserved_get_miss;
	long long num_unserved_put_miss;
};

struct extended_cache_meta_stats {
	long long num_hit;
	long long num_miss;
	long long num_stall;
	long long num_coherence_fail;
	long long num_coherence_success;
	struct cache_meta_stats metadata;
};


struct cache {
	int num_threads;
	struct mica_kv hash_table;
	long long total_ops_issued; ///this is only for get and puts
	struct extended_cache_meta_stats aggregated_meta;
	struct cache_meta_stats* meta;
};

typedef enum {
	NO_REQ,
	HOT_WRITE_REQ_BEFORE_SAVING_KEY,
	HOT_WRITE_REQ,
	HOT_READ_REQ,
	LOCAL_REQ,
	REMOTE_REQ
} req_type;


struct latency_flags {
	req_type measured_req_flag;
	uint16_t last_measured_op_i;
	struct cache_key* key_to_measure;
};

//Add latency to histogram (in microseconds)
static inline void bookkeep_latency(int useconds, req_type rt){
	uint32_t** latency_counter;
	switch (rt){
		case REMOTE_REQ:
			latency_counter = &latency_count.remote_reqs;
			break;
		case LOCAL_REQ:
			latency_counter = &latency_count.local_reqs;
			break;
		case HOT_READ_REQ:
			latency_counter = &latency_count.hot_reads;
			break;
		case HOT_WRITE_REQ:
			latency_counter = &latency_count.hot_writes;
			break;
		default: assert(0);
	}
	latency_count.total_measurements++;
	if (useconds > MAX_LATENCY)
		(*latency_counter)[MAX_LATENCY]++;
	else
		(*latency_counter)[useconds / (MAX_LATENCY / LATENCY_BUCKETS)]++;
}


// Necessary bookkeeping to initiate the latency measurement
static inline void start_measurement(struct timespec* start, struct latency_flags* latency_info, uint16_t rm_id,
																		 struct extended_cache_op *ops, uint16_t op_i, uint16_t local_client_id,
																		 uint8_t opcode, int isSC, uint16_t next_op_i) {

	if (ENABLE_ASSERTIONS) assert(ops[op_i].key.meta.state == 0);
  if ((latency_info->measured_req_flag) == NO_REQ) {
    if (w_stats[local_client_id].batches_per_client > K_32 &&
        op_i == ((((latency_count.total_measurements % CACHE_BATCH_SIZE) + next_op_i) % CACHE_BATCH_SIZE) + next_op_i) &&
        local_client_id == 0 && machine_id == 0) {
//      printf("tag a key for latency measurement \n");
      if (IS_LOCAL(opcode)) latency_info->measured_req_flag = LOCAL_REQ;
      else if (IS_REMOTE(opcode)) latency_info->measured_req_flag = REMOTE_REQ;
      if (DISABLE_CACHE == 0) {
        if (opcode == HOT_WRITE) {
          if (isSC == 1) latency_info->measured_req_flag = HOT_WRITE_REQ_BEFORE_SAVING_KEY;
          else latency_info->measured_req_flag = HOT_WRITE_REQ;
        } else if (opcode == HOT_READ) latency_info->measured_req_flag = HOT_READ_REQ;
      } else {
        if (opcode == HOT_WRITE || opcode == HOT_READ)
          if (rm_id == machine_id) latency_info->measured_req_flag = LOCAL_REQ;
          else latency_info->measured_req_flag = REMOTE_REQ;
      }

      latency_info->last_measured_op_i = op_i;
//		green_printf("Measuring a req %llu, opcode %d, flag %d op_i %d \n",
//								 w_stats[local_client_id].batches_per_worker, opcode, latency_info->measured_req_flag, latency_info->last_measured_op_i);

      clock_gettime(CLOCK_MONOTONIC, start);

      if (ENABLE_ASSERTIONS) assert(latency_info->measured_req_flag != NO_REQ);
      if ((latency_info->measured_req_flag) == REMOTE_REQ) {
        ops[op_i].key.meta.state = 1;
//      printf("tag a key for remote latency measurement \n");
      }
      // for SC the key cannot be copied yet, as it would not contain the correct version
    }
  }
}


// Take the necessary actions to measure the hot requests. Writes need special treatment in SC
static inline void hot_request_bookkeeping_for_latency_measurements(struct timespec* start, struct latency_flags* latency_info,
											struct extended_cache_op *ops, uint16_t op_i, uint16_t local_client_id,
											int isSC, struct mica_resp* resp)
{
	if (latency_info->measured_req_flag == HOT_READ_REQ || ((isSC == 0) && (latency_info->measured_req_flag == HOT_WRITE_REQ))) {
		if (resp[latency_info->last_measured_op_i].type == CACHE_GET_SUCCESS ||
				resp[latency_info->last_measured_op_i].type == CACHE_PUT_SUCCESS) {
			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC, &end);
			int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
										 ((end.tv_nsec - start->tv_nsec) / 1000);
			if (ENABLE_ASSERTIONS) assert(useconds >= 0);
//				printf("Latency of  a hot req of flag %d: %d us\n", *measured_req_flag, useconds);
			if (latency_info->measured_req_flag == HOT_READ_REQ) bookkeep_latency(useconds, HOT_READ_REQ);
			else bookkeep_latency(useconds, HOT_WRITE_REQ);
			latency_info->measured_req_flag = NO_REQ;
		}
	}
		// After going to the cache a hot write in SC now knows its version and can be copied to the 'key_to_measure' field
	else if ((isSC == 1) && (latency_info->measured_req_flag == HOT_WRITE_REQ_BEFORE_SAVING_KEY) &&
					 (resp[latency_info->last_measured_op_i].type == CACHE_PUT_SUCCESS)) {
		memcpy(latency_info->key_to_measure, &ops[latency_info->last_measured_op_i].key, sizeof(struct cache_key));
		latency_info->measured_req_flag = HOT_WRITE_REQ;
//			printf("version we copy %d, op_i %d , resp %d\n", ops[latency_info->last_measured_op_i].key.meta.version,
//						 latency_info->last_measured_op_i, resp[latency_info->last_measured_op_i].type);
	}
	else if ((isSC == 1) && (latency_info->measured_req_flag == HOT_WRITE_REQ_BEFORE_SAVING_KEY) &&
					 (resp[latency_info->last_measured_op_i].type == CACHE_PUT_FAIL)) {
//			printf("failed hot write");
		latency_info->measured_req_flag = NO_REQ;
	}
}

void cache_init(int cache_id, int num_threads);
void cache_populate_fixed_len(struct mica_kv* kv, int n, int val_len);
void cache_insert_one(struct cache_op *op, struct mica_resp *resp);

void cache_batch_op_sc_non_stalling_sessions(int op_num, int thread_id, struct extended_cache_op **op, struct mica_resp *resp);
void cache_batch_op_sc_non_stalling_sessions_with_cache_op(int op_num, int thread_id, struct cache_op **op, struct mica_resp *resp);
void cache_batch_op_sc_non_stalling_sessions_with_small_cache_op(int op_num, int thread_id, struct small_cache_op **op, struct mica_resp *resp);
void cache_batch_op_sc_non_stalling(int op_num, int thread_id, struct extended_cache_op **ops, struct mica_resp *resp);
void cache_batch_op_sc_stalling(int op_num, int thread_id, struct extended_cache_op **ops, struct mica_resp *resp);
//void cache_batch_op_ec(int op_num, int thread_id, struct extended_cache_op **ops, struct mica_resp *resp);
//void cache_batch_op_ec_with_cache_op(int op_num, int thread_id, struct cache_op **op, struct mica_resp *resp);
int batch_from_trace_to_cache(int trace_iter, int thread_id, struct trace_command *trace, struct extended_cache_op *ops,
															struct mica_resp *resp, struct key_home* kh, int isSC, uint16_t next_op_i ,
															struct latency_flags*, struct timespec*, uint16_t*);
void create_req_from_trace(int* trace_iter, int thread_id, struct trace_command *trace, struct cache_op *op);
void manage_cache_response(int trace_iter, struct trace_command *trace, struct cache_op *ops, struct mica_resp *resp, struct key_home* kh);
void str_to_binary(uint8_t* value, char* str, int size);
void print_cache_stats(struct timespec start, int id);
void cache_add_2_total_ops_issued(long long ops_issued);
void mica_insert_one_crcw(struct mica_kv *kv, struct mica_op *op, struct mica_resp *resp);

#endif
