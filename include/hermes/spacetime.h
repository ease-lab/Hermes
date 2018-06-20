//
// Created by akatsarakis on 04/05/18.
//

#ifndef HERMES_SPACETIME_H
#define HERMES_SPACETIME_H

// Optik Options
#ifndef CORE_NUM
# define DEFAULT
# define CORE_NUM 8
#endif

#include <optik_mod.h>
#include "hrd.h"
#include "mica.h"
#include "optik_mod.h"
#include "config.h"

///#define NUM_KEYS (250 * 1000)
///#define NUM_BKTS (64 * 1024) //64K buckets seems to be enough to store most of our keys
//#define SPACETIME_NUM_KEYS (50 * 1000)
#define SPACETIME_NUM_KEYS (250 * 1000)
//#define SPACETIME_NUM_KEYS (500 * 1000)
//#define SPACETIME_NUM_KEYS (1 * 1000 * 1000)
#define SPACETIME_NUM_BKTS (1 * 1024 * 1024)
//#define SPACETIME_NUM_BKTS (16 * 1024 * 1024)

///WARNING the monotonically increasing assigned numbers to States are used for comparisons (do not reorder / change numbers)
//States
#define VALID_STATE 1
#define INVALID_STATE 2
#define INVALID_WRITE_STATE 3
#define INVALID_BUFF_STATE 4
#define INVALID_WRITE_BUFF_STATE 5
#define WRITE_STATE 6
#define WRITE_BUFF_STATE 7
#define REPLAY_STATE 8
#define REPLAY_BUFF_STATE 9
#define REPLAY_WRITE_STATE 10
#define REPLAY_WRITE_BUFF_STATE 11

//Input Opcodes
#define ST_OP_GET 111
#define ST_OP_PUT 112
#define ST_OP_INV 113
#define ST_OP_ACK 114
#define ST_OP_VAL 115
#define ST_OP_CRD 116


//Response Opcodes
#define ST_GET_SUCCESS 121
#define ST_PUT_SUCCESS 122 //broadcast invs
#define ST_BUFFERED_REPLAY 123 //broadcast invs
#define ST_INV_SUCCESS 124 //send ack
#define ST_ACK_SUCCESS 125
#define ST_LAST_ACK_SUCCESS 126        //complete local write
#define ST_LAST_ACK_PREV_WRITE_SUCCESS 127        //complete local write
#define ST_PUT_COMPLETE 128 //broadcast invs
#define ST_VAL_SUCCESS 129

#define ST_MISS 130
#define ST_GET_STALL 131
#define ST_PUT_STALL 132
#define ST_INV_FAIL 133
#define ST_ACK_FAIL 134
#define ST_VAL_FAIL 135

#define ST_GET_FAIL 136
#define ST_PUT_FAIL 137
#define ST_INV_OUT_OF_GROUP 138


//ops bucket states
#define ST_EMPTY 140
#define ST_NEW 141
#define ST_BUFFERED 142
#define ST_PROCESSED 143
#define ST_COMPLETE 144
#define ST_IN_PROGRESS_WRITE 145
#define ST_BUFFERED_IN_PROGRESS_REPLAY 146

// trace opcodes
#define NOP 147

//#define BUFFERED_READ 142
//#define BUFFERED_WRITE 143
//#define BUFFERED_READ_REPLAYED_WRITE 144
//#define BUFFERED_WRITE_REPLAYED_WRITE 145
//#define BUFFERED_READ_REPLAYED_IN_PROGRESS_WRITE 147
//#define BUFFERED_WRITE_REPLAYED_IN_PROGRESS_WRITE 148

//others
#define ST_OP_HEARTBEAT 151
#define ST_OP_SUSPICION 152

#define ST_VALUE_SIZE (HERD_VALUE_SIZE - sizeof(spacetime_object_meta))

//receive_buff_types
#define ST_INV_BUFF 161
#define ST_ACK_BUFF 162
#define ST_VAL_BUFF 163
#define ST_CRD_BUFF 164

#define WRITE_BUFF_EMPTY 255
#define LAST_WRITER_ID_EMPTY 255
#define TIE_BREAKER_ID_EMPTY 255

//print binary numbers
#define BYTE_TO_BINARY_PATTERN "%c%c%c%c%c%c%c%c"
#define BYTE_TO_BINARY(byte)  \
  (byte & 0x80 ? '1' : '0'), \
  (byte & 0x40 ? '1' : '0'), \
  (byte & 0x20 ? '1' : '0'), \
  (byte & 0x10 ? '1' : '0'), \
  (byte & 0x08 ? '1' : '0'), \
  (byte & 0x04 ? '1' : '0'), \
  (byte & 0x02 ? '1' : '0'), \
  (byte & 0x01 ? '1' : '0')



typedef struct {
    uint8_t opcode; //both recv / resp
    uint8_t state_or_sender;
    uint8_t val_len_or_packet_mark;
    uint8_t tie_breaker_id;
    uint32_t version; ///timestamp{version+tie_braker_id}
}spacetime_op_meta_t;

///* Fixed-size 16 byte keys */
//typedef struct {
//    uint64 __unused; // This should be 8B
//    unsigned int bkt			:32;
//    unsigned int server			:16;
//    unsigned int tag			:16;
//}spacetime_key_t;

/* Fixed-size 8 byte keys */
typedef struct {
    unsigned int bkt			:32;
    unsigned int server			:16;
    unsigned int tag			:16;
}spacetime_key_t;

typedef struct{ ///May add    uint8_t session_id;
    spacetime_key_t key;	/* This must be the 1st field and 16B aligned */
    uint8_t opcode; //both recv / resp
    uint8_t state;
    uint8_t val_len;
    uint8_t tie_breaker_id;
    uint32_t version; ///timestamp{version+tie_braker_id}
    uint8_t value[ST_VALUE_SIZE];
}spacetime_op_t;

typedef struct{
    spacetime_key_t key;	/* This must be the 1st field and 16B aligned */
    uint8_t opcode; //both recv / batch_op resp
    uint8_t sender;
    uint8_t val_len;
    uint8_t tie_breaker_id;
    uint32_t version; ///timestamp{version+tie_braker_id}
    uint8_t value[ST_VALUE_SIZE];
}spacetime_inv_t;

typedef struct{
    spacetime_key_t key;	/* This must be the 1st field and 16B aligned */
    uint8_t opcode; //both recv / batch_op resp
    uint8_t sender;
    uint8_t __unused; //align for using a single memcpy ///Warning currently in use for marking coalesed packets!
    uint8_t tie_breaker_id;
    uint32_t version; ///timestamp{version+tie_braker_id}
}spacetime_ack_t, spacetime_val_t;

typedef struct{ //always send as immediate
    uint8_t opcode; //we do not really need this
    uint8_t sender;
    uint8_t val_credits;
}spacetime_crd_t;

/*Packet types (used for coalescing)*/
typedef struct {
    uint8_t req_num;
    spacetime_inv_t reqs[INV_MAX_REQ_COALESCE];
}spacetime_inv_packet_t;

typedef struct {
    uint8_t req_num;
    spacetime_ack_t reqs[ACK_MAX_REQ_COALESCE];
}spacetime_ack_packet_t;

typedef struct {
    uint8_t req_num;
    spacetime_val_t reqs[VAL_MAX_REQ_COALESCE];
}spacetime_val_packet_t;

/*Packets with GRH*/

typedef struct {
    struct ibv_grh grh;
    spacetime_inv_packet_t packet;
}ud_req_inv_t;

typedef struct {
    struct ibv_grh grh;
    spacetime_ack_packet_t packet;
}ud_req_ack_t;

typedef struct {
    struct ibv_grh grh;
    spacetime_val_packet_t packet;
}ud_req_val_t;

typedef struct {
    struct ibv_grh grh;
    spacetime_crd_t req;
}ud_req_crd_t;


typedef struct{
    uint8_t group_membership[GROUP_MEMBERSHIP_ARRAY_SIZE];
    uint8_t write_ack_init[GROUP_MEMBERSHIP_ARRAY_SIZE];
    spacetime_lock optik_lock;
}spacetime_group_membership;

struct spacetime_meta_stats { //TODO change this name
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

struct extended_spacetime_meta_stats {
    long long num_hit;
    long long num_miss;
    long long num_stall;
    long long num_coherence_fail;
    long long num_coherence_success;
    struct spacetime_meta_stats metadata;
};

struct spacetime_kv {
    int num_threads;
    struct mica_kv hash_table;
    long long total_ops_issued; ///this is only for get and puts
    struct spacetime_meta_stats* meta;
    struct extended_spacetime_meta_stats aggregated_meta;
};

struct spacetime_trace_command {
    spacetime_key_t key_hash;
    uint8_t opcode;
};

void spacetime_init(int spacetime_id, int num_threads);
void spacetime_populate_fixed_len(struct spacetime_kv* kv,  int n,  int val_len);
void spacetime_batch_ops(int op_num, spacetime_op_t **ops, int thread_id, uint32_t refilled_ops_debug_cnt,
                         uint32_t* ref_ops_dbg_array_cnt);
void spacetime_batch_invs(int op_num, spacetime_inv_t **op, int thread_id);
void spacetime_batch_acks(int op_num, spacetime_ack_t **op, spacetime_op_t* read_write_op, int thread_id);
void spacetime_batch_vals(int op_num, spacetime_val_t **op, int thread_id);
void group_membership_init(void);
//uint8_t is_last_ack(uint8_t const * gathered_acks);

static inline void
update_ack_bit_vector(uint8_t sender_int_id, uint8_t* ack_bit_vector){
    if(ENABLE_ASSERTIONS == 1) assert(sender_int_id < MACHINE_NUM);
	ack_bit_vector[sender_int_id / 8] |= (uint8_t) 1 << sender_int_id % 8;
}

static inline int
timestamp_is_equal(uint32_t v1, uint8_t tie_breaker1,
                   uint32_t v2, uint8_t tie_breaker2){
    return (v1 == v2 && tie_breaker1 == tie_breaker2);
}

static inline int
timestamp_is_smaller(uint32_t v1, uint8_t tie_breaker1,
                     uint32_t v2, uint8_t tie_breaker2){
    return (v1 < v2 || (v1 == v2 && tie_breaker1 < tie_breaker2));
}

static inline int
group_mem_timestamp_is_same_and_valid(uint32_t v1, uint32_t v2){
    return (v1 == v2 && v1 % 2 == 0);
}



#endif //HERMES_SPACETIME_H
