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

#include <seqlock.h>
#include "hrd.h"
#include "mica.h"
#include "seqlock.h"
#include "config.h"
#include "bit_vector.h"

#define SPACETIME_NUM_KEYS (1000 * 1000)
#define SPACETIME_NUM_BKTS (2 * 1024 * 1024)

///WARNING the monotonically increasing assigned numbers to States are used for comparisons (do not reorder / change numbers)
//States
#define VALID_STATE 1
#define INVALID_STATE 2
#define INVALID_WRITE_STATE 3
#define WRITE_STATE 4
#define REPLAY_STATE 5

//Input Opcodes
#define ST_OP_GET 111
#define ST_OP_PUT 112
#define ST_OP_INV 113
#define ST_OP_ACK 114
#define ST_OP_VAL 115
#define ST_OP_CRD 116
#define ST_OP_MEMBERSHIP_CHANGE 117
#define ST_OP_MEMBERSHIP_COMPLETE 118


//Response Opcodes
#define ST_GET_COMPLETE 121
#define ST_PUT_SUCCESS 122 //broadcast invs
#define ST_REPLAY_SUCCESS 123 //broadcast invs
#define ST_INV_SUCCESS 124 //send ack
#define ST_ACK_SUCCESS 125
#define ST_LAST_ACK_SUCCESS 126        //complete local write
#define ST_LAST_ACK_NO_BCAST_SUCCESS 127        //complete local write
#define ST_PUT_COMPLETE 128 //broadcast invs
#define ST_VAL_SUCCESS 129

#define ST_MISS 130
#define ST_GET_STALL 131
#define ST_PUT_STALL 132
#define ST_PUT_COMPLETE_SEND_VALS 133


//ops bucket states
#define ST_EMPTY 140
#define ST_NEW 141
#define ST_COMPLETE 144
#define ST_IN_PROGRESS_PUT 145
#define ST_IN_PROGRESS_REPLAY 146
#define ST_REPLAY_COMPLETE 147

// trace opcodes
#define NOP 148

//others
#define ST_OP_HEARTBEAT 151
#define ST_OP_SUSPICION 152
#define ST_INV_OUT_OF_GROUP 153

#define ST_VALUE_SIZE (KVS_VALUE_SIZE - sizeof(spacetime_object_meta))

//receive_buff_types
#define ST_INV_BUFF 161
#define ST_ACK_BUFF 162
#define ST_VAL_BUFF 163
#define ST_CRD_BUFF 164

#define ST_OP_BUFFER_INDEX_EMPTY 255
#define LAST_WRITER_ID_EMPTY 255
#define TIE_BREAKER_ID_EMPTY 255


// Fixed-size 8 (or 16) byte keys
typedef struct
{
//    uint64 __unused; // This should be 8B ////// Uncomment this for fixed-size 16 byte keys instead
    unsigned int bkt			:32;
    unsigned int server			:16;
    unsigned int tag			:16;
}
spacetime_key_t;

typedef volatile struct
{
    uint8_t state;
    bit_vector_t ack_bv;
    uint8_t last_writer_id;
    uint8_t op_buffer_index; //TODO change to uint16_t for a buffer >= 256
    conc_ctrl_t conc_ctrl;
    timestamp_t last_local_write_ts;
}
spacetime_object_meta;


typedef struct
{
    spacetime_key_t key;	/* This must be the 1st field and 16B aligned */
    uint8_t opcode; //both recv / resp
    union {
        uint8_t state; //used by spacetime_op_t
        uint8_t sender; //used by spacetime_inv/ack/val_t
    };
    uint8_t val_len; // unused for spacetime_ack_t and spacetime_val_t (align for using a single memcpy)
    timestamp_t ts;
}
spacetime_op_meta_t;

typedef struct
{
    ///May add    uint8_t session_id;
    spacetime_op_meta_t op_meta; // uses the state part of the op_meta union (not sender)
    uint16_t no_req_coalescing; //used only for skew optimizations
    uint8_t value[ST_VALUE_SIZE];
}
spacetime_op_t;

typedef struct
{
    spacetime_op_meta_t op_meta; // uses the sender part of the op_meta union (not state)
    uint16_t no_coales; //used only for skew optimizations
    uint8_t value[ST_VALUE_SIZE];
}
spacetime_inv_t;

typedef spacetime_op_meta_t spacetime_ack_t, spacetime_val_t;

typedef struct
{
    uint8_t opcode; //we do not really need this
    uint8_t sender;
    uint8_t val_credits;
}
spacetime_crd_t; //always send as immediate




// Packets for coalescing
typedef struct
{
    uint8_t req_num;
    spacetime_inv_t reqs[INV_MAX_REQ_COALESCE];
}
spacetime_inv_packet_t;

typedef struct
{
    uint8_t req_num;
    spacetime_ack_t reqs[ACK_MAX_REQ_COALESCE];
}
spacetime_ack_packet_t;

typedef struct
{
    uint8_t req_num;
    spacetime_val_t reqs[VAL_MAX_REQ_COALESCE];
}
spacetime_val_packet_t;





// Packets with GRH

typedef struct
{
    struct ibv_grh grh;
    spacetime_inv_packet_t packet;
}
ud_req_inv_t;

typedef struct
{
    struct ibv_grh grh;
    spacetime_ack_packet_t packet;
}
ud_req_ack_t;

typedef struct
{
    struct ibv_grh grh;
    spacetime_val_packet_t packet;
}
ud_req_val_t;

typedef struct
{
    struct ibv_grh grh;
    spacetime_crd_t req;
}
ud_req_crd_t;






typedef struct
{
    uint8_t num_of_alive_remotes;
    bit_vector_t g_membership;
    bit_vector_t w_ack_init;
    seqlock_t lock;
}
spacetime_group_membership;

struct spacetime_meta_stats
{
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

struct extended_spacetime_meta_stats
{
    long long num_hit;
    long long num_miss;
    long long num_stall;
    long long num_coherence_fail;
    long long num_coherence_success;
    struct spacetime_meta_stats metadata;
};

struct spacetime_kv
{
    int num_threads;
    struct mica_kv hash_table;
    long long total_ops_issued; ///this is only for get and puts
    struct spacetime_meta_stats* meta;
    struct extended_spacetime_meta_stats aggregated_meta;
};

struct spacetime_trace_command
{
    spacetime_key_t key_hash;
    uint8_t opcode;
    uint8_t key_id; // stores key ids 0-254 otherwise it is set to 255 to indicate other key ids
};

void spacetime_init(int spacetime_id, int num_threads);
void spacetime_populate_fixed_len(struct spacetime_kv* kv,  int n,  int val_len);

void batch_ops_to_KVS(int op_num, spacetime_op_t **ops, int thread_id, spacetime_group_membership curr_membership);
void batch_invs_to_KVS(int op_num, spacetime_inv_t **op, spacetime_op_t *read_write_op, int thread_id,
                       int* node_suspected, uint32_t* refilled_per_ops_debug_cnt);
void batch_acks_to_KVS(int op_num, spacetime_ack_t **op, spacetime_op_t *read_write_op,
                       spacetime_group_membership curr_membership, int thread_id);
void batch_vals_to_KVS(int op_num, spacetime_val_t **op, spacetime_op_t *read_write_op, int thread_id);


void group_membership_init(void);
int find_suspected_node(spacetime_op_t *op, int thread_id,
                        spacetime_group_membership curr_membership);
void complete_writes_and_replays_on_follower_removal(int op_num, spacetime_op_t **op,
                                                     spacetime_group_membership curr_membership, int thread_id);
void reset_bcast_send_buffers(spacetime_inv_packet_t *inv_send_packet_ops, int *inv_push_ptr,
                              spacetime_val_packet_t *val_send_packet_ops, int *val_push_ptr);
void reconfigure_wrs(struct ibv_send_wr *inv_send_wr, struct ibv_sge *inv_send_sgl,
                     struct ibv_send_wr *val_send_wr, struct ibv_sge *val_send_sgl,
                     spacetime_group_membership last_g_membership, uint16_t worker_lid);

static inline void
update_ack_bit_vector(uint16_t sender_int_id, uint8_t* ack_bit_vector){
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

extern volatile spacetime_group_membership group_membership;


#endif //HERMES_SPACETIME_H
