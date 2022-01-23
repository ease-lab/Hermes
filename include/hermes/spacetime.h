//
// Created by akatsarakis on 04/05/18.
//

#ifndef HERMES_SPACETIME_H
#define HERMES_SPACETIME_H

// Optik Options
#ifndef CORE_NUM
#define DEFAULT
#define CORE_NUM 8
#endif

#include "../utils/bit_vector.h"
#include "../utils/concur_ctrl.h"
#include "config.h"
#include "hrd.h"
#include "mica.h"

#define SPACETIME_NUM_KEYS (1000 * 1000)
#define SPACETIME_NUM_BKTS (2 * 1024 * 1024)
#define SPACETIME_LOG_CAP (1024 * 1024 * 1024)

//#define SPACETIME_NUM_KEYS (60 * 1000 * 1000)
//#define SPACETIME_NUM_BKTS (64 * 1024 * 1024)
//#define SPACETIME_LOG_CAP  (4 * ((unsigned long long) M_1024)) //(1024 * 1024
//* 1024)

#define ST_VALUE_SIZE (KVS_VALUE_SIZE - sizeof(spacetime_object_meta))

// Special EMPTY opcodes
#define NOP 150                   // trace
#define LAST_WRITER_ID_EMPTY 127  // 255
#define ST_OP_BUFFER_INDEX_EMPTY 255

/////////////////////////////////////////////
//// ENUMS
/////////////////////////////////////////////
/// WARNING the monotonically increasing assigned numbers to States are used for
/// comparisons (do not reorder / change numbers)
// States
typedef enum {
  VALID_STATE = 1,
  INVALID_STATE,
  INVALID_WRITE_STATE,
  WRITE_STATE,
  REPLAY_STATE,
} __attribute__((packed)) hermes_states_t;

// Input Opcodes
typedef enum {
  ST_OP_GET = 111,
  ST_OP_PUT,
  ST_OP_RMW,
  ST_OP_INV,
  ST_OP_ACK,
  ST_OP_VAL,
  ST_OP_CRD,
  ST_OP_MEMBERSHIP_CHANGE,
  ST_OP_MEMBERSHIP_COMPLETE  // 119

} __attribute__((packed)) input_opcodes_t;

// Response Opcodes
typedef enum {
  ST_GET_COMPLETE = 121,
  ST_PUT_SUCCESS,     // broadcast invs
  ST_REPLAY_SUCCESS,  // broadcast invs
  ST_INV_SUCCESS,     // send ack
  ST_ACK_SUCCESS,
  ST_LAST_ACK_SUCCESS,           // complete local write
  ST_LAST_ACK_NO_BCAST_SUCCESS,  // complete local write
  ST_PUT_COMPLETE,               // broadcast invs
  ST_VAL_SUCCESS,                // 129

  ST_MISS,  // 130
  ST_GET_STALL,
  ST_PUT_STALL,
  ST_PUT_COMPLETE_SEND_VALS,
  ST_SEND_CRD,  // 134

  // RMW opcodes
  ST_RMW_SUCCESS,  // 135
  ST_RMW_STALL,
  ST_RMW_COMPLETE,
  ST_RMW_ABORT,
  ST_OP_INV_ABORT,  // 139 //send inv instead of ACK

} __attribute__((packed)) response_opcodes_t;

// ops bucket states
typedef enum {
  ST_EMPTY = 140,
  ST_NEW,
  ST_COMPLETE,
  ST_IN_PROGRESS_PUT,
  ST_IN_PROGRESS_REPLAY,
  ST_REPLAY_COMPLETE,
  ST_IN_PROGRESS_GET,  // Used only in Chain Replication
  ST_REPLAY_COMPLETE_SEND_VALS,
  ST_IN_PROGRESS_RMW,
  ST_RMW_COMPLETE_SEND_VALS  // 149
} __attribute__((packed)) op_bucket_states_t;

// failure detection (deprecated)
typedef enum {
  ST_OP_HEARTBEAT = 151,  // WARNING: 150 opcode is used (see NOP define)!!
  ST_OP_SUSPICION,
  ST_INV_OUT_OF_GROUP
} __attribute__((packed)) fs_ops_t;

// receive_buff_types
typedef enum {
  ST_INV_BUFF = 161,
  ST_ACK_BUFF,
  ST_VAL_BUFF,
  ST_CRD_BUFF
} __attribute__((packed)) rcv_buff_types_t;

/////////////////////////////////////////////
//// Hermes(msg and KV -- spacetime) structs
/////////////////////////////////////////////

// Fixed-size 8 (or 16) byte keys
typedef struct {
  //    uint64 __unused; // This should be 8B ////// Uncomment this for
  //    fixed-size 16 byte keys instead
  uint64_t bkt : 48;
  unsigned int tag : 16;
} spacetime_key_t;

typedef volatile struct {
  hermes_states_t state;
  bit_vector_t ack_bv;
  uint8_t RMW_flag : 1;
  uint8_t last_writer_id : 7;
  uint8_t op_buffer_index;  // TODO change to uint16_t for a buffer >= 256
  conc_ctrl_t cctrl;
  timestamp_t last_local_write_ts;
} spacetime_object_meta;

typedef struct {
  spacetime_key_t key; /* This must be the 1st field and 8B or 16B aligned */
  uint8_t opcode;      // both recv / resp //TODO create a union
  union {
    uint8_t state;      // HERMES:  used by spacetime_op_t
    uint8_t sender;     // HERMES:  used by spacetime_inv/ack/val_t
    uint8_t initiator;  // CR:  used by spacetime_inv/ack
  };
  union {
    uint8_t val_len;   // HERMES: unused for spacetime_ack_t and spacetime_val_t
                       // (align for using a single memcpy)
    uint8_t buff_idx;  //    CR: used   for spacetime_ack_t buffer index of
                       //    write initiated this req
  };
  timestamp_t ts;
} spacetime_op_meta_t, spacetime_ack_t, spacetime_val_t;

typedef struct {
  spacetime_op_meta_t op_meta;  // op_t/inv_t: uses the state/sender part of the
                                // op_meta union (not sender/state)
  union {
    struct {                    // Hermes struct
      uint8_t RMW_flag : 1;     // 1 indicates RMWs while 0 normal writes
      uint16_t no_coales : 15;  // used only for skew optimizations
    };
    struct {              // CR struct
      uint8_t buff_idx;   //    for spacetime_inv_t buffer index of write
                          //    initiated this req
      uint8_t initiator;  //    for spacetime_inv_t buffer index of write
                          //    initiated this req
    };
  };
  uint8_t value[ST_VALUE_SIZE];
} spacetime_op_t, spacetime_inv_t;

typedef struct {
  volatile uint8_t num_of_alive_remotes;
  volatile bit_vector_t g_membership;
  volatile bit_vector_t w_ack_init;
  seqlock_t lock;
} spacetime_group_membership;

struct spacetime_kv {
  // TODO may add kvs stats
  struct mica_kv hash_table;
};

struct spacetime_trace_command {
  spacetime_key_t key_hash;
  uint8_t opcode;
  uint8_t key_id;  // stores key ids 0-254 otherwise it is set to 255 to
                   // indicate other key ids
};

void spacetime_init(int spacetime_id);
void spacetime_populate_fixed_len(struct spacetime_kv* kv, int n, int val_len);

///////////////////////////////////////
//////////////////// Hermes
///////////////////////////////////////

enum hermes_batch_type_t {
  local_ops,
  local_ops_after_membership_change,
  invs,
  acks,
  vals
};

void hermes_batch_ops_to_KVS(enum hermes_batch_type_t type, uint8_t* op_array,
                             int op_num, uint16_t sizeof_op_elem,
                             spacetime_group_membership curr_membership,
                             int* node_suspected,
                             spacetime_op_t* read_write_ops, uint8_t thread_id);

///////////////////////////////////////
//////////////////// CR(AQ)
///////////////////////////////////////
enum cr_type_t {
  Local_ops,      // All nodes
  Remote_writes,  // Head
  Remote_reads,   // Tail
  Invs,           // All except Head
  Acks            // All except Tail
};

void cr_batch_ops_to_KVS(enum cr_type_t cr_type, uint8_t* op_array, int op_num,
                         uint16_t sizeof_op_elem,
                         spacetime_op_t* read_write_op);

///////////////////////////////////////
//////////////////// Helpers
///////////////////////////////////////
static inline uint8_t
is_last_ack(bit_vector_t gathered_acks,
            spacetime_group_membership curr_g_membership)
{
  bv_and(&gathered_acks, curr_g_membership.g_membership);
  return bv_are_equal(gathered_acks, curr_g_membership.g_membership);
}

// TODO: adapt and use the following functions to re-enable variable length
// object support
static inline uint8_t
get_val_len(struct mica_op* op_t)
{
  return (op_t->val_len >> SHIFT_BITS) - sizeof(spacetime_op_meta_t);
}

static inline uint8_t
set_val_len(spacetime_op_meta_t* op_t)
{
  return (op_t->val_len >> SHIFT_BITS) + sizeof(spacetime_op_meta_t);
}

extern struct spacetime_kv kv;
extern spacetime_group_membership group_membership;

#endif  // HERMES_SPACETIME_H
