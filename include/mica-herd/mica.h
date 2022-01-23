#ifndef MICA_H
#define MICA_H

#include <stdint.h>
#include "city.h"
#include "hrd.h"

/*
 * The polling logic in HERD requires the following:
 * 1. 0 < MICA_OP_GET < MICA_OP_PUT < HERD_OP_GET < HERD_OP_PUT
 * 2. HERD_OP_GET = MICA_OP_GET + HERD_MICA_OFFSET
 * 3. HERD_OP_PUT = MICA_OP_PUT + HERD_MICA_OFFSET
 *
 * This allows us to detect HERD requests by checking if the request region
 * opcode is more than MICA_OP_PUT. And then we can convert a HERD opcode to
 * a MICA opcode by subtracting HERD_MICA_OFFSET from it.
 */
#define MICA_OP_PUT 112

/* Ensure that a mica_op is cacheline aligned */
#define MICA_OP_METADATA \
  (sizeof(struct mica_key) + sizeof(uint8_t) + sizeof(uint8_t))
#define MICA_MIN_VALUE (64 - MICA_OP_METADATA)
#define MICA_MAX_VALUE                                                \
  (USE_BIG_OBJECTS == 1 ? (MICA_MIN_VALUE + (EXTRA_CACHE_LINES * 64)) \
                        : MICA_MIN_VALUE)

#define MICA_LOG_BITS 40

#define MICA_INDEX_SHM_KEY 3185
#define MICA_LOG_SHM_KEY 4185

/*
 * Debug values:
 * 0: No safety checks on fast path
 * 1: Sanity checks for arguments
 * 2: Pretty print GET/PUT operations
 */

#define MICA_DEBUG 0

struct mica_resp {
  uint8_t type;
  uint8_t val_len;
  uint16_t unused[3]; /* Make val_ptr 8-byte aligned */
  uint8_t* val_ptr;
};

/* Fixed-size 16 byte keys */
struct mica_key {
  unsigned long long __unused : 64;
  unsigned int bkt : 32;
  unsigned int server : 16;
  unsigned int tag : 16;
};

struct mica_op {
  struct mica_key key; /* This must be the 1st field and 16B aligned */
  uint8_t opcode;
  uint8_t val_len;
  uint8_t value[MICA_MAX_VALUE];
};

struct mica_slot {
  uint32_t in_use : 1;
  uint32_t tag : (64 - MICA_LOG_BITS - 1);
  uint64_t offset : MICA_LOG_BITS;
};

struct mica_bkt {
  struct mica_slot slots[8];
};

struct mica_kv {
  struct mica_bkt* ht_index;
  uint8_t* ht_log;

  /* Metadata */
  int instance_id; /* ID of this MICA instance. Used for shm keys */

  uint64_t num_bkts; /* Number of buckets requested by user */
  uint64_t bkt_mask; /* Mask down from a mica_key's @bkt to a bucket */

  uint64_t log_cap;  /* Capacity of circular log in bytes */
  uint64_t log_mask; /* Mask down from a slot's @offset to a log offset */

  /* State */
  uint64_t log_head;

  /* Stats */
  long long num_insert_op;       /* Number of PUT requests executed */
  long long num_index_evictions; /* Number of entries evicted from index */
};

void mica_init(struct mica_kv* kv, int instance_id, int node_id, int num_bkts,
               uint64_t log_cap);

/* Single-key INSERT */
void mica_insert_one(struct mica_kv* kv, struct mica_op* op,
                     struct mica_resp* res);

/* Helpers */
uint128* mica_gen_keys(int n);

///* Debug functions */
void mica_print_op(struct mica_op* op);

#endif
