//
// Created by akatsarakis on 11/12/18.
//

#ifndef HERMES_SEQLOCK_H
#define HERMES_SEQLOCK_H

#include <stdint.h>
#include "config.h"
#include "bit_vector.h"

#define ENABLE_LOCK_ASSERTS 1

#define COMPILER_BARRIER() asm volatile ("" ::: "memory")
#define LOCK_PAUSE() asm volatile ("mfence");

#if !defined(COMPILER_NO_REORDER)
# define COMPILER_NO_REORDER(exec)	\
  COMPILER_BARRIER();				\
  exec;						        \
  COMPILER_BARRIER()
#endif


#define SEQLOCK_LOCKED  0x1
#define SEQLOCK_FREE    0x0


typedef volatile struct
{
    uint8_t lock;
    uint8_t state;
    bit_vector_t ack_bv;
    uint8_t op_buffer_index; //TODO change to uint16_t for a buffer >= 256
    uint8_t last_writer_id;
    uint8_t tie_breaker_id;
    uint8_t last_local_write_tie_breaker_id;
    uint32_t version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
    uint32_t last_local_write_version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
}
spacetime_object_meta;

typedef volatile struct
{
    uint8_t lock;
    uint32_t version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
} __attribute__((packed))
seqlock_t;

typedef volatile struct
{
    uint8_t tie_breaker_id;
    uint32_t version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
} __attribute__((packed))
timestamp_t;

typedef volatile struct
{
//    uint8_t lock;
    uint8_t state;
    bit_vector_t ack_bv;
    uint8_t op_buffer_index; //TODO change to uint16_t for a buffer >= 256
    uint8_t last_writer_id;
    union {
        seqlock_t seq_lock;
        timestamp_t ts_version; //WARNING: only version of timestamp is stored here
    };
    uint8_t ts_tie_breaker_id;
    timestamp_t last_local_write_ts;
//    uint8_t tie_breaker_id;
//    uint8_t last_local_write_tie_breaker_id;
//    uint32_t version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
//    uint32_t last_local_write_version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
} spacetime_object_meta2;

////// SPACETIME Functions

static inline void
optik_unlock(spacetime_object_meta* ol, uint8_t cid, uint32_t version)
{
    assert(version % 2 == 0);
    ol->tie_breaker_id = cid;
    ol->version = version;
    COMPILER_NO_REORDER(ol->lock = SEQLOCK_FREE);
}

static inline int
is_same_version_and_valid(volatile spacetime_object_meta* v1,
                          volatile spacetime_object_meta* v2)
{
    return  v1->version == v2->version &&
            v1->tie_breaker_id == v2->tie_breaker_id &&
            v1->version % 2 == 0;
}

static inline int
seqlock_lock(seqlock_t *ol)
{
    seqlock_t prev_lock;
    do{
        while (1){
            prev_lock = *ol;
            if (prev_lock.lock != SEQLOCK_LOCKED)
                break;
            LOCK_PAUSE();
        }

        if(__sync_val_compare_and_swap(&ol->lock, 0, 1) == 0){
            ol->version++;
            break;
        }
    }while (1);

    return 1;
}

static inline int
optik_lock(spacetime_object_meta* ol)
{
    spacetime_object_meta prev_lock;
    do{
        while (1){
            prev_lock = *ol;
            if (prev_lock.lock != SEQLOCK_LOCKED)
                break;
            LOCK_PAUSE();
        }
        if(__sync_val_compare_and_swap(&ol->lock, 0, 1) == 0){
            ol->version++;
            break;
        }
    } while (1);

    return 1;
}

static inline void
optik_unlock_write(spacetime_object_meta* ol, uint8_t cid, uint32_t* resp_version)
{
    if(ENABLE_LOCK_ASSERTS){
        assert(ol->lock == SEQLOCK_LOCKED);
        assert(ol->version % 2 == 1);
    }
    ol->tie_breaker_id = cid;
    *resp_version = ++ol->version;
    COMPILER_NO_REORDER(ol->lock = SEQLOCK_FREE);
}

static inline void
optik_unlock_decrement_version(spacetime_object_meta* ol)
{
    ol->version = --ol->version;
    if(ENABLE_LOCK_ASSERTS)
        assert(ol->version % 2 == 0);
    COMPILER_NO_REORDER(ol->lock = SEQLOCK_FREE);
}

static inline void
seqlock_unlock(seqlock_t *ol)
{
    if(ENABLE_LOCK_ASSERTS){
        assert(ol->lock == SEQLOCK_LOCKED);
        assert(ol->version % 2 == 1);
    }
    ol->version++;
    COMPILER_NO_REORDER(ol->lock = SEQLOCK_FREE);
}

#endif //HERMES_SEQLOCK_H
