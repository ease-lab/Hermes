//
// Created by akatsarakis on 11/12/18.
//

#ifndef HERMES_SEQLOCK_H
#define HERMES_SEQLOCK_H

#include <stdint.h>
#include "config.h"

#define ENABLE_LOCK_ASSERTS 1



#define SEQLOCK_LOCKED  0x1
#define SEQLOCK_FREE    0x0

#define LOCK_PAUSE() asm volatile ("mfence");

#define COMPILER_BARRIER() asm volatile ("" ::: "memory")

#if !defined(COMPILER_NO_REORDER)
#     define COMPILER_NO_REORDER(exec)	\
        COMPILER_BARRIER();				\
        exec;						    \
        COMPILER_BARRIER()
#endif

typedef volatile struct
{
    uint8_t tie_breaker_id;
    uint32_t version;
} __attribute__((packed))
timestamp_t;

typedef struct
{
    uint8_t lock;
    uint32_t version;  /// for lock-free reads
} __attribute__((packed))
seqlock_t;

typedef volatile struct
{
    uint8_t lock;
    timestamp_t ts; /// ts.version used for both lock-free reads & as part of timestamp
} __attribute__((packed))
conc_ctrl_t;




/////////////////////////////////////////
/// Timestamp  comparison  functions
/////////////////////////////////////////

static inline int
timestamp_is_equal(uint32_t v1, uint8_t tie_breaker1,
                   uint32_t v2, uint8_t tie_breaker2)
{
    return (v1 == v2 && tie_breaker1 == tie_breaker2);
}

static inline int
timestamp_is_smaller(uint32_t v1, uint8_t tie_breaker1,
                     uint32_t v2, uint8_t tie_breaker2)
{
    return (v1 < v2 || (v1 == v2 && tie_breaker1 < tie_breaker2));
}



/////////////////////////////////////////
/// seqlock locking / unlocking functions
/////////////////////////////////////////

static inline int
seqlock_lock(seqlock_t *seqlock)
{
    do{
        // Spin until the seqlock is unlocked
        while(seqlock->lock == SEQLOCK_LOCKED){ LOCK_PAUSE(); }

        // try to atomically get the lock via a CAS
        if(__sync_val_compare_and_swap(&seqlock->lock, 0, 1) == 0) {
            seqlock->version++;
            break;
        }

    }while (1); // retry if CAS failed

    return 1;
}


static inline void
seqlock_unlock(seqlock_t *seqlock)
{
    if(ENABLE_LOCK_ASSERTS) {
        assert(seqlock->lock == SEQLOCK_LOCKED);
        assert(seqlock->version % 2 == 1);
    }

    COMPILER_NO_REORDER(seqlock->version++);
    COMPILER_NO_REORDER(seqlock->lock = SEQLOCK_FREE);
}

// This is used to validate a lock-free read
// i.e. --> do { <Lock free read>  } while (!(seqlock_version_is_same_and_valid(...));
static inline int
seqlock_version_is_same_and_valid(seqlock_t *seqlock1, seqlock_t *seqlock2)
{
    return (seqlock1->version == seqlock2->version &&
            seqlock1->version % 2 == 0);
}




/////////////////////////////////////////
/// ccctrl locking / unlocking functions
/////////////////////////////////////////


static inline int
cctrl_lock(conc_ctrl_t *cctrl)
{
    do{
        // Spin until the seqlock is unlocked
        while(cctrl->lock == SEQLOCK_LOCKED){ LOCK_PAUSE(); }

        // try to atomically get the lock via a CAS
        if(__sync_val_compare_and_swap(&cctrl->lock, 0, 1) == 0){
            cctrl->ts.version++;
            break;
        }

    } while (1); // retry if CAS failed

    return 1;
}

static inline void
cctrl_unlock_write(conc_ctrl_t *cctrl, uint8_t cid, uint32_t *resp_version)
{
    if(ENABLE_LOCK_ASSERTS){
        assert(cctrl->lock == SEQLOCK_LOCKED);
        assert(cctrl->ts.version % 2 == 1);
    }
    cctrl->ts.tie_breaker_id = cid;
    COMPILER_NO_REORDER(*resp_version = ++cctrl->ts.version);
    COMPILER_NO_REORDER(cctrl->lock = SEQLOCK_FREE);
}

static inline void
cctrl_unlock(conc_ctrl_t *cctrl, uint8_t cid, uint32_t version)
{
    assert(version % 2 == 0);
    cctrl->ts.tie_breaker_id = cid;
    COMPILER_NO_REORDER(cctrl->ts.version = version);
    COMPILER_NO_REORDER(cctrl->lock = SEQLOCK_FREE);
}

static inline void
cctrl_unlock_decrement_version(conc_ctrl_t *cctrl)
{
    if(ENABLE_LOCK_ASSERTS)
        assert(cctrl->ts.version % 2 == 1);

    COMPILER_NO_REORDER(cctrl->ts.version--);
    COMPILER_NO_REORDER(cctrl->lock = SEQLOCK_FREE);
}

// This is used to validate a lock-free read
// i.e. --> do { <Lock free read>  } while (!(cctrl_timestamp_is_same_and_valid(...));
static inline int
cctrl_timestamp_is_same_and_valid(volatile conc_ctrl_t *cctrl1,
                                  volatile conc_ctrl_t *cctrl2)
{
    return cctrl1->ts.version % 2 == 0 &&
           timestamp_is_equal(cctrl1->ts.version, cctrl1->ts.tie_breaker_id,
                              cctrl2->ts.version, cctrl2->ts.tie_breaker_id);
}

#endif //HERMES_SEQLOCK_H
