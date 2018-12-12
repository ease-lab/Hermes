//
// Created by akatsarakis on 11/12/18.
//

#ifndef HERMES_SEQLOCK_H
#define HERMES_SEQLOCK_H

#include <stdint.h>
#include "config.h"

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
    uint32_t version;  /// for lock-free reads
} __attribute__((packed))
seqlock_t;

typedef volatile struct
{
    uint8_t tie_breaker_id;
    uint32_t version;
} __attribute__((packed))
timestamp_t;

typedef volatile struct
{
    uint8_t lock;
    timestamp_t ts; /// ts.version used for both lock-free reads & as part of timestamp
} __attribute__((packed))
conc_ctrl_t;

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





static inline int
is_same_version_and_valid(volatile conc_ctrl_t* cctrl1,
                          volatile conc_ctrl_t* cctrl2)
{
    return  cctrl1->ts.version == cctrl2->ts.version &&
            cctrl1->ts.tie_breaker_id == cctrl2->ts.tie_breaker_id &&
            cctrl1->ts.version % 2 == 0;
}

static inline int
optik_lock(conc_ctrl_t* cctrl)
{
    conc_ctrl_t prev_cctrl;
    do{
        while (1){
            prev_cctrl = *cctrl;
            if (prev_cctrl.lock != SEQLOCK_LOCKED)
                break;
            LOCK_PAUSE();
        }

        if(__sync_val_compare_and_swap(&cctrl->lock, 0, 1) == 0){
            cctrl->ts.version++;
            break;
        }
    } while (1);

    return 1;
}

static inline void
optik_unlock_write(conc_ctrl_t* cctrl, uint8_t cid, uint32_t* resp_version)
{
    if(ENABLE_LOCK_ASSERTS){
        assert(cctrl->lock == SEQLOCK_LOCKED);
        assert(cctrl->ts.version % 2 == 1);
    }
    cctrl->ts.tie_breaker_id = cid;
    *resp_version = ++cctrl->ts.version;
    COMPILER_NO_REORDER(cctrl->lock = SEQLOCK_FREE);
}

static inline void
optik_unlock(conc_ctrl_t* conc_ctrl, uint8_t cid, uint32_t version)
{
    assert(version % 2 == 0);
    conc_ctrl->ts.tie_breaker_id = cid;
    conc_ctrl->ts.version = version;
    COMPILER_NO_REORDER(conc_ctrl->lock = SEQLOCK_FREE);
}

static inline void
optik_unlock_decrement_version(conc_ctrl_t* cctrl)
{
    --cctrl->ts.version;
    if(ENABLE_LOCK_ASSERTS)
        assert(cctrl->ts.version % 2 == 0);
    COMPILER_NO_REORDER(cctrl->lock = SEQLOCK_FREE);
}

#endif //HERMES_SEQLOCK_H
