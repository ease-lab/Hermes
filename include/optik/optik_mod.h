/*
 *   File: optik.h
 *   Author: Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>
 *   Description:
 *   bst.h is part of ASCYLIB
 *
 * Copyright (c) 2014 Vasileios Trigonakis <vasileios.trigonakis@epfl.ch>,
 * 	     	      Tudor David <tudor.david@epfl.ch>
 *	      	      Distributed Programming Lab (LPD), EPFL
 *
 * ASCYLIB is free software: you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation, version 2
 * of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 */
/*
 * WARNING: This file has been modified to fit to "HERMES" needs.
 */

#ifndef _H_OPTIK_
#define _H_OPTIK_

// this is required in order to properly include
#ifndef _GNU_SOURCE
# define _GNU_SOURCE
#endif

#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdint.h>
#include <atomic_ops.h>
#include <config.h>

#include "utils.h"


#ifdef __tile__
#  error OPTIK does not yet include the appropriate memory barriers for TILERA.
#endif

#ifndef OPTIK_STATS
#  define OPTIK_STATS 0
#endif

#if OPTIK_STATS == 1
extern __thread size_t __optik_trylock_calls;
extern __thread size_t __optik_trylock_cas;
extern __thread size_t __optik_trylock_calls_suc;
extern size_t __optik_trylock_calls_tot;
extern size_t __optik_trylock_cas_tot;
extern size_t __optik_trylock_calls_suc_tot;
#  define OPTIK_STATS_VARS_DEFINITION()					\
  __thread size_t __optik_trylock_calls = 0;				\
  __thread size_t __optik_trylock_cas = 0;				\
  __thread size_t __optik_trylock_calls_suc = 0;			\
  size_t __optik_trylock_calls_tot = 0;					\
  size_t __optik_trylock_cas_tot = 0;					\
  size_t __optik_trylock_calls_suc_tot = 0

#  define OPTIK_STATS_TRYLOCK_CALLS_INC()       __optik_trylock_calls++;
#  define OPTIK_STATS_TRYLOCK_CAS_INC()	        __optik_trylock_cas++;
#  define OPTIK_STATS_TRYLOCK_CALLS_SUC_INC(by) __optik_trylock_calls_suc+=by;
#  define OPTIK_STATS_PUBLISH()			\
  __sync_fetch_and_add(&__optik_trylock_calls_tot, __optik_trylock_calls); \
  __sync_fetch_and_add(&__optik_trylock_cas_tot, __optik_trylock_cas); \
  __sync_fetch_and_add(&__optik_trylock_calls_suc_tot, __optik_trylock_calls_suc)

#  define OPTIK_STATS_PRINT()						\
  printf("[OPTIK] %-10s tot: %-10zu | cas: %-10zu | suc: %-10zu | "	\
	 "succ-cas: %6.2f%% | succ-tot: %6.2f%% | cas/suc: %.2f\n", "trylock", \
	 __optik_trylock_calls_tot, __optik_trylock_cas_tot, __optik_trylock_calls_suc_tot, \
	 100 * (1 - ((double) (__optik_trylock_cas_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_cas_tot)), \
	 100 * (1 - ((double) (__optik_trylock_calls_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_calls_tot)), \
	 (double) __optik_trylock_cas_tot / __optik_trylock_calls_suc_tot)

#  define OPTIK_STATS_PRINT_DUR(dur_ms)					\
  printf("[OPTIK] %-10s tot: %-10.0f | cas: %-10.0f | suc: %-10.0f | "	\
	 "succ-cas: %6.2f%% | succ-tot: %6.2f%% | cas/suc: %.2f\n", "trylock/s", \
	 __optik_trylock_calls_tot / (dur_ms / 1000.0), __optik_trylock_cas_tot / (dur_ms / 1000.0), \
	 __optik_trylock_calls_suc_tot / (dur_ms / 1000.0),		\
	 100 * (1 - ((double) (__optik_trylock_cas_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_cas_tot)), \
	 100 * (1 - ((double) (__optik_trylock_calls_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_calls_tot)), \
	 (double) __optik_trylock_cas_tot / __optik_trylock_calls_suc_tot)


#elif OPTIK_STATS == 2 // only CAS
extern __thread size_t __optik_trylock_calls;
extern __thread size_t __optik_trylock_cas;
extern __thread size_t __optik_trylock_calls_suc;
extern size_t __optik_trylock_calls_tot;
extern size_t __optik_trylock_cas_tot;
extern size_t __optik_trylock_calls_suc_tot;
#  define OPTIK_STATS_VARS_DEFINITION()					\
  __thread size_t __optik_trylock_calls = 0;				\
  __thread size_t __optik_trylock_cas = 0;				\
  __thread size_t __optik_trylock_calls_suc = 0;			\
  size_t __optik_trylock_calls_tot = 0;					\
  size_t __optik_trylock_cas_tot = 0;					\
  size_t __optik_trylock_calls_suc_tot = 0

#  define OPTIK_STATS_TRYLOCK_CALLS_INC()
#  define OPTIK_STATS_TRYLOCK_CAS_INC()	        __optik_trylock_cas++;
#  define OPTIK_STATS_TRYLOCK_CALLS_SUC_INC(by)
#  define OPTIK_STATS_PUBLISH()			\
  __sync_fetch_and_add(&__optik_trylock_cas_tot, __optik_trylock_cas);

#  define OPTIK_STATS_PRINT()						\
  printf("[OPTIK] %-10s tot: %-10zu | cas: %-10zu | suc: %-10zu | "\
	 "succ-cas: %6.2f%% | succ-tot: %6.2f%% | cas/suc: %.2f\n", "trylock", \
	 __optik_trylock_calls_tot, __optik_trylock_cas_tot, __optik_trylock_calls_suc_tot,	\
	 100 * (1 - ((double) (__optik_trylock_cas_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_cas_tot)), \
	 100 * (1 - ((double) (__optik_trylock_calls_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_calls_tot)), \
	 (double) __optik_trylock_cas_tot / __optik_trylock_calls_suc_tot)

#  define OPTIK_STATS_PRINT_DUR(dur_ms)					\
  printf("[OPTIK] %-10s tot: %-10.0f | cas: %-10.0f | suc: %-10.0f | "	\
	 "succ-cas: %6.2f%% | succ-tot: %6.2f%% | cas/suc: %.2f\n", "trylock/s", \
	 __optik_trylock_calls_tot / (dur_ms / 1000.0), __optik_trylock_cas_tot / (dur_ms / 1000.0), \
	 __optik_trylock_calls_suc_tot / (dur_ms / 1000.0),		\
	 100 * (1 - ((double) (__optik_trylock_cas_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_cas_tot)), \
	 100 * (1 - ((double) (__optik_trylock_calls_tot - __optik_trylock_calls_suc_tot) / __optik_trylock_calls_tot)), \
	 (double) __optik_trylock_cas_tot / __optik_trylock_calls_suc_tot)

#else
#  define OPTIK_STATS_VARS_DEFINITION()
#  define OPTIK_STATS_TRYLOCK_CALLS_INC()
#  define OPTIK_STATS_TRYLOCK_CAS_INC()
#  define OPTIK_STATS_TRYLOCK_CALLS_SUC_INC(by)
#  define OPTIK_STATS_PUBLISH()
#  define OPTIK_STATS_PRINT()
#  define OPTIK_STATS_PRINT_DUR(dur_ms)
#endif

#define OPTIK_RLS_ATOMIC   0
#define OPTIK_RLS_STORE    1
#define OPTIK_RLS_BARRIER  2
#define OPTIK_RLS_TYPE     OPTIK_RLS_ATOMIC

#define OPTIK_PAUSE() asm volatile ("mfence");

#  define OPTIK_INIT    { 0, 0 }
#  define OPTIK_LOCKED  0x1
#  define OPTIK_FREE    0x0

typedef volatile struct
{
    uint8_t lock;
    uint8_t state;
    uint8_t write_acks[GROUP_MEMBERSHIP_ARRAY_SIZE];
    uint8_t replay_acks;
    uint8_t write_buffer_index; //TODO change to a pointer for a buffer >= 256
    uint8_t last_writer_id;
    uint8_t tie_breaker_id;
    uint32_t version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
    uint32_t last_writer_version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
} spacetime_object_meta;

typedef volatile struct
{
    uint8_t lock;
    uint32_t version;  /// for lock-free reads & as part of timestamp:{version|tie_breaker_id}
}spacetime_lock;

////// SPACETIME Functions

static inline void
optik_unlock(spacetime_object_meta* ol, uint8_t cid, uint32_t version)
{
    assert(version % 2 == 0);
    ol->tie_breaker_id = cid;
    ol->version = version;
    COMPILER_NO_REORDER(ol->lock = OPTIK_FREE);
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
optik_is_locked(spacetime_object_meta* ol)
{
    return (ol->lock == OPTIK_LOCKED);
}
static inline int
optik_lock(spacetime_object_meta* ol)
{
    spacetime_object_meta ol_old;
    do
    {
        while (1)
        {
            ol_old = *ol;
            if (!optik_is_locked(&ol_old))
            {
                break;
            }
            OPTIK_PAUSE();
        }

        OPTIK_STATS_TRYLOCK_CAS_INC();
        if (CAS_U8(&ol->lock, 0, 1) == 0)
        {
            ol->version++;
            //assert(ol->version % 2 == 1);
            break;
        }
    }
    while (1);
    return 1;
}

static inline void
optik_unlock_write(spacetime_object_meta* ol, uint8_t cid, uint32_t* resp_version)
{
    if(ENABLE_ASSERTIONS){
        assert(ol->lock == OPTIK_LOCKED);
        assert(ol->version % 2 == 1);
    }
    ol->tie_breaker_id = cid;
    *resp_version = ++ol->version;
    COMPILER_NO_REORDER(ol->lock = OPTIK_FREE);

}

static inline void
optik_unlock_decrement_version(spacetime_object_meta* ol)
{
    ol->version = --ol->version;
    assert(ol->version % 2 == 0);
    COMPILER_NO_REORDER(ol->lock = OPTIK_FREE);
}
#endif	/* _H_OPTIK_ */