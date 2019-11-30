//
// Created by akatsarakis on 23/05/18.
//

#ifndef HERMES_INLINE_UTIL_H
#define HERMES_INLINE_UTIL_H

#include <infiniband/verbs.h>
#include "../utils/concur_ctrl.h"
#include "spacetime.h"
#include "config.h"
#include "util.h"
#include "../hades/hades.h"


/* ---------------------------------------------------------------------------
----------------------------------- MEMBERSHIP -------------------------------
---------------------------------------------------------------------------*/
static inline uint8_t
node_is_in_membership(spacetime_group_membership last_group_membership, int node_id);

static inline void
follower_removal(int suspected_node_id)
{
	red_printf("Suspected node %d!\n", suspected_node_id);

	seqlock_lock(&group_membership.lock);
    if(ENABLE_ASSERTIONS == 1){
		assert(suspected_node_id != machine_id);
		assert(suspected_node_id < MACHINE_NUM);
		assert(node_is_in_membership(*((spacetime_group_membership*) &group_membership), suspected_node_id));
	}
    group_membership.num_of_alive_remotes--;

	bv_bit_reset((bit_vector_t*) &group_membership.g_membership, (uint8_t) suspected_node_id);
	bv_copy((bit_vector_t*) &group_membership.w_ack_init, group_membership.g_membership);
	bv_reverse((bit_vector_t*) &group_membership.w_ack_init);
	bv_bit_set((bit_vector_t*) &group_membership.w_ack_init, (uint8_t) machine_id);

	green_printf("Group Membership: ");
	bv_print(group_membership.g_membership);
	printf("\n");

	seqlock_unlock(&group_membership.lock);

	red_printf("Group membership changed --> Removed follower %d!\n", suspected_node_id);

	if(group_membership.num_of_alive_remotes < (MACHINE_NUM / 2)){
		red_printf("Majority is down!\n");
		exit(-1);
	}
}

static inline void
group_membership_update(hades_ctx_t hades_ctx)
{
    seqlock_lock(&group_membership.lock);

    bv_copy((bit_vector_t*) &group_membership.g_membership, hades_ctx.curr_g_membership);
    bv_copy((bit_vector_t*) &group_membership.w_ack_init, group_membership.g_membership);
    bv_reverse((bit_vector_t*) &group_membership.w_ack_init);
    bv_bit_set((bit_vector_t*) &group_membership.w_ack_init, (uint8_t) machine_id);

    group_membership.num_of_alive_remotes = bv_no_setted_bits(group_membership.g_membership);
    seqlock_unlock(&group_membership.lock);

    if(group_membership.num_of_alive_remotes < (MACHINE_NUM / 2)){
		red_printf("Majority is down!\n");
		exit(-1);
	}
}

static inline uint8_t
group_membership_has_changed(spacetime_group_membership* last_group_membership, uint16_t worker_lid)
{
	uint32_t debug_lock_free_membership_read_cntr = 0;
	spacetime_group_membership lock_free_read_group_membership;

	do { //Lock free read of group membership
		if (ENABLE_ASSERTIONS) {
			debug_lock_free_membership_read_cntr++;
			if (debug_lock_free_membership_read_cntr == M_4) {
				printf("Worker %u stuck on a lock-free read (for group membership)\n", worker_lid);
				debug_lock_free_membership_read_cntr = 0;
			}
		}
		lock_free_read_group_membership = *((spacetime_group_membership*) &group_membership);
	} while (!(seqlock_version_is_same_and_valid(&group_membership.lock,
												 &lock_free_read_group_membership.lock)));
	for(int i = 0; i < GROUP_MEMBERSHIP_ARRAY_SIZE; i++)
	    if(!bv_are_equal(lock_free_read_group_membership.g_membership,
	    		last_group_membership->g_membership))
		{
			*last_group_membership = lock_free_read_group_membership;
			return 1;
		}
	return 0;
}

static inline uint8_t
node_is_in_membership(spacetime_group_membership last_group_membership, int node_id)
{
	return (uint8_t) (bv_bit_get(last_group_membership.g_membership, (uint8_t) node_id) == 1 ? 1 : 0);
}

/* ---------------------------------------------------------------------------
----------------------------------- LATENCY -------------------------------
---------------------------------------------------------------------------*/
//Add latency to histogram (in microseconds)
static inline void
bookkeep_latency(int useconds, uint8_t op)
{
	uint32_t* latency_array;
	int* max_latency_ptr;
	switch (op){
		case ST_OP_PUT:
			latency_array = latency_count.write_reqs;
			max_latency_ptr = &latency_count.max_write_latency;
			break;
		case ST_OP_GET:
			latency_array = latency_count.read_reqs;
			max_latency_ptr = &latency_count.max_read_latency;
			break;
		default: assert(0);
	}
	latency_count.total_measurements++;
	if (useconds > MAX_LATENCY)
		latency_array[LATENCY_BUCKETS]++;
	else
		latency_array[useconds / LATENCY_PRECISION]++;

	if(*max_latency_ptr < useconds)
		*max_latency_ptr = useconds;
}


// Necessary bookkeeping to initiate the latency measurement
static inline void
start_latency_measurement(struct timespec *start)
{
	clock_gettime(CLOCK_MONOTONIC, start);
}

static inline void
stop_latency_measurment(uint8_t req_opcode, struct timespec *start)
{
	struct timespec end;
	clock_gettime(CLOCK_MONOTONIC, &end);
	int useconds = (int) (((end.tv_sec - start->tv_sec) * 1000000) +
				   ((end.tv_nsec - start->tv_nsec) / 1000));
	if (ENABLE_ASSERTIONS) assert(useconds >= 0);
//	printf("Latency of %s %u us\n", code_to_str(req_opcode), useconds);
	bookkeep_latency(useconds, req_opcode);
}


/* ---------------------------------------------------------------------------
------------------------------------OTHERS------------------------------------
---------------------------------------------------------------------------*/
static inline void
stop_latency_of_completed_writes(spacetime_op_t *ops, uint16_t worker_lid,
								 struct timespec *stopwatch)
{
	if(MEASURE_LATENCY && machine_id == 0 && worker_lid == THREAD_MEASURING_LATENCY)
		if(ops[0].op_meta.opcode == ST_OP_PUT &&
		   (ops[0].op_meta.state == ST_MISS || ops[0].op_meta.state == ST_PUT_COMPLETE))
				stop_latency_measurment(ops[0].op_meta.opcode, stopwatch);
}

static inline void
stop_latency_of_completed_reads(spacetime_op_t *ops, uint16_t worker_lid,
								struct timespec *stopwatch)
{
	if(MEASURE_LATENCY && machine_id == 0 && worker_lid == THREAD_MEASURING_LATENCY)
		if(ops[0].op_meta.opcode == ST_OP_GET &&
		   (ops[0].op_meta.state == ST_MISS || ops[0].op_meta.state == ST_GET_COMPLETE))
				stop_latency_measurment(ops[0].op_meta.opcode, stopwatch);
}

static inline int
refill_ops(uint32_t *trace_iter, uint16_t worker_lid,
           struct spacetime_trace_command *trace, spacetime_op_t *ops,
           uint32_t *refilled_per_ops_debug_cnt, struct timespec *start,
           spacetime_op_t **n_hottest_keys_in_ops_get,
           spacetime_op_t **n_hottest_keys_in_ops_put)
{
	static uint8_t first_iter_has_passed[WORKERS_PER_MACHINE] = { 0 };

	int refilled_ops = 0, node_suspected = -1;
	for(int i = 0; i < max_batch_size; i++) {
		if(ENABLE_ASSERTIONS && first_iter_has_passed[worker_lid] == 1){
				assert(ops[i].op_meta.opcode == ST_OP_PUT ||
				       ops[i].op_meta.opcode == ST_OP_GET ||
				       (is_CR == 0 && ops[i].op_meta.opcode == ST_OP_RMW));
				assert(ops[i].op_meta.state == ST_PUT_COMPLETE ||
					   ops[i].op_meta.state == ST_GET_COMPLETE ||
					   ops[i].op_meta.state == ST_PUT_SUCCESS ||
					   ops[i].op_meta.state == ST_REPLAY_SUCCESS ||
					   ops[i].op_meta.state == ST_NEW ||
					   ops[i].op_meta.state == ST_MISS ||
					   ops[i].op_meta.state == ST_PUT_STALL ||
					   ops[i].op_meta.state == ST_REPLAY_COMPLETE ||
					   ops[i].op_meta.state == ST_IN_PROGRESS_PUT ||
					   //<RMW>
                       ops[i].op_meta.state == ST_RMW_STALL ||
                       ops[i].op_meta.state == ST_RMW_ABORT ||
                       ops[i].op_meta.state == ST_RMW_SUCCESS ||
                       ops[i].op_meta.state == ST_RMW_COMPLETE ||
					   ops[i].op_meta.state == ST_IN_PROGRESS_RMW ||
//					   ops[i].op_meta.state == ST_IN_PROGRESS_PUT ||
					   //<RMW>
					   ops[i].op_meta.state == ST_IN_PROGRESS_GET ||
					   ops[i].op_meta.state == ST_IN_PROGRESS_REPLAY ||
					   ops[i].op_meta.state == ST_OP_MEMBERSHIP_CHANGE || ///TODO check this
					   ops[i].op_meta.state == ST_OP_MEMBERSHIP_COMPLETE || ///TODO check this
					   ops[i].op_meta.state == ST_PUT_COMPLETE_SEND_VALS ||
					   ops[i].op_meta.state == ST_GET_STALL);
        }

		if (first_iter_has_passed[worker_lid] == 0 ||
			ops[i].op_meta.state == ST_MISS ||
			ops[i].op_meta.state == ST_PUT_COMPLETE ||
			ops[i].op_meta.state == ST_RMW_ABORT ||
			ops[i].op_meta.state == ST_RMW_COMPLETE ||
			ops[i].op_meta.state == ST_OP_MEMBERSHIP_COMPLETE ||
			ops[i].op_meta.state == ST_GET_COMPLETE)
		{
            if (first_iter_has_passed[worker_lid] != 0) {
                if (ENABLE_REQ_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
                    green_printf(
                            "W%d--> Key Hash:%" PRIu64 "\n\t\tType: %s, version %d, tie-b: %d, value(len-%d): %c\n",
                            worker_lid, ((uint64_t *) &ops[i].op_meta.key)[0],
                            code_to_str(ops[i].op_meta.state), ops[i].op_meta.ts.version,
                            ops[i].op_meta.ts.tie_breaker_id, ops[i].op_meta.val_len, ops[i].value[0]);

                /// Stats
                if (ops[i].op_meta.state != ST_MISS){
                    if(ops[i].op_meta.state != ST_RMW_ABORT)
                        w_stats[worker_lid].completed_ops_per_worker += ENABLE_COALESCE_OF_HOT_REQS ? ops[i].no_coales : 1;
                } else
                    w_stats[worker_lid].reqs_missed_in_kvs++;

                if(ops[i].op_meta.state == ST_PUT_COMPLETE)
                    w_stats[worker_lid].completed_wrs_per_worker++;
                else if(ops[i].op_meta.state == ST_RMW_COMPLETE)
                    w_stats[worker_lid].completed_rmws_per_worker++;
                else if(ops[i].op_meta.state == ST_RMW_ABORT)
                    w_stats[worker_lid].aborted_rmws_per_worker++;

                // reset op bucket
                ops[i].no_coales = 1;
                ops[i].op_meta.state = ST_EMPTY;
                ops[i].op_meta.opcode = ST_EMPTY;
                refilled_per_ops_debug_cnt[i] = 0;
                refilled_ops++;
            }

            if (ENABLE_ASSERTIONS)
                assert(trace[*trace_iter].opcode == ST_OP_PUT ||
                       trace[*trace_iter].opcode == ST_OP_RMW ||
                       trace[*trace_iter].opcode == ST_OP_GET);

            if (MEASURE_LATENCY && machine_id == 0 && worker_lid == THREAD_MEASURING_LATENCY && i == 0)
                start_latency_measurement(start);

            /// INSERT new req(s) to ops
            uint8_t key_id;
            if (ENABLE_COALESCE_OF_HOT_REQS && trace[*trace_iter].opcode != ST_OP_RMW) {
                // see if you could coalesce any requests
                spacetime_op_t **n_hottest_keys_in_ops;
                do {
                    key_id = trace[*trace_iter].key_id;
                    n_hottest_keys_in_ops = trace[*trace_iter].opcode == ST_OP_GET ?
                                            n_hottest_keys_in_ops_get : n_hottest_keys_in_ops_put;
                    // if we can coalesce (a hot) req
                    if (key_id < COALESCE_N_HOTTEST_KEYS && // is a hot key
                        n_hottest_keys_in_ops[key_id] != NULL && // exists in the ops array
                        n_hottest_keys_in_ops[key_id]->op_meta.opcode ==
                        trace[*trace_iter].opcode) // has the same code with the last inserted
                    {
                        n_hottest_keys_in_ops[key_id]->no_coales++;
                        *trace_iter = trace[*trace_iter + 1].opcode != NOP ? *trace_iter + 1 : 0;
                    } else
                        break;
                } while (1);

                if (key_id < COALESCE_N_HOTTEST_KEYS)
                    n_hottest_keys_in_ops[key_id] = &ops[i];
            }

            ops[i].op_meta.state = ST_NEW;
            ops[i].op_meta.opcode = (uint8_t) (CR_ENABLE_ALL_NODES_GETS_EXCEPT_HEAD && machine_id != 0 ?
                                               ST_OP_GET : trace[*trace_iter].opcode);
            memcpy(&ops[i].op_meta.key, &trace[*trace_iter].key_hash, sizeof(spacetime_key_t));

            if (ops[i].op_meta.opcode == ST_OP_PUT || ops[i].op_meta.opcode == ST_OP_RMW)
                memset(ops[i].value, ((uint8_t) 'a' + machine_id), ST_VALUE_SIZE);

            else if (ENABLE_READ_COMPLETE_AFTER_VAL_RECV_OF_HOT_REQS) {
                //if its a read reset the timestamp
                ops[i].op_meta.ts.version = 0;
                ops[i].op_meta.ts.tie_breaker_id = 0;
            }

            ops[i].RMW_flag = ops[i].op_meta.opcode == ST_OP_RMW ? 1 : 0;

            ops[i].op_meta.val_len = (uint8) (ops[i].op_meta.opcode == ST_OP_GET ? 0 : ST_VALUE_SIZE >> SHIFT_BITS);

            // instead of MOD add
            *trace_iter = trace[*trace_iter + 1].opcode != NOP ? *trace_iter + 1 : 0;


            if (ENABLE_REQ_PRINTS && worker_lid < MAX_THREADS_TO_PRINT)
                red_printf("W%d--> Op: %s, hash(1st 8B):%" PRIu64 "\n",
                           worker_lid, code_to_str(ops[i].op_meta.opcode), ((uint64_t *) &ops[i].op_meta.key)[0]);

        } else
            refilled_per_ops_debug_cnt[i]++;
	}

	if(refilled_ops == 0)
		w_stats[worker_lid].wasted_loops++;

	if(first_iter_has_passed[worker_lid] == 0)
		first_iter_has_passed[worker_lid] = 1;

	if(ENABLE_ASSERTIONS)
		for(int i = 0; i < max_batch_size; i++)
			assert(ops[i].op_meta.opcode == ST_OP_PUT ||
			       ops[i].op_meta.opcode == ST_OP_GET ||
			       (ops[i].op_meta.opcode == ST_OP_RMW && is_CR == 0));

	return node_suspected;
}
#endif //HERMES_INLINE_UTIL_H
