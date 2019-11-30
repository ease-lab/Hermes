//
// Created by akatsarakis on 04/05/18.
//
#include <config.h>
#include <spacetime.h>
#include "../../include/utils/concur_ctrl.h"
#include <util.h>
#include <inline-util.h>

/*
 * Initialize the spacetime using a Mica instances and adding the timestamps
 * and locks to the keys of mica structure
 */

struct spacetime_kv kv;
spacetime_group_membership group_membership;

void
meta_reset(struct spacetime_meta_stats* meta)
{
	meta->num_get_success = 0;
	meta->num_put_success = 0;
	meta->num_upd_success = 0;
	meta->num_inv_success = 0;
	meta->num_ack_success = 0;
	meta->num_get_stall = 0;
	meta->num_put_stall = 0;
	meta->num_upd_fail = 0;
	meta->num_inv_fail = 0;
	meta->num_ack_fail = 0;
	meta->num_get_miss = 0;
	meta->num_put_miss = 0;
	meta->num_unserved_get_miss = 0;
	meta->num_unserved_put_miss = 0;
}

void
extended_meta_reset(struct extended_spacetime_meta_stats* meta)
{
	meta->num_hit = 0;
	meta->num_miss = 0;
	meta->num_stall = 0;
	meta->num_coherence_fail = 0;
	meta->num_coherence_success = 0;

	meta_reset(&meta->metadata);
}

void
spacetime_object_meta_init(spacetime_object_meta* ol)
{
	cctrl_init(&ol->cctrl);
	ol->state = VALID_STATE;
	ol->last_writer_id = LAST_WRITER_ID_EMPTY;
	ol->op_buffer_index = ST_OP_BUFFER_INDEX_EMPTY;
}

void
spacetime_init(int instance_id, int num_threads)
{
	kv.num_threads = num_threads;
	//TODO add a Define for stats
	kv.total_ops_issued = 0;
	/// allocate and init metadata for the spacetime & threads
	extended_meta_reset(&kv.aggregated_meta);
	kv.meta = malloc(num_threads * sizeof(struct spacetime_meta_stats));
	for(int i = 0; i < num_threads; i++)
		meta_reset(&kv.meta[i]);
	mica_init(&kv.hash_table, instance_id, KV_SOCKET, SPACETIME_NUM_BKTS, SPACETIME_LOG_CAP);
	spacetime_populate_fixed_len(&kv, SPACETIME_NUM_KEYS, KVS_VALUE_SIZE);
}

void
spacetime_populate_fixed_len(struct spacetime_kv* _kv, int n, int val_len)
{
	assert(n > 0);
	assert(val_len > 0 && val_len <= KVS_VALUE_SIZE);

	/* This is needed for the eviction message below to make sense */
	assert(_kv->hash_table.num_insert_op == 0 && _kv->hash_table.num_index_evictions == 0);

	struct mica_op op;
	struct mica_resp resp;
	unsigned long long *op_key = (unsigned long long *) &op.key;
	spacetime_object_meta initial_meta;
	spacetime_object_meta_init(&initial_meta);

	/* Generate the keys to insert */
	uint128 *key_arr = mica_gen_keys(n);
	op.val_len = (uint8_t) (val_len >> SHIFT_BITS);
	op.opcode = ST_OP_PUT;
	spacetime_object_meta *value_ptr = (spacetime_object_meta*) op.value;
	memcpy((void*) value_ptr, (void*) &initial_meta, sizeof(spacetime_object_meta));
	for(int i = n - 1; i >= 0; i--) {
		op_key[0] = key_arr[i].first;
		op_key[1] = key_arr[i].second;
		///printf("Key Metadata: Lock(%u), State(%u), Counter(%u:%u)\n", op.key.meta.lock,
		/// op.key.meta.state, op.key.meta.version, op.key.meta.cid);
		uint8_t val = (uint8_t) ('a' + (i % 20));

		memset((void*) &value_ptr[1], val, ST_VALUE_SIZE);
		mica_insert_one(&_kv->hash_table, &op, &resp);
	}

	assert(_kv->hash_table.num_insert_op == n);
	yellow_printf("Spacetime: Populated instance %d with %d keys, length = %d. "
				  "Index eviction fraction = %.4f.\n",
				  _kv->hash_table.instance_id, n, val_len,
				  (double) _kv->hash_table.num_index_evictions / _kv->hash_table.num_insert_op);
}




//TODO move this to util.c

