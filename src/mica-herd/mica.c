#include "hrd.h"
#include "mica.h"

int is_power_of_2(int x)
{
	return (
		x == 1 || x == 2 || x == 4 || x == 8 || x == 16 || x == 32 ||
		x == 64 || x == 128 || x == 256 || x == 512 || x == 1024 ||
		x == 2048 || x == 4096 || x == 8192 || x == 16384 ||
		x == 32768 || x == 65536 || x == 131072 || x == 262144 ||
		x == 524288 || x == 1048576 || x == 2097152 ||
		x == 4194304 || x == 8388608 || x == 16777216 ||
		x == 33554432 || x == 67108864 || x == 134217728 ||
		x == 268435456 || x == 536870912 || x == 1073741824);
}

void mica_init(struct mica_kv *kv,
	int instance_id, int node_id, int num_bkts, uint64_t log_cap)
{
	int i, j;

	/* Verify struct sizes */
	assert(sizeof(struct mica_slot) == 8);
	assert(sizeof(struct mica_key) == 16);
	assert(sizeof(struct mica_op) % 64 == 0);

	assert(kv != NULL);
	assert(node_id == 0 || node_id == 1);

	/* 16 million buckets = a 1 GB index */
	assert(is_power_of_2(num_bkts) == 1 && num_bkts <= M_128);
	//assert(log_cap > 0 && log_cap <= M_1024 &&
	//	log_cap % M_2 == 0 && is_power_of_2(log_cap));

	assert(MICA_LOG_BITS >= 24);	/* Minimum log size = 16 MB */

	// red_printf("mica-herd-herd: Initializing MICA instance %d.\n"
	// 	"NUMA node = %d, buckets = %d (size = %u B), log capacity = %d B.\n",
	// 	instance_id,
	// 	node_id, num_bkts, num_bkts * sizeof(struct mica_bkt), log_cap);

	if(MICA_DEBUG != 0) {
		printf("mica-herd-herd: Debug mode is ON! This might reduce performance.\n");
		sleep(2);
	}

	/* Initialize metadata and stats */
	kv->instance_id = instance_id;

	kv->num_bkts = num_bkts;
	kv->bkt_mask = num_bkts - 1;	/* num_bkts is power of 2 */

	kv->log_cap = log_cap;
	kv->log_mask = log_cap - 1;	/* log_cap is a power of 2 */
	kv->log_head = 0;

	kv->num_insert_op = 0;
	kv->num_index_evictions = 0;

	/* Alloc index and initialize all entries to invalid */
	// printf("mica-herd-herd: Allocting hash table index for instance %d\n", instance_id);
	int ht_index_key = MICA_INDEX_SHM_KEY + instance_id;
	kv->ht_index = (struct mica_bkt *) hrd_malloc_socket(ht_index_key,
		num_bkts * sizeof(struct mica_bkt), node_id);

	for(i = 0; i < num_bkts; i++) {
		for(j = 0; j < 8; j++) {
			kv->ht_index[i].slots[j].in_use = 0;
		}
	}

	/* Alloc log */
	// printf("mica-herd-herd: Allocting hash table log for instance %d\n", instance_id);
	int ht_log_key = MICA_LOG_SHM_KEY + instance_id;
	kv->ht_log = (uint8_t *) hrd_malloc_socket(ht_log_key, log_cap, node_id);
}

void mica_insert_one(struct mica_kv *kv,
	struct mica_op *op, struct mica_resp *resp)
{
#if MICA_DEBUG == 1
	assert(kv != NULL);
	assert(op != NULL);
	assert(op->opcode == MICA_OP_PUT);
	assert(op->val_len > 0 && op->val_len <= MICA_MAX_VALUE);
	assert(resp != NULL);
#endif

	int i;
	unsigned int bkt = op->key.bkt & kv->bkt_mask;
	struct mica_bkt *bkt_ptr = &kv->ht_index[bkt];
	unsigned int tag = op->key.tag;

#if MICA_DEBUG == 2
	mica_print_op(op);
#endif

	kv->num_insert_op++;

	/* Find a slot to use for this key. If there is a slot with the same
	 * tag as ours, we are sure to find it because the used slots are at
	 * the beginning of the 8-slot array. */
	int slot_to_use = -1;
	for(i = 0; i < 8; i++) {
		if(bkt_ptr->slots[i].tag == tag || bkt_ptr->slots[i].in_use == 0) {
			slot_to_use = i;
		}
	}

	/* If no slot found, choose one to evict */
	if(slot_to_use == -1) {
		slot_to_use = tag & 7;	/* tag is ~ randomly distributed */
		kv->num_index_evictions++;
	}

	/* Encode the empty slot */
	bkt_ptr->slots[slot_to_use].in_use = 1;
	bkt_ptr->slots[slot_to_use].offset = kv->log_head;	/* Virtual head */
	bkt_ptr->slots[slot_to_use].tag = tag;

	/* Paste the key-value into the log */
	uint8_t *log_ptr = &kv->ht_log[kv->log_head & kv->log_mask];

	/* Data copied: key, opcode, val_len, value */
	int len_to_copy = sizeof(struct mica_key) + sizeof(uint8_t) +
		sizeof(uint8_t) + KVS_VALUE_SIZE; ///op->val_len;

	/* Ensure that we don't wrap around in the *virtual* log space even
	 * after 8-byte alignment below.*/
	assert((1ULL << MICA_LOG_BITS) - kv->log_head > len_to_copy + 8);

	memcpy(log_ptr, op, len_to_copy);
	kv->log_head += len_to_copy;

	/* Ensure that the key field of each log entry is 8-byte aligned. This
	 * makes subsequent comparisons during GETs faster. */
	kv->log_head = (kv->log_head + 7) & ~7;

	/* If we're close to overflowing in the physical log, wrap around to
	 * the beginning, but go forward in the virtual log. */
	if(unlikely(kv->log_cap - kv->log_head <= MICA_MAX_VALUE + 32)) {
		kv->log_head = (kv->log_head + kv->log_cap) & ~kv->log_mask;
        colored_printf(RED, "mica-herd-herd: Instance %d wrapping around. Wraps = %llu\n",
               kv->instance_id, kv->log_head / kv->log_cap);
	}
}

/* A fast deterministic way to generate @n ~randomly distributed 16-byte keys */
uint128* mica_gen_keys(int n)
{
	int i;
	assert(n > 0 && n <= M_1024 / sizeof(uint128));
	assert(sizeof(uint128) == 16);

	// printf("mica-herd-herd: Generating %d keys\n", n);

	uint128 *key_arr = malloc(n * sizeof(uint128));
	assert(key_arr != NULL);

	for(i = 0; i < n; i++) {
		key_arr[i] = CityHash128((char *) &i, 4);
	}

	return key_arr;
}
