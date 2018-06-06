#include "cache.h"
#include <getopt.h>

//Global Vars
uint8_t protocol;
optik_lock_t kv_lock;
uint32_t* latency_counters;
struct latency_counters latency_count;
volatile struct mica_op *local_req_region;
volatile struct client_stats w_stats[CLIENTS_PER_MACHINE];
///volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];
///volatile char local_recv_flag[WORKERS_PER_MACHINE][CLIENTS_PER_MACHINE][64]; //false sharing problem -- fixed with padding
///volatile struct remote_qp remote_wrkr_qp[WORKER_NUM_UD_QPS][WORKER_NUM];
volatile struct remote_qp remote_clt_qp[CLIENT_NUM][CLIENT_UD_QPS];
volatile char clt_needed_ah_ready, wrkr_needed_ah_ready;

#if ENABLE_WORKERS_CRCW == 1
	struct mica_kv kv;
#endif



int main(int argc, char *argv[])
{
	/* Use small queues to reduce cache pressure */
	//assert(HRD_Q_DEPTH == 128);

	/* All requests should fit into the a single worker's request region */
	//assert(WORKER_REQ_SIZE *  CLIENTS_PER_MACHINE * (MACHINE_NUM - 1) * WS_PER_WORKER < RR_SIZE / WORKERS_PER_MACHINE);
	///printf("asking %lu < %d \n", sizeof(struct ud_req) *
	///							CLIENTS_PER_MACHINE * (MACHINE_NUM - 1) * WS_PER_WORKER, RR_SIZE / WORKERS_PER_MACHINE);
  if (HERD_GET_REQ_SIZE != (KEY_SIZE + 1))
    red_printf("ATTENTION: GET REQ SIZE is not what expected!!!\n");
	// Send requests
	///assert(ENABLE_MULTI_BATCHES == 0 || CLIENT_SEND_REM_Q_DEPTH > MAX_OUTSTANDING_REQS);	// Clients remotes
	///assert(WORKER_SEND_Q_DEPTH > WORKER_MAX_BATCH); // Worker responses
	// RECVS
	//assert(WORKER_RECV_Q_DEPTH > CLIENTS_PER_MACHINE * (MACHINE_NUM - 1) * WS_PER_WORKER); /* Check if there is enough space for the RECVs*/
	if (DISABLE_CACHE == 0) {
		assert(SC_CLIENT_RECV_CR_Q_DEPTH > MAX_CREDIT_RECVS); //sc credit recvs
		assert(SC_CLIENT_RECV_BR_Q_DEPTH > MAX_COH_RECEIVES);  // sc coh recvs
	}
	///assert(CLIENT_RECV_REM_Q_DEPTH > WINDOW_SIZE);// client receives
	///assert(ENABLE_MULTI_BATCHES == 0 || CLIENT_RECV_REM_Q_DEPTH > MAX_OUTSTANDING_REQS);

	///assert(LOCAL_WINDOW <= MICA_MAX_BATCH_SIZE); // When doing locals you may batch to MICA up to WINDOW_SIZE
	///assert(WORKER_MAX_BATCH <= MICA_MAX_BATCH_SIZE);
	if (WRITE_RATIO > 0 && DISABLE_CACHE == 0) assert(BCAST_TO_CACHE_BATCH <= CACHE_BATCH_SIZE); // Cache uses cache batch size to statically allocate all its arrays

	assert(CREDITS_FOR_EACH_CLIENT % SC_CREDIT_DIVIDER == 0);
	assert(MAX_COH_MESSAGES < K_64); // incoming coh message count is a 16 bits
	assert(SESSIONS_PER_CLIENT * CLIENTS_PER_MACHINE <= 255); // well key.meta.cid is one byte..
	///assert(WS_PER_WORKER < 256); // we use 8 bit-integers to track this
	///assert(LOCAL_WINDOW % LOCAL_REGIONS == 0); // we need perfect division
	///assert(LOCAL_REGIONS <= 64); // available bytes in a cache lien to index the local_recv_flag
//	if (ENABLE_LOCAL_WORKERS) {
//		assert(LOCAL_WORKERS > 0 && LOCAL_WORKERS < WORKERS_PER_MACHINE);
//		if (CLIENTS_PER_MACHINE % LOCAL_WORKERS != 0)
//			cyan_printf("Performance Warning: Clients(%d) are not perfectly divided by the local workers(%d)\n",
//						CLIENTS_PER_MACHINE, LOCAL_WORKERS);
//	}
	///assert(CLIENTS_PER_MACHINE >= WORKER_NUM_UD_QPS);
	//SELECTIVE SIGNALING // +3 for good measure when comparing with hrd_q_depth
	assert(MAX_BCAST_BATCH < BROADCAST_SS_BATCH);

	//assert(sizeof(struct cache_op) == sizeof(struct mica_op));
	assert(sizeof(struct mica_op) > HERD_PUT_REQ_SIZE);
	assert(sizeof(struct ud_req) == UD_REQ_SIZE);
	assert(sizeof(struct mica_op) == MICA_OP_SIZE);
//	assert(sizeof(struct cache_op) == CACHE_OP_SIZE);
	assert(sizeof(struct mica_key) == KEY_SIZE);

//	cyan_printf("Size of worker req: %d, extra bytes: %d, ud req size: %d minimum worker"
//									" req size %d, actual size of req_size %d, extended cache ops size %d  \n",
//							WORKER_REQ_SIZE, EXTRA_WORKER_REQ_BYTES, UD_REQ_SIZE, MINIMUM_WORKER_REQ_SIZE, sizeof(struct wrkr_ud_req),
//							sizeof(struct extended_cache_op));
//  yellow_printf("Size of worker send req: %d, expected size %d  \n",
 //               sizeof(struct wrkr_coalesce_mica_op), WORKER_SEND_BUFF_SIZE);
	assert(sizeof(struct extended_cache_op) <= sizeof(struct wrkr_ud_req) - GRH_SIZE);
	///if (WORKER_HYPERTHREADING) assert(WORKERS_PER_MACHINE <= VIRTUAL_CORES_PER_SOCKET);

	// assert(CLIENT_SS_BATCH > WINDOW_SIZE && HRD_Q_DEPTH > CLIENT_SS_BATCH + 3);	// clients' remote reqs
	// assert(WORKER_SS_BATCH > WORKER_MAX_BATCH && HRD_Q_DEPTH > WORKER_SS_BATCH + 3);
	if (DISABLE_CACHE == 0) {
		//assert((BROADCAST_SS_BATCH * MESSAGES_IN_BCAST) + 1 < EC_CLIENT_SEND_BR_Q_DEPTH);
		// assert(HRD_Q_DEPTH > (BROADCAST_SS_BATCH * (MESSAGES_IN_BCAST)) + ACK_SS_GRANULARITY + 3);
		// assert(HRD_Q_DEPTH > CREDIT_SS_BATCH + 3);
		// assert(HRD_Q_DEPTH > EC_CREDIT_SS_BATCH + 3);
	}

	// WORKER BUFFER SIZE
	assert(EXTRA_WORKER_REQ_BYTES >= 0);
	///assert(WORKER_REQ_SIZE <= sizeof(struct wrkr_ud_req));
	assert(BASE_VALUE_SIZE % pow2roundup(SHIFT_BITS) == 0);
//  if (ENABLE_WORKER_COALESCING == 0) assert(CLIENT_REMOTE_BUFF_SIZE == UD_REQ_SIZE);
  ///if ((ENABLE_COALESCING == 1) && (DESIRED_COALESCING_FACTOR < MAX_COALESCE_PER_MACH)) assert(ENABLE_WORKER_COALESCING == 0);

  /* Cannot coalesce beyond 11 reqs, because when inlining is open it must be used, because NIC will read asynchronously otherwise */
  if (CLIENT_ENABLE_INLINING == 1) assert((HERD_GET_REQ_SIZE) + 1 <= MAXIMUM_INLINE_SIZE);
	///if (CLIENT_ENABLE_INLINING == 1) assert((MAX_COALESCE_PER_MACH * HERD_GET_REQ_SIZE) + 1 <= MAXIMUM_INLINE_SIZE);

  assert(CACHE_BATCH_OP_SC == cache_batch_op_sc_non_stalling_sessions);

	int i, c;
	is_master = -1; is_client = -1;
	int num_threads = -1;
	int  postlist = -1, update_percentage = -1;
	int base_port_index = -1, num_server_ports = -1, num_client_ports = -1;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *)malloc(16 * sizeof(char));


	struct thread_params *param_arr;
	pthread_t *thread_arr;

	static struct option opts[] = {
			{ .name = "master",				.has_arg = 1, .val = 'M' },
			{ .name = "num-threads",		.has_arg = 1, .val = 't' },
			{ .name = "base-port-index",	.has_arg = 1, .val = 'b' },
			{ .name = "num-server-ports",	.has_arg = 1, .val = 'N' },
			{ .name = "num-client-ports",	.has_arg = 1, .val = 'n' },
			{ .name = "is-client",		 	.has_arg = 1, .val = 'c' },
			{ .name = "update-percentage",	.has_arg = 1, .val = 'u' },
			{ .name = "machine-id",			.has_arg = 1, .val = 'm' },
			{ .name = "postlist",			.has_arg = 1, .val = 'p' },
			{ .name = "is-roce",			.has_arg = 1, .val = 'r' },
			{ .name = "remote-ips",			.has_arg = 1, .val = 'i' },
			{ .name = "local-ip",			.has_arg = 1, .val = 'l' },
			{ .name = "num-machines", 		.has_arg = 1, .val = 'x' },
			{ 0 }
	};

	/* Parse and check arguments */
	while(1) {
		c = getopt_long(argc, argv, "M:t:b:N:n:c:u:m:p:r:i:l:x", opts, NULL);
		if(c == -1) {
			break;
		}
		switch (c) {
			case 'M':
				is_master = atoi(optarg);
				assert(is_master == 1);
				break;
			case 't':
				num_threads = atoi(optarg);
				break;
			case 'b':
				base_port_index = atoi(optarg);
				break;
			case 'N':
				num_server_ports = atoi(optarg);
				break;
			case 'n':
				num_client_ports = atoi(optarg);
				break;
			case 'c':
				is_client = atoi(optarg);
				break;
			case 'u':
				update_percentage = atoi(optarg);
				break;
			case 'm':
				machine_id = atoi(optarg);
				break;
			case 'p':
				postlist = atoi(optarg);
				break;
			case 'r':
				is_roce = atoi(optarg);
				break;
			case 'i':
				remote_IP = optarg;
				break;
			case 'l':
				local_IP = optarg;
				break;
			case 'x':
				machines_num = atoi(optarg);
				break;
			default:
				printf("Invalid argument %d\n", c);
				assert(false);
		}
	}


//	printf("coalesce size %d worker inlining %d, client inlining %d \n",
//				 MAX_COALESCE_PER_MACH, WORKER_ENABLE_INLINING, CLIENT_ENABLE_INLINING);
	//yellow_printf("remote send queue depth %d, remote send ss batch %d \n", CLIENT_SEND_REM_Q_DEPTH, CLIENT_SS_BATCH);
	/* Launch multiple worker threads and multiple client threads */
	assert(machine_id < MACHINE_NUM && machine_id >=0);
	//num_threads = CLIENTS_PER_MACHINE > WORKERS_PER_MACHINE ? CLIENTS_PER_MACHINE : WORKERS_PER_MACHINE;
	num_threads = CLIENTS_PER_MACHINE;

	param_arr = malloc(num_threads * sizeof(struct thread_params));
	thread_arr = malloc((CLIENTS_PER_MACHINE + 1) * sizeof(pthread_t));
	///local_req_region = (struct mica_op *)malloc(CLIENTS_PER_MACHINE * LOCAL_WINDOW * sizeof(struct mica_op));
	memset((struct client_stats*) w_stats, 0, CLIENTS_PER_MACHINE * sizeof(struct client_stats));
	///memset((struct worker_stats*) w_stats, 0, WORKERS_PER_MACHINE * sizeof(struct worker_stats));
	int j, k;
//	for (i = 0; i < WORKERS_PER_MACHINE; i++) {
//		for (j = 0; j < CLIENTS_PER_MACHINE; j++) {
//			for (k = 0; k < LOCAL_REGIONS; k++)
//				local_recv_flag[i][j][k] = 0;
//			for (k = 0; k < LOCAL_WINDOW; k++) {
//				int offset = OFFSET(i, j, k);
//				local_req_region[offset].opcode = 0;
//			}
//		}
//
//	}
	clt_needed_ah_ready = 0;
	wrkr_needed_ah_ready = 0;
	cache_init(machine_id, CLIENTS_PER_MACHINE); // the first ids are taken by the workers

//#if ENABLE_WORKERS_CRCW == 1
//	mica_init(&kv, 0, 0, HERD_NUM_BKTS, HERD_LOG_CAP); // second 0 refers to numa node
//	cache_populate_fixed_len(&kv, HERD_NUM_KEYS, HERD_VALUE_SIZE);
//	optik_init(&kv_lock);
//#endif

#if MEASURE_LATENCY == 1
	latency_count.hot_writes  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.hot_reads   = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.local_reqs  = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
	latency_count.remote_reqs = (uint32_t*) malloc(sizeof(uint32_t) * (LATENCY_BUCKETS + 1)); // the last latency bucket is to capture possible outliers (> than LATENCY_MAX)
  latency_count.total_measurements = 0;
#endif
	//memset(local_req_region, 0, WORKERS_PER_MACHINE * CLIENTS_PER_MACHINE * WINDOW_SIZE * sizeof(struct mica_op));
	//memset(clt_ah, 0, CLIENT_NUM * MACHINE_NUM * sizeof(uintptr_t));
	// printf("RANDOM req opcode: %d\n", local_req_region[1][1].opcode );
	pthread_attr_t attr;
	cpu_set_t cpus_c, cpus_w, cpus_stats;
	pthread_attr_init(&attr);
	int next_node_i = -1;
	int occupied_cores[TOTAL_CORES] = { 0 };
	for(i = 0; i < num_threads; i++) {
		param_arr[i].id = i;
		param_arr[i].postlist = postlist;
		param_arr[i].base_port_index = base_port_index;
		param_arr[i].num_server_ports = num_server_ports;
		param_arr[i].num_client_ports = num_client_ports;
		param_arr[i].update_percentage = update_percentage;

		if (i < CLIENTS_PER_MACHINE ) { // spawn clients
			int c_core = pin_client(i);
			yellow_printf("Creating client thread %d at core %d \n", param_arr[i].id, c_core);
			CPU_ZERO(&cpus_c);
			CPU_SET(c_core, &cpus_c);
			pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_c);
			pthread_create(&thread_arr[i], &attr, run_client, &param_arr[i]);// change NULL here to &attr to get the thread affinity
			occupied_cores[c_core] = 1;
		}
//		if ( i < WORKERS_PER_MACHINE) { // spawn workers
//			int w_core = pin_worker(i);
//			green_printf("Creating worker thread %d at core %d \n", param_arr[i].id, w_core);
//			CPU_ZERO(&cpus_w);
//			CPU_SET(w_core, &cpus_w);
//
//			pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_w);
//			pthread_create(&thread_arr[i + CLIENTS_PER_MACHINE], &attr, run_worker, &param_arr[i]);// change NULL here to &attr to get the thread affinity
//			occupied_cores[w_core] = 1;
//		}
	}


	if (ENABLE_SS_DEBUGGING == 1) {
		if (CREDITS_IN_MESSAGE != CREDITS_FOR_EACH_CLIENT) red_printf("CREDITS IN MESSAGE is bigger than 1: %d, that could cause a deadlock.. \n", CREDITS_IN_MESSAGE);

		if (MAX_CREDIT_WRS >= MIN_SS_BATCH) printf("MAX_CREDIT_WRS is %d, CREDIT_SS_BATCH is %d \n", MAX_CREDIT_WRS, CREDIT_SS_BATCH);
///		if (EC_MAX_CREDIT_WRS >= MIN_SS_BATCH) printf("EC_MAX_CREDIT_WRS is %d, EC_CREDIT_SS_BATCH is %d \n", EC_MAX_CREDIT_WRS, EC_CREDIT_SS_BATCH);
///		if (WORKER_MAX_BATCH >= MIN_SS_BATCH) printf("WORKER_MAX_BATCH is %d, WORKER_SS_BATCH is %d \n", WORKER_MAX_BATCH, WORKER_SS_BATCH);
		if (WINDOW_SIZE >= MIN_SS_BATCH) printf("WINDOW_SIZE is %d, CLIENT_SS_BATCH is %d \n", WINDOW_SIZE, CLIENT_SS_BATCH);
		if (MESSAGES_IN_BCAST_BATCH >= MIN_SS_BATCH) printf("MAX_MSGS_IN_PCIE_BCAST_BATCH is %d, BROADCAST_SS_BATCH is %d \n", MESSAGES_IN_BCAST_BATCH, BROADCAST_SS_BATCH);
		if (BCAST_TO_CACHE_BATCH >= MIN_SS_BATCH) printf("BCAST_TO_CACHE_BATCH is %d, ACK_SS_GRANULARITY is %d \n", BCAST_TO_CACHE_BATCH, ACK_SS_BATCH);
	}


	for(i = 0; i < CLIENTS_PER_MACHINE + 1; i++) {
		pthread_join(thread_arr[i], NULL); // is this even useful?
	}

	return 0;
}



