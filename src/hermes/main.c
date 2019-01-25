#define _GNU_SOURCE
#include <stdio.h>
#include <malloc.h>
#include <infiniband/verbs.h>
#include <getopt.h>
#include <pthread.h>
#include "spacetime.h"
#include "config.h"
#include "util.h"
#include "concur_ctrl.h"
#include "bit_vector.h"
#include "hrd.h"


//	//TODO only for testing
#include "../aether/ud-wrapper.h"

//Global vars
volatile char worker_needed_ah_ready;
struct latency_counters latency_count;
volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];
volatile uint8_t node_suspicions[WORKERS_PER_MACHINE][MACHINE_NUM];
volatile struct remote_qp remote_worker_qps[WORKER_NUM][TOTAL_WORKER_UD_QPs];


int main(int argc, char *argv[])
{
	int i, c;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *) malloc(16 * sizeof(char));

    assert(MICA_MAX_VALUE >= ST_VALUE_SIZE);
    assert(MACHINE_NUM <= 8); //TODO haven't test bit vectors with more than 8 nodes
    assert(MACHINE_NUM <= GROUP_MEMBERSHIP_ARRAY_SIZE * 8);//bit vector for acks / group membership
    assert(sizeof(spacetime_crd_t) < sizeof(((struct ibv_send_wr*)0)->imm_data)); //for inlined credits
	assert(MACHINE_NUM <= 255);
    assert(CREDITS_PER_REMOTE_WORKER <= MAX_BATCH_OPS_SIZE);

    assert(MAX_PCIE_BCAST_BATCH <= INV_CREDITS);
	assert(MAX_PCIE_BCAST_BATCH <= VAL_CREDITS);
	assert(MAX_PCIE_BCAST_BATCH <= INV_SS_GRANULARITY);
	assert(MAX_PCIE_BCAST_BATCH <= VAL_SS_GRANULARITY);

//	assert(SOCKET_TO_START_SPAWNING_THREADS < TOTAL_NUMBER_OF_SOCKETS);
	assert((ENABLE_HYPERTHREADING == 1 && USE_ALL_SOCKETS == 1) || WORKERS_PER_MACHINE <= TOTAL_CORES_PER_SOCKET);
	assert(WORKERS_PER_MACHINE <= TOTAL_HW_CORES);

	assert(INCREASE_TAIL_LATENCY == 0 || NUM_OF_CORES_TO_INCREASE_TAIL <= WORKERS_PER_MACHINE);

	///Assertions for failures
	assert(!(INCREASE_TAIL_LATENCY && FAKE_FAILURE));
    assert(FAKE_FAILURE == 0 || NODES_WITH_FAILURE_DETECTOR >= 1);
	assert(FAKE_FAILURE == 0 || NODE_TO_FAIL < MACHINE_NUM);
	assert(FAKE_FAILURE == 0 || ROUNDS_BEFORE_FAILURE < PRINT_NUM_STATS_BEFORE_EXITING);
	assert(FAKE_FAILURE == 0 || WORKER_EMULATING_FAILURE_DETECTOR < WORKERS_PER_MACHINE);

	assert(MACHINE_NUM < TIE_BREAKER_ID_EMPTY);
	assert(MACHINE_NUM < LAST_WRITER_ID_EMPTY);
	assert(MAX_BATCH_OPS_SIZE < ST_OP_BUFFER_INDEX_EMPTY); /// 1B write_buffer_index and 255 is used as "empty" value

    ///Make sure that assigned numbers to States are monotonically increasing with the following order
	assert(VALID_STATE < INVALID_STATE);
	assert(INVALID_STATE < INVALID_WRITE_STATE);
	assert(INVALID_WRITE_STATE < WRITE_STATE);
	assert(WRITE_STATE < REPLAY_STATE);

	assert(MEASURE_LATENCY == 0 || THREAD_MEASURING_LATENCY < WORKERS_PER_MACHINE);
//	green_printf("UD size: %d ibv_grh + crd size: %d \n", sizeof(ud_req_crd_t), sizeof(struct ibv_grh) + sizeof(spacetime_crd_t));
//	assert(sizeof(ud_req_crd_t) == sizeof(struct ibv_grh) + sizeof(spacetime_crd_t)); ///CRD --> 48 Bytes instead of 43

//	//TODO only for testing
	ud_wrapper_unit_test();
//	bv_unit_test();
	return 0;


	printf("CREDITS %d\n",CREDITS_PER_REMOTE_WORKER);
	printf("INV_SS_GRANULARITY %d \t\t SEND_INV_Q_DEPTH %d \t\t RECV_INV_Q_DEPTH %d\n",
		   INV_SS_GRANULARITY, SEND_INV_Q_DEPTH, RECV_INV_Q_DEPTH);
	printf("ACK_SS_GRANULARITY %d \t\t SEND_ACK_Q_DEPTH %d \t\t RECV_ACK_Q_DEPTH %d\n",
		   ACK_SS_GRANULARITY, SEND_ACK_Q_DEPTH, RECV_ACK_Q_DEPTH);
	printf("VAL_SS_GRANULARITY %d \t\t SEND_VAL_Q_DEPTH %d \t\t RECV_VAL_Q_DEPTH %d\n",
		   VAL_SS_GRANULARITY, SEND_VAL_Q_DEPTH, RECV_VAL_Q_DEPTH);
	printf("CRD_SS_GRANULARITY %d \t\t SEND_CRD_Q_DEPTH %d \t\t RECV_CRD_Q_DEPTH %d\n",
		   CRD_SS_GRANULARITY, SEND_CRD_Q_DEPTH, RECV_CRD_Q_DEPTH);

	struct thread_params *param_arr;
	pthread_t *thread_arr;
//	char dev_name[50];

	static struct option opts[] = {
			{ .name = "machine-id",			.has_arg = 1, .val = 'm' },
            { .name = "is-roce",			.has_arg = 1, .val = 'r' },
			{ .name = "dev-name",			.has_arg = 1, .val = 'd'},
			{ 0 }
	};

	/* Parse and check arguments */
	while(1) {
		c = getopt_long(argc, argv, "m:r:d:", opts, NULL);
		if(c == -1) {
			break;
		}
		switch (c) {
			case 'm':
				machine_id = atoi(optarg);
				break;
			case 'r':
				is_roce = atoi(optarg);
				break;
			case 'd':
				memcpy(dev_name, optarg, strlen(optarg));
				break;
			default:
				printf("Invalid argument %d\n", c);
				assert(false);
		}
	}

	param_arr = malloc(WORKERS_PER_MACHINE * sizeof(struct thread_params));
	thread_arr = malloc(WORKERS_PER_MACHINE * sizeof(pthread_t));


	pthread_attr_t attr;
	cpu_set_t cpus_w;
	worker_needed_ah_ready = 0;
	group_membership_init();
	spacetime_init(machine_id, WORKERS_PER_MACHINE);
	init_stats();


//	// Register signal and signal handler
//	signal(SIGINT, signal_callback_handler);

	pthread_attr_init(&attr);
    int w_core, init_core = SOCKET_TO_START_SPAWNING_THREADS;
	for(i = 0; i < WORKERS_PER_MACHINE; i++) {
		if(USE_ALL_SOCKETS && ENABLE_HYPERTHREADING)
            w_core = init_core + i;
		else
			w_core = 2 * i + init_core;
//        if(w_core > 19 ) w_core+=4;
        assert(ENABLE_HYPERTHREADING || w_core < TOTAL_NUMBER_OF_SOCKETS * TOTAL_CORES_PER_SOCKET);
		assert(w_core < TOTAL_HW_CORES);
		param_arr[i].id = i;

		green_printf("Creating worker thread %d at core %d \n", param_arr[i].id, w_core);
		CPU_ZERO(&cpus_w);
		CPU_SET(w_core, &cpus_w);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_w);
		pthread_create(&thread_arr[i], &attr, run_worker, &param_arr[i]);
	}

	yellow_printf("Sizes: {Op: %d, Object Meta %d, Value %d},\n",
				 sizeof(spacetime_op_t), sizeof(spacetime_object_meta), ST_VALUE_SIZE);
	yellow_printf("Coherence msg Sizes: {Inv: %d, Ack: %d, Val: %d, Crd: %d}\n",
				 sizeof(spacetime_inv_t), sizeof(spacetime_ack_t), sizeof(spacetime_val_t), sizeof(spacetime_crd_t));
	yellow_printf("Max Coalesce Packet Sizes: {Inv: %d, Ack: %d, Val: %d}\n",
				 sizeof(spacetime_inv_packet_t), sizeof(spacetime_ack_packet_t), sizeof(spacetime_val_packet_t));

	for(i = 0; i < WORKERS_PER_MACHINE; i++)
		pthread_join(thread_arr[i], NULL);

	return 0;
}
