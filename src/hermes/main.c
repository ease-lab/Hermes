#define _GNU_SOURCE
#include <stdio.h>
#include <malloc.h>
#include <infiniband/verbs.h>
#include <hrd.h>
#include <getopt.h>
#include <pthread.h>
#include "spacetime.h"
#include "config.h"
#include "util.h"
#include <optik_mod.h>

//Global vars
volatile char worker_needed_ah_ready;
volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];
volatile struct remote_qp remote_worker_qps[WORKER_NUM][TOTAL_WORKER_UD_QPs];


int main(int argc, char *argv[]){
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

	assert(MACHINE_NUM < TIE_BREAKER_ID_EMPTY);
	assert(MACHINE_NUM < LAST_WRITER_ID_EMPTY);
	assert(MAX_BATCH_OPS_SIZE < ST_OP_BUFFER_INDEX_EMPTY); /// 1B write_buffer_index and 255 is used as "empty" value

    ///Make sure that assigned numbers to States are monotonically increasing with the following order
	assert(VALID_STATE < INVALID_STATE);
	assert(INVALID_STATE < INVALID_WRITE_STATE);
	assert(INVALID_WRITE_STATE < WRITE_STATE);
	assert(WRITE_STATE < REPLAY_STATE);
//	green_printf("UD size: %d ibv_grh + crd size: %d \n", sizeof(ud_req_crd_t), sizeof(struct ibv_grh) + sizeof(spacetime_crd_t));
//	assert(sizeof(ud_req_crd_t) == sizeof(struct ibv_grh) + sizeof(spacetime_crd_t)); ///CRD --> 48 Bytes instead of 43

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

	static struct option opts[] = {
			{ .name = "machine-id",			.has_arg = 1, .val = 'm' },
            { .name = "is-roce",			.has_arg = 1, .val = 'r' },
			{ 0 }
	};

	/* Parse and check arguments */
	while(1) {
		c = getopt_long(argc, argv, "m:r:", opts, NULL);
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

	pthread_attr_init(&attr);
    int w_core ;
	for(i = 0; i < WORKERS_PER_MACHINE; i++) {
		if(USE_ALL_CORES)
            w_core = i;
		else
			w_core = START_SPAWNING_THREADS_FROM_SOCKET +  (ENABLE_HYPERTHREADING == 1 ? 2 * i : 4 * i ); // use socket one cores
        assert(w_core < TOTAL_CORES);
		param_arr[i].id = i;

		green_printf("Creating worker thread %d at core %d \n", param_arr[i].id, w_core);
		CPU_ZERO(&cpus_w);
		CPU_SET(w_core, &cpus_w);
		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_w);
		pthread_create(&thread_arr[i], &attr, run_worker, &param_arr[i]);
	}
	yellow_printf("{Sizes} Op: %d, Meta %d, Value %d,\n",
				 sizeof(spacetime_op_t), sizeof(spacetime_object_meta), ST_VALUE_SIZE);
	yellow_printf("{Op Sizes} Inv: %d, Ack: %d, Val: %d, Crd: %d\n",
				 sizeof(spacetime_inv_t), sizeof(spacetime_ack_t), sizeof(spacetime_val_t), sizeof(spacetime_crd_t));
	yellow_printf("{Max Coalesce Packet Sizes} Inv: %d, Ack: %d, Val: %d\n",
				 sizeof(spacetime_inv_packet_t), sizeof(spacetime_ack_packet_t), sizeof(spacetime_val_packet_t));

	for(i = 0; i < WORKERS_PER_MACHINE; i++)
		pthread_join(thread_arr[i], NULL);

	return 0;
}
