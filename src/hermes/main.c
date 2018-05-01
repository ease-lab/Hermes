#define _GNU_SOURCE
#include <stdio.h>
#include <malloc.h>
#include <elf.h>
#include <infiniband/verbs.h>
#include <hrd.h>
#include <getopt.h>
#include <pthread.h>
#include "config.h"
#include "util.h"

//Global vars
volatile struct client_stats c_stats[CLIENTS_PER_MACHINE];
volatile struct worker_stats w_stats[WORKERS_PER_MACHINE];

int main(int argc, char *argv[]){
	int i, c;
	is_master = -1; is_client = -1;
	int num_threads = -1;
	int  postlist = -1, update_percentage = -1;
	int base_port_index = -1, num_server_ports = -1, num_client_ports = -1;
	is_roce = -1; machine_id = -1;
	remote_IP = (char *) malloc(16 * sizeof(char));


	struct thread_params *param_arr;
	//struct issuer_params issuer_arr[ISSUERS_PER_MACHINE];
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

	param_arr = malloc(WORKERS_PER_MACHINE * sizeof(struct thread_params));
	thread_arr = malloc(WORKERS_PER_MACHINE * sizeof(pthread_t));


/*#if ENABLE_WORKERS_CRCW == 1
	mica_init(&kv, 0, 0, HERD_NUM_BKTS, HERD_LOG_CAP); // second 0 refers to numa node
	cache_populate_fixed_len(&kv, HERD_NUM_KEYS, HERD_VALUE_SIZE);
	optik_init(&kv_lock);
#endif*/

    qps_published = 0;
	pthread_attr_t attr;
	cpu_set_t cpus_w;
	pthread_attr_init(&attr);
	for(i = 0; i < WORKERS_PER_MACHINE; i++) {
		param_arr[i].id = i;
		param_arr[i].postlist = postlist;
		param_arr[i].base_port_index = base_port_index;
		param_arr[i].num_server_ports = num_server_ports;
		param_arr[i].num_client_ports = num_client_ports;
		param_arr[i].update_percentage = update_percentage;

		int w_core = i;
		green_printf("Creating worker thread %d at core %d \n", param_arr[i].id, w_core);
		CPU_ZERO(&cpus_w);
		CPU_SET(w_core, &cpus_w);

		pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpus_w);
		pthread_create(&thread_arr[i], &attr, run_worker, &param_arr[i]);// change NULL here to &attr to get the thread affinity
	}

	for(i = 0; i < WORKERS_PER_MACHINE; i++)
		pthread_join(thread_arr[i], NULL);

	return 0;
}


/*int main(void){

	const size_t SIZE = 1024;

	//    printf("Remote clients buffer size %d\n", remote_buf_size);

	struct hrd_ctrl_blk* cb = hrd_ctrl_blk_init(
			0,                *//* local_hid *//*
			0, -1,            *//* port_index, numa_node_id *//*
			1, 1,             *//* #conn qps, uc *//*
			NULL, 4096, -1,  *//* prealloc conn buf, buf size, key *//*
			0, 0, -1, 0, 0); *//* num_dgram_qps, dgram_buf_size, key, recv q dept, send q depth*//*

	char *buffer = malloc(SIZE);
	struct ibv_mr *mr;
	uint32_t my_key;
	uint64_t my_addr;

	mr = ibv_reg_mr(
			cb->pd,
			buffer,
			SIZE,
			IBV_ACCESS_REMOTE_WRITE);

	my_key = mr->rkey;
	my_addr = (uint64_t)mr->addr;
	return 0;
}*/
