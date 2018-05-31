#include "util.h"
#include "config.h"


void *run_worker(void *arg){
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t wrkr_lid = params.id;	/* Local ID of this worker thread*/
	//int num_server_ports = params.num_server_ports, base_port_index = params.base_port_index;
	//int num_server_ports = 1, base_port_index = 0;

	// Initialize a control block
	struct hrd_ctrl_blk* cb = hrd_ctrl_blk_init(
			wrkr_lid,                /* local_hid */
			0, -1,            /* port_index, numa_node_id */
			1, 0,             /* #conn qps, uc */
			NULL, 4096, -1,   /* prealloc conn buf, buf size, key */
			0, 0, -1, 0, 0);  /* num_dgram_qps, dgram_buf_size, key, recv q dept, send q depth*/

	struct hrd_qp_attr** remote_qps = setup_and_return_qps(wrkr_lid, cb);
	const int remote_qps_size = (WORKER_NUM - WORKERS_PER_MACHINE);

	printf("Connection is up\n");
	//

	/* Some tracking info */
	int ws[WORKER_NUM] = {0}; /* Window slot to use for a worker */

	//struct mica_op* req_buf = memalign(4096, sizeof(struct mica_op));
	//assert(req_buf != NULL);

	uint64_t seed = 0xdeadbeef;
	struct ibv_send_wr wr, *bad_send_wr;
	struct ibv_sge sgl;
	struct ibv_wc wc[WINDOW_SIZE];

	//struct ibv_recv_wr recv_wr[WINDOW_SIZE], *bad_recv_wr;
	//struct ibv_sge recv_sgl[WINDOW_SIZE];

	long long rolling_iter = 0; /* For throughput measurement */
	long long nb_tx = 0;        /* Total requests performed or queued */
	int wn = 0;                 /* Worker number */
	int ret, is_update, req_counter = 0;
	int* object = (int*) malloc(sizeof(int));
	*object = 41194;
	// start the big loop
	while (1) {

		wn = hrd_fastrand(&seed) % remote_qps_size; /* Choose a remote worker */
		is_update = (hrd_fastrand(&seed) % 100 < WRITE_RATIO) ? 1 : 0;
		/* Forge the HERD request */
		//key_i = hrd_fastrand(&seed) % HERD_NUM_KEYS; /* Choose a key */

		//*(uint128*)req_buf = CityHash128((char*)&key_arr[key_i], 4);
		//req_buf->opcode = is_update ? HERD_OP_PUT : HERD_OP_GET;
		//req_buf->val_len = is_update ? HERD_VALUE_SIZE : -1;

		/* Forge the RDMA work request */
		sgl.length = sizeof(int); //is_update ? HERD_PUT_REQ_SIZE : HERD_GET_REQ_SIZE;
		sgl.addr = (uint64_t) (uintptr_t) object;

		wr.opcode = IBV_WR_RDMA_WRITE;
		wr.num_sge = 1;
		wr.next = NULL;
		wr.sg_list = &sgl;

		wr.send_flags = (req_counter & MSG_GRAN_OF_SELECTIVE_SIGNALING) == 0 ? IBV_SEND_SIGNALED : 0;
		if ((req_counter & MSG_GRAN_OF_SELECTIVE_SIGNALING_) == MSG_GRAN_OF_SELECTIVE_SIGNALING_){
			hrd_poll_cq(cb->conn_cq[0], 1, wc);
			//printf("Issuing Req (%d) - Worker %d, Machine %d: sending to remote worker %d\n", req_counter, wrkr_lid, machine_id, wn);
		}
		wr.send_flags |= IBV_SEND_INLINE;
		wr.wr.rdma.remote_addr = remote_qps[wn]->buf_addr; //+ OFFSET(wn, clt_gid, ws[wn]) * sizeof(struct mica_op);
		wr.wr.rdma.rkey = remote_qps[wn]->rkey;

		ret = ibv_post_send(cb->conn_qp[0], &wr, &bad_send_wr);
        req_counter++;
		printf("Issuing Req (%d) - Worker %d, Machine %d: sending to remote worker %d\n", req_counter, wrkr_lid, machine_id, wn);
		CPE(ret, "ibv_post_send error", ret);
//		else{
//		//if (nb_tx % WINDOW_SIZE == 0 && nb_tx > 0) {
//			hrd_poll_cq(cb->conn_cq[0], WINDOW_SIZE, wc);
//			req_counter = 0;
//		}
		rolling_iter++;
		//hrd_poll_cq_ret();
		//hrd_poll_cq();
		//HRD_MOD_ADD(ws[wn], WINDOW_SIZE);
	}
	return NULL;
}
