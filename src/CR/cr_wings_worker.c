#include <spacetime.h>
#include <concur_ctrl.h>
#include <time.h>
#include "util.h"
#include "inline-util.h"

///
#include "../../include/utils/time_rdtsc.h"
#include "../../include/wings/wings.h"
///

static inline uint8_t
head_id(void)
{
	return (uint8_t) 0;
}

static inline uint8_t
tail_id(void)
{
	return MACHINE_NUM - 1;
}


static inline uint8_t
next_node_in_chain(void)
{
	return (uint8_t) ((machine_id + 1) % MACHINE_NUM);
}

static inline uint8_t
prev_node_in_chain(void)
{
	return (uint8_t) (machine_id == 0 ? tail_id() : machine_id - 1);
}


int
inv_skip_or_fwd_to_next_node(uint8_t *req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;
	return inv_req->op_meta.opcode == ST_INV_SUCCESS ? next_node_in_chain() : -1; // invs should only be fwded to next node
}




void
inv_fwd_modify_elem_after_send(uint8_t* req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;

	//empty inv buffer
	if(inv_req->op_meta.opcode == ST_INV_SUCCESS ||
	   inv_req->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE)
		inv_req->op_meta.opcode = ST_EMPTY;

	else assert(0);
}

void
inv_fwd_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_inv_t* inv_recv = (spacetime_inv_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv and set opcode
	memcpy(inv_to_send, inv_recv, sizeof(spacetime_inv_t));
	inv_to_send->op_meta.opcode = ST_OP_INV;
}




int
inv_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(is_input_code(op_req->op_meta.opcode));
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
	}

	return op_req->op_meta.state == ST_PUT_SUCCESS ? next_node_in_chain() : -1; // since invs should only be fwded to next node
}

void
inv_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(op_req->op_meta.state == ST_PUT_SUCCESS)
		op_req->op_meta.state = ST_IN_PROGRESS_PUT;
	else assert(0);
}

void
inv_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	if(ENABLE_ASSERTIONS)
		assert(machine_id == head_id());

	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(inv_to_send, op, sizeof(spacetime_inv_t));

	inv_to_send->op_meta.opcode = ST_OP_INV;
	inv_to_send->op_meta.initiator = (uint8_t) machine_id;
}



int
remote_write_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(is_input_code(op_req->op_meta.opcode));
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
	}

	return op_req->op_meta.state == ST_PUT_SUCCESS ? head_id() : -1; // send remote writes to head
}

void
remote_write_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	if(ENABLE_ASSERTIONS)
		assert(machine_id != head_id());

	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(inv_to_send, op, sizeof(spacetime_inv_t));

	inv_to_send->op_meta.state = ST_NEW;
	inv_to_send->op_meta.opcode = ST_OP_PUT;
	inv_to_send->initiator = (uint8_t) machine_id;
	inv_to_send->op_meta.initiator = (uint8_t) machine_id;
}



int
remote_write_head_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(machine_id == head_id());
		assert(is_input_code(op_req->op_meta.opcode) || op_req->op_meta.opcode == ST_EMPTY);
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
	}

	return op_req->op_meta.state == ST_PUT_SUCCESS ? next_node_in_chain() : -1; // remote writes must always be fwded to head
}

void
remote_write_head_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_inv_t* inv_to_send = (spacetime_inv_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(inv_to_send, op, sizeof(spacetime_inv_t));

	inv_to_send->op_meta.opcode = ST_OP_INV;
	inv_to_send->op_meta.initiator = op->initiator;
}

void
remote_write_head_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(op_req->op_meta.state == ST_PUT_SUCCESS)
		op_req->op_meta.state = ST_SEND_CRD;
	else assert(0);
}



void
ack_fwd_modify_elem_after_send(uint8_t* req)
{
	spacetime_ack_t* ack_req = (spacetime_ack_t *) req;

	if(ENABLE_ASSERTIONS)
		assert(ack_req->opcode == ST_LAST_ACK_SUCCESS);

	ack_req->opcode = ST_EMPTY;
}

int
ack_fwd_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_ack_t* ack_req = (spacetime_ack_t *) req;
	if (ack_req->opcode == ST_ACK_SUCCESS)
	{
		ack_req->opcode = ST_EMPTY;
		return -1;
	} else if (ack_req->opcode == ST_EMPTY)
		return -1;

	if(ENABLE_ASSERTIONS)
		assert(ack_req->opcode == ST_LAST_ACK_SUCCESS);

	return prev_node_in_chain();
}

void
ack_fwd_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_ack_t* ack_to_send = (spacetime_ack_t *) msg_to_send;
	memcpy(ack_to_send, triggering_req, sizeof(spacetime_ack_t)); // copy req to next_req_ptr

	ack_to_send->opcode = ST_OP_ACK;
}




int
ack_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;

	if(ENABLE_ASSERTIONS)
		assert(inv_req->op_meta.opcode == ST_INV_SUCCESS || inv_req->op_meta.opcode == ST_EMPTY);

	return prev_node_in_chain();
}

void
ack_modify_elem_after_send(uint8_t* req)
{
	spacetime_inv_t* inv_req = (spacetime_inv_t *) req;

	//empty inv buffer
	if(inv_req->op_meta.opcode == ST_INV_SUCCESS ||
	   inv_req->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE)
		inv_req->op_meta.opcode = ST_EMPTY;
	else assert(0);
}

void
ack_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	spacetime_ack_t* ack_to_send = (spacetime_ack_t *) msg_to_send;
	spacetime_inv_t* inv_ptr  = (spacetime_inv_t *) triggering_req;

	memcpy(ack_to_send, inv_ptr, sizeof(spacetime_ack_t)); // copy req to next_req_ptr

	ack_to_send->opcode = ST_OP_ACK;
	ack_to_send->buff_idx = inv_ptr->buff_idx;
}


int
rem_write_crd_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_ptr = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS)
		assert(op_ptr->op_meta.state == ST_EMPTY ||
			   op_ptr->op_meta.state == ST_SEND_CRD ||
			   op_ptr->op_meta.state == ST_PUT_STALL ||
			   op_ptr->op_meta.state == ST_PUT_SUCCESS);

	return op_ptr->op_meta.state == ST_SEND_CRD ? op_ptr->initiator : -1;
}

void
rem_write_crd_modify_elem_after_send(uint8_t *req)
{
	spacetime_op_t* op = (spacetime_op_t *) req;

	//empty inv buffer
	if(op->op_meta.state == ST_SEND_CRD)
		op->op_meta.state = ST_EMPTY;
	else assert(0);
}


int
inv_crd_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_inv_t* op_ptr = (spacetime_inv_t *) req;

	if(ENABLE_ASSERTIONS)
		assert(op_ptr->op_meta.opcode == ST_EMPTY ||
			   op_ptr->op_meta.opcode == ST_INV_SUCCESS);

	return op_ptr->op_meta.opcode == ST_INV_SUCCESS ? prev_node_in_chain() : -1;
}

void
inv_crd_modify_elem_after_send(uint8_t *req)
{
	if(ENABLE_ASSERTIONS){
		spacetime_inv_t* op = (spacetime_inv_t *) req;
		assert(op->op_meta.opcode == ST_INV_SUCCESS);
	}
}




int
remote_read_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		assert(is_input_code(op_req->op_meta.opcode));
		assert(is_response_code(op_req->op_meta.state) || is_bucket_state_code(op_req->op_meta.state));
	}

	return op_req->op_meta.state == ST_GET_STALL ? tail_id() : -1; // send remote writes to head
}

void
remote_read_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(op_req->op_meta.state == ST_GET_STALL)
		op_req->op_meta.state = ST_IN_PROGRESS_GET;
	else assert(0);
}

void
remote_read_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	if(ENABLE_ASSERTIONS)
		assert(machine_id != tail_id());

	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_op_t* op_to_send = (spacetime_op_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(op_to_send, op, sizeof(spacetime_op_t));

	op_to_send->op_meta.state = ST_NEW;
	op_to_send->op_meta.opcode = ST_OP_GET;
	op_to_send->initiator = (uint8_t) machine_id;
	op_to_send->op_meta.initiator = (uint8_t) machine_id;
}




int
remote_read_resp_skip_or_get_sender_id(uint8_t *req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(ENABLE_ASSERTIONS){
		if(op_req->op_meta.opcode != ST_OP_GET){

			printf("Opcode: %d, state: %d\n", op_req->op_meta.opcode, op_req->op_meta.state);
			printf("Opcode: %s, state: %s\n",
				   code_to_str(op_req->op_meta.opcode),
				   code_to_str(op_req->op_meta.state));
		}
		assert(op_req->op_meta.opcode == ST_OP_GET);
		assert(op_req->op_meta.state == ST_GET_COMPLETE);
	}

	return op_req->initiator; // send remote writes to head
}

void
remote_read_resp_modify_elem_after_send(uint8_t* req)
{
	spacetime_op_t* op_req = (spacetime_op_t *) req;

	if(op_req->op_meta.state == ST_GET_COMPLETE)
		op_req->op_meta.state = ST_EMPTY;
	else {
		printf("St_opcode: %s\n", code_to_str(op_req->op_meta.state));
		assert(0);
	}
}

void
remote_read_resp_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
	if(ENABLE_ASSERTIONS)
		assert(machine_id == tail_id());

	spacetime_op_t* op = (spacetime_op_t *) triggering_req;
	spacetime_op_t* op_to_send = (spacetime_op_t *) msg_to_send;

	// Copy op to inv, set sender and opcode
	memcpy(op_to_send, op, sizeof(spacetime_op_t));
}












void
print_ops_and_remote_write_ops(spacetime_op_t* ops, spacetime_op_t* remote_writes)
{
//	for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i)
    for(int i = 0; i < max_batch_size; ++i)
		printf("ops[%d]: state-> %s, key-> %lu \n", i,
			   code_to_str(ops[i].op_meta.state), *((uint64_t*) &ops[i].op_meta.key));

	if(machine_id == head_id())
//		for(int i = 0; i < MAX_BATCH_OPS_SIZE; ++i)
        for(int i = 0; i < max_batch_size; ++i)
			printf("remote_writes[%d]: state-> %s, key-> %lu \n", i,
				   code_to_str(remote_writes[i].op_meta.state), *((uint64_t*) &remote_writes[i].op_meta.key));
}

void
print_total_stalls_due_to_credits(ud_channel_t *inv_ud_c, ud_channel_t *ack_ud_c,
								  ud_channel_t *rem_writes_ud_c, ud_channel_t *rem_reads_ud_c)
{
	// Stalls
	green_printf ("$$$ CRD STALLs : %s %d, %s %d, %s %d,",
				  inv_ud_c->qp_name, inv_ud_c->stats.send_total_msgs,
				  ack_ud_c->qp_name, ack_ud_c->stats.send_total_msgs,
				  rem_writes_ud_c->qp_name, rem_writes_ud_c->stats.send_total_msgs);
	if(CR_ENABLE_REMOTE_READS)
		green_printf (", %s %d\n",
					  rem_reads_ud_c->qp_name, rem_reads_ud_c->stats.send_total_msgs);
	else printf("\n");
}

void
print_total_send_recv_msgs_n_credits(ud_channel_t *inv_ud_c, ud_channel_t *inv_crd_ud_c,
									 ud_channel_t *ack_ud_c, ud_channel_t *rem_writes_ud_c,
									 ud_channel_t *crd_ud_c,
									 ud_channel_t *rem_reads_ud_c, ud_channel_t *rem_read_resp_ud_c)
{
	// Sends
	green_printf ("--> Total Send: %s %d",
				  inv_ud_c->qp_name, inv_ud_c->stats.send_total_msgs);
	if(CR_ENABLE_EARLY_INV_CRDS)
		green_printf (", %s %d",
					  inv_crd_ud_c->qp_name, inv_crd_ud_c->stats.send_total_msgs);
	green_printf (", %s %d, %s %d, %s %d",
				  ack_ud_c->qp_name, ack_ud_c->stats.send_total_msgs,
				  rem_writes_ud_c->qp_name, rem_writes_ud_c->stats.send_total_msgs,
				  crd_ud_c->qp_name, crd_ud_c->stats.send_total_msgs);
	if(CR_ENABLE_REMOTE_READS)
		green_printf (", %s %d, %s %d\n",
					  rem_reads_ud_c->qp_name, rem_reads_ud_c->stats.send_total_msgs,
					  rem_read_resp_ud_c->qp_name, rem_read_resp_ud_c->stats.send_total_msgs);
	else printf("\n");


	// Receives
	green_printf ("vvv Total Recv: %s %d",
				  inv_ud_c->qp_name, inv_ud_c->stats.recv_total_msgs);
	if(CR_ENABLE_EARLY_INV_CRDS)
		green_printf (", %s %d",
					  inv_crd_ud_c->qp_name, inv_crd_ud_c->stats.recv_total_msgs);
	green_printf (", %s %d, %s %d, %s %d",
				  ack_ud_c->qp_name, ack_ud_c->stats.recv_total_msgs,
				  rem_writes_ud_c->qp_name, rem_writes_ud_c->stats.recv_total_msgs,
				  crd_ud_c->qp_name, crd_ud_c->stats.recv_total_msgs);
	if(CR_ENABLE_REMOTE_READS)
		green_printf (", %s %d, %s %d\n",
					  rem_reads_ud_c->qp_name, rem_reads_ud_c->stats.recv_total_msgs,
					  rem_read_resp_ud_c->qp_name, rem_read_resp_ud_c->stats.recv_total_msgs);
	else printf("\n");


	// Credits
	uint8_t remote_node = (uint8_t) (machine_id == head_id() ? next_node_in_chain() : head_id());
	printf("Inv credits: %d, ack credits: %d, remote_write_crds: %d\n",
		   inv_ud_c->credits_per_channels[remote_node],
		   ack_ud_c->credits_per_channels[remote_node],
		   rem_writes_ud_c->credits_per_channels[head_id()]);
}

static inline void
cr_complete_local_reads(spacetime_op_t* remote_reads_resps,
						uint16_t remote_read_resps_polled, spacetime_op_t *ops)
{
	for(int i = 0; i < remote_read_resps_polled; ++i){
		uint16_t idx = remote_reads_resps[i].buff_idx;
		///completed read / write --> remove it from the ops buffer
		if(ENABLE_ASSERTIONS){
			assert(ops[idx].op_meta.state == ST_IN_PROGRESS_GET);
			assert(((uint64_t *) &ops[idx].op_meta.key)[0] == ((uint64_t*) &remote_reads_resps[i].op_meta.key)[0]);
		}

		if(ops[idx].op_meta.opcode == ST_OP_GET)
			ops[idx].op_meta.state = ST_GET_COMPLETE;
		else assert(0);
	}
}

// returns first free slot within a range [start_pos, end_pos) or -1 if all are occupied
static inline int
get_first_free_slot(const uint8_t* free_slot_array, uint16_t start_pos, uint16_t end_pos)
{
	if(ENABLE_ASSERTIONS)
		assert(end_pos > start_pos);

	for(int i = start_pos; i < end_pos; ++i)
		if(free_slot_array[i] == 1)
			return i;
	return -1;
}

static inline uint16_t
cr_move_stalled_writes_to_top_n_return_free_space(spacetime_op_t *remote_writes)
{
	uint8_t free_slot_array[MAX_BATCH_OPS_SIZE] = {0};
	uint16_t free_slots = 0;
	uint16_t last_free_slot = 0; //used to avoid re-iterating already non-empty slots
	for(int i = 0; i < max_batch_size; ++i){
		if(ENABLE_ASSERTIONS)
			assert(remote_writes[i].op_meta.state == ST_EMPTY      ||
				   remote_writes[i].op_meta.state == ST_PUT_STALL  ||
				   remote_writes[i].op_meta.state == ST_PUT_SUCCESS);

		if(remote_writes[i].op_meta.state == ST_EMPTY) {
			free_slots++;
			free_slot_array[i] = 1;

		} else if(free_slots > 0 &&
				  (remote_writes[i].op_meta.state == ST_PUT_STALL  ||
				   remote_writes[i].op_meta.state == ST_PUT_SUCCESS ))
		{

			int next_free_slot = get_first_free_slot(free_slot_array, last_free_slot, (uint16_t) i);

			if(next_free_slot > -1){
				free_slot_array[i] = 1;
				free_slot_array[next_free_slot] = 0;
				last_free_slot = (uint16_t) next_free_slot;
				//swap stalled request to the first free slot
				memcpy(&remote_writes[next_free_slot], &remote_writes[i], sizeof(spacetime_op_t));

				// empty this slot
				remote_writes[i].op_meta.state = ST_EMPTY;
				remote_writes[i].op_meta.opcode = ST_EMPTY;
			}
		}
	}

	if(ENABLE_ASSERTIONS)
		for(int i = 0; i < max_batch_size; ++i){
			if(i < max_batch_size - free_slots)
				assert(remote_writes[i].op_meta.state == ST_PUT_STALL   ||
					   remote_writes[i].op_meta.state == ST_PUT_SUCCESS );
			else
				assert(remote_writes[i].op_meta.state == ST_EMPTY);
		}

	return free_slots;
}

static inline void
debugg(spacetime_op_t  *ops, uint16_t worker_lid, int line_no)
{
    if(w_stats[worker_lid].total_loops > 0)
        for(int i = 0; i < max_batch_size; ++i){
            if(!(ops[i].op_meta.opcode == ST_OP_PUT || ops[i].op_meta.opcode == ST_OP_GET))
                printf("Line[%d]--> Op[%d]: %s, loop iter: %llu\n", line_no,
                       i, code_to_str(ops[i].op_meta.opcode), w_stats[worker_lid].total_loops);
            assert(ops[i].op_meta.opcode == ST_OP_PUT || ops[i].op_meta.opcode == ST_OP_GET);
        }
}

void*
run_worker(void *arg)
{
    assert(rmw_ratio == 0);
	assert(is_CR == 1);
    assert(credits_num % MACHINE_NUM == 0); // CR ONLY
	assert(ENABLE_COALESCE_OF_HOT_REQS == 0);

	///WARNING: only defines (no dynamically passed cli arguments) work for cr worker
	assert(max_coalesce <= MAX_REQ_COALESCE);
	assert(num_workers <= WORKERS_PER_MACHINE);
	assert(max_batch_size <= MAX_BATCH_OPS_SIZE);
	assert(credits_num <= CREDITS_PER_REMOTE_WORKER);

	struct thread_params params = *(struct thread_params *) arg;
	uint16_t worker_lid = (uint16_t) params.id;	// Local ID of this worker thread
	uint16_t worker_gid = (uint16_t) (machine_id * WORKERS_PER_MACHINE + params.id);	// Global ID of this worker thread




	/* --------------------------------------------------------
	------------------- RDMA WINGS DECLARATIONS---------------
	---------------------------------------------------------*/
	ud_channel_t ud_channels[CR_TOTAL_WORKER_UD_QPs];
	ud_channel_t* ud_channel_ptrs[CR_TOTAL_WORKER_UD_QPs];

	for(int i = 0; i < CR_TOTAL_WORKER_UD_QPs; ++i)
		ud_channel_ptrs[i] = &ud_channels[i];

	ud_channel_t* inv_ud_c = ud_channel_ptrs[CR_INV_UD_QP_ID];
	ud_channel_t* inv_crd_ud_c = ud_channel_ptrs[CR_INV_CRD_UD_QP_ID];
	ud_channel_t* ack_ud_c = ud_channel_ptrs[CR_ACK_UD_QP_ID];
	ud_channel_t* rem_reads_ud_c = ud_channel_ptrs[CR_REMOTE_READS_UD_QP_ID];
	ud_channel_t* rem_read_resp_ud_c = ud_channel_ptrs[CR_REMOTE_READS_RESP_UD_QP_ID];
	ud_channel_t* rem_writes_ud_c = ud_channel_ptrs[CR_REMOTE_WRITES_UD_QP_ID];
	ud_channel_t* rem_writes_crd_ud_c = ud_channel_ptrs[CR_REMOTE_WRITE_CRD_UD_QP_ID];



	const uint8_t is_bcast = 0;
	const uint8_t stats_on = 1;
	const uint8_t prints_on = 1;
	const uint8_t is_hdr_only = 0;
	const uint8_t expl_crd_ctrl = 0;
	const uint8_t disable_crd_ctrl = 0;

	char inv_qp_name[200], ack_qp_name[200], rem_writes_qp_name[200],
			rem_reads_qp_name[200], rem_read_resps_qp_name[200];
	sprintf(inv_qp_name, "%s[%d]", "\033[31mINV\033[0m", worker_lid);
	sprintf(ack_qp_name, "%s[%d]", "\033[33mACK\033[0m", worker_lid);
	sprintf(rem_writes_qp_name, "%s[%d]", "\033[1m\033[32mREMOTE_WRITES\033[0m", worker_lid);
	sprintf(rem_reads_qp_name ,  "%s[%d]", "\033[1m\033[32mREMOTE_READS\033[0m", worker_lid);
	sprintf(rem_read_resps_qp_name, "%s[%d]", "\033[1m\033[32mREMOTE_READ_RESPS\033[0m", worker_lid);

	if(CR_ENABLE_EARLY_INV_CRDS){
		wings_ud_channel_init(inv_ud_c, inv_qp_name, REQ, INV_MAX_REQ_COALESCE, sizeof(spacetime_inv_t), 0,
							  DISABLE_INV_INLINING == 0 ? 1 : 0, is_hdr_only, is_bcast, disable_crd_ctrl, 1,
							  inv_crd_ud_c, INV_CREDITS, MACHINE_NUM, (uint8_t) machine_id, stats_on, prints_on);

		wings_ud_channel_init(ack_ud_c, ack_qp_name, RESP, ACK_MAX_REQ_COALESCE, sizeof(spacetime_ack_t), 0,
							  DISABLE_ACK_INLINING == 0 ? 1 : 0, is_hdr_only, is_bcast, 1, expl_crd_ctrl,
							  NULL, CR_ACK_CREDITS, MACHINE_NUM, (uint8_t) machine_id, stats_on, prints_on);
	} else {
		wings_ud_channel_init(inv_ud_c, inv_qp_name, REQ, INV_MAX_REQ_COALESCE, sizeof(spacetime_inv_t), 0,
							  DISABLE_INV_INLINING == 0 ? 1 : 0, is_hdr_only, is_bcast, disable_crd_ctrl, expl_crd_ctrl,
							  ack_ud_c, INV_CREDITS, MACHINE_NUM, (uint8_t) machine_id, stats_on, prints_on);

		wings_ud_channel_init(ack_ud_c, ack_qp_name, RESP, ACK_MAX_REQ_COALESCE, sizeof(spacetime_ack_t), 0,
							  DISABLE_ACK_INLINING == 0 ? 1 : 0, is_hdr_only, is_bcast, disable_crd_ctrl, expl_crd_ctrl,
							  inv_ud_c, ACK_CREDITS, MACHINE_NUM, (uint8_t) machine_id, stats_on, prints_on);
	}

	wings_ud_channel_init(rem_writes_ud_c, rem_writes_qp_name, REQ, CR_REMOTE_WRITES_MAX_REQ_COALESCE,
						  sizeof(spacetime_op_t), 0, CR_DISABLE_REMOTE_WRITES_INLINING == 0 ? 1 : 0,
						  is_hdr_only, is_bcast, disable_crd_ctrl, 1, rem_writes_crd_ud_c,
						  CR_REMOTE_WRITES_CREDITS, MACHINE_NUM, (uint8_t) machine_id, stats_on, prints_on);

	///////////////
	///<4th stage>
	if(CR_ENABLE_REMOTE_READS){
		wings_ud_channel_init(rem_reads_ud_c, rem_reads_qp_name, REQ, CR_REMOTE_WRITES_MAX_REQ_COALESCE,
							  sizeof(spacetime_op_t), 0, CR_DISABLE_REMOTE_READS_INLINING == 0 ? 1 : 0,
							  is_hdr_only, is_bcast, disable_crd_ctrl, expl_crd_ctrl, rem_read_resp_ud_c,
							  CR_REMOTE_READS_CREDITS, MACHINE_NUM, (uint8_t) machine_id, stats_on, prints_on);

		wings_ud_channel_init(rem_read_resp_ud_c, rem_read_resps_qp_name, RESP, CR_REMOTE_READS_MAX_REQ_COALESCE,
							  sizeof(spacetime_op_t), 0, CR_DISABLE_REMOTE_READS_INLINING == 0 ? 1 : 0,
							  is_hdr_only, is_bcast, disable_crd_ctrl, expl_crd_ctrl, rem_reads_ud_c,
							  CR_REMOTE_READS_CREDITS, MACHINE_NUM, (uint8_t) machine_id, stats_on, prints_on);
	}
	///</4th stage>
	///////////////

	wings_setup_channel_qps_and_recvs(ud_channel_ptrs, CR_TOTAL_WORKER_UD_QPs, g_share_qs_barrier, worker_lid);

	/* -------------------------------------------------------
	------------------- OTHER DECLARATIONS--------------------
	---------------------------------------------------------*/
	//Intermediate buffs where reqs are copied from incoming_* buffs in order to get passed to the KVS
	spacetime_op_t  *ops;
	spacetime_inv_t *inv_recv_ops;
	spacetime_ack_t *ack_recv_ops;
	spacetime_val_t *val_recv_ops; // UNUSED!
    uint32_t coh_ops_len = (uint32_t) (credits_num * REMOTE_MACHINES * max_coalesce); //credits * remote_machines * max_req_coalesce

	setup_kvs_buffs(&ops, &inv_recv_ops, &ack_recv_ops, &val_recv_ops);

	//Remote writes init
	spacetime_op_t *remote_writes = memalign(4096, max_batch_size * (sizeof(spacetime_op_t)));
	memset(remote_writes, 0, max_batch_size * (sizeof(spacetime_op_t)));
	for(int i = 0; i < max_batch_size; ++i){
		remote_writes[i].op_meta.state = ST_EMPTY;
		remote_writes[i].op_meta.opcode = ST_EMPTY;
	}

	///////////////
	///<4th stage>
	//Remote reads buffer: used for polling remote reads on tail & remote read responses on the rest nodes
	spacetime_op_t *remote_reads = memalign(4096, max_batch_size * (sizeof(spacetime_op_t)));
	memset(remote_reads, 0, max_batch_size * (sizeof(spacetime_op_t)));
	for(int i = 0; i < max_batch_size; ++i){
		remote_reads[i].op_meta.state = ST_EMPTY;
		remote_reads[i].op_meta.opcode = ST_EMPTY;
	}
	///</4th stage>
	///////////////


	struct spacetime_trace_command *trace;
	trace_init(&trace, worker_gid);

	//// <UNUSED>
	spacetime_op_t* n_hottest_keys_in_ops_get[COALESCE_N_HOTTEST_KEYS];
	spacetime_op_t* n_hottest_keys_in_ops_put[COALESCE_N_HOTTEST_KEYS];
	for(int i = 0; i < COALESCE_N_HOTTEST_KEYS; ++i){
		n_hottest_keys_in_ops_get[i] = NULL;
		n_hottest_keys_in_ops_put[i] = NULL;
	}
	////</UNUSED>

	uint8_t has_outstanding_invs = 0;
	uint8_t has_outstanding_rem_writes = 0;
	uint32_t trace_iter = 0;
	uint16_t rolling_idx = 0, remote_reads_rolling_idx = 0;
	uint16_t invs_polled = 0, acks_polled = 0, remote_writes_polled = 0;
    uint32_t num_of_iters_serving_op[MAX_BATCH_OPS_SIZE] = {0};

	uint16_t free_rem_write_slots = max_batch_size;
	/// Spawn stats thread
	if (worker_lid == 0)
		if (spawn_stats_thread() != 0)
			red_printf("Stats thread was not successfully spawned \n");


	struct timespec stopwatch_for_req_latency;
	/* -----------------------------------------------------
       ------------------------Main Loop--------------------
	   ----------------------------------------------------- */
	while (true) {

		if(unlikely(w_stats[worker_lid].total_loops % M_16 == 0)){
			//Check something periodically
//	        print_total_stalls_due_to_credits(inv_ud_c, ack_ud_c, rem_writes_ud_c, rem_reads_ud_c);
//			print_total_send_recv_msgs_n_credits(inv_ud_c, inv_crd_ud_c, ack_ud_c,
//												 rem_writes_ud_c, rem_writes_crd_ud_c,
//												 rem_reads_ud_c, rem_read_resp_ud_c);
//			print_ops_and_remote_write_ops(ops, remote_writes);
		}

		/// DONE
		// 1st stage: head only initiate requests 						 [DONE]
		// 2nd stage: + rest nodes initiate (local) reads   			 [DONE]
		// 3rd stage: + rest nodes initiate (remote) writes via head     [DONE]
		// 4th stage: + rest nodes initiate remote reads when invalid    [DONE]
		// 5th stage: + add early INV credits to pipeline more reqs      [DONE]
		// 6th stage: + poll for remote writes even though stalled exist [DONE]
		// 7th stage: + poll for messages instead of pkts (ie if you have
		//              empty space buff slots < max_coalesce poll pkt
		//              and buffer additional packets                    [DONE]
		// 8th stage: + Do not stall writes that found Invalid on head   [DONE]


		if(!CR_ENABLE_ONLY_HEAD_REQS || machine_id == head_id()){
            refill_ops(&trace_iter, worker_lid, trace, ops,
                       num_of_iters_serving_op, &stopwatch_for_req_latency,
                       n_hottest_keys_in_ops_get, n_hottest_keys_in_ops_put);
            cr_batch_ops_to_KVS(Local_ops, (uint8_t *) ops, max_batch_size, sizeof(spacetime_op_t), NULL);

            //TODO: moved
            stop_latency_of_completed_reads(ops, worker_lid, &stopwatch_for_req_latency);
		}

		if (update_ratio > 0) {

			if(machine_id == head_id()) {
                const uint16_t max_outstanding_writes = (MACHINE_NUM - 1) * CR_ACK_CREDITS;

                if(!CR_ENABLE_EARLY_INV_CRDS ||
                   inv_ud_c->stats.send_total_msgs - ack_ud_c->stats.recv_total_msgs <= max_outstanding_writes)
                {   /// Initiate INVs for head writes
                    wings_issue_pkts(inv_ud_c, NULL, (uint8_t *) ops,
                                     max_batch_size, sizeof(spacetime_op_t), &rolling_idx,
                                     inv_skip_or_get_sender_id, inv_modify_elem_after_send,
                                     inv_copy_and_modify_elem);
                }

                ///////////////
                ///<3rd stage>
                if(!CR_ENABLE_ONLY_HEAD_REQS){

                    wings_poll_buff_and_post_recvs(rem_writes_ud_c, free_rem_write_slots,
                                                   (uint8_t *) &remote_writes[max_batch_size - free_rem_write_slots]);

                    cr_batch_ops_to_KVS(Remote_writes, (uint8_t *) remote_writes,
                                        max_batch_size, sizeof(spacetime_op_t), NULL);

                    if(!CR_ENABLE_EARLY_INV_CRDS ||
                       inv_ud_c->stats.send_total_msgs - ack_ud_c->stats.recv_total_msgs <= max_outstanding_writes)
                    {   /// Initiate INVs for remotes writes
                        wings_issue_pkts(inv_ud_c, NULL, (uint8_t *) remote_writes,
                                         max_batch_size, sizeof(spacetime_op_t), NULL,
                                         remote_write_head_skip_or_get_sender_id,
                                         remote_write_head_modify_elem_after_send,
                                         remote_write_head_copy_and_modify_elem);

                        /// Issue credits for remotes writes
                        wings_issue_credits(rem_writes_crd_ud_c, NULL, (uint8_t *) remote_writes,
                                            max_batch_size, sizeof(spacetime_op_t),
                                            rem_write_crd_skip_or_get_sender_id,
                                            rem_write_crd_modify_elem_after_send);
                    }

                    free_rem_write_slots = cr_move_stalled_writes_to_top_n_return_free_space(remote_writes);
                }

			} else if(!CR_ENABLE_ONLY_HEAD_REQS)
				/// Initiate Remote writes
				wings_issue_pkts(rem_writes_ud_c, NULL, (uint8_t *) ops,
                                 max_batch_size, sizeof(spacetime_op_t), &rolling_idx,
								 remote_write_skip_or_get_sender_id, inv_modify_elem_after_send,
								 remote_write_copy_and_modify_elem);

			///</3rd stage>
			///////////////

			///////////////
			///<4th stage>
			if(CR_ENABLE_REMOTE_READS) {
				if(machine_id == tail_id()){
					/// Poll Remote reads
					uint16_t remote_reads_polled = wings_poll_buff_and_post_recvs(rem_reads_ud_c,
                                                                                  max_batch_size, (uint8_t *) remote_reads);

					/// Batch Remote reads to KVS
					cr_batch_ops_to_KVS(Remote_reads, (uint8_t *) remote_reads,
										remote_reads_polled, sizeof(spacetime_op_t), NULL);

					/// Issue responses of Remote reads
					wings_issue_pkts(rem_read_resp_ud_c, NULL, (uint8_t *) remote_reads,
									 remote_reads_polled, sizeof(spacetime_op_t), NULL,
									 remote_read_resp_skip_or_get_sender_id,
									 remote_read_resp_modify_elem_after_send,
									 remote_read_resp_copy_and_modify_elem);

				} else {
					/// Initiate Remote reads
					wings_issue_pkts(rem_reads_ud_c, NULL, (uint8_t *) ops,
									 max_batch_size, sizeof(spacetime_op_t), &remote_reads_rolling_idx,
									 remote_read_skip_or_get_sender_id, remote_read_modify_elem_after_send,
									 remote_read_copy_and_modify_elem);

                    for(int i = 0; i < max_batch_size; i++)
                        assert(ops[i].op_meta.opcode == ST_OP_PUT || ops[i].op_meta.opcode == ST_OP_GET);

					/// Poll respsonses of Remote reads
					uint16_t remote_read_resps_polled = wings_poll_buff_and_post_recvs(rem_read_resp_ud_c, max_batch_size,
																					   (uint8_t *) remote_reads);
					/// Complete Remote reads
					cr_complete_local_reads(remote_reads, remote_read_resps_polled, ops);
					stop_latency_of_completed_reads(ops, worker_lid, &stopwatch_for_req_latency);

                    for(int i = 0; i < max_batch_size; i++)
                        assert(ops[i].op_meta.opcode == ST_OP_PUT || ops[i].op_meta.opcode == ST_OP_GET);
				}
			}
			///</4th stage>
			///////////////

			if(machine_id != head_id()) {
				///Poll for INVs
				if(has_outstanding_invs == 0){
                    invs_polled = wings_poll_buff_and_post_recvs(inv_ud_c, coh_ops_len, (uint8_t *) inv_recv_ops);

					if (invs_polled > 0) {
						/// Batch INVs to KVS
						cr_batch_ops_to_KVS(Invs, (uint8_t *) inv_recv_ops, invs_polled, sizeof(spacetime_inv_t), ops);

						if (CR_ENABLE_EARLY_INV_CRDS)
							/// Issue credits for INVs to previous node in chain
							wings_issue_credits(inv_crd_ud_c, NULL, (uint8_t *) inv_recv_ops, invs_polled,
												sizeof(spacetime_inv_t), inv_crd_skip_or_get_sender_id,
												inv_crd_modify_elem_after_send);
					}
				}

				if (invs_polled > 0) {
					/// Batch INVs to KVS
					if (machine_id != tail_id() && machine_id != head_id())
						/// Forward INVS to next node in chain
						has_outstanding_invs = wings_issue_pkts(inv_ud_c, NULL, (uint8_t *) inv_recv_ops, invs_polled,
																sizeof(spacetime_inv_t), NULL,
																inv_skip_or_fwd_to_next_node,
																inv_fwd_modify_elem_after_send,
																inv_fwd_copy_and_modify_elem);

					else if (machine_id == tail_id()) {
						/// Initiate ACKS (forward to prev)
						has_outstanding_invs = wings_issue_pkts(ack_ud_c, NULL, (uint8_t *) inv_recv_ops, invs_polled,
																sizeof(spacetime_inv_t), NULL, ack_skip_or_get_sender_id,
																ack_modify_elem_after_send, ack_copy_and_modify_elem);
						if(ENABLE_ASSERTIONS)
							assert(ack_ud_c->stats.send_total_msgs == inv_ud_c->stats.recv_total_msgs - inv_ud_c->num_overflow_msgs);
					}
				}
			}

			if(machine_id != tail_id()){
				///Poll for Acks
				acks_polled = wings_poll_buff_and_post_recvs(ack_ud_c, coh_ops_len, (uint8_t *) ack_recv_ops);

				if (acks_polled > 0){
					/// Batch ACKs to KVS
					cr_batch_ops_to_KVS(Acks, (uint8_t *) ack_recv_ops, acks_polled, sizeof(spacetime_ack_t), ops);

					stop_latency_of_completed_writes(ops, worker_lid, &stopwatch_for_req_latency);
				}

				if(machine_id != head_id()){

					/// FWD ACKs to previous node if not the Head
					wings_issue_pkts(ack_ud_c, NULL, (uint8_t *) ack_recv_ops, acks_polled, sizeof(spacetime_ack_t), NULL,
									 ack_fwd_skip_or_get_sender_id,
									 ack_fwd_modify_elem_after_send, ack_fwd_copy_and_modify_elem);
					if(ENABLE_ASSERTIONS)
						assert(ack_ud_c->stats.send_total_msgs == ack_ud_c->stats.recv_total_msgs - ack_ud_c->num_overflow_msgs);

				} else ///empty ack_rcv_ops in head node
                    for(int i = 0; i < coh_ops_len; ++i)
                        ack_recv_ops[i].opcode = ST_EMPTY;
			}
		}
		w_stats[worker_lid].total_loops++;
	}

	return NULL;
}


