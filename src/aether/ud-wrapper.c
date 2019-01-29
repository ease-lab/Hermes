//
// Created by akatsarakis on 22/01/19.
//

#include <stdio.h>
#include <config.h>
#include "ud-wrapper.h"

// implement a Multicast / Unicast channel
// Support for:
//      mulitcast / unicast channel
//      Coalescing
//      Variable size msgs?
//      Selective Signaling
//      Batching to the NIC
//      Inlining or not
//      Batch post receives to the NIC
//          Mode 1: poll reqs, copy incoming msgs to local buffers and (p)re-post recvs
//          Mode 2: poll reqs, do not copy msgs and post rcvs when said
//      Enable implicit (request - response mode) and explicit (batched) credits flow control





static inline void
aether_post_receives(struct hrd_ctrl_blk *cb, ud_channel_t* ud_channel, uint16_t num_of_receives)
{
	struct ibv_recv_wr *bad_recv_wr;
	void* next_buff_addr;

	if(AETHER_ENABLE_ASSERTIONS)
		assert(num_of_receives <= ud_channel->max_recv_wrs);

    int req_size = aether_ud_recv_max_pkt_size(ud_channel);
	for(int i = 0; i < num_of_receives; i++) {
        next_buff_addr = (void*) (ud_channel->recv_reqs_buff) + (ud_channel->recv_reqs_push_ptr * req_size);
		memset(next_buff_addr, 0, (size_t) req_size); //reset the buffer before posting the receive

		if(AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC)
			ud_channel->recv_wr[i].sg_list->addr = (uintptr_t) next_buff_addr;
		else
			hrd_post_dgram_recv(cb->dgram_qp[ud_channel->qp_id], next_buff_addr, req_size, cb->dgram_buf_mr->lkey);
		AETHER_MOD_ADD(ud_channel->recv_reqs_push_ptr, ud_channel->recv_q_depth);
	}

	if(AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC) {
		ud_channel->recv_wr[num_of_receives - 1].next = NULL;
		if (AETHER_ENABLE_ASSERTIONS) {
			for (int i = 0; i < num_of_receives - 1; i++) {
				assert(ud_channel->recv_wr[i].num_sge == 1);
				assert(ud_channel->recv_wr[i].next == &ud_channel->recv_wr[i + 1]);
				assert(ud_channel->recv_wr[i].sg_list->length == req_size);
				assert(ud_channel->recv_wr[i].sg_list->lkey == cb->dgram_buf_mr->lkey);
			}
			assert(ud_channel->recv_wr[num_of_receives - 1].next == NULL);
		}

		int ret = ibv_post_recv(cb->dgram_qp[ud_channel->qp_id], &ud_channel->recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs for credits before val bcast", ret);

		//recover next ptr of last wr to NULL
		ud_channel->recv_wr[num_of_receives - 1].next = (ud_channel->max_recv_wrs == num_of_receives - 1) ?
                                                        NULL : &ud_channel->recv_wr[num_of_receives];
	}
}

void
aether_setup_incoming_buff_and_post_initial_recvs(ud_channel_t* ud_channel, struct hrd_ctrl_blk *cb)
{
    uint16_t recv_req_size = aether_ud_recv_max_pkt_size(ud_channel);

    //init recv buffs as empty (not need for CRD since CRD msgs are --immediate-- header-only)
    for(int i = 0; i < ud_channel->recv_q_depth; ++i)
        ((aether_ud_recv_pkt*) &ud_channel->recv_reqs_buff[i * recv_req_size])->pkt.req_num = 0;

    if(AETHER_ENABLE_POST_RECV_PRINTS && ud_channel->enable_prints)
        yellow_printf("vvv Post Initial Receives: %s %d\n", ud_channel->qp_name, ud_channel->max_recv_wrs);
    aether_post_receives(cb, ud_channel, ud_channel->max_recv_wrs);
}








static inline uint16_t
aether_poll_buff_and_post_recvs(ud_channel_t* ud_channel, uint16_t max_pkts_to_poll,
								uint8_t* recv_ops, struct hrd_ctrl_blk *cb)
{
	int index = 0;
    uint8_t sender = 0;
	uint16_t reqs_polled = 0;
    uint8_t* next_packet_reqs, *recv_op_ptr, *next_req, *next_packet_req_num_ptr;

    uint16_t max_req_size = aether_ud_recv_max_pkt_size(ud_channel);

	//poll completion q
	uint16_t packets_polled = (uint16_t) ibv_poll_cq(ud_channel->recv_cq, max_pkts_to_poll, ud_channel->recv_wc);

	for(int i = 0; i < packets_polled; ++i){

		index = (ud_channel->recv_reqs_pull_ptr + 1) % ud_channel->recv_q_depth;
		aether_ud_recv_pkt* next_packet = (aether_ud_recv_pkt *) &ud_channel->recv_reqs_buff[index * max_req_size];

		next_packet_reqs = next_packet->pkt.reqs;
		next_packet_req_num_ptr = &next_packet->pkt.req_num;

		assert(next_packet->pkt.req_num > 0 && next_packet->pkt.req_num <= ud_channel->max_coalescing);


		//TODO add membership and functionality
//        if(node_is_in_membership(last_group_membership, sender))
		for(int j = 0; j < next_packet->pkt.req_num; ++j){
			reqs_polled++;
			next_req = &next_packet_reqs[j * ud_channel->max_req_size];
			recv_op_ptr = recv_ops + (reqs_polled * ud_channel->max_req_size);
			memcpy(recv_op_ptr, next_req, ud_channel->max_req_size);
			ud_channel->channel_providing_crds->credits_per_rem_channels[sender]++; //increment packet credits
		}


		*next_packet_req_num_ptr = 0; //TODO can be removed since we already reset on posting receives
		aether_inc_recv_pull_ptr(ud_channel);


		if(AETHER_ENABLE_ASSERTIONS)
			assert(ud_channel->channel_providing_crds->credits_per_rem_channels[sender] <=
						   ud_channel->channel_providing_crds->max_crds_per_channel);
	}

	//Refill recvs
	if(packets_polled > 0)
		aether_post_receives(cb, ud_channel, packets_polled);

    return reqs_polled;
}




void
aether_setup_send_wr_and_sgl(ud_channel_t *ud_channel)
{

    if(ud_channel->is_bcast_channel){ //Send bcast WRs
        uint16_t max_msgs_in_pcie_batch = ud_channel->max_pcie_bcast_batch * ud_channel->num_remote_channels;
        ud_channel->send_wr  = malloc(sizeof(struct ibv_send_wr) * max_msgs_in_pcie_batch);
        ud_channel->send_sgl = malloc(sizeof(struct ibv_sge) * ud_channel->max_pcie_bcast_batch);

        for(int i = 0; i < ud_channel->max_pcie_bcast_batch; ++i)
            ud_channel->send_sgl[i].length = sizeof(aether_pkt) + aether_ud_recv_max_pkt_size(ud_channel);

        for(int i = 0; i < max_msgs_in_pcie_batch; ++i){
            int sgl_index = i / ud_channel->num_remote_channels;
            int i_mod_bcast = i % ud_channel->num_remote_channels;

            ud_channel->send_wr[i].wr.ud.ah = ud_channel->remote_qps[i_mod_bcast].ah;
            ud_channel->send_wr[i].wr.ud.remote_qpn = ud_channel->remote_qps[i_mod_bcast].qpn;
            ud_channel->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;


            ud_channel->send_wr[i].num_sge = 1;
            ud_channel->send_wr[i].opcode = IBV_WR_SEND; /// Attention!! there is no immediate here
            ud_channel->send_wr[i].sg_list = &ud_channel->send_sgl[sgl_index];

            if (!ud_channel->is_inlining_enabled) {
                ud_channel->send_wr[i].send_flags = 0;
                ud_channel->send_sgl[sgl_index].lkey = ud_channel->send_mem_region->lkey;
            } else
                ud_channel->send_wr[i].send_flags = IBV_SEND_INLINE;

            ud_channel->send_wr[i].next = (i_mod_bcast == ud_channel->num_remote_channels - 1) ? NULL : &ud_channel->send_wr[i + 1];
        }

    }else{ //Send unicast WRs

        ud_channel->send_sgl = malloc(sizeof(struct ibv_sge) * ud_channel->max_send_wrs);
        ud_channel->send_wr  = malloc(sizeof(struct ibv_send_wr) * ud_channel->max_send_wrs);
        for(int i = 0; i < ud_channel->max_send_wrs; ++i){

            ud_channel->send_sgl[i].length = sizeof(aether_pkt) + aether_ud_recv_max_pkt_size(ud_channel);

            ud_channel->send_wr[i].num_sge = 1;
            ud_channel->send_wr[i].opcode = IBV_WR_SEND; /// Attention!! there is no immediate here
            ud_channel->send_wr[i].sg_list = &ud_channel->send_sgl[i];
            ud_channel->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;

            if (!ud_channel->is_inlining_enabled){
                ud_channel->send_wr[i].send_flags = 0;
                ud_channel->send_sgl[i].lkey = ud_channel->send_mem_region->lkey;
            } else
                ud_channel->send_wr[i].send_flags = IBV_SEND_INLINE;
        }
    }
}

void
aether_setup_recv_wr_and_sgl(ud_channel_t *ud_channel, struct hrd_ctrl_blk *cb)
{
    ud_channel->recv_sgl= malloc(sizeof(struct ibv_sge) * ud_channel->max_recv_wrs);
    ud_channel->recv_wr = malloc(sizeof(struct ibv_recv_wr) * ud_channel->max_recv_wrs);

	for (int i = 0; i < ud_channel->max_recv_wrs; i++) {
		ud_channel->recv_sgl[i].length = aether_ud_recv_max_pkt_size(ud_channel);
		ud_channel->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        ud_channel->recv_wr[i].sg_list = &ud_channel->recv_sgl[i];
        ud_channel->recv_wr[i].num_sge = 1;
        ud_channel->recv_wr[i].next = (i == ud_channel->max_recv_wrs - 1) ? NULL : &ud_channel->recv_wr[i + 1];
	}
}


void
aether_ud_channel_init(struct hrd_ctrl_blk *cb, ud_channel_t *ud_c,
					   uint8_t qp_id, char* qp_name, enum channel_type type,
					   uint8_t max_coalescing, uint16_t max_req_size,
					   volatile uint8_t *incoming_reqs_ptr, uint8_t enable_inlining,
					   // broadcast
					   uint8_t is_bcast_channel, qp_info_t *remote_qps,
					   // credits
					   uint8_t expl_credit_ctrl, ud_channel_t *linked_channel,
					   uint8_t credits_per_rem_channel, uint16_t max_rem_channels,
					   //Toggles
					   uint8_t enable_stats, uint8_t enable_prints)
{
	aether_assert_binary(enable_stats);
	aether_assert_binary(enable_prints);
	aether_assert_binary(enable_inlining);
	aether_assert_binary(expl_credit_ctrl);
	aether_assert_binary(is_bcast_channel);
	assert(remote_qps != NULL);

    ud_c->type = type;
	ud_c->qp_id = qp_id;
	ud_c->qp_name = qp_name;
	ud_c->remote_qps = remote_qps;
    ud_c->is_bcast_channel = is_bcast_channel;
    ud_c->num_remote_channels = max_rem_channels;
    ud_c->explicit_crd_ctrl = expl_credit_ctrl;
    ud_c->channel_providing_crds = expl_credit_ctrl ? NULL : linked_channel;

	ud_c->enable_stats = enable_stats;
    ud_c->enable_prints = enable_prints;

	ud_c->is_inlining_enabled = enable_inlining;
    if(aether_ud_recv_max_pkt_size(ud_c) > AETHER_MAX_SUPPORTED_INLINING) {
        if(ud_c->is_inlining_enabled)
            printf("Unfortunately, inlining for msgs sizes up to (%d) "
                   "is higher than the supported (%d)\n",
				   aether_ud_recv_max_pkt_size(ud_c), AETHER_MAX_SUPPORTED_INLINING);
        ud_c->is_inlining_enabled = 0;
    }

    ud_c->credits_per_rem_channels = malloc(sizeof(uint8_t) * max_rem_channels);
    for(int i = 0; i < max_rem_channels; ++i)
        ud_c->credits_per_rem_channels[i] = credits_per_rem_channel; //todo some channels might need to be init to 0 credits?


	ud_c->max_req_size = max_req_size;
	ud_c->max_coalescing = max_coalescing;


    ud_c->max_pcie_bcast_batch = (uint16_t) AETHER_MIN(AETHER_MIN_PCIE_BCAST_BATCH + 1, credits_per_rem_channel);
    //Warning! use min to avoid resetting the first req prior batching to the NIC
	//WARNING: todo check why we need to have MIN_PCIE_BCAST_BATCH + 1 instead of just MIN_PCIE_BCAST_BATCH
	uint16_t max_msgs_in_pcie_bcast = ud_c->max_pcie_bcast_batch * ud_c->num_remote_channels; //must be smaller than the q_depth


	ud_c->max_recv_wrs = credits_per_rem_channel * max_rem_channels;
    ud_c->max_send_wrs = ud_c->is_bcast_channel ? max_msgs_in_pcie_bcast : credits_per_rem_channel * max_rem_channels;

   	ud_c->ss_granularity = ud_c->is_bcast_channel ? ud_c->max_pcie_bcast_batch : ud_c->max_send_wrs;

	ud_c->recv_q_depth = ud_c->max_recv_wrs;
   	ud_c->send_q_depth = (uint16_t) (2 * ud_c->ss_granularity *
   								    (ud_c->is_bcast_channel ?
								     ud_c->num_remote_channels : 1));

	aether_setup_send_wr_and_sgl(ud_c);
	aether_setup_recv_wr_and_sgl(ud_c, cb);

	ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];
	ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);


	ud_c->recv_reqs_size = ud_c->max_recv_wrs * ud_c->max_coalescing;
	ud_c->send_reqs_size = (uint16_t) (ud_c->max_send_wrs * (ud_c->is_inlining_enabled ? 1 : 2));

	ud_c->recv_reqs_buff = incoming_reqs_ptr;
    ud_c->send_reqs_buff = malloc(aether_ud_send_max_pkt_size(ud_c) * ud_c->send_reqs_size);


    if(!ud_c->is_inlining_enabled)
        ud_c->send_mem_region = register_buffer(cb->pd, ud_c->send_reqs_buff,
												aether_ud_send_max_pkt_size(ud_c) * ud_c->send_reqs_size);

    ud_c->send_reqs_push_ptr = 0;
    ud_c->recv_reqs_push_ptr = 0;
    ud_c->recv_reqs_pull_ptr = -1;

    assert(ud_c->max_pcie_bcast_batch <= credits_per_rem_channel);
}


typedef struct
{
    uint8_t type;
    uint8_t payload[99];
}
unit_test_req_t;

void
aether_unit_test()
{
    int worker_hid = 0;
    int total_worker_UD_QPs = 5;
    int ctrl_blk_key =  BASE_SHM_KEY + worker_hid;
    int *recv_q_depths = malloc(sizeof(int) * total_worker_UD_QPs);
    int *send_q_depths = malloc(sizeof(int) * total_worker_UD_QPs);

    ud_channel_t ack_ud_chnl;


	uint8_t max_credits = 5;
	uint8_t max_coalescing = 1;
	uint16_t max_rem_channels = 1;

    uint32_t dgram_buf_size = (sizeof(ud_channel_t) + sizeof(unit_test_req_t)) * max_coalescing * max_credits;
    struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(worker_hid,	/* local_hid */
												0, -1, /* port_index, numa_node_id */
												0, 0,	/* #conn qps, uc */
												NULL, 0, -1,	/* prealloc conn buf, buf size, key */
                                                1, dgram_buf_size,	/* num_dgram_qps, dgram_buf_size */
												ctrl_blk_key, /* key */
												recv_q_depths, send_q_depths); /* Depth of the dgram RECV, SEND Q*/
	qp_info_t* remote_qps;
	ud_channel_t inv_fake_channel;

	aether_ud_channel_init(cb, &ack_ud_chnl, 0, "ACK", SEND_RECV, max_coalescing,
						   sizeof(unit_test_req_t), cb->dgram_buf, 1, 0, remote_qps,
						   0,  &inv_fake_channel, max_credits, max_rem_channels, 1, 1);

}



/* ---------------------------------------------------------------------------
---------------------------------- UNICAST SENDs -----------------------------
---------------------------------------------------------------------------*/
static inline void
forge_unicast_wr(ud_channel_t* ud_c, uint8_t dst_qp_id, uint8_t* req_to_copy,
				 uint16_t* total_pkts_to_send, struct hrd_ctrl_blk* cb)
{
	struct ibv_wc signal_send_wc;
	uint16_t pkts_to_send = *total_pkts_to_send;

	aether_ud_send_pkt* curr_pkt_ptr = aether_curr_send_pkt_ptr(ud_c);
	uint8_t* next_req_ptr = curr_pkt_ptr->reqs + ud_c->max_req_size * curr_pkt_ptr->req_num;

	curr_pkt_ptr->req_num++;
	curr_pkt_ptr->sender_id = (uint8_t) machine_id;
	memcpy(next_req_ptr, req_to_copy, ud_c->max_req_size); // copy req to next_req_ptr

	if(AETHER_ENABLE_ASSERTIONS)
		assert(curr_pkt_ptr->req_num <= ud_c->max_coalescing);

	ud_c->send_sgl[pkts_to_send].length = sizeof(aether_ud_send_pkt) +
										  ud_c->max_req_size * curr_pkt_ptr->req_num;

	if(ud_c->enable_stats)
		ud_c->stats.send_total_msgs++;

	if(curr_pkt_ptr->req_num == 1) {

		ud_c->send_sgl[pkts_to_send].addr = (uint64_t) curr_pkt_ptr;

		ud_c->send_wr[pkts_to_send].wr.ud.ah = ud_c->remote_qps[dst_qp_id].ah;
		ud_c->send_wr[pkts_to_send].wr.ud.remote_qpn = ud_c->remote_qps[dst_qp_id].qpn;
		ud_c->send_wr[pkts_to_send].send_flags = ud_c->is_inlining_enabled ? IBV_SEND_INLINE : 0;
		if (pkts_to_send > 0)
			ud_c->send_wr[pkts_to_send - 1].next = &ud_c->send_wr[pkts_to_send];


		// Selective Signaling
		// Do a Signaled Send every ACK_SS_GRANULARITY msgs
		if (pkts_to_send % ud_c->ss_granularity == 0) {

			//if not the first SS --> poll the previous SS completion
			if(pkts_to_send > 0){
				hrd_poll_cq(cb->dgram_send_cq[ud_c->qp_id], 1, &signal_send_wc);

				if(ud_c->enable_stats)
					ud_c->stats.ss_completions++;

				if (AETHER_ENABLE_SS_PRINTS && ud_c->enable_prints)
					red_printf("^^^ Polled SS completion: %s %d (total %d)\n",
							   ud_c->qp_name, 1, ud_c->stats.ss_completions);
			}

			ud_c->send_wr[pkts_to_send].send_flags |= IBV_SEND_SIGNALED;
			if (AETHER_ENABLE_SS_PRINTS && ud_c->enable_prints)
				red_printf("vvv Send SS: %s\n", ud_c->qp_name);
		}

		(*total_pkts_to_send)++;
	}
}

static inline void
batch_unicast_pkts_2_NIC(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb,
						 uint16_t pkts_to_send, uint16_t total_msgs_in_batch)
{
	int ret;
	struct ibv_send_wr *bad_send_wr;

	if(ud_c->enable_stats)
		ud_c->stats.send_total_pkts += pkts_to_send;

	ud_c->send_wr[pkts_to_send - 1].next = NULL;

	if(AETHER_ENABLE_ASSERTIONS){
		for(int j = 0; j < pkts_to_send - 1; j++){
			assert(ud_c->send_wr[j].next == &ud_c->send_wr[j+1]);
			assert(ud_c->send_wr[j].opcode == IBV_WR_SEND);
			assert(ud_c->send_wr[j].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(ud_c->send_wr[j].sg_list == &ud_c->send_sgl[j]);
			assert(ud_c->send_wr[j].num_sge == 1);
			assert(!ud_c->is_inlining_enabled || ud_c->send_wr[j].send_flags == IBV_SEND_INLINE ||
				   ud_c->send_wr[j].send_flags == (IBV_SEND_INLINE | IBV_SEND_SIGNALED));

			// application specific asserts
//			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->req_num > 0);
//			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->reqs[0].opcode == ST_OP_ACK);
//			assert(((spacetime_ack_packet_t *) send_ack_sgl[j].addr)->reqs[0].sender == machine_id);
		}
		assert(ud_c->send_wr[pkts_to_send - 1].next == NULL);
	}

	if (AETHER_ENABLE_SEND_PRINTS && ud_c->enable_prints)
		cyan_printf(">>> Send: %d packets %s %d (Total packets: %d, acks: %d)\n",
					ud_c->qp_name, pkts_to_send, total_msgs_in_batch,
					ud_c->stats.send_total_pkts, ud_c->stats.send_total_msgs);
	ret = ibv_post_send(cb->dgram_qp[ud_c->qp_id], ud_c->send_wr, &bad_send_wr);
	CPE(ret, "ibv_post_send error while sending uincasts", ret);
}

static inline void
aether_check_if_batch_n_inc_pkt_ptr(ud_channel_t *ud_channel, struct hrd_ctrl_blk *cb,
									uint16_t* send_pkts_ptr, uint16_t* total_msgs_in_batch_ptr)
{
	(*send_pkts_ptr)++;

	uint16_t send_pkts = *send_pkts_ptr;
	uint16_t total_msgs_in_batch = *total_msgs_in_batch_ptr;

	if (send_pkts == ud_channel->max_send_wrs) {
		batch_unicast_pkts_2_NIC(ud_channel, cb, send_pkts, total_msgs_in_batch);
		*send_pkts_ptr = 0;
		*total_msgs_in_batch_ptr = 0;
	}

	aether_inc_send_push_ptr(ud_channel); //go to the next pkt
}

typedef int  (*skip_or_get_sender_id_t) (uint8_t*); //Should return -1 to skip otherwise returns the sender id
typedef void (*modify_elem_after_send_t)(uint8_t*);

static inline void
issue_unicasts(ud_channel_t* ud_channel, struct hrd_ctrl_blk *cb,
			   uint8_t* input_array_of_elems,
			   uint16_t input_array_len, uint16_t size_of_input_elems,
			   skip_or_get_sender_id_t skip_or_get_sender_id_func_ptr,
			   modify_elem_after_send_t modify_elem_after_send)
{
	uint16_t total_msgs_in_batch = 0, send_pkts = 0;
	uint8_t last_msg_dst = 255;
	uint8_t curr_msg_dst;

	if(AETHER_ENABLE_ASSERTIONS)
		assert(aether_curr_send_pkt_ptr(ud_channel)->req_num == 0);

	for (int i = 0; i < input_array_len; i++) {
		// Skip or Respond (copy and send ?)
		uint8_t* curr_elem = &input_array_of_elems[i * size_of_input_elems];
		int skip_or_sender_id = skip_or_get_sender_id_func_ptr(curr_elem);
		if(skip_or_sender_id < 0) break;

//		if (inv_recv_ops[i].op_meta.opcode == ST_EMPTY)
//			break;
//
//		if(ENABLE_ASSERTIONS){
//			assert(inv_recv_ops[i].op_meta.opcode == ST_INV_SUCCESS || inv_recv_ops[i].op_meta.opcode == ST_OP_INV ||
//				   inv_recv_ops[i].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
//			assert(inv_recv_ops[i].op_meta.val_len == ST_VALUE_SIZE);
//			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].op_meta.ts.tie_breaker_id == REMOTE_MACHINES - machine_id);
//			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].value[0] == (uint8_t) 'x' + (REMOTE_MACHINES - machine_id));
//			assert(REMOTE_MACHINES != 1 || inv_recv_ops[i].op_meta.sender == REMOTE_MACHINES - machine_id);
//		}


		if(AETHER_ENABLE_ASSERTIONS)
			assert(skip_or_sender_id < 255);
		curr_msg_dst = (uint8_t) skip_or_sender_id;

		// TODO we should make this an if in case we might not always have the credits
		if (!aether_has_sufficient_crds(ud_channel, curr_msg_dst))
			assert(0); // we should always have credits for acks

		aether_dec_crds(ud_channel, curr_msg_dst);

		// Send acks because if we cannot coalesce pkts, due to different endpoints
		if(aether_curr_send_pkt_ptr(ud_channel)->req_num != 0 && curr_msg_dst != last_msg_dst)
		    aether_check_if_batch_n_inc_pkt_ptr(ud_channel, cb, &send_pkts, &total_msgs_in_batch);

		last_msg_dst = curr_msg_dst;

		// Create the messages
		forge_unicast_wr(ud_channel, curr_msg_dst, curr_elem, &send_pkts, cb);

		modify_elem_after_send(curr_elem);
		// TODO: E.g.Empty inv buffer
//		if(inv_recv_ops[i].op_meta.opcode == ST_INV_SUCCESS ||
//		   inv_recv_ops[i].op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE)
//			inv_recv_ops[i].op_meta.opcode = ST_EMPTY;
//		else assert(0);


		total_msgs_in_batch++;

		// Check if we should send a batch since we might have reached the max batch size
		if(aether_curr_send_pkt_ptr(ud_channel)->req_num == ud_channel->max_coalescing)
			aether_check_if_batch_n_inc_pkt_ptr(ud_channel, cb, &send_pkts, &total_msgs_in_batch);
	}

	aether_ud_send_pkt* curr_pkt_ptr = aether_curr_send_pkt_ptr(ud_channel);
	if(curr_pkt_ptr->req_num > 0 && curr_pkt_ptr->req_num < ud_channel->max_coalescing)
		send_pkts++;

	// Force a batch to send the last set of requests (even < max batch size)
	if (send_pkts > 0)
		batch_unicast_pkts_2_NIC(ud_channel, cb, send_pkts, total_msgs_in_batch);

	//Move to next packet and reset data left from previous unicasts
	if(curr_pkt_ptr->req_num > 0 &&
	   curr_pkt_ptr->req_num < ud_channel->max_coalescing)
	    aether_inc_send_push_ptr(ud_channel);
}
