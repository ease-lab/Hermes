//
// Created by akatsarakis on 22/01/19.
//

#include <stdio.h>
#include <config.h>
#include <spacetime.h>
#include <infiniband/verbs.h>
#include <inline-util.h>
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

void aether_setup_send_wr_and_sgl(ud_channel_t *ud_c);
void aether_setup_recv_wr_and_sgl(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb);
void aether_setup_crd_wr_and_sgl (ud_channel_t *ud_c, struct hrd_ctrl_blk *cb);

// TODO may init CRD once an explicit crd control channel is created
void
aether_ud_channel_crd_init(struct hrd_ctrl_blk *cb, ud_channel_t *ud_c,
						   uint8_t qp_id, char* qp_name, qp_info_t *remote_qps,
						   volatile uint8_t *incoming_reqs_ptr,
						   // credits
						   ud_channel_t *linked_channel,
						   uint8_t credits_per_rem_channel, uint16_t num_channels,
						   //Toggles
						   uint8_t enable_stats, uint8_t enable_prints)
{
	assert(remote_qps != NULL);
	aether_assert_binary(enable_stats);
	aether_assert_binary(enable_prints);

    ud_c->type = CRD;
	ud_c->qp_id = qp_id;
	ud_c->qp_name = qp_name;
	ud_c->remote_qps = remote_qps;
	ud_c->num_channels = num_channels; //num_channels include our own channel
	ud_c->expl_crd_ctrl = 1;
    ud_c->is_bcast_channel = 0;
	ud_c->max_pcie_bcast_batch = 0;
	ud_c->num_crds_per_channel = credits_per_rem_channel;
    ud_c->channel_providing_crds = linked_channel;

	ud_c->enable_stats = enable_stats;
    ud_c->enable_prints = enable_prints;


    ud_c->no_crds_to_send_per_endpoint = malloc(sizeof(uint16_t) * num_channels);

	static_assert(sizeof(aether_crd_t) <= 4, ""); // Credits are always send as immediate <=4B
	ud_c->max_msg_size = 0; //non immediate size
	ud_c->max_coalescing = 1;


	uint16_t remote_channels = (uint16_t) (num_channels - 1);
	ud_c->is_inlining_enabled = 1;

    ud_c->credits_per_rem_channels = malloc(sizeof(uint8_t) * (num_channels));
	for(int i = 0; i < num_channels; ++i)
		ud_c->credits_per_rem_channels[i] = 0;


	ud_c->max_recv_wrs = credits_per_rem_channel * remote_channels;
    ud_c->max_send_wrs = credits_per_rem_channel * remote_channels; //TODO correct this

   	ud_c->ss_granularity = ud_c->max_send_wrs;

	ud_c->recv_q_depth = ud_c->max_recv_wrs;
   	ud_c->send_q_depth = (uint16_t) (2 * ud_c->ss_granularity);

	ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];
	ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);


	ud_c->recv_pkt_buff_len = ud_c->max_recv_wrs * ud_c->max_coalescing;
	ud_c->send_pkt_buff_len = ud_c->max_send_wrs ;

	ud_c->recv_pkt_buff = incoming_reqs_ptr;
    ud_c->send_pkt_buff = NULL; //malloc(aether_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);

	ud_c->send_mem_region = NULL;

	aether_setup_crd_wr_and_sgl(ud_c, cb);

    ud_c->send_push_ptr = 0;
    ud_c->recv_push_ptr = 0;
    ud_c->recv_pull_ptr = -1;

	ud_c->total_pkts_send = 0;

	ud_c->stats.ss_completions = 0;
    ud_c->stats.recv_total_pkts = 0;
	ud_c->stats.recv_total_msgs = 0;
	ud_c->stats.send_total_msgs = 0;
	ud_c->stats.send_total_pkts = 0;
	ud_c->stats.send_total_pcie_batches= 0;
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
					   uint8_t credits_per_rem_channel, uint16_t num_channels,
					   //Toggles
					   uint8_t enable_stats, uint8_t enable_prints)
{
	assert(remote_qps != NULL && type != CRD && linked_channel != NULL);  // if CRD type then used the *_crd_init instead
	aether_assert_binary(enable_stats);
	aether_assert_binary(enable_prints);
	aether_assert_binary(enable_inlining);
	aether_assert_binary(expl_credit_ctrl);
	aether_assert_binary(is_bcast_channel);

    ud_c->type = type;
	ud_c->qp_id = qp_id;
	ud_c->qp_name = qp_name;
	ud_c->remote_qps = remote_qps;
	ud_c->num_channels = num_channels; //num_channels include our own channel
	ud_c->expl_crd_ctrl = expl_credit_ctrl;
	ud_c->is_bcast_channel = is_bcast_channel;
	ud_c->channel_providing_crds = linked_channel;
	ud_c->num_crds_per_channel = credits_per_rem_channel;

	ud_c->enable_stats = enable_stats;
    ud_c->enable_prints = enable_prints;


	ud_c->max_msg_size = max_req_size;
	ud_c->max_coalescing = max_coalescing;

	ud_c->no_crds_to_send_per_endpoint = NULL; // unused for type != CRD

	uint16_t remote_channels = (uint16_t) (num_channels - 1);
	ud_c->is_inlining_enabled = enable_inlining;
    if(aether_ud_send_max_pkt_size(ud_c) > AETHER_MAX_SUPPORTED_INLINING) {
        if(ud_c->is_inlining_enabled)
            printf("Unfortunately, inlining for msgs sizes up to (%d) "
                   "is higher than the supported (%d)\n",
				   aether_ud_send_max_pkt_size(ud_c), AETHER_MAX_SUPPORTED_INLINING);
        ud_c->is_inlining_enabled = 0;
    }

    ud_c->credits_per_rem_channels = malloc(sizeof(uint8_t) * (num_channels));
	for(int i = 0; i < num_channels; ++i)
		ud_c->credits_per_rem_channels[i] = (uint8_t) (type == REQ ? credits_per_rem_channel : 0);


	ud_c->max_pcie_bcast_batch = (uint16_t) AETHER_MIN(AETHER_MIN_PCIE_BCAST_BATCH + 1, credits_per_rem_channel);
    //Warning! use min to avoid resetting the first req prior batching to the NIC
	//WARNING: todo check why we need to have MIN_PCIE_BCAST_BATCH + 1 instead of just MIN_PCIE_BCAST_BATCH
	uint16_t max_msgs_in_pcie_bcast = (uint16_t) (ud_c->max_pcie_bcast_batch * remote_channels); //must be smaller than the q_depth


	ud_c->max_recv_wrs = (uint16_t) (credits_per_rem_channel * remote_channels);
    ud_c->max_send_wrs = (uint16_t) (ud_c->is_bcast_channel ? max_msgs_in_pcie_bcast : credits_per_rem_channel * remote_channels);

   	ud_c->ss_granularity = ud_c->is_bcast_channel ? ud_c->max_pcie_bcast_batch : ud_c->max_send_wrs;

	ud_c->recv_q_depth = ud_c->max_recv_wrs;
   	ud_c->send_q_depth = (uint16_t) (2 * ud_c->ss_granularity *
   								    (ud_c->is_bcast_channel ? remote_channels : 1));

	ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];
	ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);


	ud_c->recv_pkt_buff_len = ud_c->max_recv_wrs * ud_c->max_coalescing;
	ud_c->send_pkt_buff_len = (uint16_t) (ud_c->max_send_wrs * (ud_c->is_inlining_enabled ? 1 : 2));

	ud_c->recv_pkt_buff = incoming_reqs_ptr;
    ud_c->send_pkt_buff = malloc(aether_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);


	ud_c->send_mem_region = ud_c->is_inlining_enabled ?  NULL :
							register_buffer(cb->pd, ud_c->send_pkt_buff, aether_ud_send_max_pkt_size(ud_c)
																		 * ud_c->send_pkt_buff_len);

	aether_setup_send_wr_and_sgl(ud_c);
	aether_setup_recv_wr_and_sgl(ud_c, cb);

    ud_c->send_push_ptr = 0;
    ud_c->recv_push_ptr = 0;
    ud_c->recv_pull_ptr = -1;

	ud_c->total_pkts_send = 0;

	ud_c->stats.ss_completions = 0;
    ud_c->stats.recv_total_pkts = 0;
	ud_c->stats.recv_total_msgs = 0;
	ud_c->stats.send_total_msgs = 0;
	ud_c->stats.send_total_pkts = 0;
	ud_c->stats.send_total_pcie_batches= 0;

    assert(ud_c->max_pcie_bcast_batch <= credits_per_rem_channel);
}

static inline void
print_on_off_toggle(uint16_t bin_flag, char* str)
{
	if(bin_flag > 1)
		printf("\t\t%s : %s (%d)\n", str, "\033[1m\033[32mOn\033[0m", bin_flag);
	else
		printf("\t\t%s : %s\n", str, bin_flag? "\033[1m\033[32mOn\033[0m" : "\033[31mOff\033[0m");
}

void
print_ud_c_overview(ud_channel_t* ud_c)
{
	printf("%s Channel %s(%d) --> %s\n",
		   ud_c->is_bcast_channel ? "Bcast" : "Unicast",
	       ud_c->qp_name, ud_c->qp_id, ud_c->type == REQ ? "REQ" : "RESP");

	print_on_off_toggle(ud_c->is_inlining_enabled, "Inlining");
	print_on_off_toggle(ud_c->max_coalescing, "Coalescing");
	print_on_off_toggle(ud_c->max_pcie_bcast_batch, "Max PCIe batch");

	printf("\t\tMax msg size: %d\n", ud_c->max_msg_size);
	printf("\t\tMax pkt size: send = %d, recv = %d\n",
		   aether_ud_send_max_pkt_size(ud_c), aether_ud_recv_max_pkt_size(ud_c));
	printf("\t\tSS granularity: %d\n", ud_c->ss_granularity);

	printf("\t\tNum remotes: %d\n", ud_c->num_channels - 1);
	printf("\t\tCredits: %d (%s) --> %s (%d)\n", ud_c->num_crds_per_channel,
		   ud_c->expl_crd_ctrl ? "Explicit" : "Implicit",
		   ud_c->channel_providing_crds->qp_name, ud_c->channel_providing_crds->qp_id);


	printf("\t\tSend Q len: %d\n", ud_c->send_q_depth);
	printf("\t\tRecv Q len: %d\n", ud_c->recv_q_depth);

	printf("\t\tSend wr len: %d\n", ud_c->max_send_wrs);
	printf("\t\tRecv wr len: %d\n", ud_c->max_recv_wrs);

	printf("\t\tSend pkt len: %d\n", ud_c->send_pkt_buff_len);
	printf("\t\tRecv pkt len: %d\n", ud_c->recv_pkt_buff_len);

	print_on_off_toggle(ud_c->enable_stats, "Stats");
	print_on_off_toggle(ud_c->enable_prints, "Prints");
}

/* ---------------------------------------------------------------------------
----------------------------------- SETUPs ------------------------------------
---------------------------------------------------------------------------*/
static inline void
aether_post_recvs(struct hrd_ctrl_blk *cb, ud_channel_t *ud_c, uint16_t num_of_receives);
static inline void
aether_post_crd_recvs(struct hrd_ctrl_blk *cb, ud_channel_t *ud_c, uint16_t num_recvs);

void
aether_setup_crd_wr_and_sgl(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb)
{
	assert(ud_c->type == CRD);

	// Credit Send WRs / sgl
    aether_crd_t crd_tmp;
    crd_tmp.crd_num = 0;
	crd_tmp.sender_id = (uint8_t) machine_id;

	ud_c->send_sgl = malloc(sizeof(struct ibv_sge));
	ud_c->send_sgl->length = 0;

	ud_c->send_wr  = malloc(sizeof(struct ibv_send_wr) * ud_c->max_send_wrs);
    for (int i = 0; i < ud_c->max_send_wrs; ++i) {
        ud_c->send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
        ud_c->send_wr[i].num_sge = 0;
        ud_c->send_wr[i].sg_list = ud_c->send_sgl;
        ud_c->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        ud_c->send_wr[i].next = NULL;
        ud_c->send_wr[i].send_flags = IBV_SEND_INLINE;
        ud_c->send_wr[i].imm_data = 0;
        memcpy(&ud_c->send_wr[i].imm_data, &crd_tmp, sizeof(aether_crd_t));
    }

	// Credit Recv WRs / sgl
    ud_c->recv_sgl = malloc(sizeof(struct ibv_sge));
    ud_c->recv_sgl->length = 64; // TODO can we make this zero?
    ud_c->recv_sgl->lkey = cb->dgram_buf_mr->lkey;
    ud_c->recv_sgl->addr = (uint64_t) ud_c->recv_pkt_buff;

	ud_c->recv_wr = malloc(sizeof(struct ibv_recv_wr) * ud_c->max_recv_wrs);
    for (int i = 0; i < ud_c->max_recv_wrs; ++i) {
        ud_c->recv_wr[i].num_sge = 1;
		ud_c->recv_wr[i].sg_list = ud_c->recv_sgl;
    }
}

void
aether_setup_send_wr_and_sgl(ud_channel_t *ud_c)
{
	assert(ud_c->type != CRD);

    if(ud_c->is_bcast_channel){ //Send bcast WRs

		uint16_t remote_channels = (uint16_t) (ud_c->num_channels - 1);
        uint16_t max_msgs_in_pcie_batch = (uint16_t) (ud_c->max_pcie_bcast_batch * remote_channels);
        ud_c->send_wr  = malloc(sizeof(struct ibv_send_wr) * max_msgs_in_pcie_batch);
        ud_c->send_sgl = malloc(sizeof(struct ibv_sge) * ud_c->max_pcie_bcast_batch);

        for(int i = 0; i < ud_c->max_pcie_bcast_batch; ++i)
            ud_c->send_sgl[i].length = aether_ud_send_max_pkt_size(ud_c);

        for(int i = 0; i < max_msgs_in_pcie_batch; ++i){
            int sgl_index = i / remote_channels;
            int i_mod_bcast = i % remote_channels;

            uint16_t rm_qp_id;
			if (i_mod_bcast < machine_id) rm_qp_id = (uint16_t) i_mod_bcast;
			else rm_qp_id = (uint16_t) ((i_mod_bcast + 1) % ud_c->num_channels);

			ud_c->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
            ud_c->send_wr[i].wr.ud.ah = ud_c->remote_qps[rm_qp_id].ah;
            ud_c->send_wr[i].wr.ud.remote_qpn = ud_c->remote_qps[rm_qp_id].qpn;

            ud_c->send_wr[i].num_sge = 1;
            ud_c->send_wr[i].opcode = IBV_WR_SEND; /// Attention!! there is no immediate here
            ud_c->send_wr[i].sg_list = &ud_c->send_sgl[sgl_index];

            if (!ud_c->is_inlining_enabled) {
                ud_c->send_wr[i].send_flags = 0;
                ud_c->send_sgl[sgl_index].lkey = ud_c->send_mem_region->lkey;
            } else
                ud_c->send_wr[i].send_flags = IBV_SEND_INLINE;

            ud_c->send_wr[i].next = (i_mod_bcast == remote_channels - 1) ? NULL : &ud_c->send_wr[i + 1];
        }

    }else{ //Send unicast WRs

        ud_c->send_sgl = malloc(sizeof(struct ibv_sge) * ud_c->max_send_wrs);
        ud_c->send_wr  = malloc(sizeof(struct ibv_send_wr) * ud_c->max_send_wrs);
        for(int i = 0; i < ud_c->max_send_wrs; ++i){

            ud_c->send_sgl[i].length = sizeof(aether_pkt_t) + aether_ud_recv_max_pkt_size(ud_c);

            ud_c->send_wr[i].num_sge = 1;
            ud_c->send_wr[i].opcode = IBV_WR_SEND; /// Attention!! there is no immediate here
            ud_c->send_wr[i].sg_list = &ud_c->send_sgl[i];
            ud_c->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;

            if (!ud_c->is_inlining_enabled){
                ud_c->send_wr[i].send_flags = 0;
                ud_c->send_sgl[i].lkey = ud_c->send_mem_region->lkey;
            } else
                ud_c->send_wr[i].send_flags = IBV_SEND_INLINE;
        }
    }
}

void
aether_setup_recv_wr_and_sgl(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb)
{
	assert(ud_c->type != CRD);

    ud_c->recv_sgl= malloc(sizeof(struct ibv_sge) * ud_c->max_recv_wrs);
    ud_c->recv_wr = malloc(sizeof(struct ibv_recv_wr) * ud_c->max_recv_wrs);

	for (int i = 0; i < ud_c->max_recv_wrs; i++) {
		ud_c->recv_sgl[i].length = aether_ud_recv_max_pkt_size(ud_c);
		ud_c->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        ud_c->recv_wr[i].sg_list = &ud_c->recv_sgl[i];
        ud_c->recv_wr[i].num_sge = 1;
        ud_c->recv_wr[i].next = (i == ud_c->max_recv_wrs - 1) ? NULL : &ud_c->recv_wr[i + 1];
	}
}


void
aether_setup_incoming_buff_and_post_initial_recvs(ud_channel_t* ud_c, struct hrd_ctrl_blk *cb)
{
    if(ud_c->type != CRD){
		//init recv buffs as empty (not need for CRD since CRD msgs are --immediate-- header-only)
		for(uint16_t i = 0; i < ud_c->send_pkt_buff_len; ++i)
			aether_get_nth_pkt_ptr_from_send_buff(ud_c, i)->req_num = 0;
		for(uint16_t i = 0; i < ud_c->recv_pkt_buff_len; ++i)
			aether_get_nth_pkt_ptr_from_recv_buff(ud_c, i)->pkt.req_num = 0;
    }

	if(AETHER_ENABLE_POST_RECV_PRINTS && ud_c->enable_prints)
		yellow_printf("vvv Post Initial Receives: %s %d\n", ud_c->qp_name, ud_c->max_recv_wrs);

	if(ud_c->type != CRD)
		aether_post_recvs(cb, ud_c, ud_c->max_recv_wrs);
	else
		aether_post_crd_recvs(cb, ud_c, ud_c->max_recv_wrs);
}

/* --------------------------------------------------------------------------
----------------------------APP specific Asserts ----------------------------
-------------------------------------------------------------------------- */
static inline void
ack_send_asserts(aether_ud_send_pkt_t *curr_pkt, ud_channel_t* ud_c)
{
	assert(curr_pkt->req_num > 0);
	for(int i = 0; i < curr_pkt->req_num ; i++){
		spacetime_ack_t* ack_ptr = (spacetime_ack_t *) &curr_pkt->reqs[i * ud_c->max_msg_size];
		assert(ack_ptr->opcode == ST_OP_ACK); //|| ack_ptr->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
		assert(ack_ptr->sender == machine_id);
	}
}

static inline void
inv_send_asserts(aether_ud_send_pkt_t *curr_pkt, ud_channel_t* ud_c)
{
	assert(curr_pkt->req_num > 0);
	for(int i = 0; i < curr_pkt->req_num; i++){
		spacetime_inv_t* inv_ptr = (spacetime_inv_t *) &curr_pkt->reqs[i * ud_c->max_msg_size];
		assert(inv_ptr->op_meta.opcode == ST_OP_INV); //|| inv_ptr->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE);
		assert(inv_ptr->op_meta.sender == machine_id);
		assert( inv_ptr->op_meta.ts.tie_breaker_id == machine_id ||
				(ENABLE_VIRTUAL_NODE_IDS && inv_ptr->op_meta.ts.tie_breaker_id % MACHINE_NUM == machine_id));
	}
}

/* ---------------------------------------------------------------------------
----------------------------------- RECVs ------------------------------------
---------------------------------------------------------------------------*/
static inline void
aether_post_crd_recvs(struct hrd_ctrl_blk *cb, ud_channel_t *ud_c, uint16_t num_recvs)
{
    assert(ud_c->type == CRD);

	struct ibv_recv_wr *bad_recv_wr;
	for (uint16_t i = 0; i < num_recvs; ++i)
		ud_c->recv_wr[i].next = (i == num_recvs - 1) ? NULL : &ud_c->recv_wr[i + 1];

	int ret = ibv_post_recv(cb->dgram_qp[ud_c->qp_id], ud_c->recv_wr, &bad_recv_wr);
	CPE(ret, "ibv_post_recv error: posting recvs for credits", ret);
}

static inline void
aether_post_recvs(struct hrd_ctrl_blk *cb, ud_channel_t *ud_c, uint16_t num_of_receives)
{
	void* next_buff_addr;

	if(AETHER_ENABLE_ASSERTIONS)
		assert(num_of_receives <= ud_c->max_recv_wrs);

    int req_size = aether_ud_recv_max_pkt_size(ud_c);
	for(int i = 0; i < num_of_receives; ++i){
        next_buff_addr = (void*) (ud_c->recv_pkt_buff) + (ud_c->recv_push_ptr * req_size);
        // TODO optimize by reseting only the req_num of aether_recv_pkt
		memset(next_buff_addr, 0, (size_t) req_size); //reset the buffer before posting the receive

		if(AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC)
			ud_c->recv_wr[i].sg_list->addr = (uintptr_t) next_buff_addr;
		else
			hrd_post_dgram_recv(cb->dgram_qp[ud_c->qp_id], next_buff_addr, req_size, cb->dgram_buf_mr->lkey);

		aether_inc_recv_push_ptr(ud_c);
	}

	if(AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC) {
		ud_c->recv_wr[num_of_receives - 1].next = NULL;
		if (AETHER_ENABLE_ASSERTIONS) {
			for (int i = 0; i < num_of_receives; i++) {
				assert(ud_c->recv_wr[i].num_sge == 1);
				assert(ud_c->recv_wr[i].sg_list->length == req_size);
				assert(ud_c->recv_wr[i].sg_list->lkey == cb->dgram_buf_mr->lkey);
				assert(i == num_of_receives - 1 || ud_c->recv_wr[i].next == &ud_c->recv_wr[i + 1]);
			}
			assert(ud_c->recv_wr[num_of_receives - 1].next == NULL);
		}

		struct ibv_recv_wr *bad_recv_wr;
		int ret = ibv_post_recv(cb->dgram_qp[ud_c->qp_id], ud_c->recv_wr, &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: while posting recvs", ret);

		//recover next ptr of last wr to NULL
		ud_c->recv_wr[num_of_receives - 1].next = (ud_c->max_recv_wrs == num_of_receives - 1) ?
                                                        NULL : &ud_c->recv_wr[num_of_receives];
	}
}

static inline void
aether_poll_crds_and_post_recvs(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb)
{
    assert(ud_c->type == CRD);

	int crd_pkts_found = ibv_poll_cq(ud_c->recv_cq, ud_c->max_recv_wrs, ud_c->recv_wc);

	if(crd_pkts_found > 0) {
		if(unlikely(ud_c->recv_wc[crd_pkts_found -1].status != 0)) {
			fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n",
                    ud_c->recv_wc[crd_pkts_found -1].status);
			exit(0);
		}

		if(ud_c->enable_stats)
		    ud_c->stats.recv_total_pkts += crd_pkts_found;

		if(AETHER_ENABLE_RECV_PRINTS && ud_c->enable_prints)
			green_printf("^^^ Polled reqs: %s  %d, (total: %d)!\n",
                         ud_c->qp_name, crd_pkts_found, ud_c->stats.recv_total_pkts);

		for (int i = 0; i < crd_pkts_found; i++){
			aether_crd_t* crd_ptr = (aether_crd_t*) &ud_c->recv_wc[i].imm_data;

            if(ud_c->enable_stats)
                ud_c->stats.recv_total_msgs += crd_ptr->crd_num;
            ud_c->channel_providing_crds->credits_per_rem_channels[crd_ptr->sender_id] += crd_ptr->crd_num;

			if(AETHER_ENABLE_ASSERTIONS)
                assert(ud_c->channel_providing_crds->num_crds_per_channel >=
                       ud_c->channel_providing_crds->credits_per_rem_channels[crd_ptr->sender_id]);

			if(AETHER_ENABLE_CREDIT_PRINTS && ud_c->enable_prints)
				printf("$$$ Credits: %s \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",
					   ud_c->channel_providing_crds->qp_name,
					   ud_c->channel_providing_crds->credits_per_rem_channels[crd_ptr->sender_id],
					   crd_ptr->sender_id);
		}

		if (AETHER_ENABLE_POST_RECV_PRINTS && ud_c->enable_prints)
			yellow_printf("vvv Post Receives: %s %d\n", ud_c->qp_name, crd_pkts_found);

		aether_post_crd_recvs(cb, ud_c, (uint16_t) crd_pkts_found);

	} else if(unlikely(crd_pkts_found < 0)) {
		printf("ERROR In the credit CQ\n");
		exit(0);
	}
}

//static inline
uint16_t
aether_poll_buff_and_post_recvs(ud_channel_t* ud_channel, uint16_t max_pkts_to_poll,
								uint8_t* recv_ops, struct hrd_ctrl_blk *cb)
{
	int index = 0;
    uint8_t sender = 0;
	uint16_t msgs_polled = 0;
    uint8_t* next_packet_reqs, *recv_op_ptr, *next_req, *next_packet_req_num_ptr;

    uint16_t max_req_size = aether_ud_recv_max_pkt_size(ud_channel);

	//poll completion q
	uint16_t pkts_polled = (uint16_t) ibv_poll_cq(ud_channel->recv_cq, max_pkts_to_poll, ud_channel->recv_wc);
	for(int i = 0; i < pkts_polled; ++i){

		index = (ud_channel->recv_pull_ptr + 1) % ud_channel->recv_q_depth;
		aether_ud_recv_pkt_t* next_packet = (aether_ud_recv_pkt_t *) &ud_channel->recv_pkt_buff[index * max_req_size];

		sender = next_packet->pkt.sender_id;
		next_packet_reqs = next_packet->pkt.reqs;
		next_packet_req_num_ptr = &next_packet->pkt.req_num;

		if(AETHER_ENABLE_ASSERTIONS)
			assert(next_packet->pkt.req_num > 0 && next_packet->pkt.req_num <= ud_channel->max_coalescing);


		//TODO add membership and functionality
//        if(node_is_in_membership(last_group_membership, sender))
		for(int j = 0; j < next_packet->pkt.req_num; ++j){
			next_req = &next_packet_reqs[j * ud_channel->max_msg_size];
			recv_op_ptr = &recv_ops[msgs_polled * ud_channel->max_msg_size];

			memcpy(recv_op_ptr, next_req, ud_channel->max_msg_size);

			msgs_polled++;
			ud_channel->channel_providing_crds->credits_per_rem_channels[sender]++; //increment packet credits
		}



		*next_packet_req_num_ptr = 0; //TODO can be removed since we already reset on posting receives
		aether_inc_recv_pull_ptr(ud_channel);


		if(AETHER_ENABLE_ASSERTIONS)
			assert(ud_channel->channel_providing_crds->credits_per_rem_channels[sender] <=
				   ud_channel->channel_providing_crds->num_crds_per_channel);
	}


	if(pkts_polled > 0){
		//Refill recvs
		aether_post_recvs(cb, ud_channel, pkts_polled);


		if(AETHER_ENABLE_STAT_COUNTING){
			ud_channel->stats.recv_total_msgs += msgs_polled;
			ud_channel->stats.recv_total_pkts += pkts_polled;
		}

		if(AETHER_ENABLE_RECV_PRINTS && ud_channel->enable_prints)
			green_printf("^^^ Polled msgs: %d packets %s %d, (total pkts: %d, msgs %d)!\n",
						 pkts_polled, ud_channel->qp_name, msgs_polled,
						 ud_channel->stats.recv_total_pkts, ud_channel->stats.recv_total_msgs);
		if(AETHER_ENABLE_CREDIT_PRINTS && ud_channel->enable_prints)
			printf("$$$ Credits: %s \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",
				   ud_channel->channel_providing_crds->qp_name,
				   ud_channel->channel_providing_crds->credits_per_rem_channels[sender], sender);
		if (AETHER_ENABLE_POST_RECV_PRINTS && ud_channel->enable_prints)
			yellow_printf("vvv Post Receives: %s %d\n", ud_channel->qp_name, pkts_polled);

		if(AETHER_ENABLE_ASSERTIONS)
			assert(ud_channel->max_coalescing != 1 || pkts_polled == msgs_polled);
	}

    return msgs_polled;
}

/* ---------------------------------------------------------------------------
----------------------------------- CREDITS ----------------------------------
---------------------------------------------------------------------------*/
// For all the CREDIT functions --> if its a bcast channel endpoint_id is ignored
static inline uint8_t
aether_has_sufficient_crds_no_polling(ud_channel_t *ud_c, uint8_t endpoint_id)
{
    if (!ud_c->is_bcast_channel)
        return (uint8_t) (ud_c->credits_per_rem_channels[endpoint_id] > 0);
    else
        for (int i = 0; i < ud_c->num_channels; ++i) {
            if (i == machine_id) continue;
            //TODO if i == local_node_id  || !node_in_membership(i) --> continue
//            if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
            if (ud_c->credits_per_rem_channels[i] <= 0)
                return 0;
        }
    return 1;
}

static inline uint8_t
aether_has_sufficient_crds(ud_channel_t *ud_c, struct hrd_ctrl_blk* cb, uint8_t endpoint_id)
{
    if(aether_has_sufficient_crds_no_polling(ud_c, endpoint_id))
        return 1;

    if(ud_c->expl_crd_ctrl) {
		aether_poll_crds_and_post_recvs(ud_c->channel_providing_crds, cb);

        if(aether_has_sufficient_crds_no_polling(ud_c, endpoint_id))
            return 1;
    }
    return 0;
}

static inline void
aether_dec_crds(ud_channel_t *ud_c, uint8_t endpoint_id)
{
    if(AETHER_ENABLE_ASSERTIONS)
        assert(aether_has_sufficient_crds_no_polling(ud_c, endpoint_id));

    if(!ud_c->is_bcast_channel)
        ud_c->credits_per_rem_channels[endpoint_id]--;
    else
        for(int i = 0; i < ud_c->num_channels; ++i){
            if(i == machine_id) continue;
			//TODO if i == local_node_id  || !node_in_membership(i) --> continue
//            if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
            ud_c->credits_per_rem_channels[i]--;
        }

    if (AETHER_ENABLE_CREDIT_PRINTS && ud_c->enable_prints){
        if(ud_c->is_bcast_channel)
            endpoint_id = (uint8_t) (machine_id == 0 ? 1 : 0);

        printf("$$$ Credits: %s \033[31mdecremented\033[0m to %d",
               ud_c->qp_name, ud_c->credits_per_rem_channels[endpoint_id]);

        if(ud_c->is_bcast_channel)
            printf(" (all endpoints)\n");
        else
            printf(" (for endpoint %d)\n", endpoint_id);
    }
}

/* ---------------------------------------------------------------------------
----------------------------------- SENDs ------------------------------------
---------------------------------------------------------------------------*/
static inline void
aether_forge_crd_wr(ud_channel_t *ud_c, struct hrd_ctrl_blk* cb, uint16_t dst_qp_id,
					uint16_t crd_pkts_to_send, uint16_t crd_to_send)
{
    assert(ud_c->type == CRD);

	ud_c->send_wr[crd_pkts_to_send].send_flags = IBV_SEND_INLINE;
	ud_c->send_wr[crd_pkts_to_send].wr.ud.ah = ud_c->remote_qps[dst_qp_id].ah;
	ud_c->send_wr[crd_pkts_to_send].wr.ud.remote_qpn = ud_c->remote_qps[dst_qp_id].qpn;

	((aether_crd_t*) &ud_c->send_wr[crd_pkts_to_send].imm_data)->crd_num = crd_to_send;

	if(ud_c->enable_stats)
	    ud_c->stats.send_total_msgs += crd_to_send;

	if (crd_pkts_to_send > 0)
		ud_c->send_wr[crd_pkts_to_send - 1].next = &ud_c->send_wr[crd_pkts_to_send];

	// Selective Signaling --> Do a Signaled Send every ss_granularity pkts
	if (ud_c->total_pkts_send % ud_c->ss_granularity == 0) {

		//if not the first SS --> poll the previous SS completion
		if(ud_c->total_pkts_send > 0){
			struct ibv_wc signal_send_wc;
			hrd_poll_cq(cb->dgram_send_cq[ud_c->qp_id], 1, &signal_send_wc);

			if(ud_c->enable_stats)
				ud_c->stats.ss_completions++;

			if (AETHER_ENABLE_SS_PRINTS && ud_c->enable_prints)
				red_printf("^^^ Polled SS completion: %s %d (total %d)\n",
						   ud_c->qp_name, 1, ud_c->stats.ss_completions);
		}

		ud_c->send_wr[crd_pkts_to_send].send_flags |= IBV_SEND_SIGNALED;
		if (AETHER_ENABLE_SS_PRINTS && ud_c->enable_prints)
			red_printf("vvv Send SS: %s\n", ud_c->qp_name);
	}
	ud_c->total_pkts_send++;
}

static inline void
aether_forge_wr(ud_channel_t *ud_c, uint8_t dst_qp_id, uint8_t *req_to_copy,
				uint16_t pkts_in_batch, uint16_t *msgs_in_batch,
				struct hrd_ctrl_blk *cb, copy_and_modify_input_elem_t copy_and_modify_elem)
// dst_qp_id is ignored if its a bcast channel
{
	struct ibv_wc signal_send_wc;

	aether_ud_send_pkt_t* curr_pkt_ptr = aether_curr_send_pkt_ptr(ud_c);
	uint8_t* next_req_ptr = aether_get_n_msg_ptr_from_send_pkt(ud_c, curr_pkt_ptr, curr_pkt_ptr->req_num);
	curr_pkt_ptr->req_num++;
	curr_pkt_ptr->sender_id = (uint8_t) machine_id;

	//<Copy & modify elem!> --> callback func that copies and manipulated data from req_to_copy buff
	copy_and_modify_elem(next_req_ptr, req_to_copy);

	if(AETHER_ENABLE_ASSERTIONS)
		assert(curr_pkt_ptr->req_num <= ud_c->max_coalescing);

	ud_c->send_sgl[pkts_in_batch].length = sizeof(aether_ud_send_pkt_t) +
										   ud_c->max_msg_size * curr_pkt_ptr->req_num;

	if(ud_c->enable_stats)
		ud_c->stats.send_total_msgs++;

	if(curr_pkt_ptr->req_num == 1) {

		ud_c->send_sgl[pkts_in_batch].addr = (uint64_t) curr_pkt_ptr;


		if(!ud_c->is_bcast_channel){ // set the dst qp
			ud_c->send_wr[pkts_in_batch].wr.ud.ah = ud_c->remote_qps[dst_qp_id].ah;
			ud_c->send_wr[pkts_in_batch].wr.ud.remote_qpn = ud_c->remote_qps[dst_qp_id].qpn;
		}

		uint16_t wr_idx = (uint16_t) (pkts_in_batch * (ud_c->is_bcast_channel ? ud_c->num_channels - 1 : 1));
		ud_c->send_wr[wr_idx].send_flags = ud_c->is_inlining_enabled ? IBV_SEND_INLINE : 0;

		if (wr_idx > 0) // set previous send_wr to point to curr
			ud_c->send_wr[wr_idx - 1].next = &ud_c->send_wr[wr_idx];


		// Selective Signaling --> Do a Signaled Send every ss_granularity pkts
		if (ud_c->total_pkts_send % ud_c->ss_granularity == 0) {

			//if not the first SS --> poll the previous SS completion
			if(ud_c->total_pkts_send > 0){
				hrd_poll_cq(cb->dgram_send_cq[ud_c->qp_id], 1, &signal_send_wc);

				if(ud_c->enable_stats)
					ud_c->stats.ss_completions++;

				if (AETHER_ENABLE_SS_PRINTS && ud_c->enable_prints)
					red_printf("^^^ Polled SS completion: %s %d (total %d)\n",
							   ud_c->qp_name, 1, ud_c->stats.ss_completions);
			}

			ud_c->send_wr[wr_idx].send_flags |= IBV_SEND_SIGNALED;
			if (AETHER_ENABLE_SS_PRINTS && ud_c->enable_prints)
				red_printf("vvv Send SS: %s\n", ud_c->qp_name);
		}
		ud_c->total_pkts_send++;
	}

	(*msgs_in_batch)++;
}

static inline void
aether_batch_pkts_2_NIC(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb,
						uint16_t pkts_in_batch, uint16_t msgs_in_batch)
{
	int ret;
	struct ibv_send_wr *bad_send_wr;
	uint16_t remote_channels = (uint16_t) (ud_c->num_channels - 1);

	if(ud_c->enable_stats)
		ud_c->stats.send_total_pkts += pkts_in_batch;

	uint16_t wr_idx = (uint16_t) (pkts_in_batch * (ud_c->is_bcast_channel ?  remote_channels : 1));
	ud_c->send_wr[wr_idx - 1].next = NULL;

	if(AETHER_ENABLE_ASSERTIONS){

		assert(pkts_in_batch <= ud_c->max_send_wrs);
		assert(pkts_in_batch <= ud_c->send_pkt_buff_len);
		assert(ud_c->max_coalescing > 1 || msgs_in_batch ==  pkts_in_batch);
		assert(ud_c->max_coalescing > 1 || ud_c->stats.send_total_msgs ==  ud_c->stats.send_total_pkts);

		assert(ud_c->send_wr[wr_idx - 1].next == NULL);
		for(int i = 0; i < wr_idx; ++i){
			uint16_t sgl_idx = (uint16_t) (i / (ud_c->is_bcast_channel ? remote_channels : 1));

			if(ud_c->type != CRD){
				assert(ud_c->send_wr[i].num_sge == 1);
				assert(ud_c->send_wr[i].opcode == IBV_WR_SEND);
				assert(ud_c->send_wr[i].sg_list == &ud_c->send_sgl[sgl_idx]);

				aether_ud_send_pkt_t* curr_send_pkt = (aether_ud_send_pkt_t*) ud_c->send_sgl[sgl_idx].addr;
				assert(curr_send_pkt->req_num > 0);
				//TODO only for dbg application specific asserts
				if(ud_c->qp_id == INV_UD_QP_ID)
					inv_send_asserts(curr_send_pkt, ud_c);
				else if(ud_c->qp_id == ACK_UD_QP_ID)
					ack_send_asserts(curr_send_pkt, ud_c);
			} else {
				assert(ud_c->send_wr[i].num_sge == 0);
				assert(ud_c->send_wr[i].sg_list->length == 0);
				assert(ud_c->send_wr[i].opcode == IBV_WR_SEND_WITH_IMM);
				assert(((aether_crd_t*) &(ud_c->send_wr[i].imm_data))->crd_num > 0);
				assert(((aether_crd_t*) &(ud_c->send_wr[i].imm_data))->sender_id == machine_id);
			}

			assert(ud_c->send_wr[i].wr.ud.remote_qkey == HRD_DEFAULT_QKEY);
			assert(i == wr_idx - 1 || ud_c->send_wr[i].next == &ud_c->send_wr[i + 1]);
			assert(!ud_c->is_inlining_enabled || ud_c->send_wr[i].send_flags == IBV_SEND_INLINE ||
				   ud_c->send_wr[i].send_flags == (IBV_SEND_INLINE | IBV_SEND_SIGNALED));
		}
	}

	if (AETHER_ENABLE_SEND_PRINTS && ud_c->enable_prints) //TODO make this work w/ bcasts
		cyan_printf(">>> Send: %d packets %s %d (Total packets: %d, msgs: %d)\n",
					pkts_in_batch, ud_c->qp_name, msgs_in_batch,
					ud_c->stats.send_total_pkts, ud_c->stats.send_total_msgs);

	ret = ibv_post_send(cb->dgram_qp[ud_c->qp_id], ud_c->send_wr, &bad_send_wr);
	CPE(ret, "ibv_post_send error while sending msgs to the NIC", ret);
}

static inline void
aether_check_if_batch_n_inc_pkt_ptr(ud_channel_t *ud_channel, struct hrd_ctrl_blk *cb,
									uint16_t* pkts_in_batch_ptr, uint16_t* msgs_in_batch_ptr)
{

    (*pkts_in_batch_ptr)++;
	uint16_t send_pkts = *pkts_in_batch_ptr;
	uint16_t total_msgs_in_batch = *msgs_in_batch_ptr;
	uint16_t max_pkt_batch = ud_channel->is_bcast_channel ? ud_channel->max_pcie_bcast_batch :
							 								ud_channel->max_send_wrs;

	if (send_pkts == max_pkt_batch) {
		aether_batch_pkts_2_NIC(ud_channel, cb, send_pkts, total_msgs_in_batch);
		*pkts_in_batch_ptr = 0;
		*msgs_in_batch_ptr = 0;
	}

	aether_inc_send_push_ptr(ud_channel); //go to the next pkt
}

uint8_t
aether_issue_pkts(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb,
				  uint8_t *input_array_of_elems, uint16_t input_array_len,
				  uint16_t size_of_input_elems, uint16_t* input_array_rolling_idx,
				  skip_input_elem_or_get_sender_id_t skip_or_get_sender_id_func_ptr,
				  modify_input_elem_after_send_t modify_elem_after_send,
				  copy_and_modify_input_elem_t copy_and_modify_elem)
{
	uint8_t curr_msg_dst;
	uint8_t last_msg_dst = 255;
	uint8_t has_outstanding_msgs = 0;
	uint16_t msgs_in_batch = 0, pkts_in_batch = 0, idx = 0;

	if(AETHER_ENABLE_ASSERTIONS)
		assert(aether_curr_send_pkt_ptr(ud_c)->req_num == 0);

	for (int i = 0; i < input_array_len; i++) {
		idx = (uint16_t) (input_array_rolling_idx == NULL ?
						  i : (i + *input_array_rolling_idx) % input_array_len);

		// Skip or Respond (copy and send ?)
		uint8_t* curr_elem = &input_array_of_elems[idx * size_of_input_elems];
		int skip_or_sender_id = skip_or_get_sender_id_func_ptr(curr_elem);
		if(skip_or_sender_id < 0) continue;

		if(AETHER_ENABLE_ASSERTIONS) assert(skip_or_sender_id < 255);

		curr_msg_dst = (uint8_t) skip_or_sender_id;

		// Break if we do not have sufficient credits
		if (!aether_has_sufficient_crds(ud_c, cb, curr_msg_dst)) {
			has_outstanding_msgs = 1;

			if(input_array_rolling_idx != NULL)
				*input_array_rolling_idx = idx;
			break; // we need to break for broadcast (lets assume it is ok to break for unicasts as well since it may only harm perf)
		}

		aether_dec_crds(ud_c, curr_msg_dst);

		if(!ud_c->is_bcast_channel)
			// Send unicasts because if we might cannot coalesce pkts, due to different endpoints
			if(aether_curr_send_pkt_ptr(ud_c)->req_num > 0 && curr_msg_dst != last_msg_dst)
				aether_check_if_batch_n_inc_pkt_ptr(ud_c, cb, &pkts_in_batch, &msgs_in_batch);

		last_msg_dst = curr_msg_dst;

		// Create the messages
		aether_forge_wr(ud_c, curr_msg_dst, curr_elem, pkts_in_batch,
						&msgs_in_batch, cb, copy_and_modify_elem);

		modify_elem_after_send(curr_elem); // E.g. Change the state of the element which triggered a send

		// Check if we should send a batch since we might have reached the max batch size
		if(aether_curr_send_pkt_ptr(ud_c)->req_num == ud_c->max_coalescing)
			aether_check_if_batch_n_inc_pkt_ptr(ud_c, cb, &pkts_in_batch, &msgs_in_batch);
	}

	// Even if the last pkt is not full do the appropriate actions and incl to NIC batch
	aether_ud_send_pkt_t* curr_pkt_ptr = aether_curr_send_pkt_ptr(ud_c);
	if(curr_pkt_ptr->req_num > 0 && curr_pkt_ptr->req_num < ud_c->max_coalescing)
		pkts_in_batch++;

	// Force a batch to send the last set of requests (even < max batch size)
	if (pkts_in_batch > 0)
		aether_batch_pkts_2_NIC(ud_c, cb, pkts_in_batch, msgs_in_batch);

	// Move to next packet and reset data left from previous bcasts/unicasts
	if(curr_pkt_ptr->req_num > 0 && curr_pkt_ptr->req_num < ud_c->max_coalescing)
		aether_inc_send_push_ptr(ud_c);


	return has_outstanding_msgs;
}

void
aether_issue_credits(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb, uint8_t *input_array_of_elems,
					 uint16_t input_array_len, uint16_t size_of_input_elems,
					 skip_input_elem_or_get_sender_id_t skip_or_get_sender_id_func_ptr,
					 modify_input_elem_after_send_t modify_elem_after_send)
{
    assert(ud_c->type == CRD);

	for (int i = 0; i < ud_c->num_channels; ++i)
		ud_c->no_crds_to_send_per_endpoint[i] = 0;

	for (int i = 0; i < input_array_len; ++i) {
		// Skip or Respond (copy and send ?)
		uint8_t* curr_elem = &input_array_of_elems[i * size_of_input_elems];
		int skip_or_sender_id = skip_or_get_sender_id_func_ptr(curr_elem);
		if(AETHER_ENABLE_ASSERTIONS) assert(skip_or_sender_id < 255);
		if(skip_or_sender_id < 0) continue; //TODO may make this break

		uint8_t curr_msg_dst = (uint8_t) skip_or_sender_id;

		// Check if we have sufficient credits --> (we should always have enough credits for CRDs)
		if (!aether_has_sufficient_crds(ud_c, cb, curr_msg_dst))
			assert(0);
		if(ud_c->no_crds_to_send_per_endpoint[curr_msg_dst] == 0 && ud_c->credits_per_rem_channels[curr_msg_dst] == 0)
			assert(0);

		aether_dec_crds(ud_c, curr_msg_dst);

		if (AETHER_ENABLE_CREDIT_PRINTS && ud_c->enable_prints)
			printf("$$$ Credits: %s \033[31mdecremented\033[0m to %d (for machine %d)\n",
				   ud_c->qp_name, ud_c->credits_per_rem_channels[curr_msg_dst], curr_msg_dst);

		ud_c->no_crds_to_send_per_endpoint[curr_msg_dst]++;

		modify_elem_after_send(curr_elem); // E.g. Change the state of the element which triggered a send
	}

	uint16_t send_crd_packets = 0, total_credits_to_send = 0;
	for(uint16_t i = 0; i < ud_c->num_channels; ++i){
		if(i == machine_id) continue;

		if(ud_c->no_crds_to_send_per_endpoint[i] > 0) {
			aether_forge_crd_wr(ud_c, cb, i, send_crd_packets, ud_c->no_crds_to_send_per_endpoint[i]);
			send_crd_packets++;
			total_credits_to_send += ud_c->no_crds_to_send_per_endpoint[i];

			if (send_crd_packets == MAX_SEND_CRD_WRS) {
			    aether_batch_pkts_2_NIC(ud_c, cb, send_crd_packets, total_credits_to_send);
				send_crd_packets = 0;
				total_credits_to_send = 0;
			}
		}
	}

	if (send_crd_packets > 0)
		aether_batch_pkts_2_NIC(ud_c, cb, send_crd_packets, total_credits_to_send);
}
