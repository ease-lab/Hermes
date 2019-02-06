//
// Created by akatsarakis on 22/01/19.
//

#include <stdio.h>
#include <config.h>
#include <spacetime.h>
#include <infiniband/verbs.h>
#include <inline-util.h>
#include "../../include/aether/aether.h"

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

void _aether_setup_send_wr_and_sgl(ud_channel_t *ud_c);
void _aether_setup_recv_wr_and_sgl(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb);
void _aether_setup_crd_wr_and_sgl(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb);

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
	_aether_assert_binary(enable_stats);
	_aether_assert_binary(enable_prints);

	ud_c->qp = cb->dgram_qp[qp_id];
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

   	ud_c->send_cq = cb->dgram_send_cq[ud_c->qp_id];

	ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];
	ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);


	ud_c->recv_pkt_buff_len = ud_c->max_recv_wrs * ud_c->max_coalescing;
	ud_c->send_pkt_buff_len = ud_c->max_send_wrs ;

	ud_c->recv_pkt_buff = incoming_reqs_ptr;
    ud_c->send_pkt_buff = NULL; //malloc(_aether_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);

	ud_c->send_mem_region = NULL;

	_aether_setup_crd_wr_and_sgl(ud_c, cb);

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
_aether_ud_channel_init_recv(struct hrd_ctrl_blk *cb, ud_channel_t *ud_c,
							 volatile uint8_t *incoming_reqs_ptr,
							 qp_info_t *remote_qps)
{

	assert(remote_qps != NULL);
	ud_c->remote_qps = remote_qps;

	ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];

	ud_c->send_mem_region = ud_c->is_inlining_enabled ?  NULL :
							register_buffer(cb->pd, ud_c->send_pkt_buff, _aether_ud_send_max_pkt_size(ud_c)
																		 * ud_c->send_pkt_buff_len);

	_aether_setup_send_wr_and_sgl(ud_c);
	_aether_setup_recv_wr_and_sgl(ud_c, cb);
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
	assert(type != CRD);
	assert(remote_qps != NULL);
	assert(linked_channel != NULL);  // if CRD type then used the *_crd_init instead
	_aether_assert_binary(enable_stats);
	_aether_assert_binary(enable_prints);
	_aether_assert_binary(enable_inlining);
	_aether_assert_binary(expl_credit_ctrl);
	_aether_assert_binary(is_bcast_channel);

	ud_c->qp = cb->dgram_qp[qp_id];
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
    if(_aether_ud_send_max_pkt_size(ud_c) > AETHER_MAX_SUPPORTED_INLINING) {
        if(ud_c->is_inlining_enabled)
            printf("Unfortunately, inlining for msgs sizes up to (%d) "
                   "is higher than the supported (%d)\n",
				   _aether_ud_send_max_pkt_size(ud_c), AETHER_MAX_SUPPORTED_INLINING);
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

	ud_c->send_cq = cb->dgram_send_cq[ud_c->qp_id];
	ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];
//	ud_c->recv_cq = NULL; //set by init_recv
	ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);


	ud_c->recv_pkt_buff_len = ud_c->max_recv_wrs;
	ud_c->send_pkt_buff_len = (uint16_t) (ud_c->max_send_wrs * (ud_c->is_inlining_enabled ? 1 : 2));

	ud_c->recv_pkt_buff = incoming_reqs_ptr;
    ud_c->send_pkt_buff = malloc(_aether_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);

  ud_c->send_mem_region = ud_c->is_inlining_enabled ?  NULL :
							register_buffer(cb->pd, ud_c->send_pkt_buff, _aether_ud_send_max_pkt_size(ud_c)
																		 * ud_c->send_pkt_buff_len);
//	ud_c->send_mem_region = NULL; //set by init_recv

	_aether_setup_send_wr_and_sgl(ud_c);
	_aether_setup_recv_wr_and_sgl(ud_c, cb);

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
		   _aether_ud_send_max_pkt_size(ud_c), _aether_ud_recv_max_pkt_size(ud_c));
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
void
_aether_setup_crd_wr_and_sgl(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb)
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
_aether_setup_send_wr_and_sgl(ud_channel_t *ud_c)
{
	assert(ud_c->type != CRD);

    if(ud_c->is_bcast_channel){ //Send bcast WRs

		uint16_t remote_channels = (uint16_t) (ud_c->num_channels - 1);
        uint16_t max_msgs_in_pcie_batch = (uint16_t) (ud_c->max_pcie_bcast_batch * remote_channels);
        ud_c->send_wr  = malloc(sizeof(struct ibv_send_wr) * max_msgs_in_pcie_batch);
        ud_c->send_sgl = malloc(sizeof(struct ibv_sge) * ud_c->max_pcie_bcast_batch);

        for(int i = 0; i < ud_c->max_pcie_bcast_batch; ++i)
            ud_c->send_sgl[i].length = _aether_ud_send_max_pkt_size(ud_c);

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

            ud_c->send_sgl[i].length = sizeof(aether_pkt_t) + _aether_ud_recv_max_pkt_size(ud_c);

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
_aether_setup_recv_wr_and_sgl(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb)
{
	assert(ud_c->type != CRD);

    ud_c->recv_sgl= malloc(sizeof(struct ibv_sge) * ud_c->max_recv_wrs);
    ud_c->recv_wr = malloc(sizeof(struct ibv_recv_wr) * ud_c->max_recv_wrs);

	for (int i = 0; i < ud_c->max_recv_wrs; i++) {
		ud_c->recv_sgl[i].length = _aether_ud_recv_max_pkt_size(ud_c);
		ud_c->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
        ud_c->recv_wr[i].sg_list = &ud_c->recv_sgl[i];
        ud_c->recv_wr[i].num_sge = 1;
        ud_c->recv_wr[i].next = (i == ud_c->max_recv_wrs - 1) ? NULL : &ud_c->recv_wr[i + 1];
	}
}


void
_aether_setup_incoming_buff_and_post_initial_recvs(ud_channel_t *ud_c)
{

    if(ud_c->type != CRD){
		//init recv buffs as empty (not need for CRD since CRD msgs are --immediate-- header-only)
		for(uint16_t i = 0; i < ud_c->send_pkt_buff_len; ++i)
			_aether_get_nth_pkt_ptr_from_send_buff(ud_c, i)->req_num = 0;
		for(uint16_t i = 0; i < ud_c->recv_pkt_buff_len; ++i)
			_aether_get_nth_pkt_ptr_from_recv_buff(ud_c, i)->pkt.req_num = 0;
    }

	if(AETHER_ENABLE_POST_RECV_PRINTS && ud_c->enable_prints)
		yellow_printf("vvv Post Initial Receives: %s %d\n", ud_c->qp_name, ud_c->max_recv_wrs);

	if(ud_c->type != CRD)
		_aether_post_recvs(ud_c, ud_c->max_recv_wrs);
	else
		_aether_post_crd_recvs(ud_c, ud_c->max_recv_wrs);
}
