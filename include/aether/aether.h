//
// Created by akatsarakis on 06/02/19.
//

#ifndef AETHER_INTERNAL_INLINES_H
#define AETHER_INTERNAL_INLINES_H

#include "aether_api.h"
/// WARNING!!
/// 	Functions starting with underscore (i.e. "_aether_*")
/// 	are internal and should not be called directly

/* --------------------------------------------------------------------------
--------------------------------- Helper Functions --------------------------
---------------------------------------------------------------------------*/
static inline void
_aether_assert_binary(uint8_t var)
{
    assert(var == 0 || var == 1);
}

static inline void
_aether_assertions(ud_channel_t *ud_channel)
{
	_aether_assert_binary(ud_channel->expl_crd_ctrl);
	_aether_assert_binary(ud_channel->is_bcast_channel);
	_aether_assert_binary(ud_channel->is_inlining_enabled);

	assert(ud_channel->num_channels > 1);
    assert(ud_channel->max_msg_size > 0);
    assert(ud_channel->max_coalescing > 0);
	assert(ud_channel->channel_providing_crds != NULL || ud_channel->disable_crd_ctrl);
    assert(ud_channel->send_q_depth > 0 || ud_channel->recv_q_depth > 0);
}

static inline uint16_t
_aether_ud_recv_max_pkt_size(ud_channel_t *ud_c)
{
	if(AETHER_ENABLE_ASSERTIONS)
		assert(ud_c->type != CRD);
    //TODO add assertion that this must be smaller than max_MTU
    assert(ud_c->max_msg_size > 0 && ud_c->max_coalescing > 0);
    return sizeof(aether_ud_recv_pkt_t) + ud_c->max_msg_size * ud_c->max_coalescing;
}

static inline uint16_t
_aether_ud_send_max_pkt_size(ud_channel_t *ud_c)
{
	if(AETHER_ENABLE_ASSERTIONS)
		assert(ud_c->type != CRD);
    //TODO add assertion that this must be smaller than max_MTU
    assert(ud_c->max_msg_size > 0 && ud_c->max_coalescing > 0);
    return sizeof(aether_ud_send_pkt_t) + ud_c->max_msg_size * ud_c->max_coalescing;
}

static inline uint8_t*
_aether_get_n_msg_ptr_from_send_pkt(ud_channel_t *ud_c, aether_ud_send_pkt_t *pkt, uint8_t n)
{
	if(AETHER_ENABLE_ASSERTIONS)
		assert(ud_c->type != CRD);
    assert(ud_c->max_coalescing > n && pkt->req_num >= n);
    return &pkt->reqs[n * ud_c->max_msg_size];
}

static inline uint8_t*
_aether_get_n_msg_ptr_from_recv_pkt(ud_channel_t *ud_c, aether_ud_recv_pkt_t *recv_pkt, uint8_t n)
{
    return _aether_get_n_msg_ptr_from_send_pkt(ud_c, &recv_pkt->pkt, n);
}

static inline aether_ud_send_pkt_t*
_aether_get_nth_pkt_ptr_from_send_buff(ud_channel_t *ud_c, uint16_t n)
{
    return (aether_ud_send_pkt_t *) &((uint8_t*)ud_c->send_pkt_buff)[n * _aether_ud_send_max_pkt_size(ud_c)];
}

static inline aether_ud_recv_pkt_t*
_aether_get_nth_pkt_ptr_from_recv_buff(ud_channel_t *ud_c, uint16_t n)
{
    return (aether_ud_recv_pkt_t *) &ud_c->recv_pkt_buff[n * _aether_ud_recv_max_pkt_size(ud_c)];
}

static inline aether_ud_send_pkt_t*
_aether_curr_send_pkt_ptr(ud_channel_t *ud_c)
{
    return _aether_get_nth_pkt_ptr_from_send_buff(ud_c, (uint16_t) ud_c->send_push_ptr);
}

static inline void
_aether_inc_send_push_ptr(ud_channel_t *ud_c)
{
    if(ud_c->is_bcast_channel)
        AETHER_MOD_ADD(ud_c->send_push_ptr, ud_c->send_pkt_buff_len); //TODO change this to deal with failures see comment below
//      AETHER_MOD_ADD(*inv_push_ptr, INV_SEND_OPS_SIZE / REMOTE_MACHINES *
//                               last_g_membership.num_of_alive_remotes); //got to the next "packet" + dealing with failutes
    else
        AETHER_MOD_ADD(ud_c->send_push_ptr, ud_c->send_pkt_buff_len);
	_aether_curr_send_pkt_ptr(ud_c)->req_num = 0; //Reset data left from previous unicasts / bcasts
}

static inline void
_aether_inc_recv_push_ptr(ud_channel_t *ud_c)
{
    AETHER_MOD_ADD(ud_c->recv_push_ptr, ud_c->recv_q_depth);
}

static inline void
_aether_inc_recv_pull_ptr(ud_channel_t *ud_c)
{
    AETHER_MOD_ADD(ud_c->recv_pull_ptr, ud_c->recv_pkt_buff_len);
}



/* ---------------------------------------------------------------------------
----------------------------------- RECVs ------------------------------------
---------------------------------------------------------------------------*/
static inline void
_aether_post_crd_recvs(ud_channel_t *ud_c, uint16_t num_recvs)
{
    if(AETHER_ENABLE_ASSERTIONS)
		assert(ud_c->type == CRD);

	struct ibv_recv_wr *bad_recv_wr;
	for (uint16_t i = 0; i < num_recvs; ++i)
		ud_c->recv_wr[i].next = (i == num_recvs - 1) ? NULL : &ud_c->recv_wr[i + 1];

	int ret = ibv_post_recv(ud_c->qp, ud_c->recv_wr, &bad_recv_wr);
	CPE(ret, "ibv_post_recv error: posting recvs for credits", ret);
}

static inline void
_aether_post_recvs(ud_channel_t *ud_c, uint16_t num_of_receives)
{
	if(AETHER_ENABLE_ASSERTIONS)
		assert(ud_c->type != CRD);

	void* next_buff_addr;

	if(AETHER_ENABLE_ASSERTIONS)
		assert(num_of_receives <= ud_c->max_recv_wrs);

    int req_size = _aether_ud_recv_max_pkt_size(ud_c);
	for(int i = 0; i < num_of_receives; ++i){
        next_buff_addr = (void*) (ud_c->recv_pkt_buff) + (ud_c->recv_push_ptr * req_size);
        // TODO optimize by reseting only the req_num of aether_recv_pkt
		memset(next_buff_addr, 0, (size_t) req_size); //reset the buffer before posting the receive

		if(AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC)
			ud_c->recv_wr[i].sg_list->addr = (uintptr_t) next_buff_addr;
		else
		    assert(0);
//			hrd_post_dgram_recv(ud_c->qp, next_buff_addr, req_size, cb->dgram_buf_mr->lkey);

		_aether_inc_recv_push_ptr(ud_c);
	}

	if(AETHER_ENABLE_BATCH_POST_RECVS_TO_NIC) {
		ud_c->recv_wr[num_of_receives - 1].next = NULL;
		if (AETHER_ENABLE_ASSERTIONS) {
			for (int i = 0; i < num_of_receives; i++) {
				assert(ud_c->recv_wr[i].num_sge == 1);
				assert(ud_c->recv_wr[i].sg_list->length == req_size);
				//TODO add
//				assert(ud_c->recv_wr[i].sg_list->lkey == cb->dgram_buf_mr->lkey);
				assert(i == num_of_receives - 1 || ud_c->recv_wr[i].next == &ud_c->recv_wr[i + 1]);
			}
			assert(ud_c->recv_wr[num_of_receives - 1].next == NULL);
		}

		struct ibv_recv_wr *bad_recv_wr;
		int ret = ibv_post_recv(ud_c->qp, ud_c->recv_wr, &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: while posting recvs", ret);

		//recover next ptr of last wr to NULL
		ud_c->recv_wr[num_of_receives - 1].next = (ud_c->max_recv_wrs == num_of_receives - 1) ?
                                                        NULL : &ud_c->recv_wr[num_of_receives];
	}
}

static inline void
_aether_poll_crds_and_post_recvs(ud_channel_t *ud_c)
{
	if(AETHER_ENABLE_ASSERTIONS)
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
            ud_c->channel_providing_crds->credits_per_channels[crd_ptr->sender_id] += crd_ptr->crd_num;

			if(AETHER_ENABLE_ASSERTIONS)
                assert(ud_c->channel_providing_crds->num_crds_per_channel >=
                       ud_c->channel_providing_crds->credits_per_channels[crd_ptr->sender_id]);

			if(AETHER_ENABLE_CREDIT_PRINTS && ud_c->enable_prints)
				printf("$$$ Credits: %s \033[1m\033[32mincremented\033[0m to %d (for endpoint %d)\n",
					   ud_c->channel_providing_crds->qp_name,
					   ud_c->channel_providing_crds->credits_per_channels[crd_ptr->sender_id],
					   crd_ptr->sender_id);
		}

		if (AETHER_ENABLE_POST_RECV_PRINTS && ud_c->enable_prints)
			yellow_printf("vvv Post Receives: %s %d\n", ud_c->qp_name, crd_pkts_found);

		_aether_post_crd_recvs(ud_c, (uint16_t) crd_pkts_found);

	} else if(unlikely(crd_pkts_found < 0)) {
		printf("ERROR In the credit CQ\n");
		exit(0);
	}
}

static inline uint16_t
aether_poll_buff_and_post_recvs(ud_channel_t* ud_c, uint16_t max_pkts_to_poll,
								uint8_t* recv_ops)
{
	if(AETHER_ENABLE_ASSERTIONS)
		assert(ud_c->type != CRD);

	int index = 0;
    uint8_t sender = 0;
	uint16_t msgs_polled = 0;
    uint8_t* next_packet_reqs, *recv_op_ptr, *next_req, *next_packet_req_num_ptr;

    uint16_t max_req_size = _aether_ud_recv_max_pkt_size(ud_c);

	//poll completion q
	uint16_t pkts_polled = (uint16_t) ibv_poll_cq(ud_c->recv_cq, max_pkts_to_poll, ud_c->recv_wc);

	for(int i = 0; i < pkts_polled; ++i){

		index = (ud_c->recv_pull_ptr + 1) % ud_c->recv_q_depth;
		aether_ud_recv_pkt_t* next_packet = (aether_ud_recv_pkt_t *) &ud_c->recv_pkt_buff[index * max_req_size];

		sender = next_packet->pkt.sender_id;
		next_packet_reqs = next_packet->pkt.reqs;
		next_packet_req_num_ptr = &next_packet->pkt.req_num;

		if(AETHER_ENABLE_ASSERTIONS)
			assert(next_packet->pkt.req_num > 0 && next_packet->pkt.req_num <= ud_c->max_coalescing);


		//TODO add membership and functionality
//        if(node_is_in_membership(last_group_membership, sender))
		for(int j = 0; j < next_packet->pkt.req_num; ++j){
			next_req = &next_packet_reqs[j * ud_c->max_msg_size];
			recv_op_ptr = &recv_ops[msgs_polled * ud_c->max_msg_size];

			memcpy(recv_op_ptr, next_req, ud_c->max_msg_size);

			msgs_polled++;
			if(!ud_c->disable_crd_ctrl)
				ud_c->channel_providing_crds->credits_per_channels[sender]++; //increment packet credits
		}



		*next_packet_req_num_ptr = 0; //TODO can be removed since we already reset on posting receives
		_aether_inc_recv_pull_ptr(ud_c);


		if(AETHER_ENABLE_ASSERTIONS)
			if(!ud_c->disable_crd_ctrl)
				assert(ud_c->channel_providing_crds->credits_per_channels[sender] <=
					   ud_c->channel_providing_crds->num_crds_per_channel);
	}


	if(pkts_polled > 0){
		//Refill recvs
		_aether_post_recvs(ud_c, pkts_polled);


		if(AETHER_ENABLE_STAT_COUNTING){
			ud_c->stats.recv_total_msgs += msgs_polled;
			ud_c->stats.recv_total_pkts += pkts_polled;
		}

		if(AETHER_ENABLE_RECV_PRINTS && ud_c->enable_prints)
			green_printf("^^^ Polled msgs: %d packets %s %d, (total pkts: %d, msgs %d)!\n",
						 pkts_polled, ud_c->qp_name, msgs_polled,
						 ud_c->stats.recv_total_pkts, ud_c->stats.recv_total_msgs);
		if(AETHER_ENABLE_CREDIT_PRINTS && ud_c->enable_prints && !ud_c->disable_crd_ctrl)
			printf("$$$ Credits: %s \033[1m\033[32mincremented\033[0m to %d (for machine %d)\n",
				   ud_c->channel_providing_crds->qp_name,
				   ud_c->channel_providing_crds->credits_per_channels[sender], sender);
		if (AETHER_ENABLE_POST_RECV_PRINTS && ud_c->enable_prints)
			yellow_printf("vvv Post Receives: %s %d\n", ud_c->qp_name, pkts_polled);

		if(AETHER_ENABLE_ASSERTIONS)
			assert(ud_c->max_coalescing != 1 || pkts_polled == msgs_polled);
	}

    return msgs_polled;
}

/* ---------------------------------------------------------------------------
----------------------------------- CREDITS ----------------------------------
---------------------------------------------------------------------------*/
// For all the CREDIT functions --> if its a bcast channel endpoint_id is ignored
static inline uint8_t
_aether_has_sufficient_crds_no_polling(ud_channel_t *ud_c, uint8_t endpoint_id)
{
    if(ud_c->disable_crd_ctrl)
		return 1;

    else if (!ud_c->is_bcast_channel)
        return (uint8_t) (ud_c->credits_per_channels[endpoint_id] > 0);

    else
        for (int i = 0; i < ud_c->num_channels; ++i) {
            if (i == ud_c->channel_id) continue;
            //TODO if i == local_node_id  || !node_in_membership(i) --> continue
//            if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
            if (ud_c->credits_per_channels[i] <= 0)
                return 0;
        }

    return 1;
}

static inline uint8_t
_aether_has_sufficient_crds(ud_channel_t *ud_c, uint8_t endpoint_id)
{
    if(_aether_has_sufficient_crds_no_polling(ud_c, endpoint_id))
        return 1;

    if(ud_c->expl_crd_ctrl) {
		_aether_poll_crds_and_post_recvs(ud_c->channel_providing_crds);

        if(_aether_has_sufficient_crds_no_polling(ud_c, endpoint_id))
            return 1;
    }
    return 0;
}

static inline void
_aether_dec_crds(ud_channel_t *ud_c, uint8_t endpoint_id)
{
	if(ud_c->disable_crd_ctrl) return;

    if(AETHER_ENABLE_ASSERTIONS)
        assert(_aether_has_sufficient_crds_no_polling(ud_c, endpoint_id));

    if(!ud_c->is_bcast_channel)
        ud_c->credits_per_channels[endpoint_id]--;
    else
        for(int i = 0; i < ud_c->num_channels; ++i){
            if(i == ud_c->channel_id) continue;
			//TODO if i == local_node_id  || !node_in_membership(i) --> continue
//            if (!node_is_in_membership(last_g_membership, j)) continue; //skip machine which is removed from group
            ud_c->credits_per_channels[i]--;
        }

    if (AETHER_ENABLE_CREDIT_PRINTS && ud_c->enable_prints){
        if(ud_c->is_bcast_channel)
            endpoint_id = (uint8_t) (ud_c->channel_id == 0 ? 1 : 0);

        printf("$$$ Credits: %s \033[31mdecremented\033[0m to %d",
               ud_c->qp_name, ud_c->credits_per_channels[endpoint_id]);

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
_aether_forge_crd_wr(ud_channel_t *ud_c, uint16_t dst_qp_id,
					 uint16_t crd_pkts_to_send, uint16_t crd_to_send)
{

	if(AETHER_ENABLE_ASSERTIONS)
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
			hrd_poll_cq(ud_c->send_cq, 1, &signal_send_wc);

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
_aether_forge_wr(ud_channel_t *ud_c, uint8_t dst_qp_id, uint8_t *req_to_copy,
				 uint16_t pkts_in_batch, uint16_t *msgs_in_batch,
				 copy_and_modify_input_elem_t copy_and_modify_elem)
// dst_qp_id is ignored if its a bcast channel
{
	struct ibv_wc signal_send_wc;

	aether_ud_send_pkt_t* curr_pkt_ptr = _aether_curr_send_pkt_ptr(ud_c);
	uint8_t* next_req_ptr = _aether_get_n_msg_ptr_from_send_pkt(ud_c, curr_pkt_ptr, curr_pkt_ptr->req_num);
	curr_pkt_ptr->req_num++;
	curr_pkt_ptr->sender_id = ud_c->channel_id;

	//<Copy & modify elem!> --> callback func that copies and manipulated data from req_to_copy buff
	copy_and_modify_elem(next_req_ptr, req_to_copy);

	if(AETHER_ENABLE_ASSERTIONS){
		assert(dst_qp_id != machine_id);
		assert(curr_pkt_ptr->req_num <= ud_c->max_coalescing);
	}

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
				hrd_poll_cq(ud_c->send_cq, 1, &signal_send_wc);

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
_aether_batch_pkts_2_NIC(ud_channel_t *ud_c, uint16_t pkts_in_batch, uint16_t msgs_in_batch)
{
	int ret;
	struct ibv_send_wr *bad_send_wr;

	if(ud_c->enable_stats)
		ud_c->stats.send_total_pkts += pkts_in_batch;

	uint16_t remote_channels = (uint16_t) (ud_c->num_channels - 1);
	uint16_t wr_idx = (uint16_t) (pkts_in_batch * (ud_c->is_bcast_channel ?  remote_channels : 1));
	ud_c->send_wr[wr_idx - 1].next = NULL;

	if(AETHER_ENABLE_ASSERTIONS){
		assert(pkts_in_batch <= ud_c->max_send_wrs);
		assert(pkts_in_batch <= ud_c->send_pkt_buff_len);
		assert(ud_c->type == CRD || ud_c->max_coalescing > 1 || msgs_in_batch ==  pkts_in_batch);
		assert(ud_c->type == CRD || ud_c->max_coalescing > 1 || ud_c->stats.send_total_msgs ==  ud_c->stats.send_total_pkts);

		assert(ud_c->send_wr[wr_idx - 1].next == NULL);
		for(int i = 0; i < wr_idx; ++i){
			uint16_t sgl_idx = (uint16_t) (i / (ud_c->is_bcast_channel ? remote_channels : 1));

			if(ud_c->type != CRD){
				assert(ud_c->send_wr[i].num_sge == 1);
				assert(ud_c->send_wr[i].opcode == IBV_WR_SEND);
				assert(ud_c->send_wr[i].sg_list == &ud_c->send_sgl[sgl_idx]);

				aether_ud_send_pkt_t* curr_send_pkt = (aether_ud_send_pkt_t*) ud_c->send_sgl[sgl_idx].addr;
				assert(curr_send_pkt->req_num > 0);
			} else {
				assert(ud_c->send_wr[i].num_sge == 0);
				assert(ud_c->send_wr[i].sg_list->length == 0);
				assert(ud_c->send_wr[i].opcode == IBV_WR_SEND_WITH_IMM);
				assert(((aether_crd_t*) &(ud_c->send_wr[i].imm_data))->crd_num > 0);
				assert(((aether_crd_t*) &(ud_c->send_wr[i].imm_data))->sender_id == ud_c->channel_id);
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

	ret = ibv_post_send(ud_c->qp, ud_c->send_wr, &bad_send_wr);
	CPE(ret, "ibv_post_send error while sending msgs to the NIC", ret);
}

static inline void
_aether_check_if_batch_n_inc_pkt_ptr(ud_channel_t *ud_c,
									 uint16_t *pkts_in_batch_ptr, uint16_t *msgs_in_batch_ptr)
{

    (*pkts_in_batch_ptr)++;
	uint16_t send_pkts = *pkts_in_batch_ptr;
	uint16_t total_msgs_in_batch = *msgs_in_batch_ptr;
	uint16_t max_pkt_batch = ud_c->is_bcast_channel ? ud_c->max_pcie_bcast_batch :
							 								ud_c->max_send_wrs;

	if (send_pkts == max_pkt_batch) {
		_aether_batch_pkts_2_NIC(ud_c, send_pkts, total_msgs_in_batch);
		*pkts_in_batch_ptr = 0;
		*msgs_in_batch_ptr = 0;
	}

	_aether_inc_send_push_ptr(ud_c); //go to the next pkt
}

static inline uint8_t
aether_issue_pkts(ud_channel_t *ud_c,
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
		assert(_aether_curr_send_pkt_ptr(ud_c)->req_num == 0);

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
		if (!_aether_has_sufficient_crds(ud_c, curr_msg_dst)) {
			has_outstanding_msgs = 1;

			if(input_array_rolling_idx != NULL)
				*input_array_rolling_idx = idx;
			break; // we need to break for broadcast (lets assume it is ok to break for unicasts as well since it may only harm perf)
		}

		_aether_dec_crds(ud_c, curr_msg_dst);

		if(!ud_c->is_bcast_channel)
			// Send unicasts because if we might cannot coalesce pkts, due to different endpoints
			if(_aether_curr_send_pkt_ptr(ud_c)->req_num > 0 && curr_msg_dst != last_msg_dst)
				_aether_check_if_batch_n_inc_pkt_ptr(ud_c, &pkts_in_batch, &msgs_in_batch);

		last_msg_dst = curr_msg_dst;

		// Create the messages
		_aether_forge_wr(ud_c, curr_msg_dst, curr_elem, pkts_in_batch,
						 &msgs_in_batch, copy_and_modify_elem);

		modify_elem_after_send(curr_elem); // E.g. Change the state of the element which triggered a send

		// Check if we should send a batch since we might have reached the max batch size
		if(_aether_curr_send_pkt_ptr(ud_c)->req_num == ud_c->max_coalescing)
			_aether_check_if_batch_n_inc_pkt_ptr(ud_c, &pkts_in_batch, &msgs_in_batch);
	}

	// Even if the last pkt is not full do the appropriate actions and incl to NIC batch
	aether_ud_send_pkt_t* curr_pkt_ptr = _aether_curr_send_pkt_ptr(ud_c);
	if(curr_pkt_ptr->req_num > 0 && curr_pkt_ptr->req_num < ud_c->max_coalescing)
		pkts_in_batch++;

	// Force a batch to send the last set of requests (even < max batch size)
	if (pkts_in_batch > 0)
		_aether_batch_pkts_2_NIC(ud_c, pkts_in_batch, msgs_in_batch);

	// Move to next packet and reset data left from previous bcasts/unicasts
	if(curr_pkt_ptr->req_num > 0 && curr_pkt_ptr->req_num < ud_c->max_coalescing)
		_aether_inc_send_push_ptr(ud_c);


	return has_outstanding_msgs;
}

static inline void
aether_issue_credits(ud_channel_t *ud_c, uint8_t *input_array_of_elems,
					 uint16_t input_array_len, uint16_t size_of_input_elems,
					 skip_input_elem_or_get_sender_id_t skip_or_get_sender_id_func_ptr,
					 modify_input_elem_after_send_t modify_elem_after_send)
{
	if(AETHER_ENABLE_ASSERTIONS)
		assert(ud_c->type == CRD);

	for (int i = 0; i < ud_c->num_channels; ++i)
		ud_c->no_crds_to_send_per_endpoint[i] = 0;

	for (int i = 0; i < input_array_len; ++i) {
		// Skip or Respond (copy and send ?)
		uint8_t* curr_elem = &input_array_of_elems[i * size_of_input_elems];
		int skip_or_sender_id = skip_or_get_sender_id_func_ptr(curr_elem);
		if(AETHER_ENABLE_ASSERTIONS) assert(skip_or_sender_id < 255);

		if(skip_or_sender_id < 0) continue;
		uint8_t curr_msg_dst = (uint8_t) skip_or_sender_id;

		// Check if we have sufficient credits --> (we should always have enough credits for CRDs)
		if (!_aether_has_sufficient_crds(ud_c, curr_msg_dst))
			assert(0);
		if(ud_c->no_crds_to_send_per_endpoint[curr_msg_dst] == 0 && ud_c->credits_per_channels[curr_msg_dst] == 0)
			assert(0);

		_aether_dec_crds(ud_c, curr_msg_dst);

		ud_c->no_crds_to_send_per_endpoint[curr_msg_dst]++;

		modify_elem_after_send(curr_elem); // E.g. Change the state of the element which triggered a send
	}

	uint16_t send_crd_packets = 0, total_credits_to_send = 0;
	for(uint16_t i = 0; i < ud_c->num_channels; ++i){
		if(i == ud_c->channel_id) continue;

		if(ud_c->no_crds_to_send_per_endpoint[i] > 0) {
			_aether_forge_crd_wr(ud_c, i, send_crd_packets, ud_c->no_crds_to_send_per_endpoint[i]);
			send_crd_packets++;
			total_credits_to_send += ud_c->no_crds_to_send_per_endpoint[i];

			if (send_crd_packets == ud_c->max_send_wrs) {
				_aether_batch_pkts_2_NIC(ud_c, send_crd_packets, total_credits_to_send);
				send_crd_packets = 0;
				total_credits_to_send = 0;
			}
		}
	}

	if (send_crd_packets > 0)
		_aether_batch_pkts_2_NIC(ud_c, send_crd_packets, total_credits_to_send);
}

#endif //HERMES_AETHER_INTERNAL_INLINES_H
