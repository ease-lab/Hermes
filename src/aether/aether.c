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
void _aether_setup_incoming_buff_and_post_initial_recvs(ud_channel_t *ud_c);
void _aether_ud_channel_init_recv(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb, uint8_t qp_id,
							 volatile uint8_t *incoming_reqs_ptr, qp_info_t *remote_qps);



void
aether_ud_channel_init(ud_channel_t *ud_c, char *qp_name, enum channel_type type,
					   uint8_t max_coalescing, uint16_t max_req_size, uint8_t enable_inlining,
		// broadcast
					   uint8_t is_bcast,
		// credits
					   uint8_t expl_crd_ctrl, ud_channel_t *linked_channel,
					   uint8_t crds_per_channel, uint16_t num_channels,
		//Toggles
					   uint8_t stats_on, uint8_t prints_on)
{
	assert(type != CRD); // if CRD type then used the *_crd_init instead
	assert(linked_channel != NULL);
	_aether_assert_binary(stats_on);
	_aether_assert_binary(prints_on);
	_aether_assert_binary(enable_inlining);
	_aether_assert_binary(expl_crd_ctrl);
	_aether_assert_binary(is_bcast);


	ud_c->type = type;
	ud_c->qp_name = qp_name;
	ud_c->num_channels = num_channels; //num_channels include our own channel
	ud_c->expl_crd_ctrl = expl_crd_ctrl;
	ud_c->is_bcast_channel = is_bcast;
	ud_c->channel_providing_crds = linked_channel;
	ud_c->num_crds_per_channel = crds_per_channel;

	ud_c->enable_stats = stats_on;
    ud_c->enable_prints = prints_on;


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
		ud_c->credits_per_rem_channels[i] = (uint8_t) (type == REQ ? crds_per_channel : 0);


	ud_c->max_pcie_bcast_batch = (uint16_t) AETHER_MIN(AETHER_MIN_PCIE_BCAST_BATCH + 1, crds_per_channel);
    //Warning! use min to avoid resetting the first req prior batching to the NIC
	//WARNING: todo check why we need to have MIN_PCIE_BCAST_BATCH + 1 instead of just MIN_PCIE_BCAST_BATCH
	uint16_t max_msgs_in_pcie_bcast = (uint16_t) (ud_c->max_pcie_bcast_batch * remote_channels); //must be smaller than the q_depth


	ud_c->max_recv_wrs = (uint16_t) (crds_per_channel * remote_channels);
    ud_c->max_send_wrs = (uint16_t) (ud_c->is_bcast_channel ? max_msgs_in_pcie_bcast : crds_per_channel * remote_channels);

   	ud_c->ss_granularity = ud_c->is_bcast_channel ? ud_c->max_pcie_bcast_batch : ud_c->max_send_wrs;

	ud_c->recv_q_depth = ud_c->max_recv_wrs;
   	ud_c->send_q_depth = (uint16_t) (2 * ud_c->ss_granularity *
   								    (ud_c->is_bcast_channel ? remote_channels : 1));


	ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);


	ud_c->recv_pkt_buff_len = ud_c->max_recv_wrs;
	ud_c->send_pkt_buff_len = (uint16_t) (ud_c->max_send_wrs * (ud_c->is_inlining_enabled ? 1 : 2));

    ud_c->send_pkt_buff = malloc(_aether_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);


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

	//The following are set by the *_init_recv function after the creation of control block and QPs
	ud_c->qp = NULL;
	ud_c->qp_id = 0;
	ud_c->send_cq = NULL; //set by init_recv
	ud_c->recv_cq = NULL; //set by init_recv
	ud_c->remote_qps = NULL;
	ud_c->recv_pkt_buff = NULL;
	ud_c->send_mem_region = NULL; //set by init_recv
//	_aether_setup_send_wr_and_sgl(ud_c);
//	_aether_setup_recv_wr_and_sgl(ud_c, cb);

    assert(ud_c->max_pcie_bcast_batch <= crds_per_channel);
}

void
aether_ud_channel_crd_init(ud_channel_t *ud_c, char *qp_name, ud_channel_t *linked_channel,
						   uint8_t crds_per_channel, uint16_t num_channels,
						   uint8_t enable_stats, uint8_t enable_prints)
{
	_aether_assert_binary(enable_stats);
	_aether_assert_binary(enable_prints);


    ud_c->type = CRD;
	ud_c->qp_name = qp_name;
	ud_c->num_channels = num_channels; //num_channels include our own channel
	ud_c->expl_crd_ctrl = 1;
    ud_c->is_bcast_channel = 0;
	ud_c->max_pcie_bcast_batch = 0;
	ud_c->num_crds_per_channel = crds_per_channel;
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


	ud_c->max_recv_wrs = crds_per_channel * remote_channels;
    ud_c->max_send_wrs = crds_per_channel * remote_channels; //TODO correct this

   	ud_c->ss_granularity = ud_c->max_send_wrs;

	ud_c->recv_q_depth = ud_c->max_recv_wrs;
   	ud_c->send_q_depth = (uint16_t) (2 * ud_c->ss_granularity);

	ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);


	ud_c->recv_pkt_buff_len = ud_c->max_recv_wrs * ud_c->max_coalescing;
	ud_c->send_pkt_buff_len = ud_c->max_send_wrs ;

    ud_c->send_pkt_buff = NULL; //malloc(_aether_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);

	ud_c->send_mem_region = NULL;


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

	//The following are set by the *_init_recv function after the creation of control block and QPs
	ud_c->qp = NULL;
	ud_c->qp_id = 0;
	ud_c->send_cq = NULL;
	ud_c->recv_cq = NULL;
	ud_c->remote_qps = NULL;
	ud_c->recv_pkt_buff = NULL;
//	_aether_setup_crd_wr_and_sgl(ud_c, cb);
}


void
aether_allocate_and_init_all_qps(ud_channel_t** ud_c_array, uint16_t ud_c_num,
								 uint16_t worker_gid, uint16_t worker_lid)
{

	uint32_t dgram_buff_size = 0;
	int *send_q_depths = malloc(ud_c_num * sizeof(int));
	int *recv_q_depths = malloc(ud_c_num * sizeof(int));

	// Setup Q depths and buff size for incoming pkts
	for(int i = 0; i < ud_c_num; ++i){
	    send_q_depths[i] = ud_c_array[i]->send_q_depth;
		recv_q_depths[i] = ud_c_array[i]->recv_q_depth;
		dgram_buff_size += ud_c_array[i]->type == CRD ? 64 :
						   _aether_ud_recv_max_pkt_size(ud_c_array[i]) * ud_c_array[i]->recv_q_depth;
	}

	struct hrd_ctrl_blk *cb = hrd_ctrl_blk_init(worker_gid,	/* local_hid */
												0, -1, /* port_index, numa_node_id */
												0, 0,	/* #conn qps, uc */
												NULL, 0, -1,	/* prealloc conn buf, buf size, key */
												ud_c_num, dgram_buff_size,	/* num_dgram_qps, dgram_buf_size */
												BASE_SHM_KEY + worker_lid, /* key */
												recv_q_depths, send_q_depths); /* Depth of the dgram RECV, SEND Q*/

	share_qp_info_via_memcached(worker_gid, cb);

	volatile uint8_t *incoming_reqs_ptr = cb->dgram_buf;
	for(uint8_t i = 0; i < ud_c_num; ++i){
	    // Forge remote qps
	    qp_info_t* remote_qps = malloc(sizeof(qp_info_t) * ud_c_array[i]->num_channels);
	    for(int j = 0; j < ud_c_array[i]->num_channels; ++j){
			uint16_t idx = (uint16_t) (j * WORKERS_PER_MACHINE + worker_lid);
			remote_qps[j].ah = remote_worker_qps[idx][i].ah;
			remote_qps[j].qpn = (uint32_t) remote_worker_qps[idx][i].qpn;
	    }


		// Init recv and setup wrs and sgls of ud_channel
		_aether_ud_channel_init_recv(ud_c_array[i], cb, (uint8_t) i, incoming_reqs_ptr, remote_qps);
		incoming_reqs_ptr += ud_c_array[i]->type == CRD ? 64 :
							 _aether_ud_recv_max_pkt_size(ud_c_array[i]) * ud_c_array[i]->recv_q_depth;
	}
}



static inline void
_aether_print_on_off_toggle(uint16_t bin_flag, char *str)
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

	_aether_print_on_off_toggle(ud_c->is_inlining_enabled, "Inlining");
	_aether_print_on_off_toggle(ud_c->max_coalescing, "Coalescing");
	_aether_print_on_off_toggle(ud_c->max_pcie_bcast_batch, "Max PCIe batch");

	printf("\t\tMax msg size: %d\n", ud_c->max_msg_size);
	if(ud_c->type != CRD)
		printf("\t\tMax pkt size: send = %dB, recv = %dB\n",
			   _aether_ud_send_max_pkt_size(ud_c), _aether_ud_recv_max_pkt_size(ud_c));
	else
		printf("\t\tMax pkt size: send = 4B (immediate), recv = 4B(immediate)\n");
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

	_aether_print_on_off_toggle(ud_c->enable_stats, "Stats");
	_aether_print_on_off_toggle(ud_c->enable_prints, "Prints");
}




/* ---------------------------------------------------------------------------
----------------------------------- SETUPs ------------------------------------
---------------------------------------------------------------------------*/
void
_aether_ud_channel_init_recv(ud_channel_t *ud_c, struct hrd_ctrl_blk *cb, uint8_t qp_id,
							 volatile uint8_t *incoming_reqs_ptr, qp_info_t *remote_qps)
{
	assert(remote_qps != NULL);

	ud_c->qp_id = qp_id;
	ud_c->qp = cb->dgram_qp[qp_id];

	ud_c->remote_qps = remote_qps;

	ud_c->recv_pkt_buff = incoming_reqs_ptr;

	ud_c->send_cq = cb->dgram_send_cq[ud_c->qp_id];
	ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];

	if(ud_c->type != CRD){
		ud_c->send_mem_region = ud_c->is_inlining_enabled ?  NULL :
								register_buffer(cb->pd, ud_c->send_pkt_buff, _aether_ud_send_max_pkt_size(ud_c)
																			 * ud_c->send_pkt_buff_len);
		_aether_setup_send_wr_and_sgl(ud_c);
		_aether_setup_recv_wr_and_sgl(ud_c, cb);
	} else
		_aether_setup_crd_wr_and_sgl(ud_c, cb);

	// post initial receivs
	/// WARNING try to avoid races of posting initial receives and sending msgs
	_aether_setup_incoming_buff_and_post_initial_recvs(ud_c);
}

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
