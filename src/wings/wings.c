//
// Created by akatsarakis on 22/01/19.
//

#include "../../include/wings/wings.h"
#include <config.h>
#include <infiniband/verbs.h>
#include <inline-util.h>
#include <spacetime.h>
#include <stdio.h>

// implement a Multicast / Unicast channel
// Support for:
//      mulitcast / unicast channel
//      Coalescing
//      Variable size msgs?
//      Selective Signaling
//      Batching to the NIC
//      Inlining or not
//      Batch post receives to the NIC
//          Mode 1: poll reqs, copy incoming msgs to local buffers and
//          (p)re-post recvs Mode 2: poll reqs, do not copy msgs and post rcvs
//          when said
//      Enable implicit (request - response mode) and explicit (batched) credits
//      flow control

void _wings_setup_send_wr_and_sgl(ud_channel_t* ud_c);
void _wings_setup_recv_wr_and_sgl(ud_channel_t* ud_c,
                                  struct hrd_ud_ctrl_blk* cb);
void _wings_setup_crd_wr_and_sgl(ud_channel_t* ud_c,
                                 struct hrd_ud_ctrl_blk* cb);
void _wings_setup_incoming_buff_and_post_initial_recvs(ud_channel_t* ud_c);
void _wings_ud_channel_init_recv(ud_channel_t* ud_c, struct hrd_ud_ctrl_blk* cb,
                                 uint8_t qp_id,
                                 volatile uint8_t* incoming_reqs_ptr);

void _wings_ud_channel_crd_init(ud_channel_t* ud_c, char* qp_name,
                                ud_channel_t* linked_channel,
                                uint16_t crds_per_channel,
                                uint16_t num_channels, uint8_t channel_id,
                                uint8_t enable_stats, uint8_t enable_prints);

void _wings_print_on_off_toggle(uint16_t bin_flag, char* str);

void _wings_share_qp_info_via_memcached(ud_channel_t** ud_c_array,
                                        uint16_t ud_c_num,
                                        dbit_vector_t* shared_rdy_var,
                                        int worker_lid,
                                        struct hrd_ud_ctrl_blk* cb);

void
wings_ud_channel_destroy(ud_channel_t* ud_c)
{
  free(ud_c->qp_name);
  free(ud_c->recv_wc);
  free(ud_c->remote_qps);
  free(ud_c->credits_per_channels);

  if (ud_c->send_pkt_buff != NULL) free(ud_c->send_pkt_buff);

  if (ud_c->type != CRD && ud_c->max_coalescing > 1)
    free(ud_c->overflow_msg_buff);

  if (ud_c->type == CRD) free(ud_c->no_crds_to_send_per_endpoint);
}

void
wings_ud_channel_init(ud_channel_t* ud_c, char* qp_name, enum channel_type type,
                      uint8_t max_coalescing, uint16_t max_req_size,
                      uint16_t small_req_size, uint8_t enable_inlining,
                      uint8_t is_header_only,
                      // Broadcast
                      uint8_t is_bcast,
                      // Credits
                      uint8_t disable_crd_ctrl, uint8_t expl_crd_ctrl,
                      ud_channel_t* linked_channel, uint16_t crds_per_channel,
                      uint16_t num_channels, uint8_t channel_id,
                      // Toggles
                      uint8_t stats_on, uint8_t prints_on)
{
  assert(type != CRD);         // if CRD type then used the *_crd_init instead
  assert(max_coalescing > 0);  // To disable coalescing use max_coalescing == 1
  assert(channel_id < num_channels);
  assert(!(disable_crd_ctrl == 1 &&
           expl_crd_ctrl == 1));  // cannot disable crd_ctrl and then set an
                                  // explicit credit control
  assert(
      disable_crd_ctrl == 1 ||
      linked_channel !=
          NULL);  // cannot disable crd_ctrl and then set an crd control channel
  assert(is_bcast == 0 || is_header_only == 0);
  assert(small_req_size <= max_req_size);

  _wings_assert_binary(stats_on);
  _wings_assert_binary(is_bcast);
  _wings_assert_binary(prints_on);
  _wings_assert_binary(expl_crd_ctrl);
  _wings_assert_binary(is_header_only);
  _wings_assert_binary(enable_inlining);

  ud_c->is_header_only = is_header_only;
  if (ud_c->is_header_only)
    /// WARNING: hdr_only msgs have an additional 1st B indicating sender_id
    /// (which must not be taken into account on max_req_size)
    assert(max_req_size <= 3 * sizeof(uint8_t) && max_coalescing == 1);

  ud_c->type = type;
  ud_c->channel_id = channel_id;
  ud_c->num_channels = num_channels;  // num_channels include our own channel
  ud_c->expl_crd_ctrl = expl_crd_ctrl;
  ud_c->disable_crd_ctrl = disable_crd_ctrl;
  ud_c->is_bcast_channel = is_bcast;
  ud_c->num_crds_per_channel = crds_per_channel;
  ud_c->channel_providing_crds = linked_channel;

  ud_c->qp_name =
      malloc(sizeof(char) *
             (strlen(qp_name) +
              1));  // TODO make sure to destroy this when destroing a ud_c
  strcpy(ud_c->qp_name, qp_name);

  ud_c->enable_stats = stats_on;
  ud_c->enable_prints = prints_on;

  ud_c->max_coalescing = max_coalescing;
  ud_c->max_msg_size =
      (uint16_t)(max_req_size + (ud_c->is_header_only == 1
                                     ? 1
                                     : 0));  // hdr_only msgs have an additional
                                             // 1st B indicating sender_id
  ud_c->small_msg_size =
      small_req_size == 0 ? ud_c->max_msg_size : small_req_size;

  ud_c->no_crds_to_send_per_endpoint = NULL;  // unused for type != CRD

  uint16_t remote_channels = (uint16_t)(num_channels - 1);
  ud_c->is_inlining_enabled =
      (uint8_t)(ud_c->is_header_only == 1 ? 1 : enable_inlining);
  if (ud_c->is_header_only == 0 &&
      _wings_ud_send_max_pkt_size(ud_c) > WINGS_MAX_SUPPORTED_INLINING) {
    if (ud_c->is_inlining_enabled)
      printf(
          "Unfortunately, inlining for msgs sizes up to (%d) "
          "is higher than the supported (%d)\n",
          _wings_ud_send_max_pkt_size(ud_c), WINGS_MAX_SUPPORTED_INLINING);
    ud_c->is_inlining_enabled = 0;
  }

  ud_c->credits_per_channels = malloc(sizeof(uint16_t) * (num_channels));
  for (int i = 0; i < num_channels; ++i)
    ud_c->credits_per_channels[i] =
        (uint16_t)(type == REQ && !disable_crd_ctrl ? crds_per_channel : 0);

  ud_c->max_pcie_bcast_batch =
      (uint16_t)WINGS_MIN(WINGS_MIN_PCIE_BCAST_BATCH + 1, crds_per_channel);
  // Warning! use min to avoid resetting the first req prior batching to the NIC
  // WARNING: todo check why we need to have MIN_PCIE_BCAST_BATCH + 1 instead of
  // just MIN_PCIE_BCAST_BATCH
  uint16_t max_msgs_in_pcie_bcast =
      (uint16_t)(ud_c->max_pcie_bcast_batch *
                 remote_channels);  // must be smaller than the q_depth

  ud_c->max_recv_wrs = (uint16_t)(crds_per_channel * remote_channels);
  ud_c->max_send_wrs =
      (uint16_t)(ud_c->is_bcast_channel ? max_msgs_in_pcie_bcast
                                        : crds_per_channel * remote_channels);

  ud_c->ss_granularity =
      ud_c->is_bcast_channel ? ud_c->max_pcie_bcast_batch : ud_c->max_send_wrs;

  ud_c->recv_q_depth = ud_c->max_recv_wrs;
  ud_c->send_q_depth =
      (uint16_t)(2 * ud_c->ss_granularity *
                 (ud_c->is_bcast_channel ? remote_channels : 1));

  ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);

  ud_c->recv_pkt_buff_len = ud_c->max_recv_wrs;
  ud_c->send_pkt_buff_len =
      (uint16_t)(ud_c->max_send_wrs * (ud_c->is_inlining_enabled ? 1 : 2));

  ud_c->send_pkt_buff =
      ud_c->is_header_only == 1
          ? NULL
          : malloc(_wings_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);

  ud_c->overflow_msg_buff = NULL;
  // Overflow on polling
  if (ud_c->max_coalescing > 1) {
    ud_c->num_overflow_msgs = 0;
    ud_c->enable_overflow_msgs = 1;
    ud_c->overflow_msg_buff =
        malloc((size_t)(ud_c->max_msg_size * (ud_c->max_coalescing - 1)));
  } else {
    ud_c->num_overflow_msgs = 0;
    ud_c->enable_overflow_msgs = 0;
    ud_c->overflow_msg_buff = NULL;
  }

  ud_c->send_push_ptr = 0;
  ud_c->recv_push_ptr = 0;
  ud_c->recv_pull_ptr = -1;

  ud_c->total_pkts_send = 0;

  ud_c->stats.ss_completions = 0;
  ud_c->stats.recv_total_pkts = 0;
  ud_c->stats.recv_total_msgs = 0;
  ud_c->stats.send_total_msgs = 0;
  ud_c->stats.send_total_pkts = 0;
  ud_c->stats.send_total_pcie_batches = 0;
  ud_c->stats.no_stalls_due_to_credits = 0;

  // Initialize the crd channel as well
  if (ud_c->expl_crd_ctrl) {
    char crd_qp_name[1000];
    sprintf(crd_qp_name, "\033[1m\033[36mCRD\033[0m-%s", qp_name);
    _wings_ud_channel_crd_init(linked_channel, crd_qp_name, ud_c,
                               crds_per_channel, num_channels, channel_id,
                               stats_on, prints_on);
  }

  ud_c->remote_qps = malloc(sizeof(qp_info_t) * ud_c->num_channels);

  // The following are set by the *_init_recv function after the creation of
  // control block and QPs
  ud_c->qp = NULL;
  ud_c->pd = NULL;
  ud_c->qp_id = 0;
  ud_c->send_cq = NULL;  // set by init_recv
  ud_c->recv_cq = NULL;  // set by init_recv
  ud_c->recv_pkt_buff = NULL;
  ud_c->send_mem_region = NULL;  // set by init_recv
  //	_wings_setup_send_wr_and_sgl(ud_c);
  //	_wings_setup_recv_wr_and_sgl(ud_c, cb);

  _wings_assert_binary(ud_c->is_header_only);
  assert(ud_c->max_pcie_bcast_batch <= crds_per_channel);
  assert(ud_c->is_header_only == 0 || ud_c->is_header_only);
}

void
wings_setup_channel_qps_and_recvs_w_shm_key(ud_channel_t** ud_c_array,
                                            uint16_t ud_c_num,
                                            dbit_vector_t* shared_rdy_var,
                                            uint16_t worker_lid,
                                            uint16_t base_shm_key)
{
  uint32_t dgram_buff_size = 0;
  int* send_q_depths = malloc(ud_c_num * sizeof(int));
  int* recv_q_depths = malloc(ud_c_num * sizeof(int));

  // Setup Q depths and buff size for incoming pkts
  for (int i = 0; i < ud_c_num; ++i) {
    send_q_depths[i] = ud_c_array[i]->send_q_depth;
    recv_q_depths[i] = ud_c_array[i]->recv_q_depth;
    dgram_buff_size +=
        ud_c_array[i]->type == CRD || ud_c_array[i]->is_header_only == 1
            ? 64
            : _wings_ud_recv_max_pkt_size(ud_c_array[i]) *
                  ud_c_array[i]->recv_q_depth;
  }

  struct hrd_ud_ctrl_blk* cb =
      hrd_ud_ctrl_blk_init(worker_lid, 0,
                           -1,  // local_hid, port_index, numa_node_id,
                           ud_c_num,
                           dgram_buff_size,  // num_dgram_qps, dgram_buf_size
                           base_shm_key + worker_lid,  // key
                           recv_q_depths,
                           send_q_depths);  // Depth of the dgram RECV, SEND Q

  for (uint8_t i = 0; i < ud_c_num; ++i)
    ud_c_array[i]->pd = cb->pd;

  _wings_share_qp_info_via_memcached(ud_c_array, ud_c_num, shared_rdy_var,
                                     worker_lid, cb);

  volatile uint8_t* incoming_reqs_ptr = cb->dgram_buf;
  for (uint8_t i = 0; i < ud_c_num; ++i) {
    // Init recv and setup wrs and sgls of ud_channel
    _wings_ud_channel_init_recv(ud_c_array[i], cb, (uint8_t)i,
                                incoming_reqs_ptr);
    incoming_reqs_ptr +=
        ud_c_array[i]->type == CRD || ud_c_array[i]->is_header_only == 1
            ? 64
            : _wings_ud_recv_max_pkt_size(ud_c_array[i]) *
                  ud_c_array[i]->recv_q_depth;
  }

  free(send_q_depths);
  free(recv_q_depths);

  for (int i = 0; i < ud_c_num; ++i)
    if (ud_c_array[i]->type != CRD) _wings_assertions(ud_c_array[i]);

  sleep(1);  /// Give some leeway to post receives, before start bcasting!
}

void
wings_setup_channel_qps_and_recvs(ud_channel_t** ud_c_array, uint16_t ud_c_num,
                                  dbit_vector_t* shared_rdy_var,
                                  uint16_t worker_lid)
{
  wings_setup_channel_qps_and_recvs_w_shm_key(
      ud_c_array, ud_c_num, shared_rdy_var, worker_lid, BASE_SHM_KEY);
}

void
wings_print_ud_c_overview(ud_channel_t* ud_c)
{
  printf("%s Channel[%d] %s(%d) --> %s\n",
         ud_c->is_bcast_channel ? "Bcast" : "Unicast", ud_c->channel_id,
         ud_c->qp_name, ud_c->qp_id, ud_c->type == REQ ? "REQ" : "RESP");

  _wings_print_on_off_toggle(ud_c->is_inlining_enabled, "Inlining");
  _wings_print_on_off_toggle(ud_c->max_coalescing, "Coalescing");
  _wings_print_on_off_toggle(ud_c->max_pcie_bcast_batch, "Max PCIe batch");

  printf("\t\tMax msg size: %dB\n", ud_c->max_msg_size);
  if (ud_c->type != CRD && !ud_c->is_header_only)
    printf("\t\tMax pkt size: send = %dB, recv = %dB\n",
           _wings_ud_send_max_pkt_size(ud_c),
           _wings_ud_recv_max_pkt_size(ud_c));
  else
    printf(
        "\t\tMax pkt size: send = 4B (inlined_payload), recv = "
        "4B(inlined_payload)\n");
  printf("\t\tSS granularity: %d\n", ud_c->ss_granularity);

  printf("\t\tNum remotes: %d\n", ud_c->num_channels - 1);
  if (ud_c->disable_crd_ctrl)
    printf("\t\tCredits: OFF \n");
  else
    printf("\t\tCredits: %d (%s) --> %s (%d)\n", ud_c->num_crds_per_channel,
           ud_c->expl_crd_ctrl ? "Explicit" : "Implicit",
           ud_c->channel_providing_crds->qp_name,
           ud_c->channel_providing_crds->qp_id);

  printf("\t\tSend Q len: %d\n", ud_c->send_q_depth);
  printf("\t\tRecv Q len: %d\n", ud_c->recv_q_depth);

  printf("\t\tSend wr len: %d\n", ud_c->max_send_wrs);
  printf("\t\tRecv wr len: %d\n", ud_c->max_recv_wrs);

  printf("\t\tSend pkt len: %d\n", ud_c->send_pkt_buff_len);
  printf("\t\tRecv pkt len: %d\n", ud_c->recv_pkt_buff_len);

  _wings_print_on_off_toggle(ud_c->enable_stats, "Stats");
  _wings_print_on_off_toggle(ud_c->enable_prints, "Prints");
}

/* ---------------------------------------------------------------------------
----------------------------------- SETUPs ------------------------------------
---------------------------------------------------------------------------*/
void
_wings_print_on_off_toggle(uint16_t bin_flag, char* str)
{
  if (bin_flag > 1)
    printf("\t\t%s : %s (%d)\n", str, "\033[1m\033[32mOn\033[0m", bin_flag);
  else
    printf("\t\t%s : %s\n", str,
           bin_flag ? "\033[1m\033[32mOn\033[0m" : "\033[31mOff\033[0m");
}

void
_wings_ud_channel_crd_init(ud_channel_t* ud_c, char* qp_name,
                           ud_channel_t* linked_channel,
                           uint16_t crds_per_channel, uint16_t num_channels,
                           uint8_t channel_id, uint8_t enable_stats,
                           uint8_t enable_prints)
{
  assert(channel_id < num_channels);

  _wings_assert_binary(enable_stats);
  _wings_assert_binary(enable_prints);

  ud_c->type = CRD;
  ud_c->qp_name =
      malloc(sizeof(char) *
             (strlen(qp_name) +
              1));  // TODO make sure to destroy this when destroing a crd_ud_c
  strcpy(ud_c->qp_name, qp_name);

  ud_c->channel_id = channel_id;
  ud_c->num_channels = num_channels;  // num_channels include our own channel
  ud_c->expl_crd_ctrl = 1;
  ud_c->disable_crd_ctrl = 0;
  ud_c->is_bcast_channel = 0;
  ud_c->max_pcie_bcast_batch = 0;
  ud_c->num_crds_per_channel = crds_per_channel;
  ud_c->channel_providing_crds = linked_channel;

  ud_c->enable_stats = enable_stats;
  ud_c->enable_prints = enable_prints;

  static_assert(sizeof(wings_crd_t) <= 4,
                "");         // Credits are always send as inlined_payload <=4B
  ud_c->max_msg_size = 0;    // non inlined_payload size
  ud_c->small_msg_size = 0;  // non inlined_payload size
  ud_c->max_coalescing = 1;

  ud_c->no_crds_to_send_per_endpoint = malloc(sizeof(uint16_t) * num_channels);

  uint16_t remote_channels = (uint16_t)(num_channels - 1);
  ud_c->is_inlining_enabled = 1;

  ud_c->credits_per_channels = malloc(sizeof(uint16_t) * (num_channels));
  for (int i = 0; i < num_channels; ++i)
    ud_c->credits_per_channels[i] = 0;

  ud_c->max_recv_wrs = crds_per_channel * remote_channels;
  ud_c->max_send_wrs = crds_per_channel * remote_channels;  // TODO correct this

  ud_c->ss_granularity = ud_c->max_send_wrs;

  ud_c->recv_q_depth = ud_c->max_recv_wrs;
  ud_c->send_q_depth = (uint16_t)(2 * ud_c->ss_granularity);

  ud_c->recv_wc = malloc(sizeof(struct ibv_wc) * ud_c->max_recv_wrs);

  ud_c->recv_pkt_buff_len =
      ud_c->max_recv_wrs * ud_c->max_coalescing;  // TODO: is this correct?
  ud_c->send_pkt_buff_len = ud_c->max_send_wrs;

  ud_c->send_pkt_buff = NULL;  // malloc(_wings_ud_send_max_pkt_size(ud_c) *
                               // ud_c->send_pkt_buff_len);

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
  ud_c->stats.send_total_pcie_batches = 0;
  ud_c->stats.no_stalls_due_to_credits = 0;

  ud_c->remote_qps = malloc(sizeof(qp_info_t) * ud_c->num_channels);
  // The following are set by the *_init_recv function after the creation of
  // control block and QPs
  ud_c->qp = NULL;
  ud_c->pd = NULL;
  ud_c->qp_id = 0;
  ud_c->send_cq = NULL;
  ud_c->recv_cq = NULL;
  ud_c->recv_pkt_buff = NULL;
  //	_wings_setup_crd_wr_and_sgl(ud_c, cb);
}

void
_wings_ud_channel_init_recv(ud_channel_t* ud_c, struct hrd_ud_ctrl_blk* cb,
                            uint8_t qp_id, volatile uint8_t* incoming_reqs_ptr)
{
  ud_c->qp_id = qp_id;
  ud_c->qp = cb->dgram_qp[qp_id];

  ud_c->recv_pkt_buff = incoming_reqs_ptr;

  ud_c->send_cq = cb->dgram_send_cq[ud_c->qp_id];
  ud_c->recv_cq = cb->dgram_recv_cq[ud_c->qp_id];

  if (ud_c->type != CRD) {
    ud_c->send_mem_region =
        ud_c->is_inlining_enabled
            ? NULL
            : register_buffer(
                  cb->pd, ud_c->send_pkt_buff,
                  _wings_ud_send_max_pkt_size(ud_c) * ud_c->send_pkt_buff_len);
    _wings_setup_send_wr_and_sgl(ud_c);
    _wings_setup_recv_wr_and_sgl(ud_c, cb);
  } else
    _wings_setup_crd_wr_and_sgl(ud_c, cb);

  // post initial receivs
  /// WARNING try to avoid races of posting initial receives and sending msgs
  _wings_setup_incoming_buff_and_post_initial_recvs(ud_c);
}

void
_wings_setup_crd_wr_and_sgl(ud_channel_t* ud_c, struct hrd_ud_ctrl_blk* cb)
{
  assert(ud_c->type == CRD);

  // Credit Send WRs / sgl
  wings_crd_t crd_tmp;
  crd_tmp.crd_num = 0;
  crd_tmp.sender_id = (uint8_t)ud_c->channel_id;

  ud_c->send_sgl = malloc(sizeof(struct ibv_sge));
  ud_c->send_sgl->length = 0;

  ud_c->send_wr = malloc(sizeof(struct ibv_send_wr) * ud_c->max_send_wrs);
  for (int i = 0; i < ud_c->max_send_wrs; ++i) {
    ud_c->send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
    ud_c->send_wr[i].num_sge = 0;
    ud_c->send_wr[i].sg_list = ud_c->send_sgl;
    ud_c->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    ud_c->send_wr[i].next = NULL;
    ud_c->send_wr[i].send_flags = IBV_SEND_INLINE;
    ud_c->send_wr[i].imm_data = 0;
    memcpy(&ud_c->send_wr[i].imm_data, &crd_tmp, sizeof(wings_crd_t));
  }

  // Credit Recv WRs / sgl
  ud_c->recv_sgl = malloc(sizeof(struct ibv_sge));
  ud_c->recv_sgl->length = 64;  // TODO can we make this zero?
  ud_c->recv_sgl->lkey = cb->dgram_buf_mr->lkey;
  ud_c->recv_sgl->addr = (uint64_t)ud_c->recv_pkt_buff;

  ud_c->recv_wr = malloc(sizeof(struct ibv_recv_wr) * ud_c->max_recv_wrs);
  for (int i = 0; i < ud_c->max_recv_wrs; ++i) {
    ud_c->recv_wr[i].num_sge = 1;
    ud_c->recv_wr[i].sg_list = ud_c->recv_sgl;
  }
}

void
_wings_setup_send_wr_and_sgl(ud_channel_t* ud_c)
{
  assert(ud_c->type != CRD);

  wings_hdr_only_t hdr_only_tmp;
  hdr_only_tmp.sender_id = (uint8_t)ud_c->channel_id;
  memset(hdr_only_tmp.inlined_payload, 0, 3 * sizeof(uint8_t));

  if (ud_c->is_bcast_channel) {  // Send bcast WRs

    uint16_t remote_channels = (uint16_t)(ud_c->num_channels - 1);
    uint16_t max_msgs_in_pcie_batch =
        (uint16_t)(ud_c->max_pcie_bcast_batch * remote_channels);
    ud_c->send_wr = malloc(sizeof(struct ibv_send_wr) * max_msgs_in_pcie_batch);
    ud_c->send_sgl =
        malloc(sizeof(struct ibv_sge) *
               (ud_c->is_header_only == 1 ? 1 : ud_c->max_pcie_bcast_batch));

    if (ud_c->is_header_only)
      ud_c->send_sgl->length = 0;
    else
      for (int i = 0; i < ud_c->max_pcie_bcast_batch; ++i)
        ud_c->send_sgl[i].length = _wings_ud_send_max_pkt_size(ud_c);

    for (int i = 0; i < max_msgs_in_pcie_batch; ++i) {
      int sgl_index = i / remote_channels;
      int i_mod_bcast = i % remote_channels;

      uint16_t rm_qp_id;
      if (i_mod_bcast < ud_c->channel_id)
        rm_qp_id = (uint16_t)i_mod_bcast;
      else
        rm_qp_id = (uint16_t)((i_mod_bcast + 1) % ud_c->num_channels);

      ud_c->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
      ud_c->send_wr[i].wr.ud.ah = ud_c->remote_qps[rm_qp_id].ah;
      ud_c->send_wr[i].wr.ud.remote_qpn = ud_c->remote_qps[rm_qp_id].qpn;

      if (!ud_c->is_header_only) {
        ud_c->send_wr[i].num_sge = 1;
        ud_c->send_wr[i].opcode =
            IBV_WR_SEND;  /// Attention!! there is no immediate here
        ud_c->send_wr[i].sg_list = &ud_c->send_sgl[sgl_index];

      } else {
        ud_c->send_wr[i].next = NULL;
        ud_c->send_wr[i].imm_data = 0;
        ud_c->send_wr[i].num_sge = 0;
        ud_c->send_wr[i].sg_list = ud_c->send_sgl;
        ud_c->send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
        memcpy(&ud_c->send_wr[i].imm_data, &hdr_only_tmp,
               sizeof(wings_hdr_only_t));
      }

      if (!ud_c->is_inlining_enabled) {
        ud_c->send_wr[i].send_flags = 0;
        ud_c->send_sgl[sgl_index].lkey = ud_c->send_mem_region->lkey;
      } else
        ud_c->send_wr[i].send_flags = IBV_SEND_INLINE;

      ud_c->send_wr[i].next =
          (i_mod_bcast == remote_channels - 1) ? NULL : &ud_c->send_wr[i + 1];
    }

  } else {  // Send unicast WRs

    ud_c->send_wr = malloc(sizeof(struct ibv_send_wr) * ud_c->max_send_wrs);
    ud_c->send_sgl = malloc(sizeof(struct ibv_sge) *
                            (ud_c->is_header_only ? 1 : ud_c->max_send_wrs));
    for (int i = 0; i < ud_c->max_send_wrs; ++i) {
      ud_c->send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;

      if (!ud_c->is_header_only) {
        //				ud_c->send_sgl[i].length =
        // sizeof(wings_pkt_t) + _wings_ud_recv_max_pkt_size(ud_c);
        ud_c->send_sgl[i].length = _wings_ud_send_max_pkt_size(ud_c);
        ud_c->send_wr[i].num_sge = 1;
        ud_c->send_wr[i].opcode =
            IBV_WR_SEND;  /// Attention!! there is no immediate here
        ud_c->send_wr[i].sg_list = &ud_c->send_sgl[i];

      } else {
        ud_c->send_sgl->length = 0;
        ud_c->send_wr[i].next = NULL;
        ud_c->send_wr[i].imm_data = 0;
        ud_c->send_wr[i].num_sge = 0;
        ud_c->send_wr[i].sg_list = ud_c->send_sgl;
        ud_c->send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
        memcpy(&ud_c->send_wr[i].imm_data, &hdr_only_tmp,
               sizeof(wings_hdr_only_t));
      }

      if (!ud_c->is_inlining_enabled) {
        ud_c->send_wr[i].send_flags = 0;
        ud_c->send_sgl[i].lkey = ud_c->send_mem_region->lkey;
      } else
        ud_c->send_wr[i].send_flags = IBV_SEND_INLINE;
    }
  }
}

void
_wings_setup_recv_wr_and_sgl(ud_channel_t* ud_c, struct hrd_ud_ctrl_blk* cb)
{
  assert(ud_c->type != CRD);

  ud_c->recv_sgl = malloc(sizeof(struct ibv_sge) *
                          (ud_c->is_header_only == 1 ? 1 : ud_c->max_recv_wrs));

  if (ud_c->is_header_only) {
    ud_c->recv_sgl->length = 64;  // TODO can we make this zero?
    ud_c->recv_sgl->lkey = cb->dgram_buf_mr->lkey;
    ud_c->recv_sgl->addr = (uint64_t)ud_c->recv_pkt_buff;
  }

  ud_c->recv_wr = malloc(sizeof(struct ibv_recv_wr) * ud_c->max_recv_wrs);
  for (int i = 0; i < ud_c->max_recv_wrs; i++) {
    if (!ud_c->is_header_only) {
      ud_c->recv_sgl[i].lkey = cb->dgram_buf_mr->lkey;
      ud_c->recv_sgl[i].length = _wings_ud_recv_max_pkt_size(ud_c);
    }

    ud_c->recv_wr[i].num_sge = 1;
    ud_c->recv_wr[i].next =
        (i == ud_c->max_recv_wrs - 1) ? NULL : &ud_c->recv_wr[i + 1];
    ud_c->recv_wr[i].sg_list =
        ud_c->is_header_only == 1 ? ud_c->recv_sgl : &ud_c->recv_sgl[i];
  }
}

void
_wings_setup_incoming_buff_and_post_initial_recvs(ud_channel_t* ud_c)
{
  if (ud_c->is_header_only == 0 && ud_c->type != CRD) {
    // init recv buffs as empty (not need for CRD since CRD msgs are
    // --inlined_payload-- header-only)
    for (uint16_t i = 0; i < ud_c->send_pkt_buff_len; ++i)
      _wings_get_nth_pkt_ptr_from_send_buff(ud_c, i)->req_num = 0;
    for (uint16_t i = 0; i < ud_c->recv_pkt_buff_len; ++i)
      _wings_get_nth_pkt_ptr_from_recv_buff(ud_c, i)->pkt.req_num = 0;
  }

  if (WINGS_ENABLE_POST_RECV_PRINTS && ud_c->enable_prints)
    colored_printf(YELLOW, "vvv Post Initial Receives: %s %d\n", ud_c->qp_name,
                   ud_c->max_recv_wrs);

  if (ud_c->is_header_only == 0 && ud_c->type != CRD)
    _wings_post_recvs(ud_c, ud_c->max_recv_wrs);
  else
    _wings_post_hdr_only_recvs(ud_c, ud_c->max_recv_wrs);
}

/* ---------------------------------------------------------------------------
   -------------------------------- QP Sharing -------------------------------
   ---------------------------------------------------------------------------
 */
unsigned long
_wings_simple_hash(unsigned char* str)
{
  int c;
  unsigned long hash = 5381;

  while (c = *str++)
    hash = ((hash << 5) + hash) + c;  // hash * 33 + c
  return hash;
}

void
_wings_get_remote_qp(ud_channel_t* ud_c, uint8_t endpoint_id)
{
  int ib_port_index = 0;
  int local_port_i = ib_port_index;
  char qp_global_name[HRD_QP_NAME_SIZE];
  struct hrd_qp_attr*
      qp;  //= malloc(sizeof(struct hrd_qp_attr*) * max_remote_channels);
  sprintf(qp_global_name, "%lu-%d",
          _wings_simple_hash((unsigned char*)ud_c->qp_name), endpoint_id);
  // Get the UD queue pair for the ith machine
  qp = NULL;
  //	yellow_printf("Looking for %s\n", qp_global_name);
  while (qp == NULL) {
    qp = hrd_get_published_qp(qp_global_name);

    if (qp == NULL) usleep(200000);
  }
  //	green_printf("Found %s\n", qp_global_name);

  struct ibv_ah_attr ah_attr = {
      //-----INFINIBAND----------
      .is_global = 0,
      .dlid = (uint16_t)qp->lid,
      .sl = (uint8_t)qp->sl,
      .src_path_bits = 0,
      /* port_num (> 1): device-local port for responses to this worker */
      .port_num = (uint8_t)(local_port_i + 1),
  };

  if (is_roce == 1) {
    //-----RoCE----------
    ah_attr.is_global = 1;
    ah_attr.dlid = 0;
    ah_attr.grh.dgid.global.interface_id = qp->gid_global_interface_id;
    ah_attr.grh.dgid.global.subnet_prefix = qp->gid_global_subnet_prefix;
    ah_attr.grh.sgid_index = 0;
    ah_attr.grh.hop_limit = 1;
  }

  ud_c->remote_qps[endpoint_id].qpn = (uint32_t)qp->qpn;
  ud_c->remote_qps[endpoint_id].ah = ibv_create_ah(ud_c->pd, &ah_attr);
  assert(ud_c->remote_qps[endpoint_id].ah != NULL);
}

void
_wings_get_remote_qps(ud_channel_t** ud_c_array, uint16_t ud_c_num)
{
  uint16_t max_remote_channels = 0;
  for (int i = 0; i < ud_c_num; ++i)
    if (ud_c_array[i]->num_channels > max_remote_channels)
      max_remote_channels = ud_c_array[i]->num_channels;

  for (int i = 0; i < ud_c_num; ++i)
    for (int j = 0; j < ud_c_array[i]->num_channels; ++j) {
      if (j == ud_c_array[i]->channel_id)
        continue;  // skip the local channel id
      _wings_get_remote_qp(ud_c_array[i], (uint8_t)j);
    }
}

void
_wings_share_qp_info_via_memcached(ud_channel_t** ud_c_array, uint16_t ud_c_num,
                                   dbit_vector_t* shared_rdy_var,
                                   int worker_lid, struct hrd_ud_ctrl_blk* cb)
{
  for (int i = 0; i < ud_c_num; i++) {
    char qp_global_name[HRD_QP_NAME_SIZE];
    sprintf(qp_global_name, "%lu-%d",
            _wings_simple_hash((unsigned char*)ud_c_array[i]->qp_name),
            ud_c_array[i]->channel_id);
    hrd_publish_dgram_qp(cb, i, qp_global_name, WORKER_SL);
    //		yellow_printf("Publishing: %s (qpname: %s)\n",  qp_global_name,
    // ud_c_array[i]->qp_name);
  }

  _wings_get_remote_qps(ud_c_array, ud_c_num);
  if (shared_rdy_var == NULL) {
    assert(worker_lid == 0);
    return;
  }

  assert(dbv_bit_get(*shared_rdy_var, worker_lid) == 0);
  dbv_bit_set(shared_rdy_var, (uint8_t)worker_lid);

  // WARNING (global) shared_rdy_var which is used as a g_share_qs_barrier must
  // be len of num_workers + 1
  while (!dbv_is_all_set(*shared_rdy_var))
    usleep(20000);

  assert(dbv_is_all_set(*shared_rdy_var));
}

void
wings_reconfigure_wrs_ah(ud_channel_t* ud_c, uint8_t endpoint_id)
{
  _wings_get_remote_qp(ud_c, endpoint_id);
  if (!ud_c->disable_crd_ctrl)
    _wings_get_remote_qp(ud_c->channel_providing_crds, endpoint_id);

  /// TODO WARNING: this is untested and assumes that we always send to everyone
  if (ud_c->is_bcast_channel) {
    uint16_t remote_channels = (uint16_t)(ud_c->num_channels - 1);
    uint16_t max_msgs_in_pcie_batch =
        (uint16_t)(ud_c->max_pcie_bcast_batch * remote_channels);
    for (int i = 0; i < max_msgs_in_pcie_batch; ++i) {
      int i_mod_bcast = i % remote_channels;

      uint16_t rm_qp_id;
      if (i_mod_bcast < ud_c->channel_id)
        rm_qp_id = (uint16_t)i_mod_bcast;
      else
        rm_qp_id = (uint16_t)((i_mod_bcast + 1) % ud_c->num_channels);

      ud_c->send_wr[i].wr.ud.ah = ud_c->remote_qps[rm_qp_id].ah;
      ud_c->send_wr[i].wr.ud.remote_qpn = ud_c->remote_qps[rm_qp_id].qpn;
    }
  }
}
