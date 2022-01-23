#include <spacetime.h>
#include <time.h>
#include "../../include/utils/concur_ctrl.h"
#include "inline-util.h"
#include "util.h"

///
#include "../../include/hades/hades.h"
#include "../../include/wings/wings.h"
///

int
inv_skip_or_get_sender_id(uint8_t* req)
{
  spacetime_op_t* op_req = (spacetime_op_t*)req;

  if (ENABLE_ASSERTIONS) {
    assert(is_response_code(op_req->op_meta.state) ||
           is_bucket_state_code(op_req->op_meta.state));
    assert(is_input_code(op_req->op_meta.opcode));
  }

  if (op_req->op_meta.state != ST_PUT_SUCCESS &&
      op_req->op_meta.state != ST_RMW_SUCCESS &&
      op_req->op_meta.state != ST_REPLAY_SUCCESS &&
      op_req->op_meta.state != ST_OP_MEMBERSHIP_CHANGE)
    return -1;
  return 0;  // since inv is a bcast we can return any int other than -1
}

void
inv_modify_elem_after_send(uint8_t* req)
{
  spacetime_op_t* op_req = (spacetime_op_t*)req;
  switch (op_req->op_meta.state) {
    case ST_PUT_SUCCESS:
      op_req->op_meta.state = ST_IN_PROGRESS_PUT;
      break;
    case ST_RMW_SUCCESS:
      op_req->op_meta.state = ST_IN_PROGRESS_RMW;
      break;
    case ST_REPLAY_SUCCESS:
      op_req->op_meta.state = ST_IN_PROGRESS_REPLAY;
      break;
    case ST_OP_MEMBERSHIP_CHANGE:
      op_req->op_meta.state = ST_OP_MEMBERSHIP_COMPLETE;
      break;
    default:
      assert(0);
  }
}

void
inv_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
  spacetime_op_t* op = (spacetime_op_t*)triggering_req;
  spacetime_inv_t* inv_to_send = (spacetime_inv_t*)msg_to_send;

  // Copy op to inv, set sender and opcode
  memcpy(inv_to_send, op, sizeof(spacetime_inv_t));
  inv_to_send->op_meta.sender = (uint8_t)machine_id;
  inv_to_send->op_meta.opcode = ST_OP_INV;
  //	//TODO change to include membership change
  //	inv_to_send->op_meta.opcode = (uint8_t) (op->op_meta.state ==
  // ST_OP_MEMBERSHIP_CHANGE ?
  // ST_OP_MEMBERSHIP_CHANGE : ST_OP_INV);
}

int
ack_skip_or_get_sender_id(uint8_t* req)
{
  spacetime_inv_t* inv_req = (spacetime_inv_t*)req;

  if (ENABLE_ASSERTIONS)
    assert(inv_req->op_meta.opcode == ST_INV_SUCCESS ||
           inv_req->op_meta.opcode == ST_OP_INV_ABORT ||
           inv_req->op_meta.opcode == ST_EMPTY);

  uint8_t is_small_msg = inv_req->op_meta.opcode == ST_INV_SUCCESS ? 1 : 0;

  return inv_req->op_meta.opcode == ST_EMPTY
             ? -1
             : wings_set_sender_id_n_msg_type(inv_req->op_meta.sender,
                                              is_small_msg);
}

void
ack_modify_elem_after_send(uint8_t* req)
{
  spacetime_inv_t* inv_req = (spacetime_inv_t*)req;

  // empty inv buffer
  if (inv_req->op_meta.opcode == ST_INV_SUCCESS ||
      inv_req->op_meta.opcode == ST_OP_INV_ABORT ||
      inv_req->op_meta.opcode == ST_OP_MEMBERSHIP_CHANGE)
    inv_req->op_meta.opcode = ST_EMPTY;
  else
    assert(0);
}

void
ack_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
  spacetime_inv_t* inv_req = (spacetime_inv_t*)triggering_req;
  spacetime_ack_t* ack_to_send = (spacetime_ack_t*)msg_to_send;
  spacetime_inv_t* inv_to_send = (spacetime_inv_t*)msg_to_send;
  switch (inv_req->op_meta.opcode) {
    case ST_INV_SUCCESS:
      memcpy(ack_to_send, triggering_req,
             sizeof(spacetime_ack_t));  // copy req to next_req_ptr
      ack_to_send->sender = (uint8_t)machine_id;
      ack_to_send->opcode = ST_OP_ACK;
      break;
    case ST_OP_INV_ABORT:
      memcpy(inv_to_send, triggering_req, sizeof(spacetime_inv_t));
      inv_to_send->op_meta.sender = (uint8_t)machine_id;
      inv_to_send->op_meta.opcode = ST_OP_INV_ABORT;
      break;
    default:
      assert(0);
  }
}

int
val_skip_or_get_sender_id(uint8_t* req)
{
  spacetime_ack_t* ack_req = (spacetime_ack_t*)req;
  if (ack_req->opcode == ST_ACK_SUCCESS ||
      ack_req->opcode == ST_OP_MEMBERSHIP_CHANGE) {
    ack_req->opcode = ST_EMPTY;
    return -1;
  } else if (ack_req->opcode == ST_EMPTY)
    return -1;

  if (ENABLE_ASSERTIONS) assert(ack_req->opcode == ST_LAST_ACK_SUCCESS);

  return ack_req->sender;
}

void
val_modify_elem_after_send(uint8_t* req)
{
  spacetime_ack_t* ack_req = (spacetime_ack_t*)req;

  if (ENABLE_ASSERTIONS) assert(ack_req->opcode == ST_LAST_ACK_SUCCESS);

  ack_req->opcode = ST_EMPTY;
}

void
val_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
  spacetime_val_t* val_to_send = (spacetime_val_t*)msg_to_send;

  memcpy(val_to_send, triggering_req,
         sizeof(spacetime_val_t));  // copy req to next_req_ptr
  val_to_send->opcode = ST_OP_VAL;
  val_to_send->sender = (uint8_t)machine_id;
}

int
memb_change_skip_or_get_sender_id(uint8_t* req)
{
  spacetime_op_t* op_req = (spacetime_op_t*)req;
  if (op_req->op_meta.state != ST_PUT_COMPLETE_SEND_VALS &&
      op_req->op_meta.state != ST_RMW_COMPLETE_SEND_VALS &&
      op_req->op_meta.state != ST_REPLAY_COMPLETE_SEND_VALS) {
    return -1;
  }
  return 1;  // it is bcast so just return something greater than zero
}

void
memb_change_modify_elem_after_send(uint8_t* req)
{
  spacetime_op_t* op_req = (spacetime_op_t*)req;
  switch (op_req->op_meta.state) {
    case ST_PUT_COMPLETE_SEND_VALS:
      op_req->op_meta.state = ST_PUT_COMPLETE;
      break;
    case ST_RMW_COMPLETE_SEND_VALS:
      op_req->op_meta.state = ST_RMW_COMPLETE;
      break;
    case ST_REPLAY_COMPLETE_SEND_VALS:
      op_req->op_meta.state = ST_NEW;  // ST_REPLAY_COMPLETE;
      break;
    default:
      assert(0);
  }
}

void
memb_change_copy_and_modify_elem(uint8_t* msg_to_send, uint8_t* triggering_req)
{
  spacetime_op_t* op_req = (spacetime_op_t*)triggering_req;
  spacetime_val_t* val_to_send = (spacetime_val_t*)msg_to_send;

  val_to_send->opcode = ST_OP_VAL;
  val_to_send->sender = (uint8_t)machine_id;
  val_to_send->ts = op_req->op_meta.ts;
}

int
rem_write_crd_skip_or_get_sender_id(uint8_t* req)
{
  spacetime_val_t* val_ptr = (spacetime_val_t*)req;

  if (ENABLE_ASSERTIONS)
    assert(val_ptr->opcode == ST_VAL_SUCCESS || val_ptr->opcode == ST_EMPTY);

  return val_ptr->opcode == ST_EMPTY ? -1 : val_ptr->sender;
}

void
rem_write_crd_modify_elem_after_send(uint8_t* req)
{
  spacetime_val_t* val_req = (spacetime_val_t*)req;

  // empty inv buffer
  if (val_req->opcode == ST_VAL_SUCCESS)
    val_req->opcode = ST_EMPTY;
  else
    assert(0);
}

void
print_total_send_recv_msgs(ud_channel_t* inv_ud_c, ud_channel_t* ack_ud_c,
                           ud_channel_t* val_ud_c, ud_channel_t* crd_ud_c)
{
  colored_printf(
      GREEN, "Total Send: invs %d, acks %d, vals %d, crds %d\n",
      inv_ud_c->stats.send_total_msgs, ack_ud_c->stats.send_total_msgs,
      val_ud_c->stats.send_total_msgs, crd_ud_c->stats.send_total_msgs);
  colored_printf(
      GREEN, "Total Recv: invs %d, acks %d, vals %d, crds %d\n",
      inv_ud_c->stats.recv_total_msgs, ack_ud_c->stats.recv_total_msgs,
      val_ud_c->stats.recv_total_msgs, crd_ud_c->stats.recv_total_msgs);
}

void
spin_until_all_nodes_are_in_membership(
    spacetime_group_membership* last_group_membership,
    hades_wings_ctx_t* hw_ctx, uint16_t worker_lid)
{
  bit_vector_t* membership_ptr =
      (bit_vector_t*)&last_group_membership->g_membership;
  bv_reset_all(membership_ptr);
  while (bv_no_setted_bits(*membership_ptr) < machine_num) {
    if (worker_lid == WORKER_WITH_FAILURE_DETECTOR) {
      update_view_and_issue_hbs(hw_ctx);
      if (!bv_are_equal(*membership_ptr, hw_ctx->ctx.curr_g_membership))
        group_membership_update(hw_ctx->ctx);
      poll_for_remote_views(hw_ctx);
    }
    *last_group_membership = group_membership;
  }
}

static inline void
failure_detection_n_membership(ud_channel_t** ud_channel_ptrs,
                               bit_vector_t* last_membership,
                               hades_wings_ctx_t* hw_ctx, uint16_t worker_lid)
{
  if (worker_lid == WORKER_WITH_FAILURE_DETECTOR) {
    update_view_and_issue_hbs(hw_ctx);

    ///< TODO>: We need to fix recovery (RDMA side of wings)!! the following is
    ///< not fully correct
    /// Additionally, this handles only WORKER_WITH_FAILURE_DETECTOR thread
    /// instead of every thread
    if (!bv_are_equal(hw_ctx->ctx.last_local_view.view,
                      hw_ctx->ctx.intermediate_local_view.view)) {
      for (int j = 0; j < 8; ++j)
        if (bv_bit_get(hw_ctx->ctx.last_local_view.view, j) == 0 &&
            bv_bit_get(hw_ctx->ctx.intermediate_local_view.view, j) == 1) {
          printf("W[%d]: updates %d endpoint channels\n", worker_lid, j);
          for (int i = 0; i < TOTAL_WORKER_UD_QPs; ++i) {
            wings_reset_credits(ud_channel_ptrs[i], j);
            wings_reconfigure_wrs_ah(ud_channel_ptrs[i], j);
          }
        }
    }
    //</TODO>

    if (!bv_are_equal(*last_membership, hw_ctx->ctx.curr_g_membership)) {
      group_membership_update(hw_ctx->ctx);
    }

    poll_for_remote_views(hw_ctx);
  }
}

void*
run_worker(void* arg)
{
  assert(is_CR == 0);

  struct thread_params params = *(struct thread_params*)arg;
  uint16_t worker_lid = (uint16_t)params.id;  // Local ID of this worker thread
  uint16_t worker_gid =
      (uint16_t)(machine_id * num_workers +
                 params.id);  // Global ID of this worker thread

  /* --------------------------------------------------------
  ------------------- RDMA WINGS DECLARATIONS---------------
  ---------------------------------------------------------*/
  ud_channel_t ud_channels[TOTAL_WORKER_N_FAILURE_DETECTION_UD_QPs];
  ud_channel_t* ud_channel_ptrs[TOTAL_WORKER_N_FAILURE_DETECTION_UD_QPs];
  ud_channel_t* inv_ud_c = &ud_channels[INV_UD_QP_ID];
  ud_channel_t* ack_ud_c = &ud_channels[ACK_UD_QP_ID];
  ud_channel_t* val_ud_c = &ud_channels[VAL_UD_QP_ID];
  ud_channel_t* crd_ud_c = &ud_channels[CRD_UD_QP_ID];

  for (int i = 0; i < TOTAL_WORKER_N_FAILURE_DETECTION_UD_QPs; ++i)
    ud_channel_ptrs[i] = &ud_channels[i];

  const uint8_t is_bcast = 1;
  const uint8_t stats_on = 1;
  const uint8_t prints_on = 1;
  const uint8_t is_hdr_only = 0;
  const uint8_t expl_crd_ctrl = 0;
  const uint8_t disable_crd_ctrl = 0;

  char inv_qp_name[200], ack_qp_name[200], val_qp_name[200];
  sprintf(inv_qp_name, "%s%d", "\033[31mINV\033[0m", worker_lid);
  sprintf(ack_qp_name, "%s%d", "\033[33mACK\033[0m", worker_lid);
  sprintf(val_qp_name, "%s%d", "\033[1m\033[32mVAL\033[0m", worker_lid);

  // WARNING: We use the ack channel to send/recv both acks and rmw-invs if RMWs
  // are enabled
  uint16_t ack_size =
      ENABLE_RMWs ? sizeof(spacetime_inv_t) : sizeof(spacetime_ack_t);

  uint8_t inv_inlining =
      (DISABLE_INLINING == 0 &&
       max_coalesce * sizeof(spacetime_inv_t) < WINGS_MAX_SUPPORTED_INLINING)
          ? 1
          : 0;
  uint8_t ack_inlining =
      (DISABLE_INLINING == 0 &&
       max_coalesce * ack_size < WINGS_MAX_SUPPORTED_INLINING)
          ? 1
          : 0;
  uint8_t val_inlining =
      (DISABLE_INLINING == 0 &&
       max_coalesce * sizeof(spacetime_val_t) < WINGS_MAX_SUPPORTED_INLINING)
          ? 1
          : 0;

  wings_ud_channel_init(inv_ud_c, inv_qp_name, REQ, (uint8_t)max_coalesce,
                        sizeof(spacetime_inv_t), 0, inv_inlining, is_hdr_only,
                        is_bcast, disable_crd_ctrl, expl_crd_ctrl, ack_ud_c,
                        (uint8_t)credits_num, machine_num, (uint8_t)machine_id,
                        stats_on, prints_on);
  wings_ud_channel_init(ack_ud_c, ack_qp_name, RESP, (uint8_t)max_coalesce,
                        ack_size, sizeof(spacetime_ack_t), ack_inlining,
                        is_hdr_only, 0, disable_crd_ctrl, expl_crd_ctrl,
                        inv_ud_c, (uint8_t)credits_num, machine_num,
                        (uint8_t)machine_id, stats_on, prints_on);
  wings_ud_channel_init(val_ud_c, val_qp_name, REQ, (uint8_t)max_coalesce,
                        sizeof(spacetime_val_t), 0, val_inlining, is_hdr_only,
                        is_bcast, disable_crd_ctrl, 1, crd_ud_c,
                        (uint8_t)credits_num, machine_num, (uint8_t)machine_id,
                        stats_on, prints_on);

  ///< HADES> Failure Detector Init
  hades_wings_ctx_t hw_ctx;
  uint16_t total_ud_qps = TOTAL_WORKER_UD_QPs;
  if (ENABLE_HADES_FAILURE_DETECTION &&
      worker_lid == WORKER_WITH_FAILURE_DETECTOR) {
    total_ud_qps = TOTAL_WORKER_N_FAILURE_DETECTION_UD_QPs;
    ud_channel_t* hviews_c = &ud_channels[TOTAL_WORKER_UD_QPs];
    ud_channel_t* hviews_crd_c = &ud_channels[TOTAL_WORKER_UD_QPs + 1];

    const uint16_t max_views_to_poll = 10;
    const uint32_t send_view_every_us = 100;
    const uint32_t update_local_view_ms = 10;

    hades_wings_ctx_init(&hw_ctx, machine_id, machine_num, max_views_to_poll,
                         send_view_every_us, update_local_view_ms, hviews_c,
                         hviews_crd_c, worker_lid);
  }
  ///</HADES>

  wings_setup_channel_qps_and_recvs(ud_channel_ptrs, total_ud_qps,
                                    g_share_qs_barrier, worker_lid);

  uint16_t ops_len =
      (uint16_t)(credits_num * remote_machine_num *
                 max_coalesce);  // credits * remote_machines * max_req_coalesce
  assert(ops_len >= inv_ud_c->recv_pkt_buff_len);
  assert(ops_len >= ack_ud_c->recv_pkt_buff_len);
  assert(ops_len >= val_ud_c->recv_pkt_buff_len);

  /* -------------------------------------------------------
  ------------------- OTHER DECLARATIONS--------------------
  ---------------------------------------------------------*/
  // Intermediate buffs where reqs are copied from incoming_* buffs in order to
  // get passed to the KVS
  spacetime_op_t* ops;
  spacetime_inv_t* inv_recv_ops;
  spacetime_ack_t*
      ack_recv_ops;  // WARNING!! This can be spacetime_ack_t / spacetime_inv_t
                     // * depends if RMWs are disabled or not
  spacetime_val_t* val_recv_ops;

  setup_kvs_buffs(&ops, &inv_recv_ops, &ack_recv_ops, &val_recv_ops);

  struct spacetime_trace_command* trace;
  trace_init(&trace, worker_gid);

  ////
  spacetime_op_t* n_hottest_keys_in_ops_get[COALESCE_N_HOTTEST_KEYS];
  spacetime_op_t* n_hottest_keys_in_ops_put[COALESCE_N_HOTTEST_KEYS];
  for (int i = 0; i < COALESCE_N_HOTTEST_KEYS; ++i) {
    n_hottest_keys_in_ops_get[i] = NULL;
    n_hottest_keys_in_ops_put[i] = NULL;
  }
  ////

  int node_suspected = -1;
  uint32_t trace_iter = 0;
  uint16_t rolling_inv_index = 0;
  uint16_t invs_polled = 0, acks_polled = 0, vals_polled = 0;
  uint8_t has_outstanding_vals = 0, has_remaining_vals_from_memb_change = 0;

  uint32_t* num_of_iters_serving_op = malloc(max_batch_size * sizeof(uint32_t));
  for (int i = 0; i < max_batch_size; ++i)
    num_of_iters_serving_op[i] = 0;

  /// Spawn stats thread
  if (worker_lid == 0) {
    if (spawn_stats_thread() != 0)
      colored_printf(RED, "Stats thread was not successfully spawned \n");
  }

  struct timespec stopwatch_for_req_latency;

  // Membership init
  bit_vector_t* membership_ptr =
      ENABLE_HADES_FAILURE_DETECTION
          ? (bit_vector_t*)&group_membership.g_membership
          : NULL;
  if (ENABLE_HADES_FAILURE_DETECTION) {
    spin_until_all_nodes_are_in_membership(&group_membership, &hw_ctx,
                                           worker_lid);
    printf("~~~~~~~~~ Starting while ! ~~~~~~~~~\n");
  }

  /* -----------------------------------------------------
 ------------------------Main Loop--------------------
     ----------------------------------------------------- */

  struct timespec stopwatch_for_fd_warmup;
  get_rdtsc_timespec(&stopwatch_for_fd_warmup);
  uint8_t fd_warmup_time_has_passed = 0;

  while (true) {
    // Check something periodically (e.g., stats)
    if (unlikely(w_stats[worker_lid].total_loops % M_16 == 0)) {
      //			print_total_send_recv_msgs_n_credits(&inv_ud_c,
      //&ack_ud_c, &val_ud_c, &crd_ud_c);
    }

    if (!ENABLE_HADES_FAILURE_DETECTION || fd_warmup_time_has_passed == 1) {
      node_suspected =
          refill_ops(&trace_iter, worker_lid, trace, ops,
                     num_of_iters_serving_op, &stopwatch_for_req_latency,
                     n_hottest_keys_in_ops_get, n_hottest_keys_in_ops_put);

      hermes_batch_ops_to_KVS(local_ops, (uint8_t*)ops, max_batch_size,
                              sizeof(spacetime_op_t), group_membership, NULL,
                              NULL, (uint8_t)worker_lid);

      stop_latency_of_completed_reads(ops, worker_lid,
                                      &stopwatch_for_req_latency);

      if (update_ratio > 0) {
        ///~~~~~~~~~~~~~~~~~~~~~~INVS~~~~~~~~~~~~~~~~~~~~~~~~~~~
        wings_issue_pkts(inv_ud_c, membership_ptr, (uint8_t*)ops,
                         (uint16_t)max_batch_size, sizeof(spacetime_op_t),
                         &rolling_inv_index, inv_skip_or_get_sender_id,
                         inv_modify_elem_after_send, inv_copy_and_modify_elem);

        /// Poll for INVs
        invs_polled = wings_poll_buff_and_post_recvs(inv_ud_c, ops_len,
                                                     (uint8_t*)inv_recv_ops);

        if (invs_polled > 0) {
          hermes_batch_ops_to_KVS(invs, (uint8_t*)inv_recv_ops, invs_polled,
                                  sizeof(spacetime_inv_t), group_membership,
                                  &node_suspected, ops, (uint8_t)worker_lid);

          ///~~~~~~~~~~~~~~~~~~~~~~ACKS~~~~~~~~~~~~~~~~~~~~~~~~~~~
          wings_issue_pkts(
              ack_ud_c, membership_ptr, (uint8_t*)inv_recv_ops, invs_polled,
              sizeof(spacetime_inv_t), NULL, ack_skip_or_get_sender_id,
              ack_modify_elem_after_send, ack_copy_and_modify_elem);

          if (ENABLE_ASSERTIONS)
            assert(inv_ud_c->stats.recv_total_msgs ==
                   ack_ud_c->stats.send_total_msgs);
        }

        if (has_outstanding_vals == 0 &&
            has_remaining_vals_from_memb_change == 0) {
          /// Poll for Acks
          acks_polled = wings_poll_buff_and_post_recvs(ack_ud_c, ops_len,
                                                       (uint8_t*)ack_recv_ops);

          if (acks_polled > 0) {
            hermes_batch_ops_to_KVS(acks, (uint8_t*)ack_recv_ops, acks_polled,
                                    ack_size, group_membership, NULL, ops,
                                    (uint8_t)worker_lid);

            stop_latency_of_completed_writes(ops, worker_lid,
                                             &stopwatch_for_req_latency);
          }
        }

        if (!DISABLE_VALS_FOR_DEBUGGING) {
          ///~~~~~~~~~~~~~~~~~~~~~~ VALs ~~~~~~~~~~~~~~~~~~~~~~~~~~~
          if (has_remaining_vals_from_memb_change > 0)
            has_remaining_vals_from_memb_change = wings_issue_pkts(
                val_ud_c, membership_ptr, (uint8_t*)ops, max_batch_size,
                sizeof(spacetime_op_t), NULL, memb_change_skip_or_get_sender_id,
                memb_change_modify_elem_after_send,
                memb_change_copy_and_modify_elem);
          else
            has_outstanding_vals = wings_issue_pkts(
                val_ud_c, membership_ptr, (uint8_t*)ack_recv_ops,
                ack_ud_c->recv_pkt_buff_len, ack_size, NULL,
                val_skip_or_get_sender_id, val_modify_elem_after_send,
                val_copy_and_modify_elem);

          /// Poll for Vals
          vals_polled = wings_poll_buff_and_post_recvs(val_ud_c, ops_len,
                                                       (uint8_t*)val_recv_ops);

          if (vals_polled > 0) {
            hermes_batch_ops_to_KVS(vals, (uint8_t*)val_recv_ops, vals_polled,
                                    sizeof(spacetime_val_t), group_membership,
                                    NULL, NULL, (uint8_t)worker_lid);

            ///~~~~~~~~~~~~~~~~~~~~~~CREDITS~~~~~~~~~~~~~~~~~~~~~~~~~~~
            wings_issue_credits(
                crd_ud_c, membership_ptr, (uint8_t*)val_recv_ops, ops_len,
                sizeof(spacetime_val_t), rem_write_crd_skip_or_get_sender_id,
                rem_write_crd_modify_elem_after_send);
          }
        }
      }
    } else if (ENABLE_HADES_FAILURE_DETECTION &&
               time_elapsed_in_sec(stopwatch_for_fd_warmup) > 2) {
      fd_warmup_time_has_passed = 1;
      printf("~~~~~~~~~ Starting execution! ~~~~~~~~~\n");
    }

    // Failure Detection and Membership
    if (ENABLE_HADES_FAILURE_DETECTION) {
      failure_detection_n_membership(ud_channel_ptrs, membership_ptr, &hw_ctx,
                                     worker_lid);

      if (group_membership_has_changed(&group_membership, worker_lid)) {
        /// Complete inprogress updates/replays waiting for ACKS only from
        /// failed nodes
        hermes_batch_ops_to_KVS(local_ops_after_membership_change,
                                (uint8_t*)ops, max_batch_size,
                                sizeof(spacetime_op_t), group_membership, NULL,
                                NULL, (uint8_t)worker_lid);

        stop_latency_of_completed_writes(ops, worker_lid,
                                         &stopwatch_for_req_latency);

        if (!DISABLE_VALS_FOR_DEBUGGING)
          /// Bcast VAL msgs for those completed update/replays
          has_remaining_vals_from_memb_change = wings_issue_pkts(
              val_ud_c, membership_ptr, (uint8_t*)ops, max_batch_size,
              sizeof(spacetime_op_t), NULL, memb_change_skip_or_get_sender_id,
              memb_change_modify_elem_after_send,
              memb_change_copy_and_modify_elem);
      }
    }
    w_stats[worker_lid].total_loops++;
  }
}
