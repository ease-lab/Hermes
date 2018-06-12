#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H

#include "cache.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
// Function can return this enum as a control flow guidance
enum control_flow_directive {no_cf_change, break_, continue_ };



// swap 2 pointerss
static inline void swap_pointers(void** ptr_1, void** ptr_2)
{
	void* tmp = *ptr_1;
	*ptr_1 = *ptr_2;
	*ptr_2 = tmp;
}

// Swap 3 pointers in a cirular fashion
static inline void circulate_pointers(void** ptr_1, void** ptr_2, void** ptr_3)
{
		void* tmp = *ptr_1;
		*ptr_1 = *ptr_2;
		*ptr_2 = *ptr_3;
		*ptr_3 = tmp;
}

// Perform the swaping of the pointers to the OPs
static inline void swap_ops(struct extended_cache_op** ops, struct extended_cache_op** next_ops, struct extended_cache_op** third_ops,
                            struct mica_resp** resp, struct mica_resp** next_resp, struct mica_resp ** third_resp,
                            struct key_home** key_homes, struct key_home** next_key_homes, struct key_home** third_key_homes)
{
  if (CLIENT_ENABLE_INLINING == 1) {
    swap_pointers((void**) ops, (void**) next_ops);
    swap_pointers((void**) resp, (void**) next_resp);
    swap_pointers((void**) key_homes, (void**) next_key_homes);
  }
  else {
    circulate_pointers((void**) ops, (void**) next_ops, (void**) third_ops);
    circulate_pointers((void**) resp, (void**) next_resp, (void**) third_resp);
    circulate_pointers((void**) key_homes, (void**) next_key_homes, (void**) third_key_homes);
    // printf("ops %llu, next_ops %llu, third_ops %llu \n", ops, next_ops, third_ops);
  }
}

// Copy the current op to the next buffer to be used as it cannot be used in this cycle
static inline void copy_op_to_next_op(struct extended_cache_op* ops, struct extended_cache_op* next_ops,
																			struct mica_resp* resp, struct mica_resp* next_resp,
																			struct key_home* key_homes, struct key_home* next_key_homes,
																			uint16_t op_i, uint16_t* next_op_i, struct latency_flags* latency_info,
                                      uint16_t* hottest_keys_pointers)
{
	if (ENABLE_ASSERTIONS) {
		if (!(resp[op_i].type == CACHE_GET_STALL ||
					resp[op_i].type == CACHE_PUT_STALL ||
					resp[op_i].type == UNSERVED_CACHE_MISS ||
					ops[op_i].opcode == CACHE_OP_BRC))
			printf(" Code %s : %s\n", code_to_str(ops[op_i].opcode), code_to_str(resp[op_i].type));
	}
	uint16_t size_of_op;
	if (ops[op_i].opcode == CACHE_OP_BRC || ops[op_i].opcode == CACHE_OP_PUT ||
			ops[op_i].opcode == CACHE_OP_INV || ops[op_i].opcode == CACHE_OP_UPD)  // not sure if all of these opcodes can occur
		size_of_op = HERD_PUT_REQ_SIZE;
	else if (ops[op_i].opcode == CACHE_OP_GET)
		size_of_op = ENABLE_HOT_KEY_TRACKING == 1 ? HERD_GET_REQ_SIZE + 2 : HERD_GET_REQ_SIZE; // when tracking hot keys we use the val_len and the first byte of the value
	else if (ENABLE_ASSERTIONS) {
		printf("unexpected opcode %d \n", ops[op_i].opcode);
		assert(false);
	}
	if ((ENABLE_HOT_KEY_TRACKING == 1) && (resp[op_i].type == CACHE_GET_STALL)) {
    if ((ops[(op_i)].value[0]) < HOTTEST_KEYS_TO_TRACK) {
      if (hottest_keys_pointers[ops[(op_i)].value[0]]  == (op_i + 1)) {
        hottest_keys_pointers[ops[(op_i)].value[0]] = (*next_op_i) + 1; // we store the op_i + 1 because '0' has special meaning
      }
    }

	}
	memcpy(next_ops + (*next_op_i), ops + op_i, size_of_op);
	memcpy(next_resp + (*next_op_i), resp + op_i, sizeof(struct mica_resp));
	memcpy(next_key_homes + (*next_op_i), key_homes + op_i, sizeof(struct key_home));

	if ((MEASURE_LATENCY == 1) && (op_i == latency_info->last_measured_op_i) &&
			((latency_info->measured_req_flag == LOCAL_REQ) || (latency_info->measured_req_flag == HOT_READ_REQ) ||
			 (latency_info->measured_req_flag == HOT_WRITE_REQ_BEFORE_SAVING_KEY))) {
		latency_info->last_measured_op_i = *next_op_i; // if the local req has been issued this is useless, but harmless
//			yellow_printf("Changing op_i from %d to %d \n", op_i, latency_info->last_measured_op_i);
	}
	(*next_op_i)++;
}

// Check whether 2 key hashes are equal
static inline uint8_t keys_are_equal(struct cache_key* key1, struct cache_key* key2) {
	return (key1->bkt    == key2->bkt &&
					key1->server == key2->server &&
					key1->tag    == key2->tag) ? 1 : 0;
}

// Check whether 2 keys (including the metadata) are equal
static inline uint8_t keys_and_meta_are_equal(struct cache_key* key1, struct cache_key* key2) {
	return (key1->bkt    == key2->bkt &&
					key1->server == key2->server &&
					key1->tag    == key2->tag &&
					key1->meta.version == key2->meta.version) ? 1 : 0;
}

// A condition to be used to trigger periodic (but rare) measurements
static inline bool trigger_measurement(uint16_t local_client_id)
{
	return w_stats[local_client_id].cache_hits_per_client % K_32 > 0 &&
				w_stats[local_client_id].cache_hits_per_client % K_32 <= CACHE_BATCH_SIZE &&
				local_client_id == 0 && machine_id == MACHINE_NUM -1;
}
//
//// Poll for the local reqs completion to measure a local req's latency
//static inline void poll_local_req_for_latency_measurement(struct latency_flags* latency_info, struct timespec* start,
//																													struct local_latency* local_measure)
//{
//
//	if ((MEASURE_LATENCY == 1) && ((latency_info->measured_req_flag) == LOCAL_REQ) && ((local_measure->local_latency_start_polling) == 1))
//		if (*(local_measure->flag_to_poll) == 0) {
//			struct timespec end;
//			clock_gettime(CLOCK_MONOTONIC, &end);
//			int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
//										 ((end.tv_nsec - start->tv_nsec) / 1000);
//			if (ENABLE_ASSERTIONS) assert(useconds > 0);
////			yellow_printf("Latency of a local req, region %d, flag ptr %llu: %d us\n",
////										local_measure->measured_local_region, local_measure->flag_to_poll, useconds);
//			bookkeep_latency(useconds, LOCAL_REQ);
//			latency_info->measured_req_flag = NO_REQ;
//			local_measure->measured_local_region = -1;
//			local_measure->local_latency_start_polling = 0;
//			local_measure->flag_to_poll = NULL;
//
//		}
//}

static inline void report_remote_latency(struct latency_flags* latency_info, uint16_t prev_rem_req_i,
																				 struct ibv_wc* wc, struct timespec* start)
{
	uint16_t i;
	for (i = 0; i < prev_rem_req_i; i++) {
//					 		printf("Looking for the req\n" );
		if (wc[i].imm_data == REMOTE_LATENCY_MARK) {
			struct timespec end;
			clock_gettime(CLOCK_MONOTONIC, &end);
			int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
										 ((end.tv_nsec - start->tv_nsec) / 1000);  //(end.tv_nsec - start->tv_nsec) / 1000;
			if (ENABLE_ASSERTIONS) assert(useconds > 0);
//		printf("Latency of a Remote read %u us\n", useconds);
			bookkeep_latency(useconds, REMOTE_REQ);
			(latency_info->measured_req_flag) = NO_REQ;
			break;
		}
	}
}

// Poll for credits in SC -- only used by acks, not broadcasts
static inline void poll_credits_SC(struct ibv_cq* credit_recv_cq, struct ibv_wc* credit_wc, uint8_t credits[][MACHINE_NUM])
{
	int credits_found = 0;
	uint16_t j;
	credits_found = ibv_poll_cq(credit_recv_cq, MAX_CREDIT_WRS, credit_wc);
	if(credits_found > 0) {
		if(unlikely(credit_wc[credits_found -1].status != 0)) {
			fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
			exit(0);
		}
		for (j = 0; j < credits_found; j++) {
			if (credit_wc[j].imm_data >= (2* MACHINE_NUM))
				credits[UPD_VC][credit_wc[j].imm_data - (2* MACHINE_NUM)] += CRDS_IN_MESSAGE;
			else if (credit_wc[j].imm_data >= MACHINE_NUM)
				credits[INV_VC][credit_wc[j].imm_data - MACHINE_NUM] += CRDS_IN_MESSAGE;
			else credits[ACK_VC][credit_wc[j].imm_data] += CRDS_IN_MESSAGE;
		}
	}
	else if(unlikely(credits_found < 0)) {
		printf("ERROR In the credit CQ\n"); exit(0);
	}
}

// Used after polling for received credits, to increment the right Virtual Channel's credits depending on the protocol
static inline void increment_credits_based_on_protocol(int credits_found, struct ibv_wc* credit_wc, uint8_t credits[][MACHINE_NUM], int protocol)
{
	uint16_t j;
	if (protocol == STRONG_CONSISTENCY) {
		for (j = 0; j < credits_found; j++) {
			if (credit_wc[j].imm_data >= (2* MACHINE_NUM))
				credits[UPD_VC][credit_wc[j].imm_data - (2* MACHINE_NUM)] += CRDS_IN_MESSAGE;
			else if (credit_wc[j].imm_data >= MACHINE_NUM)
				credits[INV_VC][credit_wc[j].imm_data - MACHINE_NUM] += CRDS_IN_MESSAGE;
			else credits[ACK_VC][credit_wc[j].imm_data] += CRDS_IN_MESSAGE;
		}
	}
	///else if (protocol == EVENTUAL) {
	///	for (j = 0; j < credits_found; j++) credits[EC_UPD_VC][credit_wc[j].imm_data] += EC_CREDITS_IN_MESSAGE;
	///}
}

// Poll for credits and increment the credits according to the protocol
static inline void poll_credits(struct ibv_cq* credit_recv_cq, struct ibv_wc* credit_wc, uint8_t credits[][MACHINE_NUM], int protocol)
{
	int credits_found = 0;
	credits_found = ibv_poll_cq(credit_recv_cq, MAX_CREDIT_WRS, credit_wc);
	if(credits_found > 0) {
		if(unlikely(credit_wc[credits_found -1].status != 0)) {
			fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n", credit_wc[credits_found -1].status);
			exit(0);
		}
		increment_credits_based_on_protocol(credits_found, credit_wc, credits, protocol);
	}
	else if(unlikely(credits_found < 0)) {
		printf("ERROR In the credit CQ\n"); exit(0);
	}
}

// Post some receives and then send some messages
static inline void post_recvs_and_send(uint16_t recv_counter, uint16_t message_send_counter, struct ibv_send_wr* send_wr,
											struct ibv_recv_wr* recv_wr, struct ibv_qp* send_qp, struct ibv_qp* recv_qp, uint32_t MAX_RECVS,
											uint32_t MAX_SENDS)
{
	uint16_t j;
	int ret;
	struct ibv_send_wr *bad_send_wr;
	struct ibv_recv_wr *bad_recv_wr;
	if (recv_counter > 0) { // Must post receives for credits
		if (ENABLE_ASSERTIONS == 1) assert (recv_counter <= MAX_RECVS);
		for (j = 0; j < recv_counter; j++) {
			recv_wr[j].next = (j == recv_counter - 1) ?	NULL : &recv_wr[j + 1];
		}
		ret = ibv_post_recv(recv_qp, &recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs in generic function ", ret);
	}
	// Batch the messages to the NIC
	if (message_send_counter > 0) {
		if (ENABLE_ASSERTIONS == 1) assert (recv_counter <= MAX_SENDS);
		send_wr[message_send_counter - 1].next = NULL;
		ret = ibv_post_send(send_qp, &send_wr[0], &bad_send_wr);
		CPE(ret, "ibv_post_send error in generic function", ret);
	}
}


// Bookkeeping of requests that hit in the cache for SC
static inline void cache_hit_bookkeeping_SC(struct extended_cache_op* ops, struct extended_cache_op* next_ops, struct mica_resp* resp,
														struct mica_resp* next_resp, struct key_home* key_homes, struct key_home* next_key_homes,
														uint16_t* op_i, uint16_t* next_op_i, uint16_t* stalled_ops_i, uint16_t local_client_id,
														struct latency_flags* latency_info, uint16_t* hottest_keys_pointers)
{
	while (resp[(*op_i)].type != CACHE_MISS && resp[(*op_i)].type != UNSERVED_CACHE_MISS && (*op_i) < CACHE_BATCH_SIZE) {
		if (ENABLE_ASSERTIONS == 1)
			if(!(resp[(*op_i)].type == CACHE_GET_SUCCESS || resp[(*op_i)].type == CACHE_PUT_SUCCESS ||
				 resp[(*op_i)].type == CACHE_GET_STALL || resp[(*op_i)].type == CACHE_PUT_STALL ||
				 resp[(*op_i)].type == CACHE_PUT_FAIL || ops[(*op_i)].opcode == CACHE_OP_INV ||
				 ops[(*op_i)].opcode == CACHE_OP_BRC)) {
					printf("Ops: %s\n",code_to_str(ops[(*op_i)].opcode));
					printf("Response: %s\n",code_to_str(resp[(*op_i)].type));
					assert(0);
			}
		if (STALLING_DEBUG_SC) {
			if (resp[(*op_i)].type == CACHE_GET_STALL || resp[(*op_i)].type == CACHE_PUT_STALL)
				(*stalled_ops_i)++;
    }
		if (resp[(*op_i)].type == CACHE_GET_SUCCESS || ops[(*op_i)].opcode == CACHE_OP_INV ||
			resp[(*op_i)].type == CACHE_PUT_FAIL ) {// Empty the Reads and the broadcasts that have been sent
      if ((ENABLE_HOT_KEY_TRACKING == 1) && (resp[(*op_i)].type == CACHE_GET_SUCCESS)) {
        w_stats[local_client_id].cache_hits_per_client += (ops[(*op_i)].val_len + 1);
        if ((ops[(*op_i)].value[0]) < HOTTEST_KEYS_TO_TRACK) {
          if (hottest_keys_pointers[ops[(*op_i)].value[0]] == ((*op_i) + 1)) {
            hottest_keys_pointers[ops[(*op_i)].value[0]] = 0;
          }
        }
      }
      else w_stats[local_client_id].cache_hits_per_client++;
      resp[(*op_i)].type = ST_EMPTY;
		}
		else {
			copy_op_to_next_op(ops, next_ops, resp, next_resp, key_homes, next_key_homes, (*op_i), next_op_i, latency_info, hottest_keys_pointers);
		}
		(*op_i)++;
	}
}

// After creating the remote batch, iterate through the rest of the ops for bookkeeping
static inline void run_through_rest_of_ops_SC(struct extended_cache_op* ops, struct extended_cache_op* next_ops, struct mica_resp* resp,
														struct mica_resp* next_resp, struct key_home* key_homes, struct key_home* next_key_homes,
														uint16_t* op_i, uint16_t* next_op_i, uint16_t* stalled_ops_i, uint16_t local_client_id,
														struct latency_flags* latency_info, uint16_t* hottest_keys_pointers)
{
	while (*op_i < CACHE_BATCH_SIZE) {
		if (resp[(*op_i)].type == CACHE_MISS) resp[(*op_i)].type = UNSERVED_CACHE_MISS;
		if (STALLING_DEBUG_SC) {
			if (resp[(*op_i)].type == CACHE_GET_STALL || resp[(*op_i)].type == CACHE_PUT_STALL) (*stalled_ops_i)++;
		}
		if (resp[(*op_i)].type == CACHE_GET_SUCCESS || ops[(*op_i)].opcode == CACHE_OP_INV ||
			resp[(*op_i)].type == CACHE_PUT_FAIL) {
      if ((ENABLE_HOT_KEY_TRACKING == 1) && (resp[(*op_i)].type == CACHE_GET_SUCCESS)) {
        w_stats[local_client_id].cache_hits_per_client += (ops[(*op_i)].val_len + 1);
        if ((ops[(*op_i)].value[0]) < HOTTEST_KEYS_TO_TRACK) {
          if (hottest_keys_pointers[ops[(*op_i)].value[0]] == ((*op_i) + 1)) {
            hottest_keys_pointers[ops[(*op_i)].value[0]] = 0;
          }
        }
      }
      else w_stats[local_client_id].cache_hits_per_client++;
      resp[(*op_i)].type = ST_EMPTY;
		}
		else copy_op_to_next_op(ops, next_ops, resp, next_resp, key_homes, next_key_homes, (*op_i), next_op_i, latency_info, hottest_keys_pointers);
		(*op_i)++;
	}
}

// Used when sending remote requests to debug the coalescing feature
static inline void debug_coalescing(struct ibv_sge* rem_send_sgl, uint16_t local_client_id, uint16_t rem_req_i,
																			uint16_t wr_i)
{
	uint16_t i, j;
	int send_cntr = 0;
	uint16_t clt_gid = machine_id * CLIENTS_PER_MACHINE + local_client_id;
	for (i = 0; i < wr_i; i++) {
		struct mica_op* tmp = (struct mica_op*)rem_send_sgl[i].addr;
		assert(tmp->opcode == MICA_OP_PUT || tmp->opcode == MICA_OP_GET ||
							 tmp->opcode == MICA_OP_MULTI_GET);
		if (tmp->opcode == MICA_OP_PUT) {send_cntr++; continue;}
//		printf("Message %d: length %d, opcode %d\n", i, rem_send_sgl[i].length, tmp->opcode);
		if (rem_send_sgl[i].length > HERD_GET_REQ_SIZE + 1) {
			assert(tmp->opcode == MICA_OP_MULTI_GET);
			assert(rem_send_sgl[i].length == (tmp->val_len * HERD_GET_REQ_SIZE) + 1);
			send_cntr+= tmp->val_len;
			for (j = 0; j <  tmp->val_len; j++) {
				struct mica_op* tmp2 = (struct mica_op*)rem_send_sgl[i].addr;
				if (j > 0) {
					uint8_t* tmp3 = (uint8_t*)rem_send_sgl[i].addr + 1 + (j * HERD_GET_REQ_SIZE);
					tmp2 = (struct mica_op*)tmp3;
				}
				//if (j > 0 && tmp2->opcode != MICA_OP_GET){
				 if((tmp2->key.rem_meta.clt_gid != clt_gid) && (MEASURE_LATENCY == 0)) {
					red_printf("Inside sends, wr_i %d val_len %d, clt_gid %d, req %d and sent clt_gid %d rem_send_sgl_addr %llu looking at %llu\n",
								local_client_id, i, tmp->val_len, j, tmp2->key.rem_meta.clt_gid, tmp, tmp2);
					assert(false);
				}
			}
		}
		else send_cntr++;
	}
//	printf("wr_i %d, rem_req_i %d, send_ctr %d \n", wr_i, rem_req_i, send_cntr);
	assert(send_cntr == rem_req_i);
}

//Checks if there are enough credits to perform a broadcast -- protocol independent
static inline bool check_broadcast_credits(uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb, struct ibv_wc* credit_wc,
														uint32_t* credit_debug_cnt, uint16_t vc, int protocol)
{
	uint16_t enough_credits = 1, poll_for_credits = 0, j;
	for (j = 0; j < MACHINE_NUM; j++) {
		if (j == machine_id) continue; // skip the local machine
		if (credits[vc][j] == 0) {
			poll_for_credits = 1;
			break;
		}
	}
	if (poll_for_credits == 1)
		poll_credits(cb->dgram_recv_cq[FC_UD_QP_ID], credit_wc, credits, protocol);
	// We polled for credits, if we did not find enough just break
	for (j = 0; j < MACHINE_NUM; j++) {
		if (j == machine_id) continue; // skip the local machine
		if (credits[vc][j] == 0) {
			(*credit_debug_cnt)++;
			return false;
		}
	}
	*credit_debug_cnt = 0;
	return true;
}

// Broadcast logic uses this function to post appropriate number of credit recvs before sending broadcasts
static inline void post_credit_recvs_and_batch_bcasts_to_NIC(uint16_t br_i, struct hrd_ctrl_blk *cb, struct ibv_send_wr *coh_send_wr,
																														 struct ibv_recv_wr *credit_recv_wr, uint16_t *credit_recv_counter,
																														 int protocol)
{
	uint16_t j;
	int ret;
	struct ibv_send_wr *bad_send_wr;
	struct ibv_recv_wr *bad_recv_wr;
	uint32_t max_credit_recvs;
	if (protocol == STRONG_CONSISTENCY) max_credit_recvs = MAX_CREDIT_RECVS;
	///else if (protocol == EVENTUAL) max_credit_recvs = EC_MAX_CREDIT_RECVS;
	else if (ENABLE_ASSERTIONS) assert(false);

	if (*credit_recv_counter > 0) { // Must post receives for credits
		if (ENABLE_ASSERTIONS == 1) assert ((*credit_recv_counter) * (MACHINE_NUM - 1) <= max_credit_recvs);
		for (j = 0; j < (MACHINE_NUM - 1) * (*credit_recv_counter); j++) {
			credit_recv_wr[j].next = (j == ((MACHINE_NUM - 1) * (*credit_recv_counter)) - 1) ?	NULL : &credit_recv_wr[j + 1];
		}
		ret = ibv_post_recv(cb->dgram_qp[FC_UD_QP_ID], &credit_recv_wr[0], &bad_recv_wr);
		CPE(ret, "ibv_post_recv error: posting recvs for credits before broadcasting", ret);
		*credit_recv_counter = 0;
	}
	// Batch the broadcasts to the NIC
	if (br_i > 0) {
		coh_send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = NULL;
		ret = ibv_post_send(cb->dgram_qp[BROADCAST_UD_QP_ID], &coh_send_wr[0], &bad_send_wr);
		CPE(ret, "Broadcast ibv_post_send error", ret);
		if (CLIENT_ENABLE_INLINING == 1) coh_send_wr[0].send_flags = IBV_SEND_INLINE;
		else if (!CLIENT_ENABLE_INLINING && protocol != STRONG_CONSISTENCY) coh_send_wr[0].send_flags = 0;
	}
}

/* ---------------------------------------------------------------------------
------------------------------ CLIENT MAIN LOOP -----------------------------
---------------------------------------------------------------------------*/
//
//// Client uses this to post receive requests for the remote answers
//static inline void refill_recvs(uint16_t rem_req_i, struct ibv_recv_wr* rem_recv_wr, struct hrd_ctrl_blk* cb)
//{
//	if (rem_req_i > 0) {
//		if (ENABLE_ASSERTIONS == 1) assert(rem_req_i <= WINDOW_SIZE);
//		int i;
//		struct ibv_recv_wr *bad_recv_wr;
//		for(i = 0; i < rem_req_i; i++)
//			rem_recv_wr[i].next = (i == rem_req_i - 1) ?
//								  NULL : &rem_recv_wr[i + 1];
//		 int ret = ibv_post_recv(cb->dgram_qp[REMOTE_UD_QP_ID], &rem_recv_wr[0], &bad_recv_wr);
//		CPE(ret, " Refill Remote: ibv_post_recv error", ret);
//	}
//}
//
//// Checks if an op can be coalaseced with previous ops and returns true if it can
//static inline bool vector_read(struct extended_cache_op* ops, struct coalesce_inf* coalesce_struct, uint16_t* op_i_ptr,
//            uint16_t wr_i, uint8_t rm_id, int size_of_op, struct ibv_sge* rem_send_sgl, struct mica_resp* resp,
//            int local_client_id, uint8_t per_worker_outstanding, struct ibv_send_wr* rem_send_wr)
//{
//  /* a multiget is as such:
//  	 key 16B Opcode 1B(Multiget) Val_len overloaded to say how many requests in the multiget,
//  	 value contains only Keys and Opcode of the coalesced gets
//		 Sending the opcode for every key may look stupid but it makes life much easier on the worker size,
//		 where mica gets probed with an array of pointers to requests*/
//  uint16_t op_i = *op_i_ptr;
//  if (ENABLE_COALESCING == 1 && ops[op_i].opcode == CACHE_OP_GET) {
//    if (coalesce_struct[rm_id].slots == 0 && per_worker_outstanding < WS_PER_WORKER) {
//      coalesce_struct[rm_id].wr_i = wr_i;
//      coalesce_struct[rm_id].op_i = op_i;
//      coalesce_struct[rm_id].slots++;
//			rem_send_sgl[wr_i].length = HERD_GET_REQ_SIZE;
//			return false;
//      // yellow_printf("Client %d Coalescing for rm %d at op_i %d and wr_ i %d tag %d \n", local_client_id, rm_id, op_i, wr_i, ops[op_i].key.tag);
//    }
//    else if (coalesce_struct[rm_id].slots > 0 && ops[op_i].opcode == CACHE_OP_GET) {
//      coalesce_struct[rm_id].slots++;
//			uint16_t cl_slots = coalesce_struct[rm_id].slots; // make the code a bit more readable
//			uint16_t cl_wr_i = coalesce_struct[rm_id].wr_i;
//			uint16_t cl_op_i = coalesce_struct[rm_id].op_i;
//			if (cl_slots == 2) {
//				ops[cl_op_i].opcode = MICA_OP_MULTI_GET;
//				rem_send_sgl[cl_wr_i].length++; // we add the val_len field
//			}
//      if (ENABLE_ASSERTIONS == 1) assert(cl_slots <= MAX_COALESCE_PER_MACH);
//      rem_send_sgl[cl_wr_i].length += HERD_GET_REQ_SIZE;
//			/* this can only help with big objects where inlining is disabled and is used only for get reqs
//			 * when inlining is enabled, it is also mandatory as NIC reads from the buffers asynchronously */
//			if ((CLIENT_ENABLE_INLINING == 0) && rem_send_sgl[cl_wr_i].length > MAXIMUM_INLINE_SIZE) {
//				if ((rem_send_wr[cl_wr_i].send_flags & IBV_SEND_INLINE) != 0) // inline is used and must be removed
//					rem_send_wr[cl_wr_i].send_flags ^= IBV_SEND_INLINE;
//			}
//			//  printf("Increasing the length of wr_i %d to %d, op_i :%d, cl_op_i %d \n", cl_wr_i, rem_send_sgl[cl_wr_i].length, op_i, cl_op_i );
//      if (ENABLE_ASSERTIONS == 1) {
//        if (rem_send_sgl[cl_wr_i].length > MULTIGET_AVAILABLE_SIZE) {
//					printf("local_client_id %d, length %d, cl_slots %d,\n", local_client_id, rem_send_sgl[cl_wr_i].length, cl_slots);
//					assert(false);
//				}
//				assert(ops[op_i].opcode == CACHE_OP_GET);
//        assert(cl_slots > 1);
//				struct remote_meta *tmp = (struct remote_meta *)&(ops[op_i].key.meta);
//				assert(tmp->clt_gid == local_client_id + CLIENTS_PER_MACHINE * machine_id);
//        //yellow_printf("offset %d, max value %d\n",HERD_GET_REQ_SIZE * (cl_slots - 2), MICA_MAX_VALUE - HERD_GET_REQ_SIZE);
//        //assert((HERD_GET_REQ_SIZE * (cl_slots - 2)) <= MICA_MAX_VALUE - HERD_GET_REQ_SIZE);
//      }
//      memcpy(ops[cl_op_i].value + (HERD_GET_REQ_SIZE * (cl_slots - 2)), ops + op_i, HERD_GET_REQ_SIZE);
//      ops[cl_op_i].val_len = cl_slots;
//			// yellow_printf("Client %d Adding for rm %d at op_i %d and wr_ i %d on slot %d, offset %llu, key with tag %d\n",
//				// local_client_id, rm_id, op_i, wr_i, coalesce_struct[rm_id].slots, ops[cl_op_i].value + (HERD_GET_REQ_SIZE * (cl_slots - 2)),
//				// ops[op_i].key.tag);
//      if (coalesce_struct[rm_id].slots == MAX_COALESCE_PER_MACH) coalesce_struct[rm_id].slots = 0;
//      resp[op_i].type = ST_EMPTY;
//      (*op_i_ptr)++;
//      return true;
//			// green_printf("OP_i %d Opcode %d, referring wr %d, wr size %d \n", coalesce_struct[rm_id].op_i, ops[coalesce_struct[rm_id].op_i].opcode,
//			// 						coalesce_struct[rm_id].wr_i, rem_send_sgl[coalesce_struct[rm_id].wr_i].length);
//    }
//  }
//	return false;
//}
//
//// Client logic to issue the local requests
//static inline void issue_locals(uint16_t wn, uint16_t* ws, uint16_t local_client_id, uint32_t size_of_op, uint16_t worker_id,
//								uint8_t* per_worker_outstanding, long long* remote_for_each_worker, struct extended_cache_op* ops,
//								struct extended_cache_op* next_ops, struct mica_resp* resp, struct mica_resp* next_resp,
//								struct key_home* key_homes, struct key_home* next_key_homes, uint16_t op_i, uint16_t* next_op_i,
//                uint16_t local_worker_id, struct latency_flags* latency_info, struct timespec* start,
//								struct local_latency* local_measure)
//{
//	if (ENABLE_LOCAL_WORKERS) {
//		wn = local_worker_id;
//		int rm_id = worker_id / WORKERS_PER_MACHINE;
//		worker_id = rm_id * WORKERS_PER_MACHINE + wn;
//	}
//	else if (ENABLE_WORKERS_CRCW && ENABLE_STATIC_LOCAL_ALLOCATION) {
//		wn = local_client_id % WORKERS_PER_MACHINE;
//		int rm_id = worker_id / WORKERS_PER_MACHINE;
//		worker_id = rm_id * WORKERS_PER_MACHINE + wn;
//	}
//	if (DISABLE_LOCALS == 1 && ENABLE_ASSERTIONS == 1) assert (false);
//	int offset = OFFSET(wn, local_client_id, ws[wn]);
//	uint8_t loc_region = (offset % LOCAL_WINDOW) / LOCAL_REGION_SIZE;
//
//	if (local_recv_flag[wn][local_client_id][loc_region] == 0) {
//		if ((MEASURE_LATENCY == 1) && ((latency_info->measured_req_flag) == LOCAL_REQ) &&
//				(op_i == (latency_info->last_measured_op_i))) {
//			local_measure->measured_local_region = loc_region;
////			yellow_printf("Issuing a local req: op_i: %d, local_region %d \n",
////										latency_info->last_measured_op_i, local_measure->measured_local_region);
//		}
//		memcpy((struct mica_op*) local_req_region + offset, ops + op_i, size_of_op);
////		printf("op opcode %d local opcode %d, size_of op %d \n", ops[op_i].opcode, local_req_region[offset].opcode, size_of_op);
//		if (ENABLE_ASSERTIONS) assert(local_req_region[offset].opcode == MICA_OP_GET ||
//																			local_req_region[offset].opcode == MICA_OP_PUT);
//		if (ws[wn] % LOCAL_REGION_SIZE == LOCAL_REGION_SIZE - 1) {
//			local_recv_flag[wn][local_client_id][loc_region] = 1;
//			if ((MEASURE_LATENCY == 1) && ((latency_info->measured_req_flag) == LOCAL_REQ) &&
//					((local_measure->measured_local_region) == (loc_region))) {
//				local_measure->local_latency_start_polling = 1;
//				local_measure->flag_to_poll = (char*)(&local_recv_flag[wn][local_client_id][loc_region]);
////				yellow_printf("Enable polling for local req on region %d, flag ptr %llu \n ",
////											local_measure->measured_local_region, local_measure->flag_to_poll);
//			}
//			// yellow_printf("Lock: client  %d,  region %d for worker %d\n", local_client_id, loc_region, wn);
//		}
//		// yellow_printf("Client %d I issue a local req to worker %d with opcode %d at offset %d, region %d\n",
//				// clt_gid, wn, ops[op_i].opcode, offset, loc_region);
//
//		if (ws[wn] == 0)  per_worker_outstanding[worker_id] = 1;
//		else per_worker_outstanding[worker_id]++;
//		remote_for_each_worker[worker_id]++;
//		w_stats[local_client_id].locals_per_worker++;
//		HRD_MOD_ADD(ws[wn], LOCAL_WINDOW);
//		resp[op_i].type = ST_EMPTY;
//	}
//	else {
//		copy_op_to_next_op(ops, next_ops, resp, next_resp, key_homes, next_key_homes, op_i, next_op_i, latency_info, NULL);
//	}
//}

// Post Receives for coherence and then Send out the credits
static inline void send_credits(uint16_t credit_wr_i, struct ibv_sge* coh_recv_sgl, struct hrd_ctrl_blk* cb,
																int* push_ptr, struct ibv_recv_wr* coh_recv_wr, struct ibv_qp* coh_recv_qp,
																struct ibv_send_wr* credit_send_wr, uint16_t credits_in_message, uint32_t clt_buf_slots,
																void* buf)
{
	// For every credit message we send back, prepare x receives for the broadcasts
	uint16_t i, j, recv_iter = 0;
	struct ibv_recv_wr *bad_recv_wr;
	struct ibv_send_wr *bad_send_wr;
	for(i = 0; i < credit_wr_i; i++) {
		for (j = 0; j < credits_in_message; j++) {
			recv_iter = i * credits_in_message + j;
			coh_recv_sgl[recv_iter].addr = (uintptr_t) buf + ((*push_ptr) * UD_REQ_SIZE);
			HRD_MOD_ADD(*push_ptr, clt_buf_slots);
//			if (*push_ptr == 0) *push_ptr = 1;
			coh_recv_wr[recv_iter].next = (recv_iter == (credit_wr_i * credits_in_message) - 1) ?
										  NULL : &coh_recv_wr[recv_iter + 1];
		}
	}
	int ret = ibv_post_recv(coh_recv_qp, &coh_recv_wr[0], &bad_recv_wr);
	CPE(ret, "ibv_post_recv error", ret);

	credit_send_wr[credit_wr_i - 1].next = NULL;
	// yellow_printf("I am sending %d credit message(s)\n", credit_wr_i);
	ret = ibv_post_send(cb->dgram_qp[FC_UD_QP_ID], &credit_send_wr[0], &bad_send_wr);
	CPE(ret, "ibv_post_send error in credits", ret);
}
//
///* Poll the remote recv q to find enough credits to for the smaler batch allowed
//			---------------UNTESTED-------------------------- */
//static inline void find_responses_to_enable_multi_batching(double empty_req_percentage, struct hrd_ctrl_blk* cb,
//																struct ibv_wc* wc, uint8_t* per_worker_outstanding, uint32_t* outstanding_rem_reqs,
//																uint16_t local_client_id)
//{
//	uint16_t j;
//	uint32_t min_batch_ability;
//	int responses_found = 0;
//	// BACK-OFF Mechanism to make sure the buffer will get a chance to empty
//	if (empty_req_percentage < MIN_EMPTY_PERCENTAGE)
//		min_batch_ability = MAX_OUTSTANDING_REQS - 1;
//	else min_batch_ability = MINIMUM_BATCH_ABILITY;
//
//	/* Poll for whatever responses are ready until there are enough credits,
//	 	such that the minimum-size can be created */
//	do {
//		responses_found = ibv_poll_cq(cb->dgram_recv_cq[REMOTE_UD_QP_ID], MAX_OUTSTANDING_REQS, wc);
//		if(responses_found != 0) {
//			if(wc[responses_found -1].status != 0) {
//				fprintf(stderr, "Polling remote recv req: Bad wc status %d\n", wc[responses_found -1].status);
//				exit(0);
//			}
//			for (j = 0; j < responses_found; j++) {
//				if (ENABLE_ASSERTIONS == 1) assert(wc[j].imm_data < WORKER_NUM);
//				per_worker_outstanding[wc[j].imm_data]--;
//				if (ENABLE_ASSERTIONS == 1) assert(per_worker_outstanding[wc[j].imm_data] <= WS_PER_WORKER);
//			}
//		}
//		(*outstanding_rem_reqs) -= responses_found;
//		if (ENABLE_ASSERTIONS == 1) assert((*outstanding_rem_reqs) <= MAX_OUTSTANDING_REQS);
//		w_stats[local_client_id].stalled_time_per_worker++;
//	} while (MAX_OUTSTANDING_REQS - (*outstanding_rem_reqs) < min_batch_ability);
//}
//
///* Handle the lack of credits for a remote request by either breaking,
// going to the next req, or picking another receipient */ // TODO test the balance reqs
//static inline enum control_flow_directive handle_lack_of_credits_for_remotes(struct extended_cache_op* ops, struct extended_cache_op* next_ops, struct mica_resp* resp,
//														struct mica_resp* next_resp, struct key_home* key_homes, struct key_home* next_key_homes,
//														uint16_t* op_i, uint16_t* next_op_i, uint16_t local_client_id, struct ibv_send_wr* rem_send_wr,
//														uint16_t* wn, uint16_t* worker_id, uint16_t rm_id, uint8_t* per_worker_outstanding, uint16_t wr_i,
//														uint32_t outstanding_rem_reqs, struct latency_flags* latency_info)
//{
//	uint16_t i;
//	per_worker_outstanding[(*worker_id)]--;
//	if ((outstanding_rem_reqs + wr_i) >= MAX_OUTSTANDING_REQS) { //   the client has no credits at all
//		rem_send_wr[wr_i - 1].next = NULL;
//		copy_op_to_next_op(ops, next_ops, resp, next_resp, key_homes, next_key_homes, (*op_i), next_op_i, latency_info, NULL);
//		(*op_i)++;
//		return break_;
//	}
//	else {
//		if (BALANCE_REQS == 1) { // option 1: send to an alternative worker TODO test this!
////			printf(" %d %d\n", outstanding_rem_reqs + wr_i + 1, MAX_OUTSTANDING_REQS);
////			if (ENABLE_ASSERTIONS == 1) assert(outstanding_rem_reqs + wr_i > MAX_OUTSTANDING_REQS);
//			for (i = 0; i < WORKERS_PER_MACHINE; i++) {
//				if (per_worker_outstanding[rm_id * WORKERS_PER_MACHINE + i] < WS_PER_WORKER) {
////					printf("previous worker %d, new worker %d \n", (*wn), i);
//					(*wn) = i;
//					break;
//				}
//			}
//			(*worker_id) = rm_id * WORKERS_PER_MACHINE + (*wn);
//			per_worker_outstanding[(*worker_id)]++;
////			printf("new worker id %d\n", (*worker_id));
//			return no_cf_change;
//		}
//		else if (FINISH_BATCH_ON_MISSING_CREDIT == 1) { //option 2: finish the batch
//			rem_send_wr[wr_i - 1].next = NULL; /* Make sure to end the batch on the last WR*/
//			copy_op_to_next_op(ops, next_ops,resp, next_resp, key_homes, next_key_homes, (*op_i), next_op_i, latency_info, NULL);
//			(*op_i)++;
//			return break_;
//		}
//		else { // option 3: skip this request
//			copy_op_to_next_op(ops, next_ops, resp, next_resp, key_homes, next_key_homes, (*op_i), next_op_i, latency_info, NULL);
//			(*op_i)++;
//			return continue_;
//		}
//	}
//}
//
////
//static inline void forge_remote_wr(uint16_t wr_i, uint16_t op_i, uint16_t wn, uint16_t rm_id, uint16_t local_client_id, uint16_t size_of_op,
//												struct hrd_ctrl_blk* cb, struct ibv_send_wr* rem_send_wr, struct ibv_sge* rem_send_sgl, struct extended_cache_op* ops,
//												struct ibv_wc* wc, long long remote_tot_tx, uint16_t worker_id, uint16_t worker_qp_i)
//{
//	rem_send_wr[wr_i].wr.ud.ah = remote_wrkr_qp[worker_qp_i][worker_id].ah;
//	rem_send_wr[wr_i].wr.ud.remote_qpn = remote_wrkr_qp[worker_qp_i][worker_id].qpn;
//	rem_send_sgl[wr_i].length = size_of_op;
//	rem_send_sgl[wr_i].addr = (uint64_t) (uintptr_t) (ops + op_i);
////  yellow_printf("Client %d: Remote op %d with key bkt %u , machine %d , worker %d, ud_qp %d, opcode %d  \n", local_client_id, op_i, ops[op_i].key.bkt,
////	 				rm_id, wn, worker_qp_i,ops[op_i].opcode);
//
//	if (wr_i > 0) rem_send_wr[wr_i - 1].next = &rem_send_wr[wr_i];
//	if (CLIENT_ENABLE_INLINING == 0)
//		rem_send_wr[wr_i].send_flags = ops[op_i].opcode == CACHE_OP_PUT ? 0 :
//			(ENABLE_INLINE_GET_REQS == 1 ? IBV_SEND_INLINE : 0);
//	else rem_send_wr[wr_i].send_flags = IBV_SEND_INLINE; // this is necessary to delete the SIGNALED flag
//
//	if (((remote_tot_tx + wr_i ) % CLIENT_SS_BATCH) == 0) {
////		yellow_printf(" message %d, signaled \n", remote_tot_tx + wr_i);
//		rem_send_wr[wr_i].send_flags |= IBV_SEND_SIGNALED;
//	}
//
//	if (((remote_tot_tx + wr_i ) % CLIENT_SS_BATCH) == CLIENT_SS_BATCH - 1) {
////		yellow_printf(" message %d, polling \n", remote_tot_tx + wr_i);
//		hrd_poll_cq(cb->dgram_send_cq[REMOTE_UD_QP_ID], 1, wc);
//	}
//}
//
//// Handle all cold requests: issue the locals erquests and create the remote work requests
//// move to the next op buffer all requests that can not be serviced this cycle
//static inline uint16_t handle_cold_requests(struct extended_cache_op* ops, struct extended_cache_op* next_ops, struct mica_resp* resp,
//														struct mica_resp* next_resp, struct key_home* key_homes, struct key_home* next_key_homes,
//														uint16_t* rem_req_i, uint16_t* next_op_i, struct hrd_ctrl_blk* cb, struct ibv_send_wr* rem_send_wr,
//														struct ibv_sge* rem_send_sgl, struct ibv_wc* wc, long long remote_tot_tx, uint16_t worker_qp_i,
//														uint8_t* per_worker_outstanding, uint32_t* outstanding_rem_reqs, long long* remote_for_each_worker,
//														uint16_t* ws, int clt_gid, uint16_t local_client_id, uint16_t* stalled_ops_i, uint16_t local_worker_id,
//                            int protocol, struct latency_flags* latency_info, struct timespec* start, struct local_latency* local_measure,
//														uint16_t* hottest_keys_pointers)
//{
//	uint16_t op_i = 0, i, j, wr_i = 0, rm_id, wn, worker_id;
//	uint32_t size_of_op;
//	struct coalesce_inf coalesce_struct[MACHINE_NUM] = {0};
//	while ((*rem_req_i) < WINDOW_SIZE && op_i < CACHE_BATCH_SIZE) {
//		if (DISABLE_CACHE != 1) {
//			if (protocol == STRONG_CONSISTENCY)
//				cache_hit_bookkeeping_SC(ops, next_ops, resp, next_resp, key_homes, next_key_homes,
//												&op_i, next_op_i, stalled_ops_i, local_client_id, latency_info,
//												hottest_keys_pointers);
//			else if (protocol == EVENTUAL)
//				cache_hit_bookkeeping_EC(ops, next_ops, resp, next_resp, key_homes, next_key_homes,
//													&op_i, next_op_i, local_client_id, latency_info,
//																 hottest_keys_pointers);
//		}
//		//Check to see whether the batch of reqs is over
//		if (op_i >= CACHE_BATCH_SIZE) {
//			if (wr_i > 0) rem_send_wr[wr_i - 1].next = NULL;
//			break;
//		}
//
//		//Error checking
//		if (ENABLE_ASSERTIONS == 1) {
//			if (ops[op_i].opcode != CACHE_OP_GET && ops[op_i].opcode != CACHE_OP_PUT)
//				printf("Wrong Opcode %d, pointer %d\n", ops[op_i].opcode, op_i);
//			if (ops[op_i].opcode == CACHE_OP_PUT) assert(ops[op_i].val_len == (HERD_VALUE_SIZE >> SHIFT_BITS));
//		}
//
//		resp[op_i].type = UNSERVED_CACHE_MISS;
//		rm_id = key_homes[op_i].machine;	/* Choose a remote machine */
//		if (ENABLE_ASSERTIONS == 1) assert(DISABLE_LOCALS == 0 || rm_id != machine_id);
//		if (ENABLE_THREAD_PARTITIONING_C_TO_W == 1) wn = (local_client_id + machine_id) % ACTIVE_WORKERS_PER_MACHINE;//
//		else wn = key_homes[op_i].worker % ACTIVE_WORKERS_PER_MACHINE;	/* Choose a worker */
//		worker_id = rm_id * WORKERS_PER_MACHINE + wn;
//		size_of_op = ops[op_i].opcode == CACHE_OP_PUT ? HERD_PUT_REQ_SIZE : HERD_GET_REQ_SIZE;
//
//
//		/*------------------------------ LOCAL REQUESTS--------------------------------*/
//		if (rm_id == machine_id) {
//			issue_locals(wn, ws, local_client_id, size_of_op, worker_id, per_worker_outstanding,
//									 remote_for_each_worker, ops, next_ops, resp, next_resp, key_homes, next_key_homes,
//									 op_i, next_op_i, local_worker_id, latency_info,
//									 start, local_measure);
//			op_i++;
//			continue;
//		}
//		// overload part of the key to write the clt_gid
//		struct remote_meta* tmp =  (struct remote_meta*) &ops[op_i].key.meta;
//		tmp->clt_gid = clt_gid;
//
//
//		//---------------------------------DO COALESCING-----------------------------------------
//		if (ENABLE_COALESCING) {
//			if (vector_read(ops, coalesce_struct, &op_i, wr_i, rm_id, size_of_op, rem_send_sgl, resp, local_client_id, per_worker_outstanding[worker_id], rem_send_wr)) {
//				(*rem_req_i)++;
//				continue;
//			}
//		}
//
//		per_worker_outstanding[worker_id]++;
//		if (per_worker_outstanding[worker_id] > WS_PER_WORKER ) {
////			printf("outstanding %d for worker %d \n", per_worker_outstanding[worker_id], worker_id);
//			enum control_flow_directive cf = handle_lack_of_credits_for_remotes(ops, next_ops, resp,
//														next_resp, key_homes, next_key_homes, &op_i, next_op_i, local_client_id, rem_send_wr,
//														&wn, &worker_id, rm_id, per_worker_outstanding, wr_i, *outstanding_rem_reqs, latency_info);
//			if (cf == continue_) continue;
//			else if (cf == break_) break;
//		}
//
//		remote_for_each_worker[worker_id]++;
//		/* Forge the RDMA work request */
//		forge_remote_wr(wr_i, op_i, wn, rm_id, local_client_id, size_of_op, cb, rem_send_wr, rem_send_sgl, ops,
//											wc, remote_tot_tx, worker_id, worker_qp_i);
//
//		wr_i++;
//		(*rem_req_i)++;
//		resp[op_i].type = ST_EMPTY;
//		op_i++;
//	} // end while() for remotes
//
//	// Run through the rest of the ops to remove the cache hits here
//	if (protocol == STRONG_CONSISTENCY)
//		run_through_rest_of_ops_SC(ops, next_ops, resp, next_resp, key_homes, next_key_homes,
//															 &op_i, next_op_i, stalled_ops_i, local_client_id, latency_info,
//															 hottest_keys_pointers);
//	else if (protocol == EVENTUAL)
//		run_through_rest_of_ops_EC(ops, next_ops, resp, next_resp, key_homes, next_key_homes,
//															 &op_i, next_op_i, local_client_id, latency_info,
//															 hottest_keys_pointers);
//	poll_local_req_for_latency_measurement(latency_info, start, local_measure);
//
//	return wr_i;
//}

//
///* Poll for receive compeltions for answers to remote requests and then
//	 send the new batch of remote requests to the NIC */
//static inline void poll_and_send_remotes(uint16_t previous_wr_i, uint16_t local_client_id, uint32_t* outstanding_rem_reqs,
//												struct hrd_ctrl_blk* cb, struct ibv_wc* wc, uint16_t prev_rem_req_i, uint16_t wr_i,
//												struct ibv_send_wr* rem_send_wr, uint16_t rem_req_i, long long* remote_tot_tx,
//												struct ibv_sge* rem_send_sgl, struct latency_flags* latency_info, struct timespec* start)
//{
//	int ret;
//	struct ibv_send_wr* bad_send_wr;
//	uint16_t i,j;
//	//Poll for remote request receive completions
//	if (ENABLE_MULTI_BATCHES == 0) {
//		if(previous_wr_i > 0) {
////			if (local_client_id == 0) printf("polling remote recv compeltions %d \n", prev_rem_req_i);
//			w_stats[local_client_id].stalled_time_per_worker +=
//					hrd_poll_cq(cb->dgram_recv_cq[REMOTE_UD_QP_ID], prev_rem_req_i, wc);
//			// if (local_client_id == 0) printf("polled %d\n", prev_rem_req_i );
//			*outstanding_rem_reqs -= previous_wr_i;
//			if (ENABLE_ASSERTIONS) assert(prev_rem_req_i <= MAX_REMOTE_RECV_WCS);
//		  if ((MEASURE_LATENCY == 1) && ((latency_info->measured_req_flag) == REMOTE_REQ)) {
//				report_remote_latency(latency_info, prev_rem_req_i, wc, start);
//			 }
//		}
//	}
//
//	// Send remote requests
//	if (wr_i > 0) {
//		if (ENABLE_ASSERTIONS) {
//			if (rem_req_i > 0) assert(wr_i > 0);
//			assert(rem_req_i <= wr_i * MAX_COALESCE_PER_MACH);
//			assert(wr_i <= WORKERS_PER_MACHINE * WS_PER_WORKER * (MACHINE_NUM- 1));
//			assert(wr_i <= WINDOW_SIZE);
//		}
//		if (DEBUG_COALESCING && ENABLE_COALESCING)
//			debug_coalescing(rem_send_sgl, local_client_id, rem_req_i, wr_i);
////		yellow_printf("Sending %d messages \n", wr_i);
//		rem_send_wr[wr_i - 1].next = NULL;
//		ret = ibv_post_send(cb->dgram_qp[REMOTE_UD_QP_ID], &rem_send_wr[0], &bad_send_wr);
//		if (ENABLE_ASSERTIONS && ret != 0)
//			 printf("Batch size: %d, already outstanding %d  \n", wr_i, *outstanding_rem_reqs);
//		CPE(ret, "Sending Remotes: ibv_post_send error", ret);
//		// if (MEASURE_LATENCY == 1) rem_send_wr[last_measured_wr_i].imm_data = clt_gid;
//		w_stats[local_client_id].remote_messages_per_worker += wr_i;
////		if (ENABLE_WORKER_COALESCING == 0)
////			c_stats[local_client_id].remotes_per_client += wr_i;
//			*outstanding_rem_reqs += wr_i;
//		if (ENABLE_ASSERTIONS == 1) {
//			if (*outstanding_rem_reqs > MAX_OUTSTANDING_REQS) {
//				yellow_printf("Clt %d, wr_i: %d, outstanding_rem_reqs %d \n",
//				local_client_id, wr_i,	*outstanding_rem_reqs);
//				assert(0);
//			}
//		}
//		(*remote_tot_tx) += wr_i; // used for Selective Signaling -- DONT CHANGE!
//		w_stats[local_client_id].batches_per_worker++;
//
//	}
//	else if (ENABLE_STAT_COUNTING == 1) w_stats[local_client_id].wasted_loops++;
//}

//
///* ---------------------------------------------------------------------------
//------------------------------ EC SPECIFIC -----------------------------
//---------------------------------------------------------------------------*/
//
//// Poll coherence region for EC
//static inline void poll_coherence_EC(uint16_t* coh_i, struct ud_req* incoming_reqs, int* pull_ptr,
//			struct cache_op* update_ops, struct mica_resp* update_resp, int clt_gid, int local_client_id)
//{
//	while (*coh_i < BCAST_TO_CACHE_BATCH) {
//		// printf("CLIENT %d Opcode %d at offset %d at address %u \n", clt_gid, incoming_reqs[(*pull_ptr + 1) % EC_CLT_BUF_SLOTS].m_op.opcode,
//		// 			 (*pull_ptr + 1) % EC_CLT_BUF_SLOTS, &(incoming_reqs[(*pull_ptr + 1) % EC_CLT_BUF_SLOTS]));
//		if (incoming_reqs[((*pull_ptr) + 1) % EC_CLT_BUF_SLOTS].m_op.opcode != CACHE_OP_UPD) {
//			if (ENABLE_ASSERTIONS == 1) {
//				if (incoming_reqs[((*pull_ptr) + 1) % EC_CLT_BUF_SLOTS].m_op.opcode != 0) {
//					red_printf("Client %d: Position %d : Wrong Opcode  %d\n",clt_gid, ((*pull_ptr) + 1) % EC_CLT_BUF_SLOTS,
//						incoming_reqs[((*pull_ptr) + 1) % EC_CLT_BUF_SLOTS].m_op.opcode);
//					assert(false);
//				}
//			}
//			break;
//		}
//		HRD_MOD_ADD(*pull_ptr, EC_CLT_BUF_SLOTS);
//		memcpy(update_ops + (*coh_i), &(incoming_reqs[*pull_ptr].m_op), HERD_PUT_REQ_SIZE);
//		update_resp[*coh_i].type = ST_EMPTY;
//		// if (machine_id == 0); yellow_printf("CLIENT %d: I RECEIVED Broadcast %d at offset %d for key with tag  %d \n",
//		// 		clt_gid, w_stats[local_client_id].received_vals_per_worker,* pull_ptr, update_ops[*coh_i].key.tag);
//		incoming_reqs[*pull_ptr].m_op.opcode = 0;
//		(*coh_i)++;
//		w_stats[local_client_id].received_vals_per_worker++;
//	}
//}
//
//// Create credit messages for EC
//static inline uint16_t create_credits_EC(uint16_t coh_i, struct ibv_wc* coh_wc, uint8_t* broadcasts_seen, uint16_t local_client_id,
//																		struct ibv_send_wr* credit_send_wr, long long* credit_tx, struct ibv_cq* credit_send_cq )
//{
//	uint16_t i, credit_wr_i = 0;
//	struct ibv_wc signal_send_wc;
//	for (i = 0; i < coh_i; i++) {
//		if (ENABLE_ASSERTIONS) assert(i < EC_MAX_COH_RECEIVES);
//		uint16_t rm_id = coh_wc[i].imm_data;
//		if (ENABLE_ASSERTIONS) assert(rm_id < MACHINE_NUM);
//		broadcasts_seen[rm_id]++;
//		// yellow_printf("seen update from machine %d\n", rm_id);
//		// If we have seen enough broadcasts from a given machine send credits
//		if (broadcasts_seen[rm_id] == EC_CREDITS_IN_MESSAGE) {
//			uint16_t clt_i = rm_id * CLIENTS_PER_MACHINE + local_client_id;
//			credit_send_wr[credit_wr_i].wr.ud.ah = remote_clt_qp[clt_i][FC_UD_QP_ID].ah;
//			credit_send_wr[credit_wr_i].wr.ud.remote_qpn = remote_clt_qp[clt_i][FC_UD_QP_ID].qpn;
//			if (credit_wr_i > 0) credit_send_wr[credit_wr_i - 1].next = &credit_send_wr[credit_wr_i];
//			if ((*credit_tx) % EC_CREDIT_SS_BATCH == 0) {
//				credit_send_wr[credit_wr_i].send_flags |= IBV_SEND_SIGNALED;
//			}
//			else credit_send_wr[credit_wr_i].send_flags = IBV_SEND_INLINE;
//			if((*credit_tx) % EC_CREDIT_SS_BATCH == EC_CREDIT_SS_BATCH - 1) {
//				// printf("polling credits send completion\n");
//				hrd_poll_cq(credit_send_cq, 1, &signal_send_wc);
//				// printf("polled\n");
//			}
//			broadcasts_seen[rm_id] = 0;
//			credit_wr_i++;
//			(*credit_tx)++;
//		}
//	}
//	return credit_wr_i;
//}
//
///* Post Receives for coherence and then Send out the credits
//							---------DEPRICATED------------
//	A generic version is used  for both EC and SC */
//static inline void send_credits_EC(uint16_t credit_wr_i, struct ibv_sge* coh_recv_sgl, struct hrd_ctrl_blk* cb,
//																	int* push_ptr, struct ibv_recv_wr* coh_recv_wr, struct ibv_qp* coh_recv_qp,
//																	struct ibv_send_wr* credit_send_wr)
//{
//	// For every credit message we send back, prepare x receives for the broadcasts
//	uint16_t i, j, recv_iter = 0;
//	struct ibv_recv_wr *bad_recv_wr;
//	struct ibv_send_wr *bad_send_wr;
//	for(i = 0; i < credit_wr_i; i++) {
//		for (j = 0; j < EC_CREDITS_IN_MESSAGE; j++) {
//			recv_iter = i * EC_CREDITS_IN_MESSAGE + j;
//			coh_recv_sgl[recv_iter].addr = (uintptr_t) &cb->dgram_buf[*push_ptr * UD_REQ_SIZE];
//			HRD_MOD_ADD(*push_ptr, EC_CLT_BUF_SLOTS + 1);
//			if (*push_ptr == 0) *push_ptr = 1;
//			coh_recv_wr[recv_iter].next = (recv_iter == (credit_wr_i * EC_CREDITS_IN_MESSAGE) - 1) ?
//										  NULL : &coh_recv_wr[recv_iter + 1];
//		}
//	}
//	int ret = ibv_post_recv(coh_recv_qp, &coh_recv_wr[0], &bad_recv_wr);
//	CPE(ret, "ibv_post_recv error", ret);
//
//	credit_send_wr[credit_wr_i - 1].next = NULL;
//	// yellow_printf("I am sending %d credit message(s)\n", credit_wr_i);
//	ret = ibv_post_send(cb->dgram_qp[FC_UD_QP_ID], &credit_send_wr[0], &bad_send_wr);
//	CPE(ret, "ibv_post_send error in credits", ret);
//}
//
//// Form Broadcast work requests specifically for EC
//static inline void forge_bcast_wrs_EC(uint16_t op_i, struct extended_cache_op* ops, struct hrd_ctrl_blk *cb,
//                                      struct ibv_sge *coh_send_sgl, struct ibv_send_wr *coh_send_wr,
//                                      struct mica_op *coh_buf, uint16_t *coh_buf_i, long long *br_tx,
//                                      uint16_t local_client_id, uint16_t br_i)
//{
//  struct ibv_wc signal_send_wc;
//  w_stats[local_client_id].issued_vals_per_worker++;
//  // Do a Signaled Send every BROADCAST_SS_BATCH broadcasts (BROADCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
//  if ((*br_tx) % BROADCAST_SS_BATCH == 0) {
//    coh_send_wr[0].send_flags |= IBV_SEND_SIGNALED;
//  }
//  (*br_tx)++;
//  if((*br_tx) % BROADCAST_SS_BATCH == BROADCAST_SS_BATCH - 1) {
//    // printf("polling for broadcast send completions\n" );
//    hrd_poll_cq(cb->dgram_send_cq[BROADCAST_UD_QP_ID], 1, &signal_send_wc);
//    // printf("Client %d polling br_tx = %llu\n", clt_gid, (*br_tx) );
//  }
//  ops[op_i].opcode = CACHE_OP_UPD;
//  if (CLIENT_ENABLE_INLINING == 1)	coh_send_sgl[br_i].addr = (uint64_t) (uintptr_t) (ops + op_i);
//  if (CLIENT_ENABLE_INLINING == 0) {
//    if (ENABLE_ASSERTIONS == 1) assert((*coh_buf_i)< COH_BUF_SLOTS);
//    memcpy(coh_buf + (*coh_buf_i), ops + op_i, HERD_PUT_REQ_SIZE);
//    coh_send_sgl[br_i].addr = (uint64_t) (uintptr_t) (coh_buf + (*coh_buf_i));
//    MOD_ADD_WITH_BASE((*coh_buf_i), COH_BUF_SLOTS, 0);
//  } // when not inlining ops cannot be used, as the pointer will shift in the next iteration
//  // if (machine_id == 1); green_printf("CLIENT %d : I send Broadcast, opcode: %d credits: %d, (*br_tx) %llu, tag %d \n",
//  // 		clt_gid, ops[op_i].opcode, credits[EC_UPD_VC][(machine_id + 1) % MACHINE_NUM], (*br_tx), ops[op_i].key.tag);
//  if (br_i > 0)
//    coh_send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = &coh_send_wr[br_i * MESSAGES_IN_BCAST];
//}
//
//// Perform the EC broadcasts
//static inline void perform_broadcasts_EC(struct extended_cache_op* ops, uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb, struct ibv_wc* credit_wc,
//													uint32_t* credit_debug_cnt, struct ibv_sge* coh_send_sgl, struct ibv_send_wr* coh_send_wr,
//													struct mica_op* coh_buf, uint16_t* coh_buf_i, long long* br_tx, struct ibv_recv_wr* credit_recv_wr,
//													 uint16_t local_client_id, int protocol)
//{
//	uint16_t i, j, op_i = 0, br_i = 0, credit_recv_counter = 0;
//	struct ibv_wc signal_send_wc;
//	int ret;
//	struct ibv_send_wr *bad_send_wr;
//	struct ibv_recv_wr *bad_recv_wr;
//	while (op_i < CACHE_BATCH_SIZE) { // traverse all of the ops, since we cannot know where the last hot write will be
//		//if (br_i >= MAX_BCAST_BATCH) break;
//		if (ops[op_i].opcode != CACHE_OP_BRC) {
//			op_i++;
//			continue; // We only care about hot writes
//		}
//		if (!check_broadcast_credits(credits, cb, credit_wc, credit_debug_cnt, EC_UPD_VC, protocol)) {
//			break;
//		}
//    forge_bcast_wrs_EC(op_i, ops, cb, coh_send_sgl, coh_send_wr, coh_buf, coh_buf_i, br_tx, local_client_id, br_i);
//
//    for (j = 0; j < MACHINE_NUM; j++) { credits[EC_UPD_VC][j]--; }
//		br_i++;
//		op_i++;
//		if ((*br_tx) % EC_CREDITS_IN_MESSAGE == 0) credit_recv_counter++;
//		if (br_i == MAX_BCAST_BATCH) {
//			post_credit_recvs_and_batch_bcasts_to_NIC(br_i, cb, coh_send_wr, credit_recv_wr, &credit_recv_counter, protocol);
//			br_i = 0;
//		}
//	}
//	post_credit_recvs_and_batch_bcasts_to_NIC(br_i, cb, coh_send_wr, credit_recv_wr, &credit_recv_counter, protocol);
//}

/* ---------------------------------------------------------------------------
------------------------------ SC SPECIFIC -----------------------------
---------------------------------------------------------------------------*/
// Poll coherence region for SC
static inline void poll_coherence_SC(uint16_t* update_ops_i, struct ud_req* incoming_reqs, int* pull_ptr,
			struct cache_op* update_ops, int clt_gid, int local_client_id, uint16_t ack_size, uint16_t* polled_messages,
			uint16_t ack_ops_i, uint16_t inv_size, uint16_t* inv_ops_i, struct small_cache_op* inv_ops)
{
	while (*polled_messages < BCAST_TO_CACHE_BATCH) {
			// printf("CLIENT %d Opcode %d at offset %d at address %u \n", clt_gid, incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS].m_op.opcode,
			// 			(*pull_ptr + 1) % SC_CLT_BUF_SLOTS, &(incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS]));
			if (incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS].m_op.opcode != CACHE_OP_UPD &&
					incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS].m_op.opcode != CACHE_OP_ACK &&
					incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS].m_op.opcode != CACHE_OP_INV) {
				if (ENABLE_ASSERTIONS == 1) {
					if (incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS].m_op.opcode != 0) {
						red_printf("Client %d Opcode seen %d\n",clt_gid,
							incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS].m_op.opcode);
						assert(false);
					}
				}
				break;
			}
			// firtsly deal with updates and acks
			if (incoming_reqs[(*pull_ptr + 1) % SC_CLT_BUF_SLOTS].m_op.opcode != CACHE_OP_INV) {
				if (ack_size + ack_ops_i == BCAST_TO_CACHE_BATCH) break; // Back pressure from the incoming acks
				HRD_MOD_ADD(*pull_ptr, SC_CLT_BUF_SLOTS);
				uint8_t size_of_req = incoming_reqs[*pull_ptr].m_op.opcode == CACHE_OP_UPD ? HERD_PUT_REQ_SIZE : HERD_GET_REQ_SIZE;
				memcpy(update_ops + *update_ops_i, &(incoming_reqs[*pull_ptr].m_op), size_of_req);
                  assert(update_ops[*update_ops_i].key.meta.cid < MACHINE_NUM);///
//				 yellow_printf("CLIENT %d I see a message %s for key %d with version %d\n", local_client_id, code_to_str(incoming_reqs[*pull_ptr].m_op.opcode),
//				 	update_ops[*update_ops_i].key.tag , update_ops[*update_ops_i].key.meta.version);
			  (*update_ops_i)++;
				if (incoming_reqs[*pull_ptr].m_op.opcode == CACHE_OP_ACK) {
					// printf("Actually receiving an ack \n");
					ack_ops_i++;
					if (ENABLE_STAT_COUNTING == 1) w_stats[local_client_id].received_acks_per_client++;
				}
				else if (ENABLE_STAT_COUNTING == 1) {
					w_stats[local_client_id].received_updates_per_client++;
					// printf("Actually receiving an upd \n");
				}
			}
			else { // invalidations get special treatment because they must not go to the cahce just yet
				if (inv_size + *inv_ops_i == BCAST_TO_CACHE_BATCH) break; // Back pressure if no empty slots
				HRD_MOD_ADD(*pull_ptr, SC_CLT_BUF_SLOTS);
				memcpy(inv_ops + *inv_ops_i, &(incoming_reqs[*pull_ptr].m_op), HERD_GET_REQ_SIZE);
				assert(inv_ops[*inv_ops_i].key.meta.cid < MACHINE_NUM);///
//        yellow_printf("CLIENT %d I see a message %s for key %d with version %d, inv_ops_i %d\n", local_client_id,
//                      code_to_str(incoming_reqs[*pull_ptr].m_op.opcode), 	inv_ops[*inv_ops_i].key.tag,
//                                  inv_ops[*inv_ops_i].key.meta.version, (*inv_ops_i));
				(*inv_ops_i)++;
				if (ENABLE_STAT_COUNTING == 1) w_stats[local_client_id].received_invs_per_client++;
			}
			if (ENABLE_ASSERTIONS == 1) assert(*pull_ptr >= 0);

			incoming_reqs[*pull_ptr].m_op.opcode = 0;
			(*polled_messages)++;
		} // while
}


// Wake-up logic
static inline void wake_up_stalled(uint16_t index, struct mica_resp* update_resp, struct cache_op* update_ops,
															struct extended_cache_op* ops, struct mica_resp* resp)
{
	uint16_t j;
	if (update_resp[index].type == CACHE_UPD_SUCCESS) {
		for (j = 0; j < CACHE_BATCH_SIZE; j++) {
			if (keys_are_equal(&update_ops[index].key, &ops[j].key) &&
				(resp[j].type == CACHE_GET_STALL || resp[j].type == CACHE_PUT_STALL)) {
				resp[j].type = RETRY;
				//printf("Wake up someone\n" );
			}
		}
	}
}

// Count the coherence latency of a hot write
static inline void report_latency_of_hot_write(uint16_t index, struct cache_op* update_ops, struct latency_flags* latency_info,
															struct extended_cache_op* ops, struct timespec* start, uint16_t local_client_id)
{
	if (keys_and_meta_are_equal(&update_ops[index].key, latency_info->key_to_measure)) {
		struct timespec end;
		clock_gettime(CLOCK_MONOTONIC, &end);
		int useconds = ((end.tv_sec - start->tv_sec) * 1000000) +
									 ((end.tv_nsec - start->tv_nsec) / 1000);
		if (ENABLE_ASSERTIONS) assert(useconds > 0);
		bookkeep_latency(useconds, HOT_WRITE_REQ);
//		printf("Client %d Latency of a hot write %d us\n", local_client_id, useconds);
		latency_info->measured_req_flag = NO_REQ;
	}
		// This works like a lease mechanism, to find out whether the write to be measured has actually been cacnelled
	else if (keys_are_equal(&update_ops[index].key, latency_info->key_to_measure)) {
		if (update_ops[index].key.meta.version > latency_info->key_to_measure->meta.version) {
			latency_info->measured_req_flag = NO_REQ;
		}
	}
}

// Bookkeeping for received updates and acknowledgemts, optionalL wake-up logic and latency measurement
static inline void updates_and_acks_bookkeeping(uint16_t update_ops_i, struct cache_op* update_ops, struct latency_flags* latency_info,
															struct extended_cache_op* ops, struct timespec* start,
															uint16_t local_client_id, struct mica_resp* resp,
															struct mica_resp* update_resp, uint16_t coh_message_count[][MACHINE_NUM], struct cache_op* ack_bcast_ops,
															uint16_t* ack_push_ptr, uint16_t* ack_size)
{
	uint16_t i;
	for (i = 0; i < update_ops_i; i++) {
		// printf("The %s refers to key %d with cid %d and version %d\n", code_to_str(update_ops[i].opcode),
		//  			update_ops[i].key.tag, update_ops[i].key.meta.cid, update_ops[i].key.meta.version);
		if (update_ops[i].opcode == CACHE_OP_UPD) {
			coh_message_count[UPD_VC][update_ops[i].key.meta.cid]++;
		// WAKE UP
		if (ENABLE_WAKE_UP == 1)  wake_up_stalled(i, update_resp, update_ops, ops, resp);
		}
		else if (update_resp[i].type == CACHE_LAST_ACK_SUCCESS) {
			// wake-up logic
			if (ENABLE_WAKE_UP == 1) wake_up_stalled(i, update_resp, update_ops, ops, resp);
			if (MEASURE_LATENCY == 1 && (latency_info->measured_req_flag == HOT_WRITE_REQ))
				report_latency_of_hot_write(i, update_ops, latency_info, ops, start, local_client_id);
			// this assertion means that the queue is full because the update
			// at the push pointer has not been sent yet
			if (ENABLE_ASSERTIONS == 1) assert(ack_bcast_ops[*ack_push_ptr].opcode != CACHE_OP_ACK);
			ack_bcast_ops[*ack_push_ptr] = update_ops[i];
			ack_bcast_ops[*ack_push_ptr].val_len = (HERD_VALUE_SIZE >> SHIFT_BITS);
			HRD_MOD_ADD(*ack_push_ptr, BCAST_TO_CACHE_BATCH);
			(*ack_size)++;
			if (unlikely(*ack_size > BCAST_TO_CACHE_BATCH)) printf("Error: the ack_size is bigger than allowed %d\n", *ack_size);
		}
		else coh_message_count[ACK_VC][update_ops[i].key.meta.cid]++; // ack was not final, buffer space has emptied
	}
}

// Find out which invalidations were successful and set up a buffer to be used to send the acks
static inline void invs_bookkeeping(uint16_t inv_ops_i, struct mica_resp* inv_resp, uint16_t coh_message_count[][MACHINE_NUM],
												struct small_cache_op* inv_ops, struct small_cache_op* inv_to_send_ops, uint16_t* inv_push_ptr,
																		uint16_t* inv_size, uint16_t* debug_ptr)
{
	uint16_t j, i;
	for (j = 0; j < inv_ops_i; j++) {
		// printf("response: %s to the Invalidation refers to key %d with cid %d and version %d\n", code_to_str(inv_resp[j].type),
		// 			inv_ops[j].key.tag, inv_ops[j].key.meta.cid, inv_ops[j].key.meta.version);
		if (inv_resp[j].type != CACHE_INV_SUCCESS) {
			coh_message_count[INV_VC][inv_ops[j].key.meta.cid]++;
			continue;
		}
		i = 0;
		do { // Look for an empty slot in the inv_to_send_ops buffer
			if (inv_to_send_ops[(*inv_push_ptr + i) % BCAST_TO_CACHE_BATCH].opcode != CACHE_OP_BRC) {
				*inv_push_ptr = (*inv_push_ptr + i) % BCAST_TO_CACHE_BATCH; // the use of the inv_push_ptr is not mandatory but only for performance
				break;
			}
			else i++;
		} while (i < BCAST_TO_CACHE_BATCH);
		if (ENABLE_ASSERTIONS) {
			if (unlikely(i == BCAST_TO_CACHE_BATCH)) {
				printf("Something is wrong with the invalidation back pressure\n"); // Back pressure if no empty slots
				printf("Inv size %d, looking at %d/%d\n", *inv_size, j, inv_ops_i);
			}
		}
		memcpy(inv_to_send_ops + (*inv_push_ptr), &(inv_ops[j]), HERD_GET_REQ_SIZE);
		if (ENABLE_ASSERTIONS == 1) {
				assert(inv_to_send_ops[*inv_push_ptr].opcode != CACHE_OP_BRC);
				assert(inv_to_send_ops[*inv_push_ptr].key.meta.cid <= MACHINE_NUM);
		}
		inv_to_send_ops[*inv_push_ptr].opcode = CACHE_OP_BRC;
		(*inv_size)++;
	}
}

// Form Broadcast work requests specifically for SC
static inline void forge_bcast_wrs_SC(uint16_t op_i, struct extended_cache_op* ops, struct cache_op* ack_bcast_ops,
                                      uint16_t* ack_pop_ptr, struct hrd_ctrl_blk* cb, struct ibv_sge* coh_send_sgl,
                                      struct ibv_send_wr* coh_send_wr, uint16_t coh_message_count[][MACHINE_NUM],
                                      struct mica_op* coh_buf, uint16_t* coh_buf_i, long long* br_tx, uint16_t local_client_id,
                                      uint16_t* updates_sent, uint16_t br_i, uint8_t credits[][MACHINE_NUM], uint8_t vc)
{
	uint16_t i;
	struct ibv_wc signal_send_wc;
	if (op_i < CACHE_BATCH_SIZE) {
		ops[op_i].opcode = CACHE_OP_INV;
		ops[op_i].key.meta.cid = (uint8_t) machine_id;
		coh_send_sgl[br_i].addr = (uint64_t) (uintptr_t) (ops + op_i);
		coh_send_sgl[br_i].length = HERD_GET_REQ_SIZE;
		w_stats[local_client_id].invs_per_client++;
		if (CLIENT_ENABLE_INLINING == 0) {
			for (i = 0; i < MESSAGES_IN_BCAST; i++)
				coh_send_wr[(br_i * MESSAGES_IN_BCAST) + i].send_flags = IBV_SEND_INLINE;// we should inline invalidations
		}
//		 green_printf("CLIENT %d : I BROADCAST with %s credits: %d, key: %d, cid: %d and version: %d \n", local_client_id,
//                  code_to_str(ops[op_i].opcode), credits[vc][(machine_id + 1) % MACHINE_NUM],
//                  ops[op_i].key.tag, ops[op_i].key.meta.cid, 	ops[op_i].key.meta.version);
	}
	else { // create the Update
		coh_message_count[ACK_VC][ack_bcast_ops[*ack_pop_ptr].key.meta.cid]++; // we are emptying that buffer space
		ack_bcast_ops[*ack_pop_ptr].key.meta.cid = (uint8_t) machine_id;
		if (CLIENT_ENABLE_INLINING == 1)	coh_send_sgl[br_i].addr = (uint64_t) (uintptr_t) (ack_bcast_ops + (*ack_pop_ptr));
		if (CLIENT_ENABLE_INLINING == 0) {
			if (ENABLE_ASSERTIONS == 1) assert(*coh_buf_i < COH_BUF_SLOTS);
			memcpy(coh_buf + (*coh_buf_i), ack_bcast_ops + (*ack_pop_ptr), HERD_PUT_REQ_SIZE);
			coh_send_sgl[br_i].addr = (uint64_t) (uintptr_t) (coh_buf + (*coh_buf_i));
			MOD_ADD_WITH_BASE(*coh_buf_i, COH_BUF_SLOTS, 0);
			for (i = 0; i < MESSAGES_IN_BCAST; i++)
				coh_send_wr[(br_i * MESSAGES_IN_BCAST) + i].send_flags = 0;
		}
		coh_send_sgl[br_i].length = HERD_PUT_REQ_SIZE;
		(*updates_sent)++;
		w_stats[local_client_id].updates_per_client++;
//      green_printf("CLIENT %d : I BROADCAST with %s credits: %d, key: %d, cid: %d and version: %d \n", local_client_id, code_to_str(ack_bcast_ops[*ack_pop_ptr].opcode), credits[vc][(machine_id + 1) % MACHINE_NUM],
//                   ack_bcast_ops[*ack_pop_ptr].key.tag, ack_bcast_ops[*ack_pop_ptr].key.meta.cid, 	ack_bcast_ops[*ack_pop_ptr].key.meta.version);
		HRD_MOD_ADD(*ack_pop_ptr, BCAST_TO_CACHE_BATCH);
	}
	// Do a Signaled Send every BROADCAST_SS_BATCH broadcasts (BROADCAST_SS_BATCH * (MACHINE_NUM - 1) messages)
	if ((*br_tx) % BROADCAST_SS_BATCH == 0) coh_send_wr[0].send_flags |= IBV_SEND_SIGNALED;
	(*br_tx)++;
	if((*br_tx) % BROADCAST_SS_BATCH == BROADCAST_SS_BATCH - 1) {
		hrd_poll_cq(cb->dgram_send_cq[BROADCAST_UD_QP_ID], 1, &signal_send_wc);
	}
	// SET THE LAST COH_SEND_WR TO POINT TO NULL
	if (br_i > 0)
		coh_send_wr[(br_i * MESSAGES_IN_BCAST) - 1].next = &coh_send_wr[br_i * MESSAGES_IN_BCAST];
}

// Perform the SC broadcasts
static inline void perform_broadcasts_SC(uint16_t* ack_size, struct extended_cache_op* ops, struct cache_op* ack_bcast_ops,
													uint16_t* ack_pop_ptr, uint8_t credits[][MACHINE_NUM], struct hrd_ctrl_blk* cb, struct ibv_wc* credit_wc,
													uint32_t* credit_debug_cnt, struct ibv_sge* coh_send_sgl, struct ibv_send_wr* coh_send_wr,
													uint16_t coh_message_count[][MACHINE_NUM], struct mica_op* coh_buf, uint16_t* coh_buf_i,
													long long* br_tx, struct ibv_recv_wr* credit_recv_wr, uint16_t local_client_id, int protocol)
{
	uint8_t vc;
	uint16_t updates_sent = 0, op_i = 0, br_i = 0, j, credit_recv_counter = 0;
	while (op_i < CACHE_BATCH_SIZE + (*ack_size)) {
		// traverse all of the ops for the hot writes and all update_ops for completed acks
		if (op_i < CACHE_BATCH_SIZE) { // invs for hot writes
			if (ops[op_i].opcode != CACHE_OP_BRC) {
				op_i++;
				continue;
			}
			vc = INV_VC;
		}
		else {
			ack_bcast_ops[*ack_pop_ptr].opcode = CACHE_OP_UPD;
			vc = UPD_VC;
		}
		// Check if there are enough credits for a Broadcast
		if (!check_broadcast_credits(credits, cb, credit_wc, credit_debug_cnt, vc, protocol)) {
			if (op_i >= CACHE_BATCH_SIZE) break;
			else { op_i = CACHE_BATCH_SIZE; continue;} // if there are no inv credits go on to the upds
		}
		// Create the broadcast messages
		forge_bcast_wrs_SC(op_i, ops, ack_bcast_ops, ack_pop_ptr, cb, coh_send_sgl, coh_send_wr,
                       coh_message_count, coh_buf, coh_buf_i, br_tx, local_client_id, &updates_sent, br_i,
                       credits, vc);
		for (j = 0; j < MACHINE_NUM; j++) { credits[vc][j]--; }
		if ((*br_tx) % CRDS_IN_MESSAGE == 0) credit_recv_counter++;
		br_i++;
		op_i++;
		if (br_i == MAX_BCAST_BATCH) {
			post_credit_recvs_and_batch_bcasts_to_NIC(br_i, cb, coh_send_wr, credit_recv_wr, &credit_recv_counter, protocol);
			br_i = 0;
		}
	}

	struct ibv_send_wr *bad_send_wr;
	ibv_post_send(cb->dgram_qp[BROADCAST_UD_QP_ID], &coh_send_wr[0], &bad_send_wr);
    exit(1);
	post_credit_recvs_and_batch_bcasts_to_NIC(br_i, cb, coh_send_wr, credit_recv_wr, &credit_recv_counter, protocol);
	(*ack_size) -= updates_sent;
	if (ENABLE_ASSERTIONS == 1) assert(*ack_size >= 0);
}

// Pick up the buffer with invalidations that succeeded and respond to them with acks
static inline void send_acks(struct small_cache_op* inv_to_send_ops, uint8_t credits[][MACHINE_NUM], struct ibv_wc* credit_wc,
															struct ibv_send_wr* ack_wr, struct ibv_sge* ack_sgl, long long* sent_ack_tx, uint16_t* inv_size,
															uint16_t *ack_recv_counter, struct ibv_recv_wr* credit_recv_wr, uint16_t local_client_id,
															uint16_t coh_message_count[][MACHINE_NUM], struct hrd_ctrl_blk* cb, uint16_t debug_ptr)
{
	uint16_t i, send_ack_count = 0, remote_clt_id;
	struct ibv_wc signal_send_wc;
	for (i = 0; i < BCAST_TO_CACHE_BATCH; i++) {
		if (inv_to_send_ops[i].opcode != CACHE_OP_BRC) {
			continue;
		}
		if (credits[ACK_VC][inv_to_send_ops[i].key.meta.cid] == 0)
			poll_credits_SC(cb->dgram_recv_cq[FC_UD_QP_ID], credit_wc, credits);
		if (credits[ACK_VC][inv_to_send_ops[i].key.meta.cid] == 0) continue;
//      green_printf(" Sending an ack for key %d with version %d \n",
//			   inv_to_send_ops[i].key.tag, inv_to_send_ops[i].key.meta.version);
		credits[ACK_VC][inv_to_send_ops[i].key.meta.cid]--;
		remote_clt_id = inv_to_send_ops[i].key.meta.cid * CLIENTS_PER_MACHINE + local_client_id;
		coh_message_count[INV_VC][inv_to_send_ops[i].key.meta.cid]++; // we are emptying that buffer space
//		printf("inv %d/%d counter %d,  cid: %d, inv_size %d, credits %d, debug_ptr %d \n", i, BCAST_TO_CACHE_BATCH,
//					 coh_message_count[INV_VC][inv_to_send_ops[i].key.meta.cid], inv_to_send_ops[i].key.meta.cid,
//					 (*inv_size), credits[ACK_VC][inv_to_send_ops[i].key.meta.cid], debug_ptr);

		ack_wr[send_ack_count].wr.ud.ah = remote_clt_qp[remote_clt_id][BROADCAST_UD_QP_ID].ah;
		ack_wr[send_ack_count].wr.ud.remote_qpn = remote_clt_qp[remote_clt_id][BROADCAST_UD_QP_ID].qpn;
		inv_to_send_ops[i].opcode = CACHE_OP_ACK;
		inv_to_send_ops[i].key.meta.cid = machine_id;
		ack_sgl[send_ack_count].addr = (uint64_t) (uintptr_t) (inv_to_send_ops + i);
		if (send_ack_count > 0) ack_wr[send_ack_count - 1].next = &ack_wr[send_ack_count];
		ack_wr[send_ack_count].send_flags = IBV_SEND_INLINE;
		if ((*sent_ack_tx) % ACK_SS_GRANULARITY == 0) {
			ack_wr[send_ack_count].send_flags |= IBV_SEND_SIGNALED;
			// if (local_client_id == 0) green_printf("Sending ack %llu signaled \n", *sent_ack_tx);
		}
		if((*sent_ack_tx) % ACK_SS_GRANULARITY == ACK_SS_GRANULARITY - 1) {
			// if (local_client_id == 0) green_printf("Polling for ack  %llu \n", *sent_ack_tx);
			hrd_poll_cq(cb->dgram_send_cq[BROADCAST_UD_QP_ID], 1, &signal_send_wc);
		}
		send_ack_count++;
		(*inv_size)--;

		(*sent_ack_tx)++; // Selective signaling
		w_stats[local_client_id].acks_per_client++;
		// Post a receive for a credit every CRDS_IN_MESSAGE acks
		if ((*sent_ack_tx) % CRDS_IN_MESSAGE == 0) (*ack_recv_counter)++;
	}
	// post receives for credits and then send the acks
	post_recvs_and_send(*ack_recv_counter, send_ack_count, ack_wr, credit_recv_wr,
				cb->dgram_qp[BROADCAST_UD_QP_ID],cb->dgram_qp[FC_UD_QP_ID], MAX_CREDIT_RECVS,
				BCAST_TO_CACHE_BATCH);
	if (*ack_recv_counter > 0) *ack_recv_counter = 0;
}

/* Check the buffer space that has been emptied by the previous steps and forge the appropriate credits
   Buffer space empties on: propagating updates, non-final acks, failed invs, sending acks, sending updates */
static inline uint16_t forge_credits_SC(uint16_t coh_message_count[][MACHINE_NUM], uint16_t* acks_seen, uint16_t* invs_seen,
 															uint16_t* upds_seen, uint16_t local_client_id, struct ibv_send_wr* credit_wr, long long* credit_tx,
															struct hrd_ctrl_blk* cb,	struct ibv_cq* coh_recv_cq,	struct ibv_wc* coh_wc)
 {
	 // Send out credits for the polled requests
		uint16_t total_fc_message_count = 0;
		uint16_t credit_wr_i = 0, rm_id, i, j;
		struct ibv_wc signal_send_wc;
		for (rm_id = 0; rm_id < MACHINE_NUM; rm_id++) {
			uint16_t fc_message_count = coh_message_count[ACK_VC][rm_id] + coh_message_count[INV_VC][rm_id] + coh_message_count[UPD_VC][rm_id];
			if (fc_message_count > 0) {
				total_fc_message_count += fc_message_count;
				for (i = 0; i < fc_message_count; i++) {
					// if (i < coh_message_count[ACK_VC][rm_id]) printf("I received an ack from machine %d\n", rm_id);
					// else if (i < coh_message_count[INV_VC][rm_id] + coh_message_count[ACK_VC][rm_id]) printf("I received an inv from machine %d\n", rm_id);
					// else printf("I received a upd from machine %d\n", rm_id);
					//rm_id = wc[i].imm_data;

					if (i < coh_message_count[ACK_VC][rm_id]) acks_seen[rm_id]++;
					else if (i < coh_message_count[INV_VC][rm_id] + coh_message_count[ACK_VC][rm_id]) invs_seen[rm_id]++;
					else upds_seen[rm_id]++;
					// If we have seen enough broadcasts from a given machine send credits
					if (acks_seen[rm_id] == CRDS_IN_MESSAGE || invs_seen[rm_id] == CRDS_IN_MESSAGE || upds_seen[rm_id] == CRDS_IN_MESSAGE) {
						uint16_t clt_i = rm_id * CLIENTS_PER_MACHINE + local_client_id;
						credit_wr[credit_wr_i].wr.ud.ah = remote_clt_qp[clt_i][FC_UD_QP_ID].ah;
						credit_wr[credit_wr_i].wr.ud.remote_qpn = remote_clt_qp[clt_i][FC_UD_QP_ID].qpn;
						// CONVENTION to denote where the credit refers to, acks have the machine _ids from 0 to MACHINE_NUM - 1
						if (i < coh_message_count[ACK_VC][rm_id] ) { //ack
							credit_wr[credit_wr_i].imm_data = machine_id;
							acks_seen[rm_id] = 0;
//                            cyan_printf("sending an ack credit to %d\n", rm_id);
						}
						else if (i < coh_message_count[INV_VC][rm_id] + coh_message_count[ACK_VC][rm_id]) { //inv
							credit_wr[credit_wr_i].imm_data = machine_id + MACHINE_NUM;
							invs_seen[rm_id] = 0;
//                            cyan_printf("sending an inv credit to %d\n", rm_id);
						}
						else { //upd
							credit_wr[credit_wr_i].imm_data = machine_id + (2 * MACHINE_NUM);
							upds_seen[rm_id] = 0;
//                            cyan_printf("sending an upd credit to %d\n", rm_id);
						}

						if (credit_wr_i > 0) credit_wr[credit_wr_i - 1].next = &credit_wr[credit_wr_i];
						if (((*credit_tx) % CREDIT_SS_BATCH) == 0) {
							credit_wr[credit_wr_i].send_flags |= IBV_SEND_SIGNALED;
						}
						else credit_wr[credit_wr_i].send_flags = IBV_SEND_INLINE;
						if(((*credit_tx) % CREDIT_SS_BATCH) == CREDIT_SS_BATCH - 1) {
							hrd_poll_cq(cb->dgram_send_cq[FC_UD_QP_ID], 1, &signal_send_wc);
						}
						credit_wr_i++;
						(*credit_tx)++;
						if (ENABLE_ASSERTIONS == 1) assert(credit_wr_i <= CREDIT_SS_BATCH_);
					}
				}
			}
		}
		hrd_poll_cq(coh_recv_cq, total_fc_message_count, coh_wc);
		return credit_wr_i;
 }

 // Track stalling and print information on stalled Ops for SC
static inline void debug_stalling_SC(uint16_t stalled_ops_i, uint32_t* stalled_counter, uint8_t* stalled,
 																			uint16_t local_client_id)
 {
 	uint16_t i;
 	if (stalled_ops_i == CACHE_BATCH_SIZE && (*stalled) == 0) {
 		(*stalled) = 1; //printf("Things are stalled\n");
 		(*stalled_counter) = 1;
 		// for (i = 0; i < CACHE_BATCH_SIZE; i++)
 		// 	printf("Op %d : %s Waiting on key %d that is on state %d\n", i, code_to_str(ops[i].opcode), ops[i].key.tag, ops[i].key.meta.state );
 		//printf("Ack size: %d, inv size %d, credits %d\n", ack_size , inv_size, credits[(machine_id + 1) % MACHINE_NUM]);
 	}
 	if (stalled_ops_i < (CACHE_BATCH_SIZE / 2) && (*stalled) == 1) {
 		(*stalled_counter) = 0;
 		(*stalled) = 0;//printf("Things are not stalled anymore\n");
 	}
 	if ((*stalled_counter) > 0) (*stalled_counter)++;
 	if ((*stalled_counter) % 1000000 == 0 && (*stalled_counter) > 0) {
 		// for (i = 0; i < CACHE_BATCH_SIZE; i++)
 		// 	printf("Op %d : %s Waiting on key %d that is on state %d\n", i, code_to_str(ops[i].opcode), ops[i].key.tag, ops[i].key.meta.state );
 		// printf("Ack size: %d, inv size %d, credits %d\n", ack_size , inv_size, credits[(machine_id + 1) % MACHINE_NUM]);
 		red_printf("Client %d is stalled", local_client_id);
 	}
 }

//
///* ---------------------------------------------------------------------------
//------------------------------ WORKER MAIN LOOP -----------------------------
//---------------------------------------------------------------------------*/
////poll and serve the local requests
//static inline void serve_local_reqs(uint16_t wrkr_lid, struct mica_kv* kv, struct mica_op **local_op_ptr_arr,
//																		struct mica_resp* local_responses)
//{
//	uint16_t clt_i, i;
//	for (clt_i = 0; clt_i < CLIENTS_PER_MACHINE; clt_i++) {
//		for (i = 0; i < LOCAL_REGIONS; i++) {
//			if (local_recv_flag[wrkr_lid][clt_i][i] == 0) continue; // find a client that has filled his region
//			uint16_t wrkr_offset = (clt_i * LOCAL_WINDOW) + (i * LOCAL_REGION_SIZE);
//			// green_printf("Worker %d reads locals from clt %d at local region %d, offset\n", wrkr_lid, clt_i,	i, wrkr_offset );
//			struct mica_resp *local_resp_arr = local_responses + wrkr_offset;
//			KVS_BATCH_OP(kv, LOCAL_REGION_SIZE, &local_op_ptr_arr[wrkr_offset], local_resp_arr);
//			if (ENABLE_ASSERTIONS == 1) assert(local_recv_flag[wrkr_lid][clt_i][i] == 1);
//			local_recv_flag[wrkr_lid][clt_i][i] = 0;
//			w_stats[wrkr_lid].locals_per_worker += LOCAL_REGION_SIZE;
//		}
//	}
//}
//
//static inline void worker_remote_request_error_checking(int pull_ptr, uint32_t per_qp_buf_slots, struct mica_op* next_req_ptr,
//                                                   uint16_t wrkr_lid, uint16_t qp_i, uint16_t wr_i, struct wrkr_ud_req* req,
//                                                   long long nb_tx_tot, int push_ptr, int clt_i, bool multiget, uint16_t get_i)
//{
//  if (ENABLE_ASSERTIONS == 1) {
//    assert(pull_ptr <= (int)per_qp_buf_slots);
//    assert(push_ptr <= (int)per_qp_buf_slots);
//    assert((next_req_ptr)->opcode == MICA_OP_GET ||	/* XXX */
//           next_req_ptr->opcode == MICA_OP_PUT);
//    if (next_req_ptr->opcode == MICA_OP_PUT) {
//      if (next_req_ptr->val_len != (HERD_VALUE_SIZE >> SHIFT_BITS)) {
//        yellow_printf("Wrker %d: qp_i %d, pull_ptr %d, wr_i %d, len %d, op %d, total reqs %llu\n",
//                      wrkr_lid, qp_i,	pull_ptr, wr_i, req->m_op.val_len,
//                      next_req_ptr->opcode, nb_tx_tot );
//        assert(false);
//      }
//    }
//  }
//  //printf("gets %d\n", req[pull_ptr].m_op.value[0]);
//
////  green_printf("Worker %d  sees a req with key bkt %u , at qp %d , wr_i %d, recv_per_qp %d, opcode %d  \n", wrkr_lid, req.m_op.key.bkt,
////                  qp_i, wr_i, per_recv_qp_wr_i, req.m_op.opcode);
//
//
//  if (ENABLE_ASSERTIONS == 1) {
//    if (!(clt_i >= 0 && clt_i < CLIENT_NUM)) {
//      printf("mutliget %d, clt_i %d, multiget %d get_i %d\n", multiget, clt_i, multiget, get_i);
//      assert(false);
//    }
//    assert(clt_i / CLIENTS_PER_MACHINE != machine_id);
//  }
//}
//
//// poll the remote request region and return a control flow directive
//static inline enum control_flow_directive poll_remote_region(uint16_t* qp_i, int* pull_ptr, uint32_t* per_qp_buf_slots, struct wrkr_ud_req* req,
//                    bool* multiget, uint16_t wr_i, uint16_t* received_messages, uint16_t* per_qp_received_messages,
//                    struct mica_op** next_req_ptr, uint16_t* get_i, uint16_t* get_num, uint8_t* requests_per_message)
//{
//  uint32_t index = (pull_ptr[(*qp_i)] + 1) % per_qp_buf_slots[(*qp_i)];
//  if (req[index].m_op.opcode != MICA_OP_GET && req[index].m_op.opcode != MICA_OP_PUT) {
//    if (ENABLE_COALESCING && req[index].m_op.opcode == MICA_OP_MULTI_GET) {
//      if ((*multiget) == false) { // if it's the first request
//        struct mica_op *tmp = (struct mica_op *) (req[index].m_op.value + (req[index].m_op.val_len - 2) * HERD_GET_REQ_SIZE);
//        if (tmp->opcode != MICA_OP_GET) return continue_;
//        (*get_num)  = req[index].m_op.val_len;
//        if (wr_i + (*get_num) > WORKER_MAX_BATCH) {
//          // printf("breaking %d\n", wrkr_lid);
//          return break_;
//        }
//        (*multiget)  = true;
//				requests_per_message[(*received_messages)] = *get_num;
//        (*get_i)  = 0;
//        req[index].m_op.opcode = MICA_OP_GET;// necessary to work with the MICA API
//        (*next_req_ptr) = (struct mica_op *) &(req[index].m_op);
//
//        (*received_messages)++;
//        per_qp_received_messages[(*qp_i)]++;
////        green_printf("I see a Multi Get of %d gets \n", (*get_num) );
//        if (ENABLE_ASSERTIONS && (*next_req_ptr)->opcode != MICA_OP_GET) {
//          red_printf("req opcode %d, next_rq_ptr opcode %d\n",req[index].m_op.opcode,  (*next_req_ptr)->opcode);
//        }
//      }
//      else if (ENABLE_ASSERTIONS) assert(false);
//    }
//    else { // we need to check the rest of the buffers
//      if (ENABLE_ASSERTIONS)
//        if(req[index].m_op.opcode != 0) {
//          red_printf("Wrong Opcode %d \n", req[index].m_op.opcode);
//          assert(false);
//        }
//      if ((*qp_i)  == WORKER_NUM_UD_QPS -1) return break_; // all qp buffers were checked
//      else {
//        HRD_MOD_ADD((*qp_i) , WORKER_NUM_UD_QPS);
//        return continue_; // move to the next buffer
//      }
//    }
//  }
//  return no_cf_change;
//}
//
////do the required bookkeeping for a request
//static inline void request_bookkeeping(bool multiget, int* pull_ptr, uint16_t qp_i, uint32_t* per_qp_buf_slots,
//                                       struct mica_op** next_req_ptr, uint16_t* received_messages, uint16_t* per_qp_received_messages,
//                                       struct wrkr_ud_req* req, uint16_t* get_i, uint16_t wr_i, struct ibv_send_wr* wr, long long* nb_tx_tot,
//                                       int* clt_i, uint32_t* index, uint16_t send_qp_i, struct hrd_ctrl_blk* cb, uint16_t wrkr_lid,
//                                       int push_ptr, uint16_t* last_measured_wr_i, uint8_t* requests_per_message)
//{
//  struct ibv_wc send_wc;
//  if (!multiget) {
//    MOD_ADD_WITH_BASE(pull_ptr[qp_i], per_qp_buf_slots[qp_i], 0);
//    (*index) = pull_ptr[qp_i];
//    (*next_req_ptr) = (struct mica_op *) &(req[*index].m_op);
//    if (ENABLE_ASSERTIONS) assert((*next_req_ptr)->opcode == MICA_OP_GET || (*next_req_ptr)->opcode == MICA_OP_PUT);
//		requests_per_message[(*received_messages)] = 1;
//    (*received_messages)++;
//    per_qp_received_messages[qp_i]++;
//  }
//  else { // if multiget
//    (*get_i)++;
//    if ((*get_i) > 1) { // move to the next get req
//      // hrd_green_printf("Multiget is moving to get_i  %d, get_num %d\n", (*get_i), get_num );
//      (*next_req_ptr) = (struct mica_op *)(req[(*index)].m_op.value + (((*get_i) - 2) * HERD_GET_REQ_SIZE));
//    }
//  }
//
//
//	if ((ENABLE_WORKER_COALESCING == 0) || ((multiget == 0) || (*get_i == 1))) {
//
//		if (wr_i > 0) wr[wr_i - 1].next = &wr[wr_i];
//		wr[wr_i].send_flags = ((nb_tx_tot[send_qp_i] % WORKER_SS_BATCH) == 0)
//													? IBV_SEND_SIGNALED : 0;
//		if ((nb_tx_tot[send_qp_i] % WORKER_SS_BATCH) == WORKER_SS_BATCH - 1) {
//			hrd_poll_cq(cb->dgram_send_cq[send_qp_i], 1, &send_wc);
//		}
//		if (WORKER_ENABLE_INLINING == 1) wr[wr_i].send_flags |= IBV_SEND_INLINE;
//		(*clt_i) = (*next_req_ptr)->key.rem_meta.clt_gid;
//		wr[wr_i].wr.ud.ah = remote_clt_qp[(*clt_i)][REMOTE_UD_QP_ID].ah;
//		wr[wr_i].wr.ud.remote_qpn = remote_clt_qp[(*clt_i)][REMOTE_UD_QP_ID].qpn;
//
////		green_printf("Creating a request %d for client %d\n", wr_i, *clt_i);
//
//		if (ENABLE_ASSERTIONS)
//			worker_remote_request_error_checking(pull_ptr[qp_i], per_qp_buf_slots[qp_i], *next_req_ptr,
//																					 wrkr_lid, qp_i, wr_i, &req[*index], nb_tx_tot[send_qp_i],
//																					 push_ptr, *clt_i, multiget, *get_i);
//		nb_tx_tot[send_qp_i]++;
//	}
//
//	if (MEASURE_LATENCY == 1) {
//		cache_meta *tmp = (cache_meta *) &(*next_req_ptr)->key.rem_meta;
//		if (tmp->state != 0) {
//      if ((ENABLE_WORKER_COALESCING == 0) || ((multiget == 0) || (*get_i == 1)))
//        *last_measured_wr_i = wr_i;
//      else
//        *last_measured_wr_i = wr_i - 1;
////			printf("I see a req for measuring from client %d, state: %d wr_i %d\n", *clt_i, tmp->state, wr_i);
//			wr[*last_measured_wr_i].opcode = IBV_WR_SEND_WITH_IMM;
//			wr[*last_measured_wr_i].imm_data = REMOTE_LATENCY_MARK;
////			(*clt_i) -= CLIENT_NUM;
//		}
//	}
//
//}
//
//// Check all the conditions to stop polling for remote requests, or switch qp or exit the  current multiget
//static inline enum control_flow_directive check_polling_conditions(bool* multiget, uint16_t get_i, uint16_t get_num,
//                                       int* pull_ptr, uint16_t* qp_i, uint32_t per_qp_buf_slots, uint16_t per_qp_received_messages,
//                                       uint16_t wr_i, uint16_t received_messages, uint16_t wrkr_lid, uint32_t max_reqs)
//{
//  if ((*multiget) == true && get_i == get_num) { // check if the multiget should be over
//    (*multiget) = false;
//    MOD_ADD_WITH_BASE(pull_ptr[(*qp_i)], per_qp_buf_slots, 0);
//    // if (wrkr_lid == 0)
//    // 	hrd_green_printf("Multiget from clt %d is over wr_i %d, received messages %d, pull_ptr %d, get_num %d\n",
//    // 			clt_i, wr_i, received_messages, pull_ptr[(*qp_i)],get_num );
//  }
//  if (WORKER_NUM_UD_QPS > 1) {
//    if (per_qp_received_messages== per_qp_buf_slots && (*multiget) == false) {
//      if ((*qp_i) == WORKER_NUM_UD_QPS -1 ) return break_; // all qp buffers were checked
//      else MOD_ADD_WITH_BASE((*qp_i), WORKER_NUM_UD_QPS, 0);
//    }
//  }
//  if (wr_i == WORKER_MAX_BATCH || (received_messages == max_reqs && (*multiget) == false)) {
//    if (ENABLE_ASSERTIONS) {
//      if ((*multiget) == true) {
//        red_printf("Worker %d: wr_i = %d, received_messages %d, max reqs %d, get_i %d, get_num %d\n",
//                   wrkr_lid, wr_i, received_messages, max_reqs, get_i, get_num);
//        assert(false);
//      }
//    }
//    return break_;
//  }
//  return no_cf_change;
//}
//
//// poll the worker's receive completions according to the messages that have been polled from the REquest Region
//static inline void poll_workers_recv_completions(uint16_t* per_qp_received_messages, uint16_t received_messages,
//                                    struct hrd_ctrl_blk* cb, struct ibv_wc* wc, bool multiget, uint32_t* debug_recv,
//                                    uint16_t wr_i, uint16_t wrkr_lid, uint32_t max_reqs)
//{
//  uint16_t wc_i = 0, qp_i;
//  for (qp_i = 0; qp_i < WORKER_NUM_UD_QPS; qp_i++) { // take care to poll the correct recv queues
////      printf("Polling wr_i= %d, received_messages = %d, per_qp_received_messages[qp_i] = %d, qp_i =%d \n", wr_i,
////      						received_messages, per_qp_received_messages[qp_i], qp_i );
//    if (per_qp_received_messages[qp_i] > 0) {
//      if (ENABLE_ASSERTIONS) {
//        assert(per_qp_received_messages[qp_i] <= WS_PER_WORKER * (CLIENT_NUM - CLIENTS_PER_MACHINE));
//        if (WORKER_NUM_UD_QPS == 1) assert(per_qp_received_messages[qp_i] == received_messages);
//      }
//      hrd_poll_cq(cb->dgram_recv_cq[qp_i], per_qp_received_messages[qp_i], &wc[wc_i]);
//      if (DEBUG_WORKER_RECVS) *debug_recv-= per_qp_received_messages[qp_i];
//      wc_i += per_qp_received_messages[qp_i];
//    }
//  }
//  if (ENABLE_ASSERTIONS == 1) {
//    assert(multiget == false);
//    assert(wc_i == received_messages);
//    assert(wr_i <= WORKER_MAX_BATCH);
//    if (received_messages * MAX_COALESCE_PER_MACH < wr_i)
//      red_printf("Worker %d: wr_i = %d, received_messages %d, max reqs %d \n",
//                 wrkr_lid, wr_i, received_messages, max_reqs);
//    assert(received_messages <= max_reqs);
//    if (WORKER_NUM_UD_QPS == 1)
//      assert(per_qp_received_messages[0] == received_messages);
//    assert(received_messages <= WORKER_MAX_BATCH);
//  }
//}
//
//
//// Have each send work request point to the MICA response and delete the corresponding request from the Request Region
//static inline void append_responses_to_work_requests_and_delete_requests(uint16_t wr_i, struct mica_op** op_ptr_arr,
//                                      struct ibv_send_wr* wr, struct mica_resp* mica_resp_arr, uint16_t* resp_buf_i,
//                                      uint16_t wrkr_lid, struct ibv_sge* sgl, struct wrkr_coalesce_mica_op* response_buffer,
//																			uint8_t* requests_per_message)
//{
//  uint16_t i;
//	uint16_t message_i = 0;
//	uint8_t per_message_reqs = 0;
//  for (i = 0; i < wr_i; i++) {
//    if (ENABLE_ASSERTIONS) assert(op_ptr_arr[i] !=  NULL);
//    op_ptr_arr[i]->opcode = 0;
////    printf("size  is %d, it should be at most %d  \n", mica_resp_arr[i].val_len << SHIFT_BITS, HERD_PUT_REQ_SIZE);
//
//		if (ENABLE_WORKER_COALESCING == 1) {
//			if (per_message_reqs == 0) {
//				wr[message_i].sg_list->length = 0;
//				sgl[message_i].addr = (uintptr_t) response_buffer[(*resp_buf_i)].value;
//			}
//			memcpy(response_buffer[(*resp_buf_i)].value + wr[message_i].sg_list->length, mica_resp_arr[i].val_ptr,
//						 (mica_resp_arr[i].val_len << SHIFT_BITS));
//			wr[message_i].sg_list->length += (mica_resp_arr[i].val_len << SHIFT_BITS);
//			if (ENABLE_ASSERTIONS) {
//				assert(wr[message_i].sg_list->length <= sizeof(struct wrkr_coalesce_mica_op));}
//			per_message_reqs++;
//
//			if (per_message_reqs == requests_per_message[message_i]) {
////				printf("Created message %d, with %d reqs, message length %d, payload at %llu \n", message_i, per_message_reqs,
////							 wr[message_i].sg_list->length, response_buffer[(*resp_buf_i)].value);
//				message_i++;
//				per_message_reqs = 0;
//				MOD_ADD_WITH_BASE((*resp_buf_i), WORKER_SS_BATCH, 0);
//			}
//		}
//		else {
//			wr[i].sg_list->length = mica_resp_arr[i].val_len << SHIFT_BITS;
//			if (WORKER_ENABLE_INLINING == 1) {
//				wr[i].sg_list->addr = (uint64_t) mica_resp_arr[i].val_ptr;
//			} else {				if (ENABLE_ASSERTIONS) {
//					if (!((*resp_buf_i) < WORKER_SS_BATCH)) {
//						printf("Worker %d, resp_buf_i %d, ss_batch %d, wr_i %d, current batch size %d\n",
//									 wrkr_lid, (*resp_buf_i), WORKER_SS_BATCH, i, wr_i);
//						assert(0);
//					}
//				}
//				sgl[i].addr = (uintptr_t) response_buffer[(*resp_buf_i)].value;
//				memcpy(response_buffer[(*resp_buf_i)].value, mica_resp_arr[i].val_ptr, wr[i].sg_list->length);
////       printf("Sending val_len %d\n", wr[i].sg_list->length);
//				MOD_ADD_WITH_BASE((*resp_buf_i), WORKER_SS_BATCH, 0);
//			}
//		}
//  }
//}

//// Worker post receives on all of its qps and then posts its sends
//static inline void worker_post_receives_and_sends(uint16_t nb_new_req_tot, struct hrd_ctrl_blk* cb, uint16_t* per_qp_received_messages,
//                                       struct ibv_recv_wr* recv_wr, struct ibv_sge* recv_sgl, int* push_ptr, uint32_t* qp_buf_base,
//                                       uint32_t* per_qp_buf_slots, uint32_t* debug_recv, uint16_t* clts_per_qp, struct ibv_send_wr* wr,
//                                       uint16_t send_qp_i, uint16_t wrkr_lid, uint16_t last_measured_wr_i, uint16_t total_reqs_sent)
//{
//  uint16_t qp_i, i;
//  int ret;
//  struct ibv_recv_wr *bad_recv_wr;
//  struct ibv_send_wr *bad_send_wr;
//  // Refill the depleted RECVs before sending anything
//  for (qp_i = 0; qp_i < WORKER_NUM_UD_QPS; qp_i++) {
////      printf("Received %d\n",  per_qp_received_messages[qp_i]);
//    // printf("Total slots %d\n", per_qp_buf_slots[qp_i]);
//    if(WORKER_NUM_UD_QPS == 1 && nb_new_req_tot > 0 && ENABLE_ASSERTIONS)
//      assert(per_qp_received_messages[qp_i] > 0 && per_qp_received_messages[qp_i] <= WORKER_MAX_BATCH);
//    for (i = 0; i < per_qp_received_messages[qp_i]; i++) {
//      // printf("Receiving at slot  %d\n", qp_buf_base[qp_i] + push_ptr[qp_i] );
//      recv_sgl[i].addr = (uintptr_t) &cb->dgram_buf[(qp_buf_base[qp_i] + push_ptr[qp_i]) * sizeof(struct wrkr_ud_req)];
//      MOD_ADD_WITH_BASE(push_ptr[qp_i], per_qp_buf_slots[qp_i], 0);
//      recv_wr[i].next = (i == per_qp_received_messages[qp_i] - 1) ? NULL : &recv_wr[i + 1];
//      if (DEBUG_WORKER_RECVS) (*debug_recv)++;
//    }
//    if (per_qp_received_messages[qp_i] > 0) {
//      ret = ibv_post_recv(cb->dgram_qp[qp_i], &recv_wr[0], &bad_recv_wr);
//      CPE(ret, " Worker ibv_post_recv error", ret);
//    }
//    if (ENABLE_ASSERTIONS && DEBUG_WORKER_RECVS) assert(*debug_recv >= clts_per_qp[0]  * WS_PER_WORKER);
//  }
//
//  // Send the batch to the NIC
//  if (nb_new_req_tot > 0) {
//    wr[nb_new_req_tot - 1].next = NULL;
//    ret = ibv_post_send(cb->dgram_qp[send_qp_i], &wr[0], &bad_send_wr);
//    CPE(ret, "worker ibv_post_send error", ret);
////		green_printf("Sending %d responses through qp %d\n", nb_new_req_tot, send_qp_i);
//    w_stats[wrkr_lid].batches_per_worker++;
//    w_stats[wrkr_lid].remotes_per_worker+= total_reqs_sent;
//    if (MEASURE_LATENCY == 1) {
//      wr[last_measured_wr_i].opcode = IBV_WR_SEND;
//    }
//    MOD_ADD_WITH_BASE(send_qp_i, WORKER_NUM_UD_QPS, 0);
//  }
//}

#endif /* INLINE_UTILS_H */
