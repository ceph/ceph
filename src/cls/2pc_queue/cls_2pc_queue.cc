// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/types.h"

#include "cls/2pc_queue/cls_2pc_queue_types.h"
#include "cls/2pc_queue/cls_2pc_queue_ops.h"
#include "cls/2pc_queue/cls_2pc_queue_const.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_src.h"
#include "objclass/objclass.h"

CLS_VER(1,0)
CLS_NAME(2pc_queue)

static int cls_2pc_queue_init(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  auto in_iter = in->cbegin();

  cls_queue_init_op op;
  try {
    decode(op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_init: failed to decode entry: %s", err.what());
    return -EINVAL;
  }

  cls_2pc_urgent_data urgent_data;

  cls_queue_init_op init_op;

  CLS_LOG(20, "INFO: cls_2pc_queue_init: max size is %lu (bytes)", op.queue_size);

  init_op.queue_size = op.queue_size;
  init_op.max_urgent_data_size = 23552; // overall head is 24KB ~ pending 1K reservations ops
  encode(urgent_data, init_op.bl_urgent_data);

  return queue_init(hctx, init_op);
}

static int cls_2pc_queue_reserve(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  cls_2pc_queue_reserve_op res_op;
  try {
    auto in_iter = in->cbegin();
    decode(res_op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_reserve: failed to decode entry: %s", err.what());
    return -EINVAL;
  }

  if (res_op.size == 0) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_reserve: cannot reserve zero bytes");
    return -EINVAL;
  }
  if (res_op.entries == 0) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_reserve: cannot reserve zero entries");
    return -EINVAL;
  }

  // get head
  cls_queue_head head;
  int ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_2pc_urgent_data urgent_data;
  try {
    auto in_iter = head.bl_urgent_data.cbegin();
    decode(urgent_data, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_reserve: failed to decode entry: %s", err.what());
    return -EINVAL;
  }

  const auto overhead = res_op.entries*QUEUE_ENTRY_OVERHEAD;
  const auto remaining_size = (head.tail.offset >= head.front.offset) ?
    (head.queue_size - head.tail.offset) + (head.front.offset - head.max_head_size) :
    head.front.offset - head.tail.offset;  


  if (res_op.size + urgent_data.reserved_size + overhead > remaining_size) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_reserve: reservations exceeded maximum capacity");
    CLS_LOG(10, "INFO: cls_2pc_queue_reserve: remaining size: %lu (bytes)", remaining_size);
    CLS_LOG(10, "INFO: cls_2pc_queue_reserve: current reservations: %lu (bytes)", urgent_data.reserved_size);
    CLS_LOG(10, "INFO: cls_2pc_queue_reserve: requested size: %lu (bytes)", res_op.size);
    return -ENOSPC;
  }

  urgent_data.reserved_size += res_op.size + overhead;
  bool result;
  std::tie(std::ignore, result) = urgent_data.reservations.emplace(std::piecewise_construct,
          std::forward_as_tuple(++urgent_data.last_id),
          std::forward_as_tuple(res_op.size, ceph::real_clock::now()));
  if (!result) {
    // an old reservation that was never committed or aborted is in the map
    // caller should try again assuming other IDs are ok
    CLS_LOG(1, "ERROR: cls_2pc_queue_reserve: reservation id conflict after rollover: %u", urgent_data.last_id);
    return -EAGAIN;
  }

  // write back head
  head.bl_urgent_data.clear();
  encode(urgent_data, head.bl_urgent_data);

  const uint64_t urgent_data_length = head.bl_urgent_data.length();

  if (head.max_urgent_data_size < urgent_data_length) {
    // TODO: use objects xattr for spillover
    CLS_LOG(1, "ERROR: cls_2pc_queue_reserve: urgent data size: %lu exceeded maximum: %lu", urgent_data_length, head.max_urgent_data_size);
    return -ENOSPC;
  }

  ret = queue_write_head(hctx, head);
  if (ret < 0) {
    return ret;
  }
  
  CLS_LOG(20, "INFO: cls_2pc_queue_reserve: remaining size: %lu (bytes)", remaining_size);
  CLS_LOG(20, "INFO: cls_2pc_queue_reserve: current reservations: %lu (bytes)", urgent_data.reserved_size);
  CLS_LOG(20, "INFO: cls_2pc_queue_reserve: requested size: %lu (bytes)", res_op.size);
  CLS_LOG(20, "INFO: cls_2pc_queue_reserve: urgent data size: %lu (bytes)", urgent_data_length);
  CLS_LOG(20, "INFO: cls_2pc_queue_reserve: current reservation ops: %lu", urgent_data.reservations.size());

  cls_2pc_queue_reserve_ret op_ret;
  op_ret.id = urgent_data.last_id;
  encode(op_ret, *out);

  return 0;
}

static int cls_2pc_queue_commit(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  cls_2pc_queue_commit_op commit_op;
  try {
    auto in_iter = in->cbegin();
    decode(commit_op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_commit: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
  
  // get head
  cls_queue_head head;
  int ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_2pc_urgent_data urgent_data;
  try {
    auto in_iter = head.bl_urgent_data.cbegin();
    decode(urgent_data, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_commit: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
  
  auto it = urgent_data.reservations.find(commit_op.id);
  if (it == urgent_data.reservations.end()) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_commit: reservation does not exist: %u", commit_op.id);
    return -ENOENT;
  }

  auto& res = it->second;
  const auto actual_size = std::accumulate(commit_op.bl_data_vec.begin(), 
          commit_op.bl_data_vec.end(), 0UL, [] (uint64_t sum, const bufferlist& bl) {
            return sum + bl.length();
          });
   
  if (res.size < actual_size) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_commit: trying to commit %lu bytes to a %lu bytes reservation", 
            actual_size,
            res.size);
    return -EINVAL;
  }

  // commit the data to the queue
  cls_queue_enqueue_op enqueue_op;
  enqueue_op.bl_data_vec = std::move(commit_op.bl_data_vec);
  ret = queue_enqueue(hctx, enqueue_op, head);
  if (ret < 0) {
    return ret;
  }

  // remove the reservation
  urgent_data.reserved_size -= res.size;
  urgent_data.reservations.erase(it);
  
  CLS_LOG(20, "INFO: cls_2pc_queue_commit: current reservations: %lu (bytes)", urgent_data.reserved_size);
  CLS_LOG(20, "INFO: cls_2pc_queue_reserve: current reservation ops: %lu", urgent_data.reservations.size());
  // write back head
  head.bl_urgent_data.clear();
  encode(urgent_data, head.bl_urgent_data);
  return queue_write_head(hctx, head);
}

static int cls_2pc_queue_abort(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  cls_2pc_queue_abort_op abort_op;
  try {
    auto in_iter = in->cbegin();
    decode(abort_op, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_abort: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
  
  // get head
  cls_queue_head head;
  int ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_2pc_urgent_data urgent_data;
  try {
    auto in_iter = head.bl_urgent_data.cbegin();
    decode(urgent_data, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_abort: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
  
  auto it = urgent_data.reservations.find(abort_op.id);
  if (it == urgent_data.reservations.end()) {
    CLS_LOG(10, "INFO: cls_2pc_queue_abort: reservation does not exist: %u", abort_op.id);
    return 0;
  }

  // remove the reservation
  urgent_data.reserved_size -= it->second.size;
  urgent_data.reservations.erase(it);

  CLS_LOG(20, "INFO: cls_2pc_queue_abort: current reservations: %lu (bytes)", urgent_data.reserved_size);
  CLS_LOG(20, "INFO: cls_2pc_queue_reserve: current reservation ops: %lu", urgent_data.reservations.size());

  // write back head
  head.bl_urgent_data.clear();
  encode(urgent_data, head.bl_urgent_data);
  return queue_write_head(hctx, head);
}

static int cls_2pc_queue_list_reservations(cls_method_context_t hctx, bufferlist *in, bufferlist *out) {
  //get head
  cls_queue_head head;
  int ret = queue_read_head(hctx, head);
  if (ret < 0) {
    return ret;
  }

  cls_2pc_urgent_data urgent_data;
  try {
    auto in_iter = head.bl_urgent_data.cbegin();
    decode(urgent_data, in_iter);
  } catch (buffer::error& err) {
    CLS_LOG(1, "ERROR: cls_2pc_queue_list_reservations: failed to decode entry: %s", err.what());
    return -EINVAL;
  }
  
  cls_2pc_queue_reservations_ret op_ret;
  op_ret.reservations = std::move(urgent_data.reservations);

  encode(op_ret, *out);

  return 0;
}

CLS_INIT(2pc_queue)
{
  CLS_LOG(1, "Loaded 2pc queue class!");

  cls_handle_t h_class;
  cls_method_handle_t h_2pc_queue_init;
  cls_method_handle_t h_2pc_queue_reserve;
  cls_method_handle_t h_2pc_queue_commit;
  cls_method_handle_t h_2pc_queue_abort;
  cls_method_handle_t h_2pc_queue_list_reservations;

  cls_register(TPC_QUEUE_CLASS, &h_class);

  cls_register_cxx_method(h_class, TPC_QUEUE_INIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_2pc_queue_init, &h_2pc_queue_init);
  cls_register_cxx_method(h_class, TPC_QUEUE_RESERVE, CLS_METHOD_RD | CLS_METHOD_WR, cls_2pc_queue_reserve, &h_2pc_queue_reserve);
  cls_register_cxx_method(h_class, TPC_QUEUE_COMMIT, CLS_METHOD_RD | CLS_METHOD_WR, cls_2pc_queue_commit, &h_2pc_queue_commit);
  cls_register_cxx_method(h_class, TPC_QUEUE_ABORT, CLS_METHOD_RD | CLS_METHOD_WR, cls_2pc_queue_abort, &h_2pc_queue_abort);
  cls_register_cxx_method(h_class, TPC_QUEUE_LIST_RESERVATIONS, CLS_METHOD_RD, cls_2pc_queue_list_reservations, &h_2pc_queue_list_reservations);

  return;
}

