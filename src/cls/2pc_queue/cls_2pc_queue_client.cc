// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/2pc_queue/cls_2pc_queue_client.h"
#include "cls/2pc_queue/cls_2pc_queue_ops.h"
#include "cls/2pc_queue/cls_2pc_queue_const.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"

using namespace librados;

void cls_2pc_queue_init(ObjectWriteOperation& op, const std::string& queue_name, uint64_t size) {
  bufferlist in;
  cls_queue_init_op call;
  call.queue_size = size;
  encode(call, in);
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_INIT, in);
}

int cls_2pc_queue_get_capacity_result(const bufferlist& bl, uint64_t& size) {
  cls_queue_get_capacity_ret op_ret;
  auto iter = bl.cbegin();
  try {
    decode(op_ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  size = op_ret.queue_capacity;

  return 0;
}

#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_2pc_queue_get_capacity(IoCtx& io_ctx, const string& queue_name, uint64_t& size) {
  bufferlist in, out;
  const auto r = io_ctx.exec(queue_name, TPC_QUEUE_CLASS, TPC_QUEUE_GET_CAPACITY, in, out);
  if (r < 0 ) {
    return r;
  }

  return cls_2pc_queue_get_capacity_result(out, size);
}
#endif

// optionally async method for getting capacity (bytes) 
// after answer is received, call cls_2pc_queue_get_capacity_result() to prase the results
void cls_2pc_queue_get_capacity(ObjectReadOperation& op, bufferlist* obl, int* prval) {
  bufferlist in;
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_GET_CAPACITY, in, obl, prval);
}


int cls_2pc_queue_reserve_result(const bufferlist& bl, cls_2pc_reservation::id_t& res_id) {
  cls_2pc_queue_reserve_ret op_ret;
  auto iter = bl.cbegin();
  try {
    decode(op_ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  res_id = op_ret.id;

  return 0;
}

int cls_2pc_queue_reserve(IoCtx& io_ctx, const string& queue_name, 
        uint64_t res_size, uint32_t entries, cls_2pc_reservation::id_t& res_id) {
  bufferlist in, out;
  cls_2pc_queue_reserve_op reserve_op;
  reserve_op.size = res_size;
  reserve_op.entries = entries;

  encode(reserve_op, in);
  int rval;
  ObjectWriteOperation op;
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_RESERVE, in, &out, &rval);
  const auto r = io_ctx.operate(queue_name, &op, librados::OPERATION_RETURNVEC);

  if (r < 0) {
    return r;
  }
  
  return cls_2pc_queue_reserve_result(out, res_id);
}

void cls_2pc_queue_reserve(ObjectWriteOperation& op, uint64_t res_size, 
    uint32_t entries, bufferlist* obl, int* prval) {
  bufferlist in;
  cls_2pc_queue_reserve_op reserve_op;
  reserve_op.size = res_size;
  reserve_op.entries = entries;
  encode(reserve_op, in);
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_RESERVE, in, obl, prval);
}

void cls_2pc_queue_commit(ObjectWriteOperation& op, std::vector<bufferlist> bl_data_vec, 
        cls_2pc_reservation::id_t res_id) {
  bufferlist in;
  cls_2pc_queue_commit_op commit_op;
  commit_op.id = res_id;
  commit_op.bl_data_vec = std::move(bl_data_vec);
  encode(commit_op, in);
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_COMMIT, in);
}

void cls_2pc_queue_abort(ObjectWriteOperation& op, cls_2pc_reservation::id_t res_id) {
  bufferlist in;
  cls_2pc_queue_abort_op abort_op;
  abort_op.id = res_id;
  encode(abort_op, in);
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_ABORT, in);
}

int cls_2pc_queue_list_entries_result(const bufferlist& bl, std::vector<cls_queue_entry>& entries,
                            bool *truncated, std::string& next_marker) {
  cls_queue_list_ret ret;
  auto iter = bl.cbegin();
  try {
    decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  entries = std::move(ret.entries);
  *truncated = ret.is_truncated;

  next_marker = std::move(ret.next_marker);

  return 0;
}

#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_2pc_queue_list_entries(IoCtx& io_ctx, const string& queue_name, const string& marker, uint32_t max,
                            std::vector<cls_queue_entry>& entries,
                            bool *truncated, std::string& next_marker) {
  bufferlist in, out;
  cls_queue_list_op op;
  op.start_marker = marker;
  op.max = max;
  encode(op, in);

  const auto r  = io_ctx.exec(queue_name, TPC_QUEUE_CLASS, TPC_QUEUE_LIST_ENTRIES, in, out);
  if (r < 0) {
    return r;
  }
  return cls_2pc_queue_list_entries_result(out, entries, truncated, next_marker);
}
#endif

void cls_2pc_queue_list_entries(ObjectReadOperation& op, const std::string& marker, uint32_t max, bufferlist* obl, int* prval) {
  bufferlist in;
  cls_queue_list_op list_op;
  list_op.start_marker = marker;
  list_op.max = max;
  encode(list_op, in);

  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_LIST_ENTRIES, in, obl, prval);
}

int cls_2pc_queue_list_reservations_result(const bufferlist& bl, cls_2pc_reservations& reservations) {
  cls_2pc_queue_reservations_ret ret;
  auto iter = bl.cbegin();
  try {
    decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  reservations = std::move(ret.reservations);

  return 0;
}

#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_2pc_queue_list_reservations(IoCtx& io_ctx, const std::string& queue_name, cls_2pc_reservations& reservations) {
  bufferlist in, out;

  const auto r = io_ctx.exec(queue_name, TPC_QUEUE_CLASS, TPC_QUEUE_LIST_RESERVATIONS, in, out);
  if (r < 0) {
    return r;
  }
  return cls_2pc_queue_list_reservations_result(out, reservations);
}
#endif

void cls_2pc_queue_list_reservations(ObjectReadOperation& op, bufferlist* obl, int* prval) {
  bufferlist in;

  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_LIST_RESERVATIONS, in, obl, prval);
}

void cls_2pc_queue_remove_entries(ObjectWriteOperation& op, const std::string& end_marker) {
  bufferlist in;
  cls_queue_remove_op rem_op;
  rem_op.end_marker = end_marker;
  encode(rem_op, in);
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_REMOVE_ENTRIES, in);
}

void cls_2pc_queue_expire_reservations(librados::ObjectWriteOperation& op, ceph::coarse_real_time stale_time) {
  bufferlist in;
  cls_2pc_queue_expire_op expire_op;
  expire_op.stale_time = stale_time;
  encode(expire_op, in);
  op.exec(TPC_QUEUE_CLASS, TPC_QUEUE_EXPIRE_RESERVATIONS, in);
}

