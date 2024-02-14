// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "cls/queue/cls_queue_ops.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/queue/cls_queue_client.h"

using namespace std;
using namespace librados;

void cls_queue_init(ObjectWriteOperation& op, const string& queue_name, uint64_t size)
{
  bufferlist in;
  cls_queue_init_op call;
  call.max_urgent_data_size = 0;
  call.queue_size = size;
  encode(call, in);
  op.exec(QUEUE_CLASS, QUEUE_INIT, in);
}

int cls_queue_get_capacity(IoCtx& io_ctx, const string& oid, uint64_t& size)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid, QUEUE_CLASS, QUEUE_GET_CAPACITY, in, out);
  if (r < 0)
    return r;

  cls_queue_get_capacity_ret op_ret;
  auto iter = out.cbegin();
  try {
    decode(op_ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  size = op_ret.queue_capacity;

  return 0;
}

void cls_queue_enqueue(ObjectWriteOperation& op, uint32_t expiration_secs, vector<bufferlist> bl_data_vec)
{
  bufferlist in;
  cls_queue_enqueue_op call;
  call.bl_data_vec = std::move(bl_data_vec);
  encode(call, in);
  op.exec(QUEUE_CLASS, QUEUE_ENQUEUE, in);
}

int cls_queue_list_entries_inner(IoCtx& io_ctx, const string& oid, vector<cls_queue_entry>& entries,
                                 bool *truncated, string& next_marker, bufferlist& in, bufferlist& out)
{
  int r = io_ctx.exec(oid, QUEUE_CLASS, QUEUE_LIST_ENTRIES, in, out);
  if (r < 0)
    return r;

  cls_queue_list_ret ret;
  auto iter = out.cbegin();
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

int cls_queue_list_entries(IoCtx& io_ctx, const string& oid, const string& marker, uint32_t max,
                            vector<cls_queue_entry>& entries,
                            bool *truncated, string& next_marker)
{
  bufferlist in, out;
  cls_queue_list_op op;
  op.start_marker = marker;
  op.max = max;
  encode(op, in);

  return cls_queue_list_entries_inner(io_ctx, oid, entries, truncated, next_marker, in, out);
}

int cls_queue_list_entries(IoCtx& io_ctx, const string& oid, const string& marker, const string& end_marker,
                           vector<cls_queue_entry>& entries,
                           bool *truncated, string& next_marker)
{
  bufferlist in, out;
  cls_queue_list_op op;
  op.start_marker = marker;
  op.max = std::numeric_limits<uint64_t>::max();
  op.end_marker = end_marker;
  encode(op, in);

  return cls_queue_list_entries_inner(io_ctx, oid, entries, truncated, next_marker, in, out);
}

void cls_queue_remove_entries(ObjectWriteOperation& op, const string& end_marker)
{
  bufferlist in, out;
  cls_queue_remove_op rem_op;
  rem_op.end_marker = end_marker;
  encode(rem_op, in);
  op.exec(QUEUE_CLASS, QUEUE_REMOVE_ENTRIES, in);
}
