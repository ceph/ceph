// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <errno.h>

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_ops.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_const.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"

using namespace librados;

void cls_rgw_gc_queue_init(ObjectWriteOperation& op, uint64_t size, uint64_t num_deferred_entries)
{
  bufferlist in;
  cls_rgw_gc_queue_init_op call;
  call.size = size;
  call.num_deferred_entries = num_deferred_entries;
  encode(call, in);
  op.exec(RGW_GC_CLASS, RGW_GC_QUEUE_INIT, in);
}

int cls_rgw_gc_queue_get_capacity(IoCtx& io_ctx, const string& oid, uint64_t& size)
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

void cls_rgw_gc_queue_enqueue(ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info)
{
  bufferlist in;
  cls_rgw_gc_set_entry_op call;
  call.expiration_secs = expiration_secs;
  call.info = info;
  encode(call, in);
  op.exec(RGW_GC_CLASS, RGW_GC_QUEUE_ENQUEUE, in);
}

int cls_rgw_gc_queue_list_entries(IoCtx& io_ctx, const string& oid, const string& marker, uint32_t max, bool expired_only,
                                  list<cls_rgw_gc_obj_info>& entries, bool *truncated, string& next_marker)
{
  bufferlist in, out;
  cls_rgw_gc_list_op op;
  op.marker = marker;
  op.max = max;
  op.expired_only = expired_only;
  encode(op, in);

  int r = io_ctx.exec(oid, RGW_GC_CLASS, RGW_GC_QUEUE_LIST_ENTRIES, in, out);
  if (r < 0)
    return r;

  cls_rgw_gc_list_ret ret;
  auto iter = out.cbegin();
  try {
    decode(ret, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  entries.swap(ret.entries);

  *truncated = ret.truncated;

  next_marker = std::move(ret.next_marker);

  return 0;
}

void cls_rgw_gc_queue_remove_entries(ObjectWriteOperation& op, uint32_t num_entries)
{
  bufferlist in, out;
  cls_rgw_gc_queue_remove_entries_op rem_op;
  rem_op.num_entries = num_entries;
  encode(rem_op, in);
  op.exec(RGW_GC_CLASS, RGW_GC_QUEUE_REMOVE_ENTRIES, in);
}

void cls_rgw_gc_queue_defer_entry(ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info)
{
  bufferlist in;
  cls_rgw_gc_queue_defer_entry_op defer_op;
  defer_op.expiration_secs = expiration_secs;
  defer_op.info = info;
  encode(defer_op, in);
  op.exec(RGW_GC_CLASS, RGW_GC_QUEUE_UPDATE_ENTRY, in);
}
