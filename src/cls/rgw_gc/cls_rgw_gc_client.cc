// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_ops.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_const.h"
#include "cls/queue/cls_queue_const.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"

using std::list;
using std::string;

using ceph::decode;
using ceph::encode;

using namespace librados;
using namespace cls::rgw_gc;

void cls_rgw_gc_queue_init(ObjectWriteOperation& op, uint64_t size, uint64_t num_deferred_entries)
{
  bufferlist in;
  cls_rgw_gc_queue_init_op call;
  call.size = size;
  call.num_deferred_entries = num_deferred_entries;
  encode(call, in);
  op.exec(method::init, in);
}

int cls_rgw_gc_queue_get_capacity(IoCtx& io_ctx, const string& oid, uint64_t& size)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid, cls::queue::method::get_capacity, in, out);
  if (r < 0)
    return r;

  cls_queue_get_capacity_ret op_ret;
  auto iter = out.cbegin();
  try {
    decode(op_ret, iter);
  } catch (ceph::buffer::error& err) {
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
  op.exec(method::enqueue, in);
}

int cls_rgw_gc_queue_list_entries(IoCtx& io_ctx, const string& oid, const string& marker, uint32_t max, bool expired_only,
                                  list<cls_rgw_gc_obj_info>& entries, bool& truncated, string& next_marker)
{
  ObjectReadOperation op;
  bufferlist out;
  cls_rgw_gc_queue_list_entries(op, marker, max, expired_only, out);

  int r = io_ctx.operate(oid, &op, nullptr);
  if (r < 0)
    return r;

  return cls_rgw_gc_queue_list_entries_decode(out, entries, truncated, next_marker);
}

void cls_rgw_gc_queue_list_entries(ObjectReadOperation& op, const string& marker, uint32_t max,
                                   bool expired_only, bufferlist& out)
{
  bufferlist in;
  cls_rgw_gc_list_op call;
  call.marker = marker;
  call.max = max;
  call.expired_only = expired_only;
  encode(call, in);
  op.exec(method::list_entries, in, &out, nullptr);
}

int cls_rgw_gc_queue_list_entries_decode(const bufferlist& out,
                                         list<cls_rgw_gc_obj_info>& entries,
                                         bool& truncated, string& next_marker)
{
  cls_rgw_gc_list_ret ret;
  auto iter = out.cbegin();
  try {
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  entries.swap(ret.entries);

  truncated = ret.truncated;

  next_marker = std::move(ret.next_marker);

  return 0;
}

void cls_rgw_gc_queue_remove_entries(ObjectWriteOperation& op, uint32_t num_entries)
{
  bufferlist in, out;
  cls_rgw_gc_queue_remove_entries_op rem_op;
  rem_op.num_entries = num_entries;
  encode(rem_op, in);
  op.exec(method::remove_entries, in);
}

void cls_rgw_gc_queue_defer_entry(ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info)
{
  bufferlist in;
  cls_rgw_gc_queue_defer_entry_op defer_op;
  defer_op.expiration_secs = expiration_secs;
  defer_op.info = info;
  encode(defer_op, in);
  op.exec(method::update_entry, in);
}
