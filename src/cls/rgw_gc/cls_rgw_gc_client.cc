// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_ops.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_const.h"
#include "cls/queue/cls_queue_const.h"

#include "rgw/datum/adapters/cls/queued_gc.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"

using std::list;
using std::string;

using ceph::decode;
using ceph::encode;

using namespace librados;
using namespace cls::rgw_gc;

void cls_rgw_gc_queue_init(ObjectWriteOperation& op, uint64_t size, uint64_t num_deferred_entries)
{
  auto in = ::cls::rgw::queued_gc_datum::encode_init(size, num_deferred_entries);
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
  auto in = ::cls::rgw::gc_datum::encode_set(expiration_secs, info);
  op.exec(method::enqueue, in);
}

int cls_rgw_gc_queue_list_entries(IoCtx& io_ctx, const string& oid, const string& marker, uint32_t max, bool expired_only,
                                  list<cls_rgw_gc_obj_info>& entries, bool& truncated, string& next_marker)
{
  auto in = ::cls::rgw::gc_datum::encode_list(marker, max, expired_only);
  bufferlist out;

  int r = io_ctx.exec(oid, method::list_entries, in, out);
  if (r < 0)
    return r;

  try {
    auto candidate = ::cls::rgw::gc_datum::decode_reply<std::list<cls_rgw_gc_obj_info>>(out);
    entries.swap(candidate.entries);
    truncated = candidate.truncated;
    next_marker = std::move(candidate.next_marker);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  return 0;
}

void cls_rgw_gc_queue_remove_entries(ObjectWriteOperation& op, uint32_t num_entries)
{
  auto in = ::cls::rgw::queued_gc_datum::encode_remove(num_entries);
  op.exec(method::remove_entries, in);
}

void cls_rgw_gc_queue_defer_entry(ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info)
{
  auto in = ::cls::rgw::gc_datum::encode_set(expiration_secs, info);
  op.exec(method::update_entry, in);
}
