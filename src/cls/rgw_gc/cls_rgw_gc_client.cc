// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>

#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_ops.h"
#include "cls/queue/cls_queue_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_const.h"
#include "cls/queue/cls_queue_const.h"

#ifdef WITH_RGW_CLS_DATUM
 #include "cls_rgw_gc_datum.h"
#endif
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
#ifdef WITH_RGW_CLS_DATUM
  in = ::cls::rgw::queued_gc_datum::encode_init(size, num_deferred_entries);
#else
  cls_rgw_gc_queue_init_op call;
  call.size = size;
  call.num_deferred_entries = num_deferred_entries;
  encode(call, in);
#endif
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
#ifdef WITH_RGW_CLS_DATUM
  in = ::cls::rgw::gc_datum::encode_set(expiration_secs, info);
#else
  cls_rgw_gc_set_entry_op call;
  call.expiration_secs = expiration_secs;
  call.info = info;
  encode(call, in);
#endif
  op.exec(method::enqueue, in);
}

int cls_rgw_gc_queue_list_entries(IoCtx& io_ctx, const string& oid, const string& marker, uint32_t max, bool expired_only,
                                  list<cls_rgw_gc_obj_info>& entries, bool& truncated, string& next_marker)
{
  bufferlist in, out;
#ifdef WITH_RGW_CLS_DATUM
  in = ::cls::rgw::gc_datum::encode_list(marker, max, expired_only);
#else
  cls_rgw_gc_list_op op;
  op.marker = marker;
  op.max = max;
  op.expired_only = expired_only;
  encode(op, in);
#endif

  int r = io_ctx.exec(oid, method::list_entries, in, out);
  if (r < 0)
    return r;

#ifdef WITH_RGW_CLS_DATUM
  ::cls::rgw::gc_datum::list_reply<std::list<cls_rgw_gc_obj_info>> ret;
#else
  cls_rgw_gc_list_ret ret;
#endif
  try {
#ifdef WITH_RGW_CLS_DATUM
    ret = ::cls::rgw::gc_datum::decode_reply<std::list<cls_rgw_gc_obj_info>>(out);
#else
    auto iter = out.cbegin();
    decode(ret, iter);
#endif
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
#ifdef WITH_RGW_CLS_DATUM
  in = ::cls::rgw::queued_gc_datum::encode_remove(num_entries);
#else
  cls_rgw_gc_queue_remove_entries_op rem_op;
  rem_op.num_entries = num_entries;
  encode(rem_op, in);
#endif
  op.exec(method::remove_entries, in);
}

void cls_rgw_gc_queue_defer_entry(ObjectWriteOperation& op, uint32_t expiration_secs, const cls_rgw_gc_obj_info& info)
{
  bufferlist in;
#ifdef WITH_RGW_CLS_DATUM
  in = ::cls::rgw::gc_datum::encode_set(expiration_secs, info);
#else
  cls_rgw_gc_queue_defer_entry_op defer_op;
  defer_op.expiration_secs = expiration_secs;
  defer_op.info = info;
  encode(defer_op, in);
#endif
  op.exec(method::update_entry, in);
}
