// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>

#include "cls/refcount/cls_refcount_client.h"
#include "cls/refcount/cls_refcount_ops.h"
#include "include/rados/librados.hpp"

using std::list;
using std::string;

using ceph::bufferlist;

int cls_refcount_get(librados::ObjectWriteOperation& op, const string& tag, const std::string& src_tag)
{
  // tag must be non-empty strings
  if (unlikely(tag.empty() || src_tag.empty())) {
    return -EINVAL;
  }

  // tags must be different
  if (unlikely(tag == src_tag)) {
    return -EINVAL;
  }

  bufferlist in;
  cls_refcount_get_op call;
  call.tag = tag;
  call.src_tag = src_tag;
  encode(call, in);
  op.exec("refcount", "get", in);

  return 0;
}

int cls_refcount_put(librados::ObjectWriteOperation& op, const string& tag)
{
  // tag must be non-empty strings
  if (unlikely(tag.empty())) {
    return -EINVAL;
  }

  bufferlist in;
  cls_refcount_put_op call;
  call.tag = tag;
  encode(call, in);
  op.exec("refcount", "put", in);

  return 0;
}

int cls_refcount_set(librados::ObjectWriteOperation& op, list<string>& refs)
{
  bufferlist in;
  cls_refcount_set_op call;
  for (auto iter = refs.begin(); iter != refs.end(); ++iter) {
    // tag must be non-empty strings
    if (unlikely(iter->empty())) {
      return -EINVAL;
    }
  }

  call.refs = refs;
  encode(call, in);
  op.exec("refcount", "set", in);

  return 0;
}

int cls_refcount_read(librados::IoCtx& io_ctx, string& oid, list<string> *refs)
{
  bufferlist in, out;
  cls_refcount_read_op call;
  encode(call, in);
  int r = io_ctx.exec(oid, "refcount", "read", in, out);
  if (r < 0)
    return r;

  cls_refcount_read_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  *refs = ret.refs;

  return r;
}
