// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "cls/cas/cls_cas_client.h"
#include "cls/cas/cls_cas_ops.h"
#include "include/rados/librados.hpp"

using std::set;
using std::string;

using ceph::bufferlist;
using ceph::decode;
using ceph::encode;

void cls_chunk_refcount_get(librados::ObjectWriteOperation& op, const hobject_t& soid)
{
  bufferlist in;
  cls_chunk_refcount_get_op call;
  call.source = soid;
  encode(call, in);
  op.exec("cas", "chunk_get", in);
}

void cls_chunk_refcount_put(librados::ObjectWriteOperation& op, const hobject_t& soid)
{
  bufferlist in;
  cls_chunk_refcount_put_op call;
  call.source = soid;
  encode(call, in);
  op.exec("cas", "chunk_put", in);
}

void cls_chunk_refcount_set(librados::ObjectWriteOperation& op, set<hobject_t>& refs)
{
  bufferlist in;
  cls_chunk_refcount_set_op call;
  call.refs = refs;
  encode(call, in);
  op.exec("cas", "chunk_set", in);
}

int cls_chunk_refcount_read(librados::IoCtx& io_ctx, string& oid, set<hobject_t> *refs)
{
  bufferlist in, out;
  int r = io_ctx.exec(oid, "cas", "chunk_read", in, out);
  if (r < 0)
    return r;

  cls_chunk_refcount_read_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  *refs = ret.refs;

  return r;
}

int cls_chunk_has_chunk(librados::IoCtx& io_ctx, string& oid, string& fp_oid)
{
  bufferlist in, out;
  encode(fp_oid, in);
  int r = io_ctx.exec(oid, "cas", "has_chunk", in, out);
  return r;
}
