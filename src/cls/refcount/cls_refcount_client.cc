// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <errno.h>

#include <utility>
#include <iterator>

#include "cls/refcount/cls_refcount_client.h"
#include "cls/refcount/cls_refcount_ops.h"
#include "include/rados/librados.hpp"

using std::list;
using std::string;

using ceph::bufferlist;
using namespace cls::refcount;

void cls_refcount_get(librados::ObjectWriteOperation& op, const string& tag, bool implicit_ref)
{
  bufferlist in;
  cls_refcount_get_op call;
  call.tag = tag;
  call.implicit_ref = implicit_ref;
  encode(call, in);
  op.exec(method::get, in);
}

void cls_refcount_put(librados::ObjectWriteOperation& op, const string& tag, bool implicit_ref)
{
  bufferlist in;
  cls_refcount_put_op call;
  call.tag = tag;
  call.implicit_ref = implicit_ref;
  encode(call, in);
  op.exec(method::put, in);
}

namespace {

void set_refs(librados::ObjectWriteOperation& op, const auto& refs)
{
  bufferlist in;
  cls_refcount_set_op call;
  call.refs.assign(std::cbegin(refs), std::cend(refs));
  encode(call, in);
  op.exec(method::set, in);
}

} // namespace

void cls_refcount_set(librados::ObjectWriteOperation& op,
                      const std::vector<std::string>& refs)
{
  set_refs(op, refs);
}

void cls_refcount_set(librados::ObjectWriteOperation& op, list<string>& refs)
{
  set_refs(op, refs);
}

int cls_refcount_read(librados::IoCtx& io_ctx, string& oid,
                      std::vector<std::string>& refs, bool implicit_ref)
{
  bufferlist in, out;
  cls_refcount_read_op call;
  call.implicit_ref = implicit_ref;
  encode(call, in);
  const int r = io_ctx.exec(oid, method::read, in, out);
  if (r < 0)
    return r;

  cls_refcount_read_ret ret;
  try {
    auto iter = out.cbegin();
    decode(ret, iter);
  } catch (ceph::buffer::error& err) {
    return -EIO;
  }

  refs = std::move(ret.refs);

  return r;
}

int cls_refcount_read(librados::IoCtx& io_ctx, string& oid,
                      list<string> *refs, bool implicit_ref)
{
  std::vector<std::string> result;
  const int r = cls_refcount_read(io_ctx, oid, result, implicit_ref);
  if (r < 0)
    return r;

  refs->assign(std::make_move_iterator(std::begin(result)),
               std::make_move_iterator(std::end(result)));

  return r;
}
