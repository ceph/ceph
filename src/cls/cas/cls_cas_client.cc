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

void cls_cas_chunk_create_or_get_ref(
  librados::ObjectWriteOperation& op,
  const hobject_t& soid,
  const bufferlist& data,
  bool verify)
{
  bufferlist in;
  cls_cas_chunk_create_or_get_ref_op call;
  call.source = soid;
  if (verify) {
    call.flags |= cls_cas_chunk_create_or_get_ref_op::FLAG_VERIFY;
  }
  call.data = data;
  encode(call, in);
  op.exec("cas", "chunk_create_or_get_ref", in);
}

void cls_cas_chunk_get_ref(
  librados::ObjectWriteOperation& op,
  const hobject_t& soid)
{
  bufferlist in;
  cls_cas_chunk_get_ref_op call;
  call.source = soid;
  encode(call, in);
  op.exec("cas", "chunk_get_ref", in);
}

void cls_cas_chunk_put_ref(
  librados::ObjectWriteOperation& op,
  const hobject_t& soid)
{
  bufferlist in;
  cls_cas_chunk_put_ref_op call;
  call.source = soid;
  encode(call, in);
  op.exec("cas", "chunk_put_ref", in);
}

int cls_cas_references_chunk(
  librados::IoCtx& io_ctx,
  const string& oid,
  const string& chunk_oid)
{
  bufferlist in, out;
  encode(chunk_oid, in);
  int r = io_ctx.exec(oid, "cas", "references_chunk", in, out);
  return r;
}
