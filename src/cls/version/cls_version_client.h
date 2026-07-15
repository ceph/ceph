// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_CLS_VERSION_CLIENT_H
#define CEPH_CLS_VERSION_CLIENT_H

#include "include/rados/librados_fwd.hpp"
#include "cls_version_ops.h"

/*
 * version objclass
 */

void cls_version_set(librados::ObjectWriteOperation& op, obj_version& ver);

/* increase anyway */
void cls_version_inc(librados::ObjectWriteOperation& op);

/* conditional increase, return -EAGAIN if condition fails */
void cls_version_inc(librados::ObjectWriteOperation& op, obj_version& ver, VersionCond cond);

void cls_version_read(librados::ObjectReadOperation& op, obj_version *objv);

// these overloads which call io_ctx.operate() or io_ctx.exec() should not be called in the rgw.
// rgw_rados_operate() should be called after the overloads w/o calls to io_ctx.operate()/exec()
#ifndef CLS_CLIENT_HIDE_IOCTX
int cls_version_read(librados::IoCtx& io_ctx, std::string& oid, obj_version *ver);
#endif

[[deprecated("in favor of read/write variants")]]
void cls_version_check(librados::ObjectOperation& op, obj_version& ver, VersionCond cond);

template <typename ObjectOperation>
void cls_version_check(ObjectOperation& op, obj_version& objv, VersionCond cond)
{
  bufferlist in;
  cls_version_check_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec(cls::version::method::check_conds, in);
}

#endif
