// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "cls/version/cls_version_ops.h"

#include "version.h"

namespace RADOS::CLS::version {
namespace bs = boost::system;
namespace cb = ceph::buffer;

void set(WriteOp& op, obj_version& objv)
{
  cb::list in;
  cls_version_set_op call;
  call.objv = objv;
  encode(call, in);
  op.exec("version", "set", in);
}

void inc(WriteOp& op)
{
  cb::list in;
  cls_version_inc_op call;
  encode(call, in);
  op.exec("version", "inc", in);
}

void inc(WriteOp& op, obj_version& objv, VersionCond cond)
{
  cb::list in;
  cls_version_inc_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "inc_conds", in);
}

void check(ReadOp& op, obj_version& objv, VersionCond cond)
{
  cb::list in;
  cls_version_check_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "check_conds", in, nullptr);
}

void check(WriteOp& op, obj_version& objv, VersionCond cond)
{
  cb::list in;
  cls_version_check_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "check_conds", in);
}

void read(ReadOp& op, obj_version *objv)
{
  cb::list inbl;
  op.exec("version", "read", inbl,
          [objv](boost::system::error_code ec,
                 const cb::list& bl) {
            cls_version_read_ret ret;
            if (!ec) {
              try {
                auto iter = bl.cbegin();
                decode(ret, iter);
                *objv = ret.objv;
              } catch (const ceph::buffer::error& err) {
                // nothing we can do about it atm
              }
            }
          });
}
}
