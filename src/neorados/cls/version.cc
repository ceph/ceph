// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM
 *
 * See file COPYING for license information.
 *
 */
#include "version.h"

#include <utility>

#include <boost/system/error_code.hpp>

#include "include/buffer.h"

#include "cls/version/cls_version_ops.h"
#include "cls/version/cls_version_types.h"


namespace neorados::cls::version {
namespace buffer = ceph::buffer;

using boost::system::error_code;

void set(WriteOp& op, const obj_version& objv)
{
  buffer::list in;
  cls_version_set_op call;
  call.objv = objv;
  encode(call, in);
  op.exec("version", "set", in);
}

void inc(WriteOp& op)
{
  buffer::list in;
  cls_version_inc_op call;
  encode(call, in);
  op.exec("version", "inc", in);
}

void inc(WriteOp& op, const obj_version& objv, const VersionCond cond)
{
  buffer::list in;
  cls_version_inc_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "inc_conds", in);
}

void check(Op& op, const obj_version& objv, const VersionCond cond)
{
  buffer::list in;
  cls_version_check_op call;
  call.objv = objv;

  obj_version_cond c;
  c.cond = cond;
  c.ver = objv;

  call.conds.push_back(c);

  encode(call, in);
  op.exec("version", "check_conds", in);
}

void read(ReadOp& op, obj_version* const objv)
{
  buffer::list inbl;
  op.exec("version", "read", inbl,
          [objv](error_code ec,
                 const buffer::list& bl) {
            cls_version_read_ret ret;
            if (!ec) {
	      auto iter = bl.cbegin();
	      decode(ret, iter);
	      if (objv)
		*objv = std::move(ret.objv);
	    }
          });
}
} // namespace neorados::cls::version
