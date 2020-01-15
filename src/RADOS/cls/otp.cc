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

#include "include/buffer.h"

#include "common/expected.h"

#include "cls/otp/cls_otp_ops.h"

#include "rgw/rgw_common.h"

#include "otp.h"

namespace RADOS::CLS::OTP {
namespace cb = ceph::buffer;

token random_token(CephContext* cct)
{
  token buf;
  gen_rand_alphanumeric(cct, buf.data(), sizeof(buf));
  return buf;
}

ReadOp check(std::string_view id,
	     std::string_view val,
	     token const& t)
{
  ReadOp op;
  cls_otp_check_otp_op check_otp;
  check_otp.id = id;
  check_otp.val = val;
  check_otp.token = t.data();
  cb::list bl;
  encode(check_otp, bl);
  op.exec("otp", "otp_check", bl, nullptr);
  return op;
}


ReadOp get_result(token const& t,
		  rados::cls::otp::otp_check_t* res,
		  bs::error_code* oec)
{
  ReadOp op;
  cls_otp_get_result_op gr;
  gr.token = t.data();
  cb::list bl;
  encode(gr, bl);
  op.exec("otp", "otp_get_result", bl,
	  [res, oec](bs::error_code ec, cb::list bl) {
	    auto iter = bl.cbegin();
	    cls_otp_get_result_reply ret;
	    if (!ec) try {
		decode(ret, iter);
		if (res) {
		  *res = std::move(ret.result);
		}
	      } catch (cb::error const& err) {
		ec = err.code();
	      }
	    if (oec)
	      *oec = ec;
	  });
  return op;
}

void create(WriteOp& wop, rados::cls::otp::otp_info_t const& config) {
  cls_otp_set_otp_op op;
  op.entries.push_back(config);
  cb::list in;
  encode(op, in);
  wop.exec("otp", "otp_set", in);
}

void remove(WriteOp& wop, std::string_view id)
{
  cls_otp_remove_otp_op op;
  op.ids.push_back(std::string(id));
  cb::list in;
  encode(op, in);
  wop.exec("otp", "otp_remove", in);
}

void get(ReadOp& rop, std::vector<std::string> const* ids, bool get_all,
	 std::vector<rados::cls::otp::otp_info_t>* res,
	 bs::error_code* oec)
{
  cls_otp_get_otp_op get_otp;
  if (ids)
    get_otp.ids = *ids;

  get_otp.get_all = get_all;
  cb::list bl;
  encode(get_otp, bl);
  rop.exec("otp", "otp_get", bl,
	   [res, oec](bs::error_code ec, cb::list bl) {
	     if (!ec) try {
		 cls_otp_get_otp_reply ret;
		 auto iter = bl.cbegin();
		 decode(ret, iter);
		 if (res)
		   *res = std::move(ret.found_entries);
	       } catch (cb::error const& err) {
		 ec = err.code();
	       }
	     if (oec)
	       *oec = ec;
	   });
}


ReadOp get(std::vector<std::string> const* ids, bool get_all,
	   std::vector<rados::cls::otp::otp_info_t>* res,
	   bs::error_code* oec)
{
  ReadOp op;
  get(op, ids, get_all, res, oec);
  return op;
}


ReadOp get_current_time(ceph::real_time* t, bs::error_code* oec)
{
  ReadOp op;
  cls_otp_get_current_time_op gct;
  cb::list bl;
  encode(gct, bl);
  op.exec("otp", "get_current_time", bl,
	  [t, oec](bs::error_code ec, cb::list bl) {
	    if (!ec) try {
		cls_otp_get_current_time_reply ret;
		auto iter = bl.cbegin();
		decode(ret, iter);
		if (t)
		  *t = ret.time;
	      } catch (cb::error const& err) {
		ec = err.code();
	      }
	    if (oec)
	      *oec = ec;
	  });
  return op;
}

void set(WriteOp& rados_op,
         std::vector<rados::cls::otp::otp_info_t> const& entries) {
  cls_otp_set_otp_op op;
  op.entries = entries;
  cb::list in;
  encode(op, in);
  rados_op.exec("otp", "otp_set", in);
}
}
