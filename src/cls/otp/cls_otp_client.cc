// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include "include/types.h"
#include "msg/msg_types.h"
#include "include/rados/librados.hpp"
#include "include/utime.h"
 
using namespace librados;

#include "cls/otp/cls_otp_ops.h"
#include "cls/otp/cls_otp_client.h"

#include "rgw/rgw_common.h" /* for gen_random_bytes() */

namespace rados {
  namespace cls {
    namespace otp {

      void OTP::create(librados::ObjectWriteOperation *rados_op,
                       const otp_info_t& config) {
        cls_otp_set_otp_op op;
        op.entries.push_back(config);
        bufferlist in;
        ::encode(op, in);
        rados_op->exec("otp", "otp_set", in);
      }

      void OTP::remove(librados::ObjectWriteOperation *rados_op,
                       const string& id) {
        cls_otp_remove_otp_op op;
        op.ids.push_back(id);
        bufferlist in;
        ::encode(op, in);
        rados_op->exec("otp", "otp_remove", in);
      }

      int OTP::check(CephContext *cct, librados::IoCtx& ioctx, const string& oid,
                     const string& id, const string& val, otp_check_t *result) {
        cls_otp_check_otp_op op;
        op.id = id;
        op.val = val;
#define TOKEN_LEN 16
        char buf[TOKEN_LEN + 1];
        gen_rand_alphanumeric(cct, buf, sizeof(buf));;
        op.token = buf;
        
        bufferlist in;
        bufferlist out;
        ::encode(op, in);
        int r = ioctx.exec(oid, "otp", "otp_check", in, out);
        if (r < 0) {
          return r;
        }

        cls_otp_get_result_op op2;
        op2.token = buf;
        bufferlist in2;
        bufferlist out2;
        ::encode(op2, in2);
        r = ioctx.exec(oid, "otp", "otp_get_result", in, out);
        if (r < 0) {
          return r;
        }

        auto iter = out.begin();
        cls_otp_get_result_reply ret;
        try {
          ::decode(ret, iter);
        } catch (buffer::error& err) {
	  return -EBADMSG;
        }

        *result = ret.result;

        return 0;
      }

      int OTP::get(librados::IoCtx& ioctx, const string& oid,
                    const list<string> *ids, bool get_all, list<otp_info_t> *result) {
        cls_otp_get_otp_op op;
        if (ids) {
          op.ids = *ids;
        }
        op.get_all = get_all;
        bufferlist in;
        bufferlist out;
        ::encode(op, in);
        int r = ioctx.exec(oid, "otp", "otp_get", in, out);
        if (r < 0) {
          return r;
        }

        cls_otp_get_otp_reply ret;
        auto iter = out.begin();
        try {
          ::decode(ret, iter);
        } catch (buffer::error& err) {
	  return -EBADMSG;
        }

        *result = ret.found_entries;;

        return 0;
      }

      int OTP::get(librados::IoCtx& ioctx, const string& oid,
                    const string& id, otp_info_t *result) {
        list<string> ids{ id };
        list<otp_info_t> ret;

        int r = get(ioctx, oid, &ids, false, &ret);
        if (r < 0) {
          return r;
        }
        if (ret.empty()) {
          return -ENOENT;
        }
        *result = ret.front();

        return 0;
      }

      int OTP::get_all(librados::IoCtx& ioctx, const string& oid,
                       list<otp_info_t> *result) {
        return get(ioctx, oid, nullptr, true, result);
      }

    } // namespace otp
  } // namespace cls
} // namespace rados

