// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 IBM Corp.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "include/rados/librados.hpp"
#include "client.h"
#include "ops.h"
#include "common/dout.h"
#include "common/errno.h"

static constexpr auto dout_subsys = ceph_subsys_rgw_dedup;
namespace cls::cmpxattr {
  //---------------------------------------------------------------------------
  int cmp_vals_set_vals(librados::ObjectWriteOperation& op,
			Mode mode, Op comparison,
			const ComparisonMap& cmp_pairs,
			const std::map<std::string, bufferlist>& set_pairs,
			bufferlist *out)
  {
    // Caller must supply non-empty cmp_pairs and set_pairs
    // However, it is legal to pass an empty value bl for the key
    //         this is used when try to set a new key/value atomically
    if (unlikely(cmp_pairs.size() > max_keys)) {
      return -E2BIG;
    }
    if (unlikely(cmp_pairs.empty() || set_pairs.empty())) {
      return -EINVAL;
    }

    cmp_vals_set_vals_op call;
    call.mode = mode;
    call.comparison = comparison;
    call.cmp_pairs = std::move(cmp_pairs);
    call.set_pairs = std::move(set_pairs);
    // not sure what is it used for, so don't need to pass an argument for it
    // in any case, keep a static place for an async reply
    static int rval;
    bufferlist in;
    encode(call, in);
    op.exec("cmpxattr", "cmp_vals_set_vals", in, out, &rval);
    return 0;
  }

  //---------------------------------------------------------------------------
  int report_cmp_set_error(const DoutPrefixProvider *dpp,
			   int err_code,
			   const bufferlist &err_bl,
			   const char *caller)
  {
    int ret = err_code;
    if (err_code != 0) {
      ldpp_dout(dpp, 1) << caller << "::ERR: failed cmp_vals_set_vals::"
			<< cpp_strerror(err_code)
			<< ", err_code=" << err_code << dendl;
    }
    else if (err_bl.length() ){
      try {
	std::string key;
	auto bl_iter = err_bl.cbegin();
	using ceph::decode;
	decode(key, bl_iter);
	ldpp_dout(dpp, 5) << caller << "::ERR: failed cmp_vals_set_vals for key: "
			  << key << dendl;
	// another thread set a new value causing CMP to fail
	ret = -EBUSY;
      } catch (buffer::error& err) {
	std::cerr << caller << "::ERR: unable to decode err_bl" << std::endl;
	ret = -EINVAL;
      }
    }
    else {
      // Should only call this API to report error!
      ldpp_dout(dpp, 10) << caller << "::success (should not be called)" << dendl;
    }

    return ret;
  }
} // namespace cls::cmpxattr
