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

namespace cls::cmpxattr {
  int cmp_vals_set_vals(librados::ObjectWriteOperation& op,
			Mode mode, Op comparison,
			const ComparisonMap& cmp_pairs,
			const std::map<std::string, bufferlist>& set_pairs)
  {
    if (cmp_pairs.size() > max_keys || cmp_pairs.empty() || set_pairs.empty() ) {
      return -E2BIG;
    }
    cmp_vals_set_vals_op call;
    call.mode = mode;
    call.comparison = comparison;
    call.cmp_pairs = std::move(cmp_pairs);
    call.set_pairs = std::move(set_pairs);

    bufferlist in;
    encode(call, in);
    op.exec("cmpxattr", "cmp_vals_set_vals", in);
    return 0;
  }

  int lock_update(librados::ObjectWriteOperation& writeop,
		  const std::string& owner,
		  const std::string& key_name,
		  const utime_t& max_lock_duration)
  {
    // TBD: snaity check paramters

    lock_update_op call;
    call.owner = owner;
    call.key_name = key_name;
    call.max_lock_duration = max_lock_duration;

    bufferlist in;
    encode(call, in);
    writeop.exec("cmpxattr", "lock_update", in);
    return 0;
  }

} // namespace cls::cmpxattr
