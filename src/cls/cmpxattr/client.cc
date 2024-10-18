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

  void lock_update(librados::ObjectWriteOperation& writeop,
		   const std::string& owner,
		   const std::string& key_name,
		   const utime_t&     max_lock_duration,
		   operation_flags_t  op_flags,
		   ceph::bufferlist   in_bl,
		   uint64_t           progress_a,
		   uint64_t           progress_b,
		   int32_t            urgent_msg)
  {
    // TBD: snaity check paramters

    lock_update_op call;
    call.owner = owner;
    call.progress_a = progress_a;
    call.progress_b = progress_b;
    call.key_name = key_name;
    call.max_lock_duration = max_lock_duration;
    call.op_flags = op_flags;
    call.in_bl = in_bl;
    call.urgent_msg = urgent_msg;
    bufferlist in;
    encode(call, in);
    writeop.exec("cmpxattr", "lock_update", in);
  }

  static const char* s_urgent_msg_names[] = {
    "URGENT_MSG_NONE",
    "URGENT_MSG_ABORT",
    "URGENT_MSG_PASUE",
    "URGENT_MSG_RESUME",
    "URGENT_MSG_SKIP",
    "URGENT_MSG_INVALID"
  };

  const char* get_urgent_msg_names(int msg) {
    if (msg <= URGENT_MSG_INVALID && msg >= URGENT_MSG_NONE) {
      return s_urgent_msg_names[msg];
    }
    else {
      return s_urgent_msg_names[URGENT_MSG_INVALID];
    }
  }
} // namespace cls::cmpxattr
