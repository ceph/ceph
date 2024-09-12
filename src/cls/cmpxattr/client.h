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

#pragma once

#include <optional>
#include "include/rados/librados_fwd.hpp"
#include "types.h"
#include "ops.h"
#include "include/utime.h"
#include "common/ceph_time.h"

namespace cls::cmpxattr {
  /// requests with too many key comparisons will be rejected with -E2BIG
  static constexpr uint32_t max_keys = 8;

  /// process each of the xattrs value comparisons according to the same rules as
  /// cmpxattr(). IFF **all** key/value pairs in @cmp_pairs compare successfully
  /// a set operation will be perfrom for **all** key/value pairs in @set_pairs.
  /// for comparisons with Mode::U64, failure to decode an input value is
  /// reported as -EINVAL.
  /// a decode failure of a stored value is treated as an unsuccessful comparison
  /// and is not reported as an error
  [[nodiscard]] int cmp_vals_set_vals(librados::ObjectWriteOperation& writeop,
				      Mode mode, Op comparison,
				      const ComparisonMap& cmp_pairs,
				      const std::map<std::string, bufferlist>& set_pairs);

  // A server side locking facility using the server internal clock.
  // This guarantees a consistent view over multiple unsynchronized RGWs
  //
  // Create a lock object if doesn't exist with your name and curr time
  // If exists and you are the owner -> update time to now
  // If exists and you are *NOT* the owner:
  //    -> If duration since lock-time is higher than allowed_duration:
  //         -> Break the lock and set a new lock under your name with curr time
  //    -> Otherwise, fail operation
  void lock_update(librados::ObjectWriteOperation& writeop,
		   const std::string& owner,
		   const std::string& key_name,
		   const utime_t&     max_lock_duration,
		   operation_flags_t  op_flags,
		   ceph::bufferlist   in_bl,
		   uint64_t           progress_a,
		   uint64_t           progress_b,
		   int32_t            urgent_msg);

  // bufferlist factories for comparison values
  inline ceph::bufferlist string_buffer(const std::string_view& value) {
    ceph::bufferlist bl;
    bl.append(value);
    return bl;
  }
  inline ceph::bufferlist u64_buffer(uint64_t value) {
    ceph::bufferlist bl;
    using ceph::encode;
    encode(value, bl);
    return bl;
  }

} // namespace cls::cmpxattr
