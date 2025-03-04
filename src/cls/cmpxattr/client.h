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
#include "common/dout.h"

namespace cls::cmpxattr {
  /// requests with too many key comparisons will be rejected with -E2BIG
  static constexpr uint32_t max_keys = 8;

  /// process each of the xattrs value comparisons according to the same rules as
  /// cmpxattr(). IFF **all** key/value pairs in @cmp_pairs compare successfully
  /// a set operation will be perfrom for **all** key/value pairs in @set_pairs.
  /// Caller must supply non-empty cmp_pairs and set_pairs
  /// However, it is legal to pass an empty value bl for any key
  ///         An empty value will match key with an empty value or a non-existing key!
  ///
  /// for comparisons with Mode::U64, failure to decode an input value is
  /// reported as -EINVAL.
  /// a decode failure of a stored value is treated as an unsuccessful comparison
  /// and is not reported as an error
  [[nodiscard]] int cmp_vals_set_vals(librados::ObjectWriteOperation& writeop,
				      Mode mode, Op comparison,
				      const ComparisonMap&& cmp_pairs,
				      const std::map<std::string, bufferlist>&& set_pairs,
				      bufferlist *out);

  int report_cmp_set_error(const DoutPrefixProvider *dpp,
			   int err_code,
			   const bufferlist &err_bl,
			   const char *caller);

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
