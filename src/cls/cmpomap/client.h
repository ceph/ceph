// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat, Inc
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

namespace cls::cmpomap {

/// requests with too many key comparisons will be rejected with -E2BIG
static constexpr uint32_t max_keys = 1000;

/// process each of the omap value comparisons according to the same rules as
/// cmpxattr(), and return -ECANCELED if a comparison is unsuccessful. for
/// comparisons with Mode::U64, failure to decode an input value is reported
/// as -EINVAL, while failure to decode a stored value is reported as -EIO
[[nodiscard]] int cmp_vals(librados::ObjectReadOperation& op,
                           Mode mode, Op comparison, ComparisonMap values,
                           std::optional<ceph::bufferlist> default_value);

/// process each of the omap value comparisons according to the same rules as
/// cmpxattr(). any key/value pairs that compare successfully are overwritten
/// with the corresponding input value. for comparisons with Mode::U64, failure
/// to decode an input value is reported as -EINVAL. decode failure of a stored
/// value is treated as an unsuccessful comparison and is not reported as an
/// error
[[nodiscard]] int cmp_set_vals(librados::ObjectWriteOperation& writeop,
                               Mode mode, Op comparison, ComparisonMap values,
                               std::optional<ceph::bufferlist> default_value);

/// process each of the omap value comparisons according to the same rules as
/// cmpxattr(). any key/value pairs that compare successfully are removed. for
/// comparisons with Mode::U64, failure to decode an input value is reported as
/// -EINVAL. decode failure of a stored value is treated as an unsuccessful
/// comparison and is not reported as an error
[[nodiscard]] int cmp_rm_keys(librados::ObjectWriteOperation& writeop,
                              Mode mode, Op comparison, ComparisonMap values);


// bufferlist factories for comparison values
inline ceph::bufferlist string_buffer(std::string_view value) {
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

} // namespace cls::cmpomap
