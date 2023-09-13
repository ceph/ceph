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

#include <string>
#include <boost/container/flat_map.hpp>
#include "include/rados.h" // CEPH_OSD_CMPXATTR_*
#include "include/encoding.h"

namespace cls::cmpomap {

/// comparison operand type
enum class Mode : uint8_t {
  String = CEPH_OSD_CMPXATTR_MODE_STRING,
  U64    = CEPH_OSD_CMPXATTR_MODE_U64,
};

/// comparison operation, where the left-hand operand is the input value and
/// the right-hand operand is the stored value (or the optional default)
enum class Op : uint8_t {
  EQ  = CEPH_OSD_CMPXATTR_OP_EQ,
  NE  = CEPH_OSD_CMPXATTR_OP_NE,
  GT  = CEPH_OSD_CMPXATTR_OP_GT,
  GTE = CEPH_OSD_CMPXATTR_OP_GTE,
  LT  = CEPH_OSD_CMPXATTR_OP_LT,
  LTE = CEPH_OSD_CMPXATTR_OP_LTE,
};

/// mapping of omap keys to value comparisons
using ComparisonMap = boost::container::flat_map<std::string, ceph::bufferlist>;

} // namespace cls::cmpomap
