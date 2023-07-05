// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "common/ceph_time.h"
#include "osdc/Objecter.h"

namespace librados {

// Wraps Objecter's ObjectOperation with storage for an optional mtime argument.
struct ObjectOperationImpl {
  ::ObjectOperation o;
  ceph::real_time rt;
  ceph::real_time *prt = nullptr;
};

} // namespace librados
