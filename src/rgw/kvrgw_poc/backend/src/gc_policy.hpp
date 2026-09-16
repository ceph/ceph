// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <cstdint>

namespace kvrgw {

struct GcPolicy {
  bool suspended = false;
  int interval_sec = 10;
  int max_objects_per_sec = 0;
  int max_mb_per_sec = 0;
};

struct GcConfigSnapshot {
  uint32_t active_age = 0;
  uint32_t pending_age = 0;
  GcPolicy active{};
};

}  // namespace kvrgw
