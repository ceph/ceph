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
#include "common/ceph_clock.h"

utime_t ceph_clock_now() {
  return ceph_clock<utime_t>();
}

template <>
coarse_mono_time ceph_clock_now() {
  return ceph_clock<coarse_mono_time>();
}

utime_t get_duration(utime_t start) {
  return ceph_clock<utime_t>() - start;
}

template <>
ceph::time_detail::signedspan get_duration(coarse_mono_time start) {
  return ceph_clock<coarse_mono_time>() - start;
}
