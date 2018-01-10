// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * Author: Shinobu Kinjo <shinobu@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_CLOCK_H
#define CEPH_CLOCK_H

#include "common/ceph_time.h"
#include "include/utime.h"
#include <time.h>
#include <type_traits>

template <typename T>
T ceph_clock();

template <>
coarse_mono_time ceph_clock() {
  return ceph::coarse_mono_clock::now();
}

template <>
utime_t ceph_clock() {
#if defined(CLOCK_REALTIME)
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    utime_t n(tp);
#else
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    utime_t n(&tv);
#endif
  return n;
}

template <typename T,
  typename std::enable_if_t<
    std::is_same<T, coarse_mono_time>::value>* = nullptr>
auto get_duration(T start) -> ceph::time_detail::signedspan {
  return ceph_clock<T>() - start;
}

template <typename T,
  typename std::enable_if_t<
    std::is_same<T, utime_t>::value>* =nullptr>
T get_duration(T start) {
  return ceph_clock<T>() - start;
}

template <typename T = utime_t>
T ceph_clock_now() {
  return ceph_clock<T>();
}
#endif
