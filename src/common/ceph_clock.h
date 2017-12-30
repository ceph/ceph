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
#include <type_traits>
#include <time.h>

struct ceph_clock {
  ceph_clock() :
    mc_start_(ceph::mono_clock::now()),
    ut_start_(ceph_clock_now_()) {} 

  /* mono_clock */
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, ceph::mono_time>::value>* = nullptr>
  T now() {
    mc_now_ = get_time_point_();
    return mc_now_;
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, ceph::mono_time>::value>* = nullptr>
  void reset() {
    mc_now_ = get_time_point_();
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, ceph::timespan>::value>* = nullptr>
  T elapsed_from_start() {
    mc_end_ = get_time_point_();
    return mc_end_ - mc_start_;
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, ceph::timespan>::value>* = nullptr>
  T elapsed_from_now() {
    mc_end_ = get_time_point_();
    return mc_end_ - mc_now_;
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, std::chrono::seconds>::value>* = nullptr>
  void set_duration(ceph::mono_clock::duration& d, int64_t s) {
    d = std::chrono::seconds(s);
  }

  /* Compatibility: utime_t */
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, utime_t>::value>* = nullptr>
  T now() {
    ut_now_ = ceph_clock_now_();
    return ut_now_;
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, utime_t>::value>* = nullptr>
  void reset() {
    ut_now_ = ceph_clock_now_();
  }

  ostream& set_localtime(ostream& out, utime_t& ut) const {
    return ut.localtime(out);
  }

private:
  /** mono_clock **/
  ceph::mono_time mc_start_;
  ceph::mono_time mc_now_;
  ceph::mono_time mc_end_;

  /** Compatibility: utime_t **/
  utime_t ut_start_;
  utime_t ut_now_;
  utime_t ut_end_;

  ceph::mono_time get_time_point_() {
    return ceph::mono_clock::now();
  }

  utime_t ceph_clock_now_() {
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
};
#endif
