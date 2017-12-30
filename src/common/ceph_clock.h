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

using namespace std::chrono;

constexpr unsigned long long operator"" _ns (unsigned long long ns) {
  return ns / 1000000000;
}
constexpr unsigned long long operator"" _ms (unsigned long long ms) {
  return ms / 1000;
}
constexpr unsigned long long operator"" _s (unsigned long long s) {
  return s;
}
constexpr unsigned long long operator"" _m (unsigned long long m) {
  return m * 60;
}

namespace ceph {

struct ceph_clock {
  ceph_clock() :
    mt_start_(ceph::mono_clock::now()),
    cmt_start_(ceph::coarse_mono_clock::now()),
    ut_start_(ceph_clock_now_()) {} 

  /* mono_clock */
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, real_time>::value ||
      std::is_same<T, coarse_real_time>::value ||
      std::is_same<T, mono_time>::value ||
      std::is_same<T, coarse_mono_time>::value>* = nullptr>
  auto now() -> T {
    return get_time_point_<T>();
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, real_time>::value ||
      std::is_same<T, coarse_real_time>::value ||
      std::is_same<T, mono_time>::value ||
      std::is_same<T, coarse_mono_time>::value>* = nullptr>
  void reset() {
    get_time_point_<T>();
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, real_time>::value ||
      std::is_same<T, coarse_real_time>::value ||
      std::is_same<T, mono_time>::value ||
      std::is_same<T, coarse_mono_time>::value>* = nullptr>
  auto elapsed_from_start() -> ceph::timespan {
    return elapsed_from_start_<T>();
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, real_time>::value ||
      std::is_same<T, coarse_real_time>::value ||
      std::is_same<T, mono_time>::value ||
      std::is_same<T, coarse_mono_time>::value>* = nullptr>
  auto elapsed_from_now() -> ceph::timespan {
    return elapsed_from_now_<T>();
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, nanoseconds>::value ||
      std::is_same<T, milliseconds>::value ||
      std::is_same<T, seconds>::value>* = nullptr>
  void set_duration(ceph::mono_clock::duration& d, int64_t s) {
    set_duration_<T>(d, s);
  }

  template <typename T, typename S,
    typename std::enable_if_t<
      std::is_same<T, nanoseconds>::value ||
      std::is_same<T, milliseconds>::value ||
      std::is_same<T, seconds>::value &&
      std::is_same<S, coarse_real_time>::value ||
      std::is_same<S, coarse_mono_time>::value>* = nullptr>
  double get_count() {
    return get_count_<T, S>();
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
  T elapsed_from_start() {
    ut_end_ = ceph_clock_now_();
    return ut_end_ - ut_start_; 
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, utime_t>::value>* = nullptr>
  T elapsed_from_now() {
    ut_end_ = ceph_clock_now_();
    return ut_end_ - ut_start_;
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
  /** real_clock **/
  real_time rt_start_;
  real_time rt_now_;
  real_time rt_end_;
 
  /** coarse_real_clock **/
  coarse_real_time crt_start_;
  coarse_real_time crt_now_;
  coarse_real_time crt_end_;

  /** mono_clock **/
  mono_time mt_start_;
  mono_time mt_now_;
  mono_time mt_end_;

  /** coarse_mono_clock **/
  coarse_mono_time cmt_start_;
  coarse_mono_time cmt_now_;
  coarse_mono_time cmt_end_;

  /** Compatibility: utime_t **/
  utime_t ut_start_;
  utime_t ut_now_;
  utime_t ut_end_;

  /** get_time_point_ **/
  // real_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, real_time>::value>* = nullptr>
  auto get_time_point_() -> real_time {
    rt_now_ = ceph::real_clock::now();
    return rt_now_;
  }
  // corase_real_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, coarse_real_time>::value>* = nullptr>
  auto get_time_point_() -> coarse_real_time {
    crt_now_ = ceph::coarse_real_clock::now();
    return crt_now_;
  }
  // mono_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, mono_time>::value>* = nullptr>
  auto get_time_point_() -> mono_time {
    mt_now_ = ceph::mono_clock::now();
    return mt_now_;
  }
  // coarse_mono_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, coarse_mono_time>::value>* = nullptr>
  auto get_time_point_() -> coarse_mono_time {
    cmt_now_ = ceph::coarse_mono_clock::now();
    return cmt_now_;
  }

  /** elapsed_from_start_ **/
  // real_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, real_time>::value>* = nullptr>
  auto elapsed_from_start_() -> ceph::timespan {
    rt_end_ = ceph::real_clock::now();
    return rt_end_ - rt_start_;
  }
  // coarse_real_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, coarse_real_time>::value>* = nullptr>
  auto elapsed_from_start_() -> ceph::timespan {
    crt_end_ = ceph::coarse_real_clock::now();
    return crt_end_ - crt_start_;
  }
  // mono_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, mono_time>::value>* = nullptr>
  auto elapsed_from_start_() -> ceph::timespan {
    mt_end_ = ceph::mono_clock::now();
    return mt_end_ - mt_start_;
  }
  // coarse_mono_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, coarse_mono_time>::value>* = nullptr>
  auto elapsed_from_start_() -> ceph::timespan {
    cmt_end_ = ceph::coarse_mono_clock::now();
    return cmt_end_ - cmt_start_;
  }

  /** elapsed_from_now_ **/
  // real_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, real_time>::value>* = nullptr>
  auto elapsed_from_now_() -> ceph::timespan {
    rt_end_ = ceph::real_clock::now();
    return rt_end_ - rt_now_;
  }
  // coarse_real_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, coarse_real_time>::value>* = nullptr>
  auto elapsed_from_now_() -> ceph::timespan {
    crt_end_ = ceph::coarse_real_clock::now();
    return crt_end_ - crt_now_;
  }
  // mono_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, mono_time>::value>* = nullptr>
  auto elapsed_from_now_() -> ceph::timespan {
    mt_end_ = ceph::mono_clock::now();
    return mt_end_ - mt_now_;
  }
  // coarse_mono_clock
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, coarse_mono_time>::value>* = nullptr>
  auto elapsed_from_now_() -> ceph::timespan {
    cmt_end_ = ceph::coarse_mono_clock::now();
    return cmt_end_ - cmt_now_;
  }

  /** set_duration_ **/
  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, nanoseconds>::value>* = nullptr>
  void set_duration_(ceph::mono_clock::duration& d, int64_t s) {
    d = nanoseconds(s);
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, milliseconds>::value>* = nullptr>
  void set_duration_(ceph::mono_clock::duration& d, int64_t s) {
    d = milliseconds(s);
  }

  template <typename T,
    typename std::enable_if_t<
      std::is_same<T, seconds>::value>* = nullptr>
  void set_duration_(ceph::mono_clock::duration& d, int64_t s) {
    d = seconds(s);
  }

  template <typename T, typename S,
    typename std::enable_if_t<
      std::is_same<T, nanoseconds>::value &&
      std::is_same<S, coarse_real_time>::value>* = nullptr>
  double get_count_() {
    crt_end_ = ceph::coarse_real_clock::now();
    return duration_cast<T>(crt_end_ - crt_now_).count();
  }

  template <typename T, typename S,
    typename std::enable_if_t<
      std::is_same<T, milliseconds>::value &&
      std::is_same<S, coarse_real_time>::value>* = nullptr>
  double get_count_() {
    crt_end_ = ceph::coarse_real_clock::now();
    return duration_cast<T>(crt_end_ - crt_now_).count();
  }

  template <typename T, typename S,
    typename std::enable_if_t<
      std::is_same<T, seconds>::value &&
      std::is_same<S, coarse_real_time>::value>* = nullptr>
  double get_count_() {
    crt_end_ = ceph::coarse_real_clock::now();
    return duration_cast<T>(crt_end_ - crt_now_).count();
  }

  template <typename T, typename S,
    typename std::enable_if_t<
      std::is_same<T, nanoseconds>::value &&
      std::is_same<S, coarse_mono_time>::value>* = nullptr>
  double get_count_() {
    cmt_end_ = ceph::coarse_mono_clock::now();
    return duration_cast<T>(cmt_end_ - cmt_now_).count();
  }

  template <typename T, typename S,
    typename std::enable_if_t<
      std::is_same<T, milliseconds>::value &&
      std::is_same<S, coarse_mono_time>::value>* = nullptr>
  double get_count_() {
    cmt_end_ = ceph::coarse_mono_clock::now();
    return duration_cast<T>(cmt_end_ - cmt_now_).count();
  }

  template <typename T, typename S,
    typename std::enable_if_t<
      std::is_same<T, seconds>::value &&
      std::is_same<S, coarse_mono_time>::value>* = nullptr>
  double get_count_() {
    cmt_end_ = ceph::coarse_mono_clock::now();
    return duration_cast<T>(cmt_end_ - cmt_now_).count();
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

} // namespace ceph
#endif
