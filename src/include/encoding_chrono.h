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

#pragma once

#include <chrono>

#include "encoding.h"

namespace std {

// Time, since the templates are defined in std::chrono. The default encodings
// for time_point and duration are backward-compatible with utime_t, but
// truncate seconds to 32 bits so are not guaranteed to round-trip.

template<clock_with_timespec Clock, typename Duration>
void encode(const std::chrono::time_point<Clock, Duration>& t,
	    ceph::bufferlist &bl) {
  using ceph::encode;

  auto ts = Clock::to_timespec(t);
  // A 32 bit count of seconds causes me vast unhappiness.
  uint32_t s = ts.tv_sec;
  uint32_t ns = ts.tv_nsec;
  encode(s, bl);
  encode(ns, bl);
}

template<clock_with_timespec Clock, typename Duration>
void decode(std::chrono::time_point<Clock, Duration>& t,
	    bufferlist::const_iterator& p) {
  using ceph::decode;

  uint32_t s;
  uint32_t ns;
  decode(s, p);
  decode(ns, p);
  struct timespec ts = {
    static_cast<time_t>(s),
    static_cast<long int>(ns)};

  t = Clock::from_timespec(ts);
}

template<std::integral Rep, typename Period>
void encode(const std::chrono::duration<Rep, Period>& d,
	    ceph::bufferlist &bl) {
  using namespace std::chrono;
  using ceph::encode;
  int32_t s = duration_cast<seconds>(d).count();
  int32_t ns = (duration_cast<nanoseconds>(d) % seconds(1)).count();
  encode(s, bl);
  encode(ns, bl);
}

template<std::integral Rep, typename Period>
void decode(std::chrono::duration<Rep, Period>& d,
	    bufferlist::const_iterator& p) {
  using ceph::decode;

  int32_t s;
  int32_t ns;
  decode(s, p);
  decode(ns, p);
  d = std::chrono::seconds(s) + std::chrono::nanoseconds(ns);
}

} // namespace std

namespace ceph {

// Provide encodings for chrono::time_point and duration that use
// the underlying representation so are guaranteed to round-trip.

template <std::integral Rep, typename Period>
void round_trip_encode(const std::chrono::duration<Rep, Period>& d,
                       ceph::bufferlist &bl) {
  const Rep r = d.count();
  encode(r, bl);
}

template <std::integral Rep, typename Period>
void round_trip_decode(std::chrono::duration<Rep, Period>& d,
                       bufferlist::const_iterator& p) {
  Rep r;
  decode(r, p);
  d = std::chrono::duration<Rep, Period>(r);
}

template <typename Clock, typename Duration>
void round_trip_encode(const std::chrono::time_point<Clock, Duration>& t,
                       ceph::bufferlist &bl) {
  round_trip_encode(t.time_since_epoch(), bl);
}

template <typename Clock, typename Duration>
void round_trip_decode(std::chrono::time_point<Clock, Duration>& t,
                       bufferlist::const_iterator& p) {
  Duration dur;
  round_trip_decode(dur, p);
  t = std::chrono::time_point<Clock, Duration>(dur);
}

} // namespace ceph
