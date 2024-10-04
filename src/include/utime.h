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

#ifndef CEPH_UTIME_H
#define CEPH_UTIME_H

#include <math.h>
#include <sys/time.h>
#include <time.h>

#ifdef WITH_CRIMSON
#include <seastar/core/lowres_clock.hh>
#endif

#include "common/ceph_time.h"
#include "include/denc.h"
#include "include/encoding.h" // for WRITE_CLASS_ENCODER()
#include "include/rados.h" // for struct ceph_timespec

#include <iosfwd>

namespace ceph { class Formatter; }

// --------
// utime_t

inline __u32 cap_to_u32_max(__u64 t) {
  return std::min(t, (__u64)std::numeric_limits<uint32_t>::max());
}
/* WARNING: If add member in utime_t, please make sure the encode/decode function
 * work well. For little-endian machine, we should make sure there is no padding
 * in 32-bit machine and 64-bit machine.
 * You should also modify the padding_check function.
 */
class utime_t {
public:
  struct {
    __u32 tv_sec, tv_nsec;
  } tv;

 public:
  bool is_zero() const {
    return (tv.tv_sec == 0) && (tv.tv_nsec == 0);
  }

  constexpr void normalize() {
    if (tv.tv_nsec > 1000000000ul) {
      tv.tv_sec = cap_to_u32_max(tv.tv_sec + tv.tv_nsec / (1000000000ul));
      tv.tv_nsec %= 1000000000ul;
    }
  }

  static inline constexpr utime_t max() {
    return utime_t{time_t{std::numeric_limits<uint32_t>::max()}, 999'999'999ul};
  }

  // cons
  constexpr utime_t() { tv.tv_sec = 0; tv.tv_nsec = 0; }
  constexpr utime_t(time_t s, int n) { tv.tv_sec = s; tv.tv_nsec = n; normalize(); }
  constexpr utime_t(const struct ceph_timespec &v) {
    decode_timeval(&v);
  }
  constexpr utime_t(const struct timespec v)
  {
    // NOTE: this is used by ceph_clock_now() so should be kept
    // as thin as possible.
    tv.tv_sec = v.tv_sec;
    tv.tv_nsec = v.tv_nsec;
  }
  // conversion from ceph::real_time/coarse_real_time
  template <typename Clock, typename std::enable_if_t<
            ceph::converts_to_timespec_v<Clock>>* = nullptr>
  explicit utime_t(const std::chrono::time_point<Clock>& t)
    : utime_t(Clock::to_timespec(t)) {} // forward to timespec ctor

  template<class Rep, class Period>
  explicit utime_t(const std::chrono::duration<Rep, Period>& dur) {
    using common_t = std::common_type_t<Rep, int>;
    tv.tv_sec = std::max<common_t>(std::chrono::duration_cast<std::chrono::seconds>(dur).count(), 0);
    tv.tv_nsec = std::max<common_t>((std::chrono::duration_cast<std::chrono::nanoseconds>(dur) %
				     std::chrono::seconds(1)).count(), 0);
  }
#ifdef WITH_CRIMSON
  explicit utime_t(const seastar::lowres_system_clock::time_point& t) {
    tv.tv_sec = std::chrono::duration_cast<std::chrono::seconds>(
        t.time_since_epoch()).count();
    tv.tv_nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(
        t.time_since_epoch() % std::chrono::seconds(1)).count();
  }
  explicit operator seastar::lowres_system_clock::time_point() const noexcept {
    using clock_t = seastar::lowres_system_clock;
    return clock_t::time_point{std::chrono::duration_cast<clock_t::duration>(
      std::chrono::seconds{tv.tv_sec} + std::chrono::nanoseconds{tv.tv_nsec})};
  }
#endif

  utime_t(const struct timeval &v) {
    set_from_timeval(&v);
  }
  utime_t(const struct timeval *v) {
    set_from_timeval(v);
  }
  void to_timespec(struct timespec *ts) const {
    ts->tv_sec = tv.tv_sec;
    ts->tv_nsec = tv.tv_nsec;
  }
  void set_from_double(double d) {
    tv.tv_sec = (__u32)trunc(d);
    tv.tv_nsec = (__u32)((d - (double)tv.tv_sec) * 1000000000.0);
  }

  ceph::real_time to_real_time() const {
    ceph_timespec ts;
    encode_timeval(&ts);
    return ceph::real_clock::from_ceph_timespec(ts);
  }

  // accessors
  constexpr time_t  sec()  const { return tv.tv_sec; }
  constexpr long    usec() const { return tv.tv_nsec/1000; }
  constexpr int     nsec() const { return tv.tv_nsec; }

  // ref accessors/modifiers
  __u32&         sec_ref()  { return tv.tv_sec; }
  __u32&         nsec_ref() { return tv.tv_nsec; }

  uint64_t to_nsec() const {
    return (uint64_t)tv.tv_nsec + (uint64_t)tv.tv_sec * 1000000000ull;
  }
  uint64_t to_msec() const {
    return (uint64_t)tv.tv_nsec / 1000000ull + (uint64_t)tv.tv_sec * 1000ull;
  }

  void copy_to_timeval(struct timeval *v) const {
    v->tv_sec = tv.tv_sec;
    v->tv_usec = tv.tv_nsec/1000;
  }
  void set_from_timeval(const struct timeval *v) {
    tv.tv_sec = v->tv_sec;
    tv.tv_nsec = v->tv_usec*1000;
  }
  void padding_check() {
    static_assert(
      sizeof(utime_t) ==
        sizeof(tv.tv_sec) +
        sizeof(tv.tv_nsec)
      ,
      "utime_t have padding");
  }
  void encode(ceph::buffer::list &bl) const {
#if defined(CEPH_LITTLE_ENDIAN)
    bl.append((char *)(this), sizeof(__u32) + sizeof(__u32));
#else
    using ceph::encode;
    encode(tv.tv_sec, bl);
    encode(tv.tv_nsec, bl);
#endif
  }
  void decode(ceph::buffer::list::const_iterator &p) {
#if defined(CEPH_LITTLE_ENDIAN)
    p.copy(sizeof(__u32) + sizeof(__u32), (char *)(this));
#else
    using ceph::decode;
    decode(tv.tv_sec, p);
    decode(tv.tv_nsec, p);
#endif
  }

  DENC(utime_t, v, p) {
    denc(v.tv.tv_sec, p);
    denc(v.tv.tv_nsec, p);
  }

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<utime_t*>& o);
  
  void encode_timeval(struct ceph_timespec *t) const {
    t->tv_sec = tv.tv_sec;
    t->tv_nsec = tv.tv_nsec;
  }
  constexpr void decode_timeval(const struct ceph_timespec *t) {
    tv.tv_sec = t->tv_sec;
    tv.tv_nsec = t->tv_nsec;
  }

  utime_t round_to_minute() {
    struct tm bdt;
    time_t tt = sec();
    localtime_r(&tt, &bdt);
    bdt.tm_sec = 0;
    tt = mktime(&bdt);
    return utime_t(tt, 0);
  }

  utime_t round_to_hour() {
    struct tm bdt;
    time_t tt = sec();
    localtime_r(&tt, &bdt);
    bdt.tm_sec = 0;
    bdt.tm_min = 0;
    tt = mktime(&bdt);
    return utime_t(tt, 0);
  }

  utime_t round_to_day() {
    struct tm bdt;
    time_t tt = sec();
    localtime_r(&tt, &bdt);
    bdt.tm_sec = 0;
    bdt.tm_min = 0;
    bdt.tm_hour = 0;
    tt = mktime(&bdt);
    return utime_t(tt, 0);
  }

  // cast to double
  constexpr operator double() const {
    return (double)sec() + ((double)nsec() / 1000000000.0f);
  }
  operator ceph_timespec() const {
    ceph_timespec ts;
    ts.tv_sec = sec();
    ts.tv_nsec = nsec();
    return ts;
  }

  void sleep() const {
    struct timespec ts;
    to_timespec(&ts);
    nanosleep(&ts, NULL);
  }

  // output
  std::ostream& gmtime(std::ostream& out, bool legacy_form=false) const;

  // output
  std::ostream& gmtime_nsec(std::ostream& out) const;

  // output
  std::ostream& asctime(std::ostream& out) const;

  std::ostream& localtime(std::ostream& out, bool legacy_form=false) const;

  static int invoke_date(const std::string& date_str, utime_t *result);

  static int parse_date(const std::string& date, uint64_t *epoch, uint64_t *nsec,
                        std::string *out_date=nullptr,
			std::string *out_time=nullptr);

  bool parse(const std::string& s) {
    uint64_t epoch, nsec;
    int r = parse_date(s, &epoch, &nsec);
    if (r < 0) {
      return false;
    }
    *this = utime_t(epoch, nsec);
    return true;
  }
};
WRITE_CLASS_ENCODER(utime_t)
WRITE_CLASS_DENC(utime_t)

// arithmetic operators
inline utime_t operator+(const utime_t& l, const utime_t& r) {
  __u64 sec = (__u64)l.sec() + r.sec();
  return utime_t(cap_to_u32_max(sec), l.nsec() + r.nsec());
}
inline utime_t& operator+=(utime_t& l, const utime_t& r) {
  l.sec_ref() = cap_to_u32_max((__u64)l.sec() + r.sec());
  l.nsec_ref() += r.nsec();
  l.normalize();
  return l;
}
inline utime_t& operator+=(utime_t& l, double f) {
  double fs = trunc(f);
  double ns = (f - fs) * 1000000000.0;
  l.sec_ref() = cap_to_u32_max(l.sec() + (__u64)fs);
  l.nsec_ref() += (long)ns;
  l.normalize();
  return l;
}

inline utime_t operator-(const utime_t& l, const utime_t& r) {
  return utime_t( l.sec() - r.sec() - (l.nsec()<r.nsec() ? 1:0),
                  l.nsec() - r.nsec() + (l.nsec()<r.nsec() ? 1000000000:0) );
}
inline utime_t& operator-=(utime_t& l, const utime_t& r) {
  l.sec_ref() -= r.sec();
  if (l.nsec() >= r.nsec())
    l.nsec_ref() -= r.nsec();
  else {
    l.nsec_ref() += 1000000000L - r.nsec();
    l.sec_ref()--;
  }
  return l;
}
inline utime_t& operator-=(utime_t& l, double f) {
  double fs = trunc(f);
  double ns = (f - fs) * 1000000000.0;
  l.sec_ref() -= (long)fs;
  long nsl = (long)ns;
  if (nsl) {
    l.sec_ref()--;
    l.nsec_ref() = 1000000000L + l.nsec_ref() - nsl;
  }
  l.normalize();
  return l;
}


// comparators
inline bool operator>(const utime_t& a, const utime_t& b)
{
  return (a.sec() > b.sec()) || (a.sec() == b.sec() && a.nsec() > b.nsec());
}
inline bool operator<=(const utime_t& a, const utime_t& b)
{
  return !(operator>(a, b));
}
inline bool operator<(const utime_t& a, const utime_t& b)
{
  return (a.sec() < b.sec()) || (a.sec() == b.sec() && a.nsec() < b.nsec());
}
inline bool operator>=(const utime_t& a, const utime_t& b)
{
  return !(operator<(a, b));
}

inline bool operator==(const utime_t& a, const utime_t& b)
{
  return a.sec() == b.sec() && a.nsec() == b.nsec();
}
inline bool operator!=(const utime_t& a, const utime_t& b)
{
  return a.sec() != b.sec() || a.nsec() != b.nsec();
}


// output

// ostream
inline std::ostream& operator<<(std::ostream& out, const utime_t& t)
{
  return t.localtime(out);
}

inline std::string utimespan_str(const utime_t& age) {
  auto age_ts = ceph::timespan(age.nsec()) + std::chrono::seconds(age.sec());
  return ceph::timespan_str(age_ts);
}

#endif
