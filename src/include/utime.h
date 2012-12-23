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

#include "include/types.h"


// --------
// utime_t

class utime_t {
public:
  struct {
    __u32 tv_sec, tv_nsec;
  } tv;

  friend class Clock;
 
 public:
  bool is_zero() {
    return (tv.tv_sec == 0) && (tv.tv_nsec == 0);
  }
  void normalize() {
    if (tv.tv_nsec > 1000000000ul) {
      tv.tv_sec += tv.tv_nsec / (1000000000ul);
      tv.tv_nsec %= 1000000000ul;
    }
  }

  // cons
  utime_t() { tv.tv_sec = 0; tv.tv_nsec = 0; }
  utime_t(time_t s, int n) { tv.tv_sec = s; tv.tv_nsec = n; normalize(); }
  utime_t(const struct ceph_timespec &v) {
    decode_timeval(&v);
  }
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
    tv.tv_nsec = (__u32)((d - (double)tv.tv_sec) * (double)1000000000.0);
  }

  // accessors
  time_t        sec()  const { return tv.tv_sec; } 
  long          usec() const { return tv.tv_nsec/1000; }
  int           nsec() const { return tv.tv_nsec; }

  // ref accessors/modifiers
  __u32&         sec_ref()  { return tv.tv_sec; }
  __u32&         nsec_ref() { return tv.tv_nsec; }

  uint64_t to_nsec() const {
    return (uint64_t)tv.tv_nsec + (uint64_t)tv.tv_sec * 1000000000ull;
  }

  void copy_to_timeval(struct timeval *v) const {
    v->tv_sec = tv.tv_sec;
    v->tv_usec = tv.tv_nsec/1000;
  }
  void set_from_timeval(const struct timeval *v) {
    tv.tv_sec = v->tv_sec;
    tv.tv_nsec = v->tv_usec*1000;
  }

  void encode(bufferlist &bl) const {
    ::encode(tv.tv_sec, bl);
    ::encode(tv.tv_nsec, bl);
  }
  void decode(bufferlist::iterator &p) {
    ::decode(tv.tv_sec, p);
    ::decode(tv.tv_nsec, p);
  }

  void encode_timeval(struct ceph_timespec *t) const {
    t->tv_sec = tv.tv_sec;
    t->tv_nsec = tv.tv_nsec;
  }
  void decode_timeval(const struct ceph_timespec *t) {
    tv.tv_sec = t->tv_sec;
    tv.tv_nsec = t->tv_nsec;
  }

  utime_t round_to_minute() {
    struct tm bdt;
    time_t tt = sec();
    gmtime_r(&tt, &bdt);
    bdt.tm_sec = 0;
    tt = mktime(&bdt);
    return utime_t(tt, 0);
  }

  utime_t round_to_hour() {
    struct tm bdt;
    time_t tt = sec();
    gmtime_r(&tt, &bdt);
    bdt.tm_sec = 0;
    bdt.tm_min = 0;
    tt = mktime(&bdt);
    return utime_t(tt, 0);
  }

  // cast to double
  operator double() const {
    return (double)sec() + ((double)nsec() / 1000000000.0L);
  }
  operator ceph_timespec() {
    ceph_timespec ts;
    ts.tv_sec = sec();
    ts.tv_nsec = nsec();
    return ts;
  }

  void sleep() {
    struct timespec ts = { tv.tv_sec, tv.tv_nsec };
    nanosleep(&ts, &ts);
  }

  // output
  ostream& gmtime(ostream& out) const {
    out.setf(std::ios::right);
    char oldfill = out.fill();
    out.fill('0');
    if (sec() < ((time_t)(60*60*24*365*10))) {
      // raw seconds.  this looks like a relative time.
      out << (long)sec() << "." << std::setw(6) << usec();
    } else {
      // localtime.  this looks like an absolute time.
      //  aim for http://en.wikipedia.org/wiki/ISO_8601
      struct tm bdt;
      time_t tt = sec();
      gmtime_r(&tt, &bdt);
      out << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
	  << '-' << std::setw(2) << (bdt.tm_mon+1)
	  << '-' << std::setw(2) << bdt.tm_mday
	  << ' '
	  << std::setw(2) << bdt.tm_hour
	  << ':' << std::setw(2) << bdt.tm_min
	  << ':' << std::setw(2) << bdt.tm_sec;
      out << "." << std::setw(6) << usec();
      out << "Z";
    }
    out.fill(oldfill);
    out.unsetf(std::ios::right);
    return out;
  }

  ostream& localtime(ostream& out) const {
    out.setf(std::ios::right);
    char oldfill = out.fill();
    out.fill('0');
    if (sec() < ((time_t)(60*60*24*365*10))) {
      // raw seconds.  this looks like a relative time.
      out << (long)sec() << "." << std::setw(6) << usec();
    } else {
      // localtime.  this looks like an absolute time.
      //  aim for http://en.wikipedia.org/wiki/ISO_8601
      struct tm bdt;
      time_t tt = sec();
      localtime_r(&tt, &bdt);
      out << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
	  << '-' << std::setw(2) << (bdt.tm_mon+1)
	  << '-' << std::setw(2) << bdt.tm_mday
	  << ' '
	  << std::setw(2) << bdt.tm_hour
	  << ':' << std::setw(2) << bdt.tm_min
	  << ':' << std::setw(2) << bdt.tm_sec;
      out << "." << std::setw(6) << usec();
      //out << '_' << bdt.tm_zone;
    }
    out.fill(oldfill);
    out.unsetf(std::ios::right);
    return out;
  }

  int sprintf(char *out, int outlen) const {
    struct tm bdt;
    time_t tt = sec();
    localtime_r(&tt, &bdt);

    return snprintf(out, outlen,
		    "%04d-%02d-%02d %02d:%02d:%02d.%06ld",
		    bdt.tm_year + 1900, bdt.tm_mon + 1, bdt.tm_mday,
		    bdt.tm_hour, bdt.tm_min, bdt.tm_sec, usec());
  }
};
WRITE_CLASS_ENCODER(utime_t)


// arithmetic operators
inline utime_t operator+(const utime_t& l, const utime_t& r) {
  return utime_t( l.sec() + r.sec() + (l.nsec()+r.nsec())/1000000000L,
                  (l.nsec()+r.nsec())%1000000000L );
}
inline utime_t& operator+=(utime_t& l, const utime_t& r) {
  l.sec_ref() += r.sec() + (l.nsec()+r.nsec())/1000000000L;
  l.nsec_ref() += r.nsec();
  l.nsec_ref() %= 1000000000L;
  return l;
}
inline utime_t& operator+=(utime_t& l, double f) {
  double fs = trunc(f);
  double ns = (f - fs) * (double)1000000000.0;
  l.sec_ref() += (long)fs;
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
  double ns = (f - fs) * (double)1000000000.0;
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

#endif
