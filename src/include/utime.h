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

#ifndef __UTIME_H
#define __UTIME_H

#include <math.h>
#include <sys/time.h>
#include <time.h>
#include "ceph_fs.h"

#include "buffer.h"
#include "encodable.h"

// --------
// utime_t

class utime_t {
  struct {
    __u32 tv_sec, tv_usec;
  } tv;

  friend class Clock;
 
 public:
  void normalize() {
    if (tv.tv_usec > 1000*1000) {
      tv.tv_sec += tv.tv_usec / (1000*1000);
      tv.tv_usec %= 1000*1000;
    }
  }

  // cons
  utime_t() { tv.tv_sec = 0; tv.tv_usec = 0; normalize(); }
  //utime_t(time_t s) { tv.tv_sec = s; tv.tv_usec = 0; }
  utime_t(time_t s, int u) { tv.tv_sec = s; tv.tv_usec = u; normalize(); }
  utime_t(const struct ceph_timeval &v) {
    decode_timeval(&v);
  }
  utime_t(const struct timeval &v) {
    set_from_timeval(&v);
  }
  utime_t(const struct timeval *v) {
    set_from_timeval(v);
  }

  void set_from_double(double d) { 
    tv.tv_sec = (__u32)trunc(d);
    tv.tv_usec = (__u32)((d - tv.tv_sec) / (double)1000000.0);
  }

  // accessors
  time_t        sec()  const { return tv.tv_sec; } 
  long          usec() const { return tv.tv_usec; }
  int           nsec() const { return tv.tv_usec*1000; }

  // ref accessors/modifiers
  __u32&         sec_ref()  { return tv.tv_sec; }
  __u32&         usec_ref() { return tv.tv_usec; }

  void copy_to_timeval(struct timeval *v) const {
    v->tv_sec = tv.tv_sec;
    v->tv_usec = tv.tv_usec;
  }
  void set_from_timeval(const struct timeval *v) {
    tv.tv_sec = v->tv_sec;
    tv.tv_usec = v->tv_usec;
  }

  void encode_timeval(struct ceph_timeval *t) const {
    t->tv_sec = cpu_to_le32(tv.tv_sec);
    t->tv_usec = cpu_to_le32(tv.tv_usec);
  }
  void decode_timeval(const struct ceph_timeval *t) {
    tv.tv_sec = le32_to_cpu(t->tv_sec);
    tv.tv_usec = le32_to_cpu(t->tv_usec);
  }
  void _encode(bufferlist &bl) {
    ::_encode_simple(tv.tv_sec, bl);
    ::_encode_simple(tv.tv_usec, bl);
  }
  void _decode(bufferlist &bl, int& off) {
    ::_decode(tv.tv_sec, bl, off);
    ::_decode(tv.tv_usec, bl, off);
  }

  // cast to double
  operator double() {
    return (double)sec() + ((double)usec() / 1000000.0L);
  }
};

// arithmetic operators
inline utime_t operator+(const utime_t& l, const utime_t& r) {
  return utime_t( l.sec() + r.sec() + (l.usec()+r.usec())/1000000L,
                  (l.usec()+r.usec())%1000000L );
}
inline utime_t& operator+=(utime_t& l, const utime_t& r) {
  l.sec_ref() += r.sec() + (l.usec()+r.usec())/1000000L;
  l.usec_ref() += r.usec();
  l.usec_ref() %= 1000000L;
  return l;
}
inline utime_t& operator+=(utime_t& l, double f) {
  double fs = trunc(f);
  double us = (f - fs) * (double)1000000.0;
  l.sec_ref() += (long)fs;
  l.usec_ref() += (long)us;
  l.normalize();
  return l;
}

inline utime_t operator-(const utime_t& l, const utime_t& r) {
  return utime_t( l.sec() - r.sec() - (l.usec()<r.usec() ? 1:0),
                  l.usec() - r.usec() + (l.usec()<r.usec() ? 1000000:0) );
}
inline utime_t& operator-=(utime_t& l, const utime_t& r) {
  l.sec_ref() -= r.sec();
  if (l.usec() >= r.usec())
    l.usec_ref() -= r.usec();
  else {
    l.usec_ref() += 1000000L - r.usec();
    l.sec_ref()--;
  }
  return l;
}
inline utime_t& operator-=(utime_t& l, double f) {
  l += -f;
  return l;
}

inline bool operator>(const utime_t& a, const utime_t& b)
{
  return (a.sec() > b.sec()) || (a.sec() == b.sec() && a.usec() > b.usec());
}
inline bool operator<(const utime_t& a, const utime_t& b)
{
  return (a.sec() < b.sec()) || (a.sec() == b.sec() && a.usec() < b.usec());
}

// ostream
inline std::ostream& operator<<(std::ostream& out, const utime_t& t)
{
  out.setf(std::ios::right);
  out.fill('0');
  if (t.sec() < ((time_t)(60*60*24*365*10))) {
    // raw seconds.  this looks like a relative time.
    out << (long)t.sec();
  } else {
    // localtime.  this looks like an absolute time.
    struct tm bdt;
    time_t tt = t.sec();
    localtime_r(&tt, &bdt);
    out << std::setw(2) << (bdt.tm_year-100)  // 2007 -> '07'
	<< std::setw(2) << (bdt.tm_mon+1)
	<< std::setw(2) << bdt.tm_mday
	<< "."
	<< std::setw(2) << bdt.tm_hour
	<< std::setw(2) << bdt.tm_min
	<< std::setw(2) << bdt.tm_sec;
  }
  out << ".";
  out << std::setw(6) << t.usec();
  out.unsetf(std::ios::right);
  return out;
}

#endif
