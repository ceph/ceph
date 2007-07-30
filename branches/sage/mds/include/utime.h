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

// --------
// utime_t

typedef struct timeval _utime_t;

class utime_t {
 private:
  struct timeval tv;

  struct timeval& timeval()  { return tv; }
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
  utime_t(const _utime_t &v) : tv(v) {}
  /*
  utime_t(double d) { 
    tv.tv_sec = (time_t)trunc(d);
    tv.tv_usec = (__suseconds_t)((d - tv.tv_sec) / (double)1000000.0);
  }
  */

  // accessors
  time_t        sec()  const { return tv.tv_sec; } 
  long          usec() const { return tv.tv_usec; }
  int           nsec() const { return tv.tv_usec*1000; }

  // ref accessors/modifiers
  time_t&         sec_ref()  { return tv.tv_sec; }
  // FIXME: tv.tv_usec is a __darwin_suseconds_t on Darwin.
  // is just casting it to long& OK? 
  long&           usec_ref() { return (long&) tv.tv_usec; }

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
	<< std::setw(2) << bdt.tm_mon
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
