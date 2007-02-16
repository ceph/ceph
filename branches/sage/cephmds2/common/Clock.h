// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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



#ifndef __CLOCK_H
#define __CLOCK_H

#include <iostream>
#include <iomanip>

#include <sys/time.h>
#include <time.h>
#include <math.h>

#include "Mutex.h"


// --------
// utime_t

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
  utime_t(time_t s, int u) { tv.tv_sec = s; tv.tv_usec = u; normalize(); }
  
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
  double us = (f - fs) / (double)1000000.0;
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
  //return out << t.sec() << "." << t.usec();
  out << (long)t.sec() << ".";
  out.setf(std::ios::right);
  out.fill('0');
  out << std::setw(6) << t.usec();
  out.unsetf(std::ios::right);
  return out;
  
  //return out << (long)t.sec << "." << ios::setf(ios::right) << ios::fill('0') << t.usec() << ios::usetf();
}




// -- clock --
class Clock {
 protected:
  //utime_t start_offset;
  //utime_t abs_last;
  utime_t last;
  utime_t zero;

  Mutex lock;

 public:
  Clock() {
    // set offset
    tare();
  }

  // real time.
  utime_t real_now() {
    utime_t realnow = now();
    realnow += zero;
    //gettimeofday(&realnow.timeval(), NULL);
    return realnow;
  }

  // relative time (from startup)
  void tare() {
    gettimeofday(&zero.timeval(), NULL);
  }
  utime_t now() {
    //lock.Lock();  
    utime_t n;
    gettimeofday(&n.timeval(), NULL);
    n -= zero;
    if (n < last) {
      //std::cerr << "WARNING: clock jumped backwards from " << last << " to " << n << std::endl;
      n = last;    // clock jumped backwards!
    } else
      last = n;
    //lock.Unlock();
    return n;
  }
  utime_t recent_now() {
    return last;
  }

  void realify(utime_t& t) {
    t += zero;
  }

  void make_timespec(utime_t& t, struct timespec *ts) {
    utime_t real = t;
    realify(real);

    memset(ts, 0, sizeof(*ts));
    ts->tv_sec = real.sec();
    ts->tv_nsec = real.nsec();
  }



  // absolute time
  time_t gettime() {
    return real_now().sec();
  }

};

extern Clock g_clock;

#endif
