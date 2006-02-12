// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */


#ifndef __CLOCK_H
#define __CLOCK_H

#include <iostream>
#include <iomanip>

#include <sys/time.h>
#include <time.h>

#include <list>
using namespace std;


// --------
// utime_t

class utime_t {
 private:
  struct timeval tv;

  void normalize() {
	if (tv.tv_usec > 1000*1000) {
	  tv.tv_sec += tv.tv_usec / (1000*1000);
	  tv.tv_usec %= 1000*1000;
	}
  }

 public:
  // cons
  utime_t() { tv.tv_sec = 0; tv.tv_usec = 0; normalize(); }
  utime_t(time_t s, int u) { tv.tv_sec = s; tv.tv_usec = u; normalize(); }
  
  // accessors
  time_t        sec()  const { return tv.tv_sec; } 
  long          usec() const { return tv.tv_usec; }
  int           nsec() const { return tv.tv_usec*1000; }

  // ref accessors/modifiers
  time_t&         sec_ref()  { return tv.tv_sec; } 
  long&           usec_ref() { return tv.tv_usec; }

  struct timeval& timeval()  { return tv; }

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

inline bool operator>(const utime_t& a, const utime_t& b)
{
  return (a.sec() > b.sec()) || (a.sec() == b.sec() && a.usec() > b.usec());
}
inline bool operator<(const utime_t& a, const utime_t& b)
{
  return (a.sec() < b.sec()) || (a.sec() == b.sec() && a.usec() < b.usec());
}

// ostream
inline ostream& operator<<(ostream& out, const utime_t& t)
{
  //return out << t.sec() << "." << t.usec();
  out << (long)t.sec() << ".";
  out.setf(ios::right);
  out.fill('0');
  out << setw(6) << t.usec();
  out.unsetf(ios::right);
  return out;
  
  //return out << (long)t.sec << "." << ios::setf(ios::right) << ios::fill('0') << t.usec() << ios::usetf();
}




// -- clock --
class Clock {
 protected:
  //utime_t start_offset;
  //utime_t abs_last;
  utime_t last;

 public:
  Clock() {
	// set offset
	//start_offset = now();
  }

  // relative time (from startup)
  const utime_t& now() {
	gettimeofday(&last.timeval(), NULL);
	//last = abs_last - start_offset;
	return last;
  }

  const utime_t& recent_now() {
	return last;
  }

  // absolute time
  time_t gettime() {
	now();
	return last.sec();
  }

};

extern Clock g_clock;

#endif
