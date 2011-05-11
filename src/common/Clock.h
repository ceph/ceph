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

#ifndef CEPH_CLOCK_H
#define CEPH_CLOCK_H

#include <iomanip>
#include <iostream>
#include <sys/time.h>
#include <time.h>

#include "include/utime.h"

class Clock {
 protected:
  utime_t last;

 public:
  Clock();
  ~Clock();

  utime_t now();

  utime_t recent_now() {
    return last;
  }

  void make_timespec(utime_t& t, struct timespec *ts) {
    utime_t time = t;

    memset(ts, 0, sizeof(*ts));
    ts->tv_sec = time.sec();
    ts->tv_nsec = time.nsec();
  }

  // absolute time
  time_t gettime() {
    return now().sec();
  }
};

extern Clock g_clock;

#endif
