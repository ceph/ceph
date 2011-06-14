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


#include "common/config.h"
#include "common/Clock.h"

#include <time.h>

utime_t ceph_clock_now(CephContext *cct)
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  utime_t n(&tv);
  n += cct->_conf->clock_offset;
  return n;
}

time_t ceph_clock_gettime(CephContext *cct)
{
  time_t ret = time(NULL);
  ret += ((time_t)cct->_conf->clock_offset);
  return ret;
}

// old global clock stuff
// TODO: remove
Clock g_clock;

Clock::Clock() {
}

Clock::~Clock() {
}

void Clock::make_timespec(utime_t& t, struct timespec *ts) {
  utime_t time = t;

  memset(ts, 0, sizeof(*ts));
  ts->tv_sec = time.sec();
  ts->tv_nsec = time.nsec();
}

utime_t Clock::now() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  utime_t n(&tv);
  n += g_conf->clock_offset;
  return n;
}

time_t Clock::gettime() {
  return now().sec();
}
