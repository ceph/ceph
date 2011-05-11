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
#include "Clock.h"

// public
Clock g_clock;

Clock::Clock() {
}

Clock::~Clock() {
}

utime_t Clock::now() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  utime_t n(&tv);
  n += g_conf.clock_offset;
  if (n < last) {
    //derr << "WARNING: clock jumped backwards from " << last << " to " << n << dendl;
    n = last;    // clock jumped backwards!
  } else
    last = n;
  return n;
}
