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
#include "include/utime.h"
#include <time.h>

struct ceph_clock {
public:
  ceph_clock() : start_(now_()) {}

  utime_t now() {
    return now_();
  }

  void reset() {
    reset_();
  }

  utime_t elapsed() {
    return elapsed_();
  }

private:
  utime_t now_() {
#if defined(CLOCK_REALTIME)
    struct timespec tp;
    clock_gettime(CLOCK_REALTIME, &tp);
    utime_t n(tp);
#else
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    utime_t n(&tv);
#endif
    return n;
  } 

  void reset_() {
    start_ = now_();
  }

  utime_t elapsed_() {
    return now_() - start_;
  }

  utime_t start_;
};

utime_t ceph_clock_now();
#endif
