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

static inline utime_t ceph_clock_now()
{
#if defined(__linux__)
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

#endif
