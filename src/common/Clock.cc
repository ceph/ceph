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


#include "common/Clock.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "include/utime.h"

#include <time.h>

utime_t ceph_clock_now(CephContext *cct)
{
#if defined(__linux__)
  struct timespec tp;
  clock_gettime(CLOCK_REALTIME, &tp);
  utime_t n(tp);
#else
  struct timeval tv;
  gettimeofday(&tv, NULL);
  utime_t n(&tv);
#endif
  if (cct)
    n += cct->_conf->clock_offset;
  return n;
}

time_t ceph_clock_gettime(CephContext *cct)
{
  time_t ret = time(NULL);
  if (cct)
    ret += ((time_t)cct->_conf->clock_offset);
  return ret;
}
