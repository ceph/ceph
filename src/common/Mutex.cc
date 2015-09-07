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
#include <string>

#include "common/Mutex.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "include/utime.h"
#include "common/Clock.h"

Mutex::Mutex(const char *n, bool r, bool ld,
	     bool bt,
	     CephContext *cct) :
  name(n), id(-1), recursive(r), lockdep(ld), backtrace(bt),
  nlock(0), locked_by(0), cct(cct), logger(0)
{
  if (cct) {
    PerfCountersBuilder b(cct, string("mutex-") + name,
			  l_mutex_first, l_mutex_last);
    b.add_time_avg(l_mutex_wait, "wait");
    logger = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
    logger->set(l_mutex_wait, 0);
  }
  if (recursive) {
    // Mutexes of type PTHREAD_MUTEX_RECURSIVE do all the same checks as
    // mutexes of type PTHREAD_MUTEX_ERRORCHECK.
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&_m,&attr);
    pthread_mutexattr_destroy(&attr);
    if (g_lockdep)
      _register();
  }
  else if (lockdep) {
    // If the mutex type is PTHREAD_MUTEX_ERRORCHECK, then error checking
    // shall be provided. If a thread attempts to relock a mutex that it
    // has already locked, an error shall be returned. If a thread
    // attempts to unlock a mutex that it has not locked or a mutex which
    // is unlocked, an error shall be returned.
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_ERRORCHECK);
    pthread_mutex_init(&_m, &attr);
    pthread_mutexattr_destroy(&attr);
    if (g_lockdep)
      _register();
  }
  else {
    // If the mutex type is PTHREAD_MUTEX_NORMAL, deadlock detection
    // shall not be provided. Attempting to relock the mutex causes
    // deadlock. If a thread attempts to unlock a mutex that  it  has not
    // locked or a mutex which is unlocked, undefined behavior results.
    pthread_mutex_init(&_m, NULL);
  }
}

Mutex::~Mutex() {
  assert(nlock == 0);
  pthread_mutex_destroy(&_m);
  if (cct && logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
  }
}

void Mutex::Lock(bool no_lockdep) {
  if (lockdep && g_lockdep && !no_lockdep) _will_lock();

  if (TryLock()) {
    return;
  }

  utime_t start;
  if (logger && cct && cct->_conf->mutex_perf_counter)
    start = ceph_clock_now(cct);
  int r = pthread_mutex_lock(&_m);
  if (logger && cct && cct->_conf->mutex_perf_counter)
    logger->tinc(l_mutex_wait,
		 ceph_clock_now(cct) - start);
  assert(r == 0);
  if (lockdep && g_lockdep) _locked();
  _post_lock();
}
