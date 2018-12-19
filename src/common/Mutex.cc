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

#include "common/Mutex.h"
#include "common/config.h"
#include "common/Clock.h"
#include "common/valgrind.h"

Mutex::Mutex(const std::string &n, bool r, bool ld,
	     bool bt) :
  name(n), id(-1), recursive(r), lockdep(ld), backtrace(bt), nlock(0),
  locked_by(0)
{
  ANNOTATE_BENIGN_RACE_SIZED(&id, sizeof(id), "Mutex lockdep id");
  ANNOTATE_BENIGN_RACE_SIZED(&nlock, sizeof(nlock), "Mutex nlock");
  ANNOTATE_BENIGN_RACE_SIZED(&locked_by, sizeof(locked_by), "Mutex locked_by");
  if (recursive) {
    // Mutexes of type PTHREAD_MUTEX_RECURSIVE do all the same checks as
    // mutexes of type PTHREAD_MUTEX_ERRORCHECK.
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&_m,&attr);
    pthread_mutexattr_destroy(&attr);
    if (lockdep && g_lockdep)
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
    // If the mutex type is PTHREAD_MUTEX_DEFAULT, attempting to recursively
    // lock the mutex results in undefined behavior. Attempting to unlock the
    // mutex if it was not locked by the calling thread results in undefined
    // behavior. Attempting to unlock the mutex if it is not locked results in
    // undefined behavior.
    pthread_mutex_init(&_m, NULL);
  }
}

Mutex::~Mutex() {
  ceph_assert(nlock == 0);

  // helgrind gets confused by condition wakeups leading to mutex destruction
  ANNOTATE_BENIGN_RACE_SIZED(&_m, sizeof(_m), "Mutex primitive");
  pthread_mutex_destroy(&_m);

  if (lockdep && g_lockdep) {
    lockdep_unregister(id);
  }
}

void Mutex::lock(bool no_lockdep)
{
  if (lockdep && g_lockdep && !no_lockdep && !recursive) _will_lock();
  int r = pthread_mutex_lock(&_m);
  ceph_assert(r == 0);
  if (lockdep && g_lockdep) _locked();
  _post_lock();
}

void Mutex::unlock()
{
  _pre_unlock();
  if (lockdep && g_lockdep) _will_unlock();
  int r = pthread_mutex_unlock(&_m);
  ceph_assert(r == 0);
}
