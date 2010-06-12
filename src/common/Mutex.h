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

#ifndef CEPH_MUTEX_H
#define CEPH_MUTEX_H

#include <pthread.h>
#include "include/assert.h"
#include "lockdep.h"

#define LOCKDEP

using namespace ceph;

class Mutex {
private:
  const char *name;
  int id;
  bool recursive;
  bool lockdep;
  bool backtrace;  // gather backtrace on lock acquisition

  pthread_mutex_t _m;
  int nlock;

  // don't allow copying.
  void operator=(Mutex &M) {}
  Mutex( const Mutex &M ) {}

#ifdef LOCKDEP
  void _register() {
    id = lockdep_register(name);
  }
  void _will_lock() { // about to lock
    id = lockdep_will_lock(name, id);
  }
  void _locked() {    // just locked
    id = lockdep_locked(name, id, backtrace);
  }
  void _will_unlock() {  // about to unlock
    id = lockdep_will_unlock(name, id);
  }
#else
  void _register() {}
  void _will_lock() {} // about to lock
  void _locked() {}    // just locked
  void _will_unlock() {}  // about to unlock
#endif

public:
  Mutex(const char *n, bool r = false, bool ld=true, bool bt=false) :
    name(n), id(-1), recursive(r), lockdep(ld), backtrace(bt), nlock(0) {
    if (recursive) {
      pthread_mutexattr_t attr;
      pthread_mutexattr_init(&attr);
      pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init(&_m,&attr);
      pthread_mutexattr_destroy(&attr);
    } else {
      pthread_mutex_init(&_m, NULL);
    }
    if (lockdep && g_lockdep) _register();
  }
  ~Mutex() {
    assert(nlock == 0);
    pthread_mutex_destroy(&_m); 
  }

  bool is_locked() {
    return (nlock > 0);
  }

  bool TryLock() {
    int r = pthread_mutex_trylock(&_m);
    if (r == 0) {
      if (lockdep && g_lockdep) _locked();
      nlock++;
    }
    return r == 0;
  }

  void Lock(bool no_lockdep=false) {
    if (lockdep && g_lockdep && !no_lockdep) _will_lock();
    int r = pthread_mutex_lock(&_m);
    if (lockdep && g_lockdep) _locked();
    assert(r == 0);
    nlock++;
  }

  void Unlock() {
    assert(nlock > 0);
    --nlock;
    if (lockdep && g_lockdep) _will_unlock();
    int r = pthread_mutex_unlock(&_m);
    assert(r == 0);
  }

  friend class Cond;


public:
  class Locker {
    Mutex &mutex;

  public:
    Locker(Mutex& m) : mutex(m) {
      mutex.Lock();
    }
    ~Locker() {
      mutex.Unlock();
    }
  };
};


#endif
