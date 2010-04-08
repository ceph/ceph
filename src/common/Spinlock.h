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

#ifndef __SPINLOCK_H
#define __SPINLOCK_H

#include <pthread.h>
#include "include/assert.h"

//#define SPINLOCK_LOCKDEP

#ifdef SPINLOCK_LOCKDEP
# include "lockdep.h"
#endif

class Spinlock {
private:
  pthread_spinlock_t _s;
  int nlock;

  // don't allow copying.
  void operator=(Spinlock &M) {}
  Spinlock( const Spinlock &M ) {}

#ifdef SPINLOCK_LOCKDEP
  const char *name;
  int id;
  bool lockdep;
  bool backtrace;  // gather backtrace on lock acquisition

  void _register() {
    if (lockdep && g_lockdep)
      id = lockdep_register(name);
  }
  void _will_lock() { // about to lock
    if (lockdep && g_lockdep)
      id = lockdep_will_lock(name, id);
  }
  void _locked() {    // just locked
    if (lockdep && g_lockdep)
      id = lockdep_locked(name, id, backtrace);
  }
  void _will_unlock() {  // about to unlock
    if (lockdep && g_lockdep)
      id = lockdep_will_unlock(name, id);
  }
#else
  void _register() {}
  void _will_lock() {} // about to lock
  void _locked() {}    // just locked
  void _will_unlock() {}  // about to unlock
#endif

public:
  Spinlock(const char *n, bool ld=true, bool bt=false) :
    nlock(0)
#ifdef SPINLOCK_LOCKDEP
    , name(n), id(-1), lockdep(ld), backtrace(bt)
#endif
  {
    pthread_spin_init(&_s, 0);
    _register();
  }
  ~Spinlock() {
    assert(nlock == 0);
    pthread_spin_destroy(&_s); 
  }

  bool is_locked() {
    return (nlock > 0);
  }

  bool try_lock() {
    int r = pthread_spin_trylock(&_s);
    if (r == 0) {
      _locked();
      nlock++;
    }
    return r == 0;
  }

  void lock() {
    _will_lock();
    int r = pthread_spin_lock(&_s);
    _locked();
    assert(r == 0);
    nlock++;
  }

  void unlock() {
    assert(nlock > 0);
    --nlock;
    _will_unlock();
    int r = pthread_spin_unlock(&_s);
    assert(r == 0);
  }

public:
  class Locker {
    Spinlock &spinlock;

  public:
    Locker(Spinlock& m) : spinlock(m) {
      spinlock.lock();
    }
    ~Locker() {
      spinlock.unlock();
    }
  };
};


#endif
