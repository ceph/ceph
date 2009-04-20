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
#include "lockdep.h"

#define LOCKDEP

class Spinlock {
private:
  const char *name;
  int id;
  bool lockdep;
  bool backtrace;  // gather backtrace on lock acquisition

  pthread_spinlock_t _s;
  int nlock;

  // don't allow copying.
  void operator=(Spinlock &M) {}
  Spinlock( const Spinlock &M ) {}

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
  void _unlocked() {  // just unlocked
    id = lockdep_unlocked(name, id);
  }
#else
  void _register() {}
  void _will_lock() {} // about to lock
  void _locked() {}    // just locked
  void _unlocked() {}  // just unlocked
#endif

public:
  Spinlock(const char *n, bool ld=true, bool bt=false) :
    name(n), id(-1), lockdep(ld), backtrace(bt), nlock(0) {
    pthread_spin_init(&_s, 0);
    if (lockdep && g_lockdep) _register();
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
      if (lockdep && g_lockdep) _locked();
      nlock++;
    }
    return r == 0;
  }

  void lock() {
    if (lockdep && g_lockdep) _will_lock();
    int r = pthread_spin_lock(&_s);
    if (lockdep && g_lockdep) _locked();
    assert(r == 0);
    nlock++;
  }

  void unlock() {
    assert(nlock > 0);
    --nlock;
    int r = pthread_spin_unlock(&_s);
    assert(r == 0);
    if (lockdep && g_lockdep) _unlocked();
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
