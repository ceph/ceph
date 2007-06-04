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

#ifndef __MUTEX_H
#define __MUTEX_H

#include <pthread.h>
#include <cassert>

class Mutex {
private:
  pthread_mutex_t _m;
  int nlock;
  bool recursive;

  // don't allow copying.
  void operator=(Mutex &M) {}
  Mutex( const Mutex &M ) {}

public:
  Mutex(bool r = true) : nlock(0), recursive(r) {
    if (recursive) {
      pthread_mutexattr_t attr;
      pthread_mutexattr_init(&attr);
      pthread_mutexattr_settype(&attr,PTHREAD_MUTEX_RECURSIVE);
      pthread_mutex_init(&_m,&attr);
      pthread_mutexattr_destroy(&attr);
    } else {
      pthread_mutex_init(&_m,NULL);
    }
  }
  virtual ~Mutex() {
    assert(nlock == 0);
    pthread_mutex_destroy(&_m); 
  }

  bool is_locked() {
    return (nlock > 0);
  }

  void Lock() {
    int r = pthread_mutex_lock(&_m);
    assert(r == 0);
    nlock++;
    assert(nlock == 1 || recursive);
  }

  void Unlock() {
    assert(nlock > 0);
    --nlock;
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
