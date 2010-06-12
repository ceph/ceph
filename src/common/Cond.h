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


#ifndef CEPH_COND_H
#define CEPH_COND_H

#include <time.h>

#include "Mutex.h"
#include "Clock.h"

#include "include/Context.h"

#include <pthread.h>

class Cond {
  // my bits
  pthread_cond_t _c;

  // don't allow copying.
  void operator=(Cond &C) {}
  Cond( const Cond &C ) {}

 public:
  Cond() {
    int r = pthread_cond_init(&_c,NULL);
    assert(r == 0);
  }
  virtual ~Cond() { 
    pthread_cond_destroy(&_c); 
  }

  int Wait(Mutex &mutex)  { 
    int r = pthread_cond_wait(&_c, &mutex._m);
    return r;
  }

  int Wait(Mutex &mutex, char* s)  { 
    //cout << "Wait: " << s << endl;
    int r = pthread_cond_wait(&_c, &mutex._m);
    return r;
  }

  int WaitUntil(Mutex &mutex, utime_t when) {
    struct timespec ts;
    g_clock.make_timespec(when, &ts);
    //cout << "timedwait for " << ts.tv_sec << " sec " << ts.tv_nsec << " nsec" << endl;
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    return r;
  }
  int WaitInterval(Mutex &mutex, utime_t interval) {
    utime_t when = g_clock.now();
    when += interval;
    return WaitUntil(mutex, when);
  }

  int Signal() { 
    //int r = pthread_cond_signal(&_c);
    int r = pthread_cond_broadcast(&_c);
    return r;
  }
  int SignalOne() { 
    int r = pthread_cond_signal(&_c);
    return r;
  }
  int SignalAll() { 
    //int r = pthread_cond_signal(&_c);
    int r = pthread_cond_broadcast(&_c);
    return r;
  }
};

class C_SafeCond : public Context {
  Mutex *lock;
  Cond *cond;
  bool *done;
  int *rval;
public:
  C_SafeCond(Mutex *l, Cond *c, bool *d, int *r=0) : lock(l), cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) {
    lock->Lock();
    if (rval) *rval = r;
    *done = true;
    cond->Signal();
    lock->Unlock();
  }
};

#endif
