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

#include "common/Clock.h"
#include "include/Context.h"

class Cond {
  // my bits
  pthread_cond_t _c;

  Mutex *waiter_mutex;

  // don't allow copying.
  void operator=(Cond &C);
  Cond(const Cond &C);

 public:
  Cond() : waiter_mutex(NULL) {
    int r = pthread_cond_init(&_c,NULL);
    ceph_assert(r == 0);
  }
  virtual ~Cond() { 
    pthread_cond_destroy(&_c); 
  }

  int Wait(Mutex &mutex)  { 
    // make sure this cond is used with one mutex only
    ceph_assert(waiter_mutex == NULL || waiter_mutex == &mutex);
    waiter_mutex = &mutex;

    ceph_assert(mutex.is_locked());

    mutex._pre_unlock();
    int r = pthread_cond_wait(&_c, &mutex._m);
    mutex._post_lock();
    return r;
  }

  int WaitUntil(Mutex &mutex, utime_t when) {
    // make sure this cond is used with one mutex only
    ceph_assert(waiter_mutex == NULL || waiter_mutex == &mutex);
    waiter_mutex = &mutex;

    ceph_assert(mutex.is_locked());

    struct timespec ts;
    when.to_timespec(&ts);

    mutex._pre_unlock();
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    mutex._post_lock();

    return r;
  }

  int WaitInterval(Mutex &mutex, utime_t interval) {
    utime_t when = ceph_clock_now();
    when += interval;
    return WaitUntil(mutex, when);
  }

  template<typename Duration>
  int WaitInterval(Mutex &mutex, Duration interval) {
    ceph::real_time when(ceph::real_clock::now());
    when += interval;

    struct timespec ts = ceph::real_clock::to_timespec(when);

    mutex._pre_unlock();
    int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
    mutex._post_lock();

    return r;
  }

  int SloppySignal() { 
    int r = pthread_cond_broadcast(&_c);
    return r;
  }
  int Signal() { 
    // make sure signaler is holding the waiter's lock.
    ceph_assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_broadcast(&_c);
    return r;
  }
  int SignalOne() { 
    // make sure signaler is holding the waiter's lock.
    ceph_assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_signal(&_c);
    return r;
  }
  int SignalAll() { 
    // make sure signaler is holding the waiter's lock.
    ceph_assert(waiter_mutex == NULL ||
	   waiter_mutex->is_locked());

    int r = pthread_cond_broadcast(&_c);
    return r;
  }
};

/**
 * context to signal a cond
 *
 * Generic context to signal a cond and store the return value.  We
 * assume the caller is holding the appropriate lock.
 */
class C_Cond : public Context {
  Cond *cond;   ///< Cond to signal
  bool *done;   ///< true if finish() has been called
  int *rval;    ///< return value
public:
  C_Cond(Cond *c, bool *d, int *r) : cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) override {
    *done = true;
    *rval = r;
    cond->Signal();
  }
};

/**
 * context to signal a cond, protected by a lock
 *
 * Generic context to signal a cond under a specific lock. We take the
 * lock in the finish() callback, so the finish() caller must not
 * already hold it.
 */
class C_SafeCond : public Context {
  Mutex *lock;    ///< Mutex to take
  Cond *cond;     ///< Cond to signal
  bool *done;     ///< true after finish() has been called
  int *rval;      ///< return value (optional)
public:
  C_SafeCond(Mutex *l, Cond *c, bool *d, int *r=0) : lock(l), cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) override {
    lock->lock();
    if (rval)
      *rval = r;
    *done = true;
    cond->Signal();
    lock->unlock();
  }
};

/**
 * Context providing a simple wait() mechanism to wait for completion
 *
 * The context will not be deleted as part of complete and must live
 * until wait() returns.
 */
class C_SaferCond : public Context {
  Mutex lock;    ///< Mutex to take
  Cond cond;     ///< Cond to signal
  bool done;     ///< true after finish() has been called
  int rval;      ///< return value
public:
  C_SaferCond() : lock("C_SaferCond"), done(false), rval(0) {}
  explicit C_SaferCond(const std::string &name) : lock(name), done(false), rval(0) {}
  void finish(int r) override { complete(r); }

  /// We overload complete in order to not delete the context
  void complete(int r) override {
    std::lock_guard l(lock);
    done = true;
    rval = r;
    cond.Signal();
  }

  /// Returns rval once the Context is called
  int wait() {
    std::lock_guard l(lock);
    while (!done)
      cond.Wait(lock);
    return rval;
  }

  /// Wait until the \c secs expires or \c complete() is called
  int wait_for(double secs) {
    utime_t interval;
    interval.set_from_double(secs);
    std::lock_guard l{lock};
    if (done) {
      return rval;
    }
    cond.WaitInterval(lock, interval);
    return done ? rval : ETIMEDOUT;
  }
};

#endif
