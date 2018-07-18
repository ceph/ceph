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

#include "include/Context.h"
#include "CondVar.h"

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
    lock->Lock();
    if (rval)
      *rval = r;
    *done = true;
    cond->Signal();
    lock->Unlock();
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
  C_SaferCond(const std::string &name) : lock(name), done(false), rval(0) {}
  void finish(int r) override { complete(r); }

  /// We overload complete in order to not delete the context
  void complete(int r) override {
    Mutex::Locker l(lock);
    done = true;
    rval = r;
    cond.Signal();
  }

  /// Returns rval once the Context is called
  int wait() {
    Mutex::Locker l(lock);
    while (!done)
      cond.Wait(lock);
    return rval;
  }

  /// Wait until the \c secs expires or \c complete() is called
  int wait_for(double secs) {
    utime_t interval;
    interval.set_from_double(secs);
    Mutex::Locker l{lock};
    if (done) {
      return rval;
    }
    cond.WaitInterval(lock, interval);
    return done ? rval : ETIMEDOUT;
  }
};

#endif
