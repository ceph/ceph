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
#include "common/ceph_mutex.h"
#include "include/Context.h"

/**
 * context to signal a cond
 *
 * Generic context to signal a cond and store the return value.  We
 * assume the caller is holding the appropriate lock.
 */
class C_Cond : public Context {
  ceph::condition_variable& cond;   ///< Cond to signal
  bool *done;   ///< true if finish() has been called
  int *rval;    ///< return value
public:
  C_Cond(ceph::condition_variable &c, bool *d, int *r) : cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) override {
    *done = true;
    *rval = r;
    cond.notify_all();
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
  ceph::mutex& lock;    ///< Mutex to take
  ceph::condition_variable& cond;     ///< Cond to signal
  bool *done;     ///< true after finish() has been called
  int *rval;      ///< return value (optional)
public:
  C_SafeCond(ceph::mutex& l, ceph::condition_variable& c, bool *d, int *r=0)
    : lock(l), cond(c), done(d), rval(r) {
    *done = false;
  }
  void finish(int r) override {
    std::lock_guard l{lock};
    if (rval)
      *rval = r;
    *done = true;
    cond.notify_all();
  }
};

/**
 * Context providing a simple wait() mechanism to wait for completion
 *
 * The context will not be deleted as part of complete and must live
 * until wait() returns.
 */
class C_SaferCond : public Context {
protected:
  ceph::mutex lock;  ///< Mutex to take
  ceph::condition_variable cond;     ///< Cond to signal
  bool done = false; ///< true after finish() has been called
  int rval = 0;      ///< return value
public:
  C_SaferCond() :
    C_SaferCond("C_SaferCond")
  {}
  explicit C_SaferCond(const std::string &name)
    : lock(ceph::make_mutex(name)) {}
  void finish(int r) override { complete(r); }

  /// We overload complete in order to not delete the context
  void complete(int r) override {
    std::lock_guard l(lock);
    done = true;
    rval = r;
    cond.notify_all();
  }

  /// Returns rval once the Context is called
  int wait() {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return done;});
    return rval;
  }

  /// Wait until the \c secs expires or \c complete() is called
  int wait_for(double secs) {
    return wait_for(ceph::make_timespan(secs));
  }

  int wait_for(ceph::timespan secs) {
    std::unique_lock l{lock};
    if (done) {
      return rval;
    }
    if (cond.wait_for(l, secs, [this] { return done; })) {
      return rval;
    } else {
      return ETIMEDOUT;
    }
  }
};

#endif
