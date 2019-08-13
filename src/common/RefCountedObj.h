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

#ifndef CEPH_REFCOUNTEDOBJ_H
#define CEPH_REFCOUNTEDOBJ_H
 
#include "common/ceph_mutex.h"
#include "common/ref.h"

#include <atomic>

struct RefCountedObject {
public:
  void set_cct(class CephContext *c) {
    cct = c;
  }

  uint64_t get_nref() const {
    return nref;
  }

  const RefCountedObject *get() const {
    _get();
    return this;
  }
  RefCountedObject *get() {
    _get();
    return this;
  }
  void put() const;

protected:
  RefCountedObject() = default;
  RefCountedObject(const RefCountedObject& o) : cct(o.cct) {}
  RefCountedObject& operator=(const RefCountedObject& o) = delete;
  RefCountedObject(RefCountedObject&&) = delete;
  RefCountedObject& operator=(RefCountedObject&&) = delete;
  RefCountedObject(class CephContext* c = nullptr, int n = 1) : cct(c), nref(n) {}

  virtual ~RefCountedObject();

private:
  void _get() const;

#ifndef WITH_SEASTAR
  mutable std::atomic<uint64_t> nref{1};
#else
  // crimson is single threaded at the moment
  mutable uint64_t nref{1};
#endif
  class CephContext *cct{nullptr};
};

#ifndef WITH_SEASTAR

/**
 * RefCountedCond
 *
 *  a refcounted condition, will be removed when all references are dropped
 */
struct RefCountedCond : public RefCountedObject {
  RefCountedCond() = default;
  ~RefCountedCond() = default;

  int wait() {
    std::unique_lock l(lock);
    while (!complete) {
      cond.wait(l);
    }
    return rval;
  }

  void done(int r) {
    std::lock_guard l(lock);
    rval = r;
    complete = true;
    cond.notify_all();
  }

  void done() {
    done(0);
  }

private:
  bool complete = false;
  ceph::mutex lock = ceph::make_mutex("RefCountedCond::lock");
  ceph::condition_variable cond;
  int rval = 0;
};

/**
 * RefCountedWaitObject
 *
 * refcounted object that allows waiting for the object's last reference.
 * Any referrer can either put or put_wait(). A simple put() will return
 * immediately, a put_wait() will return only when the object is destroyed.
 * e.g., useful when we want to wait for a specific event completion. We
 * use RefCountedCond, as the condition can be referenced after the object
 * destruction. 
 *    
 */
struct RefCountedWaitObject {
  std::atomic<uint64_t> nref = { 1 };
  RefCountedCond *c;

  RefCountedWaitObject() {
    c = new RefCountedCond;
  }
  virtual ~RefCountedWaitObject() {
    c->put();
  }

  RefCountedWaitObject *get() {
    nref++;
    return this;
  }

  bool put() {
    bool ret = false;
    RefCountedCond *cond = c;
    cond->get();
    if (--nref == 0) {
      cond->done();
      delete this;
      ret = true;
    }
    cond->put();
    return ret;
  }

  void put_wait() {
    RefCountedCond *cond = c;

    cond->get();
    if (--nref == 0) {
      cond->done();
      delete this;
    } else {
      cond->wait();
    }
    cond->put();
  }
};

#endif // WITH_SEASTAR

static inline void intrusive_ptr_add_ref(const RefCountedObject *p) {
  p->get();
}
static inline void intrusive_ptr_release(const RefCountedObject *p) {
  p->put();
}

using RefCountedPtr = ceph::ref_t<RefCountedObject>;

#endif
