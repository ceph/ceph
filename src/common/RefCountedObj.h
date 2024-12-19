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
#include "include/common_fwd.h"

#include <atomic>

/* This class provides mechanisms to make a sub-class work with
 * boost::intrusive_ptr (aka ceph::ref_t).
 *
 * Generally, you'll want to inherit from RefCountedObjectSafe and not from
 * RefCountedObject directly. This is because the ::get and ::put methods are
 * public and can be used to create/delete references outside of the
 * ceph::ref_t pointers with the potential to leak memory.
 *
 * It is also suggested that you make constructors and destructors private in
 * your final class. This prevents instantiation of the object with assignment
 * to a raw pointer. Consequently, you'll want to use ceph::make_ref<> to
 * create a ceph::ref_t<> holding your object:
 *
 *    auto ptr = ceph::make_ref<Foo>(...);
 *
 * Use FRIEND_MAKE_REF(ClassName) to allow ceph::make_ref to call the private
 * constructors.
 *
 */
namespace TOPNSPC::common {
class RefCountedObject {
public:
  void set_cct(CephContext *c) {
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
  RefCountedObject(CephContext* c) : cct(c) {}

  virtual ~RefCountedObject();

private:
  void _get() const;

  mutable std::atomic<uint64_t> nref{1};
  CephContext *cct{nullptr};
};

class RefCountedObjectSafe : public RefCountedObject {
public:
  RefCountedObject *get() = delete;
  const RefCountedObject *get() const = delete;
  void put() const = delete;
protected:
template<typename... Args>
  RefCountedObjectSafe(Args&&... args) : RefCountedObject(std::forward<Args>(args)...) {}
  virtual ~RefCountedObjectSafe() override {}
};

#if !defined(WITH_SEASTAR)|| defined(WITH_ALIEN)

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

static inline void intrusive_ptr_add_ref(RefCountedWaitObject *p) {
  p->get();
}
static inline void intrusive_ptr_release(RefCountedWaitObject *p) {
  p->put();
}
#endif // !defined(WITH_SEASTAR)|| defined(WITH_ALIEN)

static inline void intrusive_ptr_add_ref(const RefCountedObject *p) {
  p->get();
}
static inline void intrusive_ptr_release(const RefCountedObject *p) {
  p->put();
}
struct UniquePtrDeleter
{
  void operator()(RefCountedObject *p) const
  {
    // Don't expect a call to `get()` in the ctor as we manually set nref to 1
    p->put();
  }
};
} // namespace TOPNSPC::common
using RefCountedPtr = ceph::ref_t<TOPNSPC::common::RefCountedObject>;

#endif
