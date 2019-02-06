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
#include "common/ceph_context.h"
#include "common/valgrind.h"
#include "common/debug.h"

#include <boost/smart_ptr/intrusive_ptr.hpp>

// re-include our assert to clobber the system one; fix dout:
#include "include/ceph_assert.h"

struct RefCountedObject {
public:
  using ref = boost::intrusive_ptr<RefCountedObject>;
  using const_ref = boost::intrusive_ptr<RefCountedObject const>;

  RefCountedObject(CephContext *c = NULL, int n=1) : nref(n), cct(c) {}
  virtual ~RefCountedObject() {
    ceph_assert(nref == 0);
  }
  
  const RefCountedObject *get() const {
    int v = ++nref;
    if (cct)
      lsubdout(cct, refs, 1) << "RefCountedObject::get " << this << " "
			     << (v - 1) << " -> " << v
			     << dendl;
    return this;
  }
  RefCountedObject *get() {
    int v = ++nref;
    if (cct)
      lsubdout(cct, refs, 1) << "RefCountedObject::get " << this << " "
			     << (v - 1) << " -> " << v
			     << dendl;
    return this;
  }
  void put() const {
    CephContext *local_cct = cct;
    auto v = --nref;
    if (local_cct)
      lsubdout(local_cct, refs, 1) << "RefCountedObject::put " << this << " "
				   << (v + 1) << " -> " << v
				   << dendl;
    if (v == 0) {
      ANNOTATE_HAPPENS_AFTER(&nref);
      ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&nref);
      delete this;
    } else {
      ANNOTATE_HAPPENS_BEFORE(&nref);
    }
  }
  void set_cct(CephContext *c) {
    cct = c;
  }

  uint64_t get_nref() const {
    return nref;
  }

private:
  mutable std::atomic<int64_t> nref;
  CephContext *cct;
};

template <class RefCountedObjectType>
class RefCountedObjectFactory {
public:
template<typename... Args>
  static typename RefCountedObjectType::ref build(Args&&... args) {
    return typename RefCountedObjectType::ref(new RefCountedObjectType(std::forward<Args>(args)...), false);
  }
};

template<class T, class R = RefCountedObject>
class RefCountedObjectSubType : public R {
public:
  using ref = boost::intrusive_ptr<T>;
  using const_ref = boost::intrusive_ptr<T const>;

  static auto ref_cast(const auto& m) {
    if constexpr(std::is_const<typename std::remove_reference<decltype(m)>::type::element_type>::value) {
      return boost::static_pointer_cast<typename T::const_ref::element_type, typename std::remove_reference<decltype(m)>::type::element_type>(m);
    } else {
      return boost::static_pointer_cast<typename T::ref::element_type, typename std::remove_reference<decltype(m)>::type::element_type>(m);
    }
  }
  static auto const_ref_cast(const auto& m) {
    return boost::static_pointer_cast<typename T::const_ref::element_type, typename std::remove_reference<decltype(m)>::type::element_type>(m);
  }

  const T* get() const {
    return static_cast<T*>(R::get());
  }
  T* get() {
    return static_cast<T*>(R::get());
  }

protected:
template<typename... Args>
  RefCountedObjectSubType(Args&&... args) : R(std::forward<Args>(args)...) {}
  virtual ~RefCountedObjectSubType() override {}
};


template<class T, class R = RefCountedObject>
class RefCountedObjectInstance : public RefCountedObjectSubType<T, R> {
public:
  using factory = RefCountedObjectFactory<T>;

  template<typename... Args>
  static auto create(Args&&... args) {
    return RefCountedObjectFactory<T>::build(std::forward<Args>(args)...);
  }

protected:
template<typename... Args>
  RefCountedObjectInstance(Args&&... args) : RefCountedObjectSubType<T,R>(std::forward<Args>(args)...) {}
  virtual ~RefCountedObjectInstance() override {}
};

using RefCountedPtr = boost::intrusive_ptr<RefCountedObject>;

#ifndef WITH_SEASTAR

/**
 * RefCountedCond
 *
 *  a refcounted condition, will be removed when all references are dropped
 */

struct RefCountedCond : public RefCountedObject {
  bool complete;
  ceph::mutex lock = ceph::make_mutex("RefCountedCond::lock");
  ceph::condition_variable cond;
  int rval;

  RefCountedCond() : complete(false), rval(0) {}

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

#endif
