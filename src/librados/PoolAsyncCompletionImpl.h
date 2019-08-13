// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2012 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOS_POOLASYNCCOMPLETIONIMPL_H
#define CEPH_LIBRADOS_POOLASYNCCOMPLETIONIMPL_H

#include "common/ceph_mutex.h"

#include <boost/intrusive_ptr.hpp>

#include "include/rados/librados.h"
#include "include/rados/librados.hpp"

namespace librados {
  struct PoolAsyncCompletionImpl {
    ceph::mutex lock = ceph::make_mutex("PoolAsyncCompletionImpl lock");
    ceph::condition_variable cond;
    int ref = 1;
    int rval = 0;
    bool released = false;
    bool done = false;

    rados_callback_t callback = nullptr;
    void *callback_arg = nullptr;

    PoolAsyncCompletionImpl() = default;

    int set_callback(void *cb_arg, rados_callback_t cb) {
      std::scoped_lock l(lock);
      callback = cb;
      callback_arg = cb_arg;
      return 0;
    }
    int wait() {
      std::unique_lock l(lock);
      while (!done)
	cond.wait(l);
      return 0;
    }
    int is_complete() {
      std::scoped_lock l(lock);
      return done;
    }
    int get_return_value() {
      std::scoped_lock l(lock);
      return rval;
    }
    void get() {
      std::scoped_lock l(lock);
      ceph_assert(ref > 0);
      ref++;
    }
    void release() {
      std::scoped_lock l(lock);
      ceph_assert(!released);
      released = true;
    }
    void put() {
      std::unique_lock l(lock);
      int n = --ref;
      l.unlock();
      if (!n)
	delete this;
    }
  };

  inline void intrusive_ptr_add_ref(PoolAsyncCompletionImpl* p) {
    p->get();
  }
  inline void intrusive_ptr_release(PoolAsyncCompletionImpl* p) {
    p->put();
  }

  class CB_PoolAsync_Safe {
    boost::intrusive_ptr<PoolAsyncCompletionImpl> p;

  public:
    explicit CB_PoolAsync_Safe(boost::intrusive_ptr<PoolAsyncCompletionImpl> p)
      : p(p) {}
    ~CB_PoolAsync_Safe() = default;

    void operator()(int r) {
      auto c(std::move(p));
      std::unique_lock l(c->lock);
      c->rval = r;
      c->done = true;
      c->cond.notify_all();

      if (c->callback) {
	rados_callback_t cb = c->callback;
	void *cb_arg = c->callback_arg;
	l.unlock();
	cb(c.get(), cb_arg);
	l.lock();
      }
    }
  };
}
#endif
