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
#include "include/Context.h"
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

    rados_callback_t callback = 0;
    void *callback_arg = nullptr;;

    PoolAsyncCompletionImpl() = default;

    int set_callback(void *cb_arg, rados_callback_t cb) {
      std::scoped_lock l{lock};
      callback = cb;
      callback_arg = cb_arg;
      return 0;
    }
    int wait() {
      std::unique_lock l{lock};
      cond.wait(l, [this] { return done;});
      return 0;
    }
    int is_complete() {
      std::scoped_lock l{lock};
      return done;
    }
    int get_return_value() {
      std::scoped_lock l{lock};
      return rval;
    }
    void get() {
      std::scoped_lock l{lock};
      ceph_assert(ref > 0);
      ref++;
    }
    void release() {
      lock.lock();
      ceph_assert(!released);
      released = true;
      put_unlock();
    }
    void put() {
      lock.lock();
      put_unlock();
    }
    void put_unlock() {
      ceph_assert(ref > 0);
      int n = --ref;
      lock.unlock();
      if (!n)
	delete this;
    }
  };

  class C_PoolAsync_Safe : public Context {
    PoolAsyncCompletionImpl *c;

  public:
    explicit C_PoolAsync_Safe(PoolAsyncCompletionImpl *_c) : c(_c) {
      c->get();
    }
    ~C_PoolAsync_Safe() override {
      c->put();
    }
  
    void finish(int r) override {
      c->lock.lock();
      c->rval = r;
      c->done = true;
      c->cond.notify_all();

      if (c->callback) {
	rados_callback_t cb = c->callback;
	void *cb_arg = c->callback_arg;
	c->lock.unlock();
	cb(c, cb_arg);
	c->lock.lock();
      }

      c->lock.unlock();
    }
  };
}
#endif
