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

#include "common/Cond.h"
#include "common/Mutex.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"

struct librados::PoolAsyncCompletionImpl {
  Mutex lock;
  Cond cond;
  int ref, rval;
  bool released;
  bool done;

  rados_callback_t callback;
  void *callback_arg;

  PoolAsyncCompletionImpl() : lock("PoolAsyncCompletionImpl lock"),
			ref(1), rval(0), released(false), done(false),
			callback(0), callback_arg(0) {}

  int set_callback(void *cb_arg, rados_callback_t cb) {
    lock.Lock();
    callback = cb;
    callback_arg = cb_arg;
    lock.Unlock();
    return 0;
  }
  int wait() {
    lock.Lock();
    while (!done)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int is_complete() {
    lock.Lock();
    int r = done;
    lock.Unlock();
    return r;
  }
  int get_return_value() {
    lock.Lock();
    int r = rval;
    lock.Unlock();
    return r;
  }
  void get() {
    lock.Lock();
    assert(ref > 0);
    ref++;
    lock.Unlock();
  }
  void release() {
    lock.Lock();
    assert(!released);
    released = true;
    put_unlock();
  }
  void put() {
    lock.Lock();
    put_unlock();
  }
  void put_unlock() {
    assert(ref > 0);
    int n = --ref;
    lock.Unlock();
    if (!n)
      delete this;
  }
};

#endif
