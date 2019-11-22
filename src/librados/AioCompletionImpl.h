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

#ifndef CEPH_LIBRADOS_AIOCOMPLETIONIMPL_H
#define CEPH_LIBRADOS_AIOCOMPLETIONIMPL_H

#include "common/ceph_mutex.h"
#include "include/buffer.h"
#include "include/xlist.h"
#include "osd/osd_types.h"

class IoCtxImpl;

struct librados::AioCompletionImpl {
  ceph::mutex lock = ceph::make_mutex("AioCompletionImpl lock", false);
  ceph::condition_variable cond;
  int ref = 1, rval = 0;
  bool released = false;
  bool complete = false;
  version_t objver = 0;
  ceph_tid_t tid = 0;

  rados_callback_t callback_complete = nullptr, callback_safe = nullptr;
  void *callback_complete_arg = nullptr, *callback_safe_arg = nullptr;

  // for read
  bool is_read = false;
  bufferlist bl;
  bufferlist *blp = nullptr;
  char *out_buf = nullptr;

  IoCtxImpl *io = nullptr;
  ceph_tid_t aio_write_seq = 0;
  xlist<AioCompletionImpl*>::item aio_write_list_item;

  AioCompletionImpl() : aio_write_list_item(this) { }

  int set_complete_callback(void *cb_arg, rados_callback_t cb) {
    std::scoped_lock l{lock};
    callback_complete = cb;
    callback_complete_arg = cb_arg;
    return 0;
  }
  int set_safe_callback(void *cb_arg, rados_callback_t cb) {
    std::scoped_lock l{lock};
    callback_safe = cb;
    callback_safe_arg = cb_arg;
    return 0;
  }
  int wait_for_complete() {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return complete; });
    return 0;
  }
  int wait_for_safe() {
    return wait_for_complete();
  }
  int is_complete() {
    std::scoped_lock l{lock};
    return complete;
  }
  int is_safe() {
    return is_complete();
  }
  int wait_for_complete_and_cb() {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return complete && !callback_complete && !callback_safe; });
    return 0;
  }
  int wait_for_safe_and_cb() {
    return wait_for_complete_and_cb();
  }
  int is_complete_and_cb() {
    std::scoped_lock l{lock};
    return complete && !callback_complete && !callback_safe;
  }
  int is_safe_and_cb() {
    return is_complete_and_cb();
  }
  int get_return_value() {
    std::scoped_lock l{lock};
    return rval;
  }
  uint64_t get_version() {
    std::scoped_lock l{lock};
    return objver;
  }

  void get() {
    std::scoped_lock l{lock};
    _get();
  }
  void _get() {
    ceph_assert(ceph_mutex_is_locked(lock));
    ceph_assert(ref > 0);
    ++ref;
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

namespace librados {
struct C_AioComplete : public Context {
  AioCompletionImpl *c;

  explicit C_AioComplete(AioCompletionImpl *cc) : c(cc) {
    c->_get();
  }

  void finish(int r) override {
    rados_callback_t cb_complete = c->callback_complete;
    void *cb_complete_arg = c->callback_complete_arg;
    if (cb_complete)
      cb_complete(c, cb_complete_arg);

    rados_callback_t cb_safe = c->callback_safe;
    void *cb_safe_arg = c->callback_safe_arg;
    if (cb_safe)
      cb_safe(c, cb_safe_arg);

    c->lock.lock();
    c->callback_complete = NULL;
    c->callback_safe = NULL;
    c->cond.notify_all();
    c->put_unlock();
  }
};

/**
  * Fills in all completed request data, and calls both
  * complete and safe callbacks if they exist.
  *
  * Not useful for usual I/O, but for special things like
  * flush where we only want to wait for things to be safe,
  * but allow users to specify any of the callbacks.
  */
struct C_AioCompleteAndSafe : public Context {
  AioCompletionImpl *c;

  explicit C_AioCompleteAndSafe(AioCompletionImpl *cc) : c(cc) {
    c->get();
  }

  void finish(int r) override {
    c->lock.lock();
    c->rval = r;
    c->complete = true;
    c->lock.unlock();

    rados_callback_t cb_complete = c->callback_complete;
    void *cb_complete_arg = c->callback_complete_arg;
    if (cb_complete)
      cb_complete(c, cb_complete_arg);

    rados_callback_t cb_safe = c->callback_safe;
    void *cb_safe_arg = c->callback_safe_arg;
    if (cb_safe)
      cb_safe(c, cb_safe_arg);

    c->lock.lock();
    c->callback_complete = NULL;
    c->callback_safe = NULL;
    c->cond.notify_all();
    c->put_unlock();
  }
};

}

#endif
