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

#include "common/Cond.h"
#include "common/Mutex.h"

#include "include/buffer.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/xlist.h"
#include "osd/osd_types.h"

class IoCtxImpl;

struct librados::AioCompletionImpl {
  Mutex lock;
  Cond cond;
  int ref, rval;
  bool released;
  bool ack, safe;
  version_t objver;
  ceph_tid_t tid;

  rados_callback_t callback_complete, callback_safe;
  void *callback_complete_arg, *callback_safe_arg;

  // for read
  bool is_read;
  bufferlist bl;
  bufferlist *blp;
  char *out_buf;

  IoCtxImpl *io;
  ceph_tid_t aio_write_seq;
  xlist<AioCompletionImpl*>::item aio_write_list_item;

  AioCompletionImpl() : lock("AioCompletionImpl lock", false, false),
			ref(1), rval(0), released(false), ack(false), safe(false),
			objver(0),
                        tid(0),
			callback_complete(0),
			callback_safe(0),
			callback_complete_arg(0),
			callback_safe_arg(0),
			is_read(false), blp(nullptr), out_buf(nullptr),
			io(NULL), aio_write_seq(0), aio_write_list_item(this) { }

  int set_complete_callback(void *cb_arg, rados_callback_t cb) {
    lock.Lock();
    callback_complete = cb;
    callback_complete_arg = cb_arg;
    lock.Unlock();
    return 0;
  }
  int set_safe_callback(void *cb_arg, rados_callback_t cb) {
    lock.Lock();
    callback_safe = cb;
    callback_safe_arg = cb_arg;
    lock.Unlock();
    return 0;
  }
  int wait_for_complete() {
    lock.Lock();
    while (!ack)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int wait_for_safe() {
    lock.Lock();
    while (!safe)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int is_complete() {
    lock.Lock();
    int r = ack;
    lock.Unlock();
    return r;
  }
  int is_safe() {
    lock.Lock();
    int r = safe;
    lock.Unlock();
    return r;
  }
  int wait_for_complete_and_cb() {
    lock.Lock();
    while (!ack || callback_complete)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int wait_for_safe_and_cb() {
    lock.Lock();
    while (!safe || callback_safe)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int is_complete_and_cb() {
    lock.Lock();
    int r = ack && !callback_complete;
    lock.Unlock();
    return r;
  }
  int is_safe_and_cb() {
    lock.Lock();
    int r = safe && !callback_safe;
    lock.Unlock();
    return r;
  }
  int get_return_value() {
    lock.Lock();
    int r = rval;
    lock.Unlock();
    return r;
  }
  uint64_t get_version() {
    lock.Lock();
    version_t v = objver;
    lock.Unlock();
    return v;
  }

  void get() {
    lock.Lock();
    _get();
    lock.Unlock();
  }
  void _get() {
    assert(lock.is_locked());
    assert(ref > 0);
    ++ref;
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

namespace librados {
struct C_AioComplete : public Context {
  AioCompletionImpl *c;

  explicit C_AioComplete(AioCompletionImpl *cc) : c(cc) {
    c->_get();
  }

  void finish(int r) {
    rados_callback_t cb = c->callback_complete;
    void *cb_arg = c->callback_complete_arg;
    cb(c, cb_arg);

    c->lock.Lock();
    c->callback_complete = NULL;
    c->cond.Signal();
    c->put_unlock();
  }
};

struct C_AioSafe : public Context {
  AioCompletionImpl *c;

  explicit C_AioSafe(AioCompletionImpl *cc) : c(cc) {
    c->_get();
  }

  void finish(int r) {
    rados_callback_t cb = c->callback_safe;
    void *cb_arg = c->callback_safe_arg;
    cb(c, cb_arg);

    c->lock.Lock();
    c->callback_safe = NULL;
    c->cond.Signal();
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

  void finish(int r) {
    c->lock.Lock();
    c->rval = r;
    c->ack = true;
    c->safe = true;
    c->lock.Unlock();
    rados_callback_t cb_complete = c->callback_complete;
    void *cb_complete_arg = c->callback_complete_arg;
    if (cb_complete)
      cb_complete(c, cb_complete_arg);

    rados_callback_t cb_safe = c->callback_safe;
    void *cb_safe_arg = c->callback_safe_arg;
    if (cb_safe)
      cb_safe(c, cb_safe_arg);

    c->lock.Lock();
    c->callback_complete = NULL;
    c->callback_safe = NULL;
    c->cond.Signal();
    c->put_unlock();
  }
};

}

#endif
