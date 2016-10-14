// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Sebastien Ponce <sebastien.ponce@cern.ch>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LIBRADOSSTRIPERSTRIPER_MULTIAIOCOMPLETIONIMPL_H
#define CEPH_LIBRADOSSTRIPERSTRIPER_MULTIAIOCOMPLETIONIMPL_H

#include "common/Cond.h"
#include "common/Mutex.h"

#include "include/radosstriper/libradosstriper.hpp"

struct libradosstriper::MultiAioCompletionImpl {

  Mutex lock;
  Cond cond;
  int ref, rval;
  int pending_complete, pending_safe;
  rados_callback_t callback_complete, callback_safe;
  void *callback_complete_arg, *callback_safe_arg;
  bool building;       ///< true if we are still building this completion
  bufferlist bl;       /// only used for read case in C api of rados striper
  std::list<bufferlist*> bllist; /// keep temporary buffer lists used for destriping

  MultiAioCompletionImpl() : lock("MultiAioCompletionImpl lock", false, false),
    ref(1), rval(0),
    pending_complete(0), pending_safe(0),
    callback_complete(0), callback_safe(0),
    callback_complete_arg(0), callback_safe_arg(0),
    building(true) {};

  ~MultiAioCompletionImpl() {
    // deallocate temporary buffer lists
    for (std::list<bufferlist*>::iterator it = bllist.begin();
	 it != bllist.end();
	 it++) {
      delete *it;
    }
    bllist.clear();
  }

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
    while (pending_complete)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  int wait_for_safe() {
    lock.Lock();
    while (pending_safe)
      cond.Wait(lock);
    lock.Unlock();
    return 0;
  }
  bool is_complete() {
    lock.Lock();
    int r = pending_complete;
    lock.Unlock();
    return 0 == r;
  }
  bool is_safe() {
    lock.Lock();
    int r = pending_safe;
    lock.Unlock();
    return r == 0;
  }
  void wait_for_complete_and_cb() {
    lock.Lock();
    while (pending_complete || callback_complete)
      cond.Wait(lock);
    lock.Unlock();
  }
  void wait_for_safe_and_cb() {
    lock.Lock();
    while (pending_safe || callback_safe)
      cond.Wait(lock);
    lock.Unlock();
  }
  bool is_complete_and_cb() {
    lock.Lock();
    bool r = ((0 == pending_complete) && !callback_complete);
    lock.Unlock();
    return r;
  }
  bool is_safe_and_cb() {
    lock.Lock();
    int r = ((0 == pending_safe) && !callback_safe);
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
    _get();
    lock.Unlock();
  }
  void _get() {
    assert(lock.is_locked());
    assert(ref > 0);
    ++ref;
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
  void add_request() {
    lock.Lock();
    pending_complete++;
    _get();
    pending_safe++;
    _get();
    lock.Unlock();
  }
  void complete() {
    assert(lock.is_locked());
    if (callback_complete) {
      callback_complete(this, callback_complete_arg);
      callback_complete = 0;
    }
    cond.Signal();
  }
  void safe() {
    assert(lock.is_locked());
    if (callback_safe) {
      callback_safe(this, callback_safe_arg);
      callback_safe = 0;
    }
    cond.Signal();
  };

  void complete_request(ssize_t r);
  void safe_request(ssize_t r);
  void finish_adding_requests();

};

#endif // CEPH_LIBRADOSSTRIPERSTRIPER_MULTIAIOCOMPLETIONIMPL_H
