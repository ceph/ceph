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

#include <list>
#include <mutex>
#include "common/ceph_mutex.h"
#include "include/radosstriper/libradosstriper.hpp"

struct libradosstriper::MultiAioCompletionImpl {

  ceph::mutex lock = ceph::make_mutex("MultiAioCompletionImpl lock", false);
  ceph::condition_variable cond;
  int ref, rval;
  int pending_complete, pending_safe;
  rados_callback_t callback_complete, callback_safe;
  void *callback_complete_arg, *callback_safe_arg;
  bool building;       ///< true if we are still building this completion
  bufferlist bl;       /// only used for read case in C api of rados striper
  std::list<bufferlist*> bllist; /// keep temporary buffer lists used for destriping

  MultiAioCompletionImpl()
  : ref(1), rval(0),
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
    cond.wait(l, [this] { return !pending_complete; });
    return 0;
  }
  int wait_for_safe() {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return !pending_safe; });
    return 0;
  }
  bool is_complete() {
    std::scoped_lock l{lock};
    return pending_complete == 0;
  }
  bool is_safe() {
    std::scoped_lock l{lock};
    return pending_safe == 0;
  }
  void wait_for_complete_and_cb() {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return !pending_complete && !callback_complete; });
  }
  void wait_for_safe_and_cb() {
    std::unique_lock l{lock};
    cond.wait(l, [this] { return !pending_safe && !callback_safe; });
  }
  bool is_complete_and_cb() {
    std::scoped_lock l{lock};
    return ((0 == pending_complete) && !callback_complete);
  }
  bool is_safe_and_cb() {
    std::scoped_lock l{lock};
    return ((0 == pending_safe) && !callback_safe);
  }
  int get_return_value() {
    std::scoped_lock l{lock};
    return rval;
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
  void add_request() {
    std::scoped_lock l{lock};
    pending_complete++;
    _get();
    pending_safe++;
    _get();
  }
  void add_safe_request() {
    std::scoped_lock l{lock};
    pending_complete++;
    _get();
  }
  void complete() {
    ceph_assert(ceph_mutex_is_locked(lock));
    if (callback_complete) {
      callback_complete(this, callback_complete_arg);
      callback_complete = 0;
    }
    cond.notify_all();
  }
  void safe() {
    ceph_assert(ceph_mutex_is_locked(lock));
    if (callback_safe) {
      callback_safe(this, callback_safe_arg);
      callback_safe = 0;
    }
    cond.notify_all();
  };

  void complete_request(ssize_t r);
  void safe_request(ssize_t r);
  void finish_adding_requests();

};

void intrusive_ptr_add_ref(libradosstriper::MultiAioCompletionImpl*);
void intrusive_ptr_release(libradosstriper::MultiAioCompletionImpl*);

#endif // CEPH_LIBRADOSSTRIPERSTRIPER_MULTIAIOCOMPLETIONIMPL_H
