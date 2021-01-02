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
#include "common/RefCountedObj.h"
#include "include/radosstriper/libradosstriper.hpp"

namespace libradosstriper {

class MultiAioCompletionImpl : public RefCountedObject {
public:
  ceph::mutex lock = ceph::make_mutex("MultiAioCompletionImpl lock", false);
  ceph::condition_variable cond;
  int rval = 0;
  int pending_complete = 0, pending_safe = 0;
  rados_callback_t callback_complete = 0, callback_safe = 0;
  void *callback_complete_arg = nullptr, *callback_safe_arg = nullptr;
  bool building = true;  ///< true if we are still building this completion
  bufferlist bl;         /// only used for read case in C api of rados striper
  std::list<bufferlist*> bllist; /// keep temporary buffer lists used for destriping

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
  void add_request() {
    std::scoped_lock l{lock};
    pending_complete++;
    get();
    pending_safe++;
    get();
  }
  void add_safe_request() {
    std::scoped_lock l{lock};
    pending_complete++;
    get();
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

protected:
  FRIEND_MAKE_REF(MultiAioCompletionImpl);
  MultiAioCompletionImpl() = default;
  ~MultiAioCompletionImpl() {
    // deallocate temporary buffer lists
    for (auto& blp : bllist) {
      delete blp;
    }
    bllist.clear();
  }
};

}

#endif // CEPH_LIBRADOSSTRIPERSTRIPER_MULTIAIOCOMPLETIONIMPL_H
