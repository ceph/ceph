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

#include "common/mutex_debug.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"
#include "common/config.h"

namespace ceph {
namespace mutex_debug_detail {
enum {
  l_mutex_first = 999082,
  l_mutex_wait,
  l_mutex_last
};

mutex_debugging_base::mutex_debugging_base(std::string group, bool ld, bool bt)
  : group(std::move(group)),
    lockdep(ld),
    backtrace(bt)
{
  if (_enable_lockdep()) {
    _register();
  }
}

mutex_debugging_base::~mutex_debugging_base() {
  ceph_assert(nlock == 0);
  if (_enable_lockdep()) {
    lockdep_unregister(id);
  }
}

void mutex_debugging_base::_register() {
  id = lockdep_register(group.c_str());
}
void mutex_debugging_base::_will_lock(bool recursive) { // about to lock
  id = lockdep_will_lock(group.c_str(), id, backtrace, recursive);
}
void mutex_debugging_base::_locked() {    // just locked
  id = lockdep_locked(group.c_str(), id, backtrace);
}
void mutex_debugging_base::_will_unlock() {  // about to unlock
  id = lockdep_will_unlock(group.c_str(), id);
}

} // namespace mutex_debug_detail
} // namespace ceph
