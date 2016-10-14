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
#include <string>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "common/mutex_debug.h"
#include "common/perf_counters.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "include/stringify.h"

namespace ceph {
namespace mutex_debug_detail {
enum {
  l_mutex_first = 999082,
  l_mutex_wait,
  l_mutex_last
};

mutex_debugging_base::mutex_debugging_base(const std::string &n, bool bt,
					   CephContext *cct) :
  id(-1), backtrace(bt), nlock(0), locked_by(thread::id()),
  cct(cct), logger(0) {
  if (n.empty()) {
    uuid_d uu;
    uu.generate_random();
    name = string("Unnamed-Mutex-") + uu.to_string();
  } else {
    name = n;
  }
  if (cct) {
    PerfCountersBuilder b(cct, string("mutex-") + name,
			  l_mutex_first, l_mutex_last);
    b.add_time_avg(l_mutex_wait, "wait",
		   "Average time of mutex in locked state");
    logger = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
    logger->set(l_mutex_wait, 0);
  }
  if (g_lockdep)
    _register();
}

mutex_debugging_base::~mutex_debugging_base() {
  assert(nlock == 0);
  if (cct && logger) {
    cct->get_perfcounters_collection()->remove(logger);
    delete logger;
  }
  if (g_lockdep) {
    lockdep_unregister(id);
  }
}

void mutex_debugging_base::_register() {
  id = lockdep_register(name.c_str());
}
void mutex_debugging_base::_will_lock() { // about to lock
  id = lockdep_will_lock(name.c_str(), id, backtrace);
}
void mutex_debugging_base::_locked() {    // just locked
  id = lockdep_locked(name.c_str(), id, backtrace);
}
void mutex_debugging_base::_will_unlock() {  // about to unlock
  id = lockdep_will_unlock(name.c_str(), id);
}

ceph::mono_time mutex_debugging_base::before_lock_blocks() {
  if (logger && cct && cct->_conf->mutex_perf_counter)
    return ceph::mono_clock::now();
  return ceph::mono_time::min();
}

void mutex_debugging_base::after_lock_blocks(ceph::mono_time start,
					     bool no_lockdep) {
  if (logger && cct && cct->_conf->mutex_perf_counter)
    logger->tinc(l_mutex_wait,
		 ceph::mono_clock::now() - start);
  if (!no_lockdep && g_lockdep)
    _locked();
}
} // namespace mutex_debug_detail
} // namespace ceph
