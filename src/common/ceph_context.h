// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_CEPHCONTEXT_H
#define CEPH_CEPHCONTEXT_H

#include <iostream>
#include <stdint.h>

/* Forward declarations */ 
template <typename T, typename U>
class DoutStreambuf;

class AdminSocketConfigObs;
class CephContextServiceThread;
class DoutLocker;
class PerfCountersCollection;
class md_config_obs_t;
class md_config_t;

namespace ceph {
  class HeartbeatMap;
}

/* A CephContext represents the context held by a single library user.
 * There can be multiple CephContexts in the same process.
 *
 * For daemons and utility programs, there will be only one CephContext.  The
 * CephContext contains the configuration, the dout object, and anything else
 * that you might want to pass to libcommon with every function call.
 */
class CephContext {
public:
  CephContext(uint32_t module_type_);
  ~CephContext();
  md_config_t *_conf;
  DoutStreambuf <char, std::basic_string<char>::traits_type> *_doss;
  std::ostream _dout;

  /* Start the Ceph Context's service thread */
  void start_service_thread();

  /* Reopen the log files */
  void reopen_logs();

  /* Lock the dout lock. */
  void dout_lock(DoutLocker *locker);

  /* Try to lock the dout lock. */
  void dout_trylock(DoutLocker *locker);

  /* Get the module type (client, mon, osd, mds, etc.) */
  uint32_t get_module_type() const;

  /* Get the PerfCountersCollection of this CephContext */
  PerfCountersCollection *GetPerfCountersCollection();

  ceph::HeartbeatMap *get_heartbeat_map() {
    return _heartbeat_map;
  }

private:
  CephContext(const CephContext &rhs);
  CephContext &operator=(const CephContext &rhs);

  /* Stop and join the Ceph Context's service thread */
  void join_service_thread();

  uint32_t _module_type;

  /* libcommon service thread.
   * SIGHUP wakes this thread, which then reopens logfiles */
  friend class CephContextServiceThread;
  CephContextServiceThread *_service_thread;

  /* The collection of profiling loggers associated with this context */
  AdminSocketConfigObs *_admin_socket_config_obs;

  /* lock which protects service thread creation, destruction, etc. */
  pthread_spinlock_t _service_thread_lock;

  /* The collection of profiling loggers associated with this context */
  PerfCountersCollection *_perf_counters_collection;

  md_config_obs_t *_perf_counters_conf_obs;

  ceph::HeartbeatMap *_heartbeat_map;
};

#endif
