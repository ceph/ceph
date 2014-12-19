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
#include <string>

#include "include/buffer.h"
#include "include/atomic.h"
#include "common/cmdparse.h"
#include "include/Spinlock.h"

class AdminSocket;
class CephContextServiceThread;
class PerfCountersCollection;
class md_config_obs_t;
struct md_config_t;
class CephContextHook;
class CryptoNone;
class CryptoAES;
class CryptoHandler;

namespace ceph {
  class HeartbeatMap;
  namespace log {
    class Log;
  }
}

using ceph::bufferlist;

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

  // ref count!
private:
  ~CephContext();
  atomic_t nref;
public:
  class AssociatedSingletonObject {
   public:
    virtual ~AssociatedSingletonObject() {}
  };
  CephContext *get() {
    nref.inc();
    return this;
  }
  void put() {
    if (nref.dec() == 0)
      delete this;
  }

  md_config_t *_conf;
  ceph::log::Log *_log;

  /* Start the Ceph Context's service thread */
  void start_service_thread();

  /* Reopen the log files */
  void reopen_logs();

  /* Get the module type (client, mon, osd, mds, etc.) */
  uint32_t get_module_type() const;

  /* Get the PerfCountersCollection of this CephContext */
  PerfCountersCollection *get_perfcounters_collection();

  ceph::HeartbeatMap *get_heartbeat_map() {
    return _heartbeat_map;
  }

  /**
   * Get the admin socket associated with this CephContext.
   *
   * Currently there is always an admin socket object,
   * so this will never return NULL.
   *
   * @return the admin socket
   */
  AdminSocket *get_admin_socket();

  /**
   * process an admin socket command
   */
  void do_command(std::string command, cmdmap_t& cmdmap, std::string format,
		  bufferlist *out);

  template<typename T>
  void lookup_or_create_singleton_object(T*& p, const std::string &name) {
    ceph_spin_lock(&_associated_objs_lock);
    if (!_associated_objs.count(name)) {
      p = new T(this);
      _associated_objs[name] = reinterpret_cast<AssociatedSingletonObject*>(p);
    } else {
      p = reinterpret_cast<T*>(_associated_objs[name]);
    }
    ceph_spin_unlock(&_associated_objs_lock);
  }
  /**
   * get a crypto handler
   */
  CryptoHandler *get_crypto_handler(int type);

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

  md_config_obs_t *_log_obs;

  /* The admin socket associated with this context */
  AdminSocket *_admin_socket;

  /* lock which protects service thread creation, destruction, etc. */
  ceph_spinlock_t _service_thread_lock;

  /* The collection of profiling loggers associated with this context */
  PerfCountersCollection *_perf_counters_collection;

  md_config_obs_t *_perf_counters_conf_obs;

  CephContextHook *_admin_hook;

  ceph::HeartbeatMap *_heartbeat_map;

  ceph_spinlock_t _associated_objs_lock;
  std::map<std::string, AssociatedSingletonObject*> _associated_objs;

  // crypto
  CryptoNone *_crypto_none;
  CryptoAES *_crypto_aes;
};

#endif
