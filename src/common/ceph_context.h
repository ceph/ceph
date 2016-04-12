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

#include <iosfwd>
#include <stdint.h>
#include <string>
#include <set>

#include "include/assert.h"
#include "include/buffer_fwd.h"
#include "include/atomic.h"
#include "common/cmdparse.h"
#include "include/Spinlock.h"
#include <boost/noncopyable.hpp>

class AdminSocket;
class CephContextServiceThread;
class PerfCountersCollection;
class PerfCounters;
class md_config_obs_t;
struct md_config_t;
class CephContextHook;
class CephContextObs;
class CryptoHandler;

namespace ceph {
  class PluginRegistry;
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
  CephContext(uint32_t module_type_, int init_flags_ = 0);

  // ref count!
private:
  ~CephContext();
  atomic_t nref;
public:
  CephContext *get() {
    nref.inc();
    return this;
  }
  void put();

  md_config_t *_conf;
  ceph::log::Log *_log;

  /* init ceph::crypto */
  void init_crypto();

  /* Start the Ceph Context's service thread */
  void start_service_thread();

  /* Reopen the log files */
  void reopen_logs();

  /* Get the module type (client, mon, osd, mds, etc.) */
  uint32_t get_module_type() const;

  int get_init_flags() const;

  /* Get the PerfCountersCollection of this CephContext */
  PerfCountersCollection *get_perfcounters_collection();

  ceph::HeartbeatMap *get_heartbeat_map() {
    return _heartbeat_map;
  }

  /**
   * Enable the performance counter, currently we only have counter for the
   * number of total/unhealthy workers.
   */
  void enable_perf_counter();

  /**
   * Disable the performance counter.
   */
  void disable_perf_counter();

  /**
   * Refresh perf counter values.
   */
  void refresh_perf_values();

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
      _associated_objs[name] = new TypedSingletonWrapper<T>(p);
    } else {
      TypedSingletonWrapper<T> *wrapper =
        dynamic_cast<TypedSingletonWrapper<T> *>(_associated_objs[name]);
      assert(wrapper != NULL);
      p = wrapper->singleton;
    }
    ceph_spin_unlock(&_associated_objs_lock);
  }
  /**
   * get a crypto handler
   */
  CryptoHandler *get_crypto_handler(int type);

  /// check if experimental feature is enable, and emit appropriate warnings
  bool check_experimental_feature_enabled(const std::string& feature);
  bool check_experimental_feature_enabled(const std::string& feature,
					  std::ostream *message);

  PluginRegistry *get_plugin_registry() {
    return _plugin_registry;
  }

  void set_uid_gid(uid_t u, gid_t g) {
    _set_uid = u;
    _set_gid = g;
  }
  uid_t get_set_uid() const {
    return _set_uid;
  }
  gid_t get_set_gid() const {
    return _set_gid;
  }

  void set_uid_gid_strings(std::string u, std::string g) {
    _set_uid_string = u;
    _set_gid_string = g;
  }
  std::string get_set_uid_string() const {
    return _set_uid_string;
  }
  std::string get_set_gid_string() const {
    return _set_gid_string;
  }

private:
  struct SingletonWrapper : boost::noncopyable {
    virtual ~SingletonWrapper() {}
  };

  template <typename T>
  struct TypedSingletonWrapper : public SingletonWrapper {
    TypedSingletonWrapper(T *p) : singleton(p) {
    }
    virtual ~TypedSingletonWrapper() {
      delete singleton;
    }

    T *singleton;
  };

  CephContext(const CephContext &rhs);
  CephContext &operator=(const CephContext &rhs);

  /* Stop and join the Ceph Context's service thread */
  void join_service_thread();

  uint32_t _module_type;

  int _init_flags;

  uid_t _set_uid; ///< uid to drop privs to
  gid_t _set_gid; ///< gid to drop privs to
  std::string _set_uid_string;
  std::string _set_gid_string;

  bool _crypto_inited;

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
  std::map<std::string, SingletonWrapper*> _associated_objs;

  // crypto
  CryptoHandler *_crypto_none;
  CryptoHandler *_crypto_aes;

  // experimental
  CephContextObs *_cct_obs;
  ceph_spinlock_t _feature_lock;
  std::set<std::string> _experimental_features;

  PluginRegistry *_plugin_registry;

  md_config_obs_t *_lockdep_obs;

  enum {
    l_cct_first,
    l_cct_total_workers,
    l_cct_unhealthy_workers,
    l_cct_last
  };
  PerfCounters *_cct_perf;
  ceph_spinlock_t _cct_perf_lock;

  friend class CephContextObs;
};

#endif
