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

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <typeinfo>
#include <typeindex>

#include "include/any.h"

#include "common/cmdparse.h"
#include "common/code_environment.h"
#ifdef WITH_SEASTAR
#include "crimson/common/config_proxy.h"
#include "crimson/common/perf_counters_collection.h"
#else
#include "common/config_proxy.h"
#include "include/spinlock.h"
#include "common/perf_counters_collection.h"
#endif


#include "crush/CrushLocation.h"

class AdminSocket;
class CephContextServiceThread;
class CephContextHook;
class CephContextObs;
class CryptoHandler;
class CryptoRandom;

namespace ceph {
  class PluginRegistry;
  class HeartbeatMap;
  namespace logging {
    class Log;
  }
}

#ifdef WITH_SEASTAR
class CephContext {
public:
  CephContext();
  CephContext(uint32_t,
	      code_environment_t=CODE_ENVIRONMENT_UTILITY,
	      int = 0)
    : CephContext{}
  {}
  ~CephContext();

  uint32_t get_module_type() const;
  bool check_experimental_feature_enabled(const std::string& feature) {
    // everything crimson is experimental...
    return true;
  }
  CryptoRandom* random() const;
  PerfCountersCollectionImpl* get_perfcounters_collection();
  ceph::common::ConfigProxy& _conf;
  ceph::common::PerfCountersCollection& _perf_counters_collection;
  CephContext* get();
  void put();
private:
  std::unique_ptr<CryptoRandom> _crypto_random;
  unsigned nref;
};
#else
/* A CephContext represents the context held by a single library user.
 * There can be multiple CephContexts in the same process.
 *
 * For daemons and utility programs, there will be only one CephContext.  The
 * CephContext contains the configuration, the dout object, and anything else
 * that you might want to pass to libcommon with every function call.
 */
class CephContext {
public:
  CephContext(uint32_t module_type_,
              enum code_environment_t code_env=CODE_ENVIRONMENT_UTILITY,
              int init_flags_ = 0);

  CephContext(const CephContext&) = delete;
  CephContext& operator =(const CephContext&) = delete;
  CephContext(CephContext&&) = delete;
  CephContext& operator =(CephContext&&) = delete;

  bool _finished = false;

  // ref count!
private:
  ~CephContext();
  std::atomic<unsigned> nref;
public:
  CephContext *get() {
    ++nref;
    return this;
  }
  void put();

  ConfigProxy _conf;
  ceph::logging::Log *_log;

  /* init ceph::crypto */
  void init_crypto();

  /// shutdown crypto (should match init_crypto calls)
  void shutdown_crypto();

  /* Start the Ceph Context's service thread */
  void start_service_thread();

  /* Reopen the log files */
  void reopen_logs();

  /* Get the module type (client, mon, osd, mds, etc.) */
  uint32_t get_module_type() const;

  // this is here only for testing purposes!
  void _set_module_type(uint32_t t) {
    _module_type = t;
  }

  void set_init_flags(int flags);
  int get_init_flags() const;

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
  void do_command(std::string_view command, const cmdmap_t& cmdmap,
		  std::string_view format, ceph::bufferlist *out);

  static constexpr std::size_t largest_singleton = 8 * 72;

  template<typename T, typename... Args>
  T& lookup_or_create_singleton_object(std::string_view name,
				       bool drop_on_fork,
				       Args&&... args) {
    static_assert(sizeof(T) <= largest_singleton,
		  "Please increase largest singleton.");
    std::lock_guard lg(associated_objs_lock);
    std::type_index type = typeid(T);

    auto i = associated_objs.find(std::make_pair(name, type));
    if (i == associated_objs.cend()) {
      if (drop_on_fork) {
	associated_objs_drop_on_fork.insert(std::string(name));
      }
      i = associated_objs.emplace_hint(
	i,
	std::piecewise_construct,
	std::forward_as_tuple(name, type),
	std::forward_as_tuple(std::in_place_type<T>,
			      std::forward<Args>(args)...));
    }
    return ceph::any_cast<T&>(i->second);
  }

  /**
   * get a crypto handler
   */
  CryptoHandler *get_crypto_handler(int type);

  CryptoRandom* random() const { return _crypto_random.get(); }

  /// check if experimental feature is enable, and emit appropriate warnings
  bool check_experimental_feature_enabled(const std::string& feature);
  bool check_experimental_feature_enabled(const std::string& feature,
					  std::ostream *message);

  ceph::PluginRegistry *get_plugin_registry() {
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

  void set_uid_gid_strings(const std::string &u, const std::string &g) {
    _set_uid_string = u;
    _set_gid_string = g;
  }
  std::string get_set_uid_string() const {
    return _set_uid_string;
  }
  std::string get_set_gid_string() const {
    return _set_gid_string;
  }

  class ForkWatcher {
   public:
    virtual ~ForkWatcher() {}
    virtual void handle_pre_fork() = 0;
    virtual void handle_post_fork() = 0;
  };

  void register_fork_watcher(ForkWatcher *w) {
    std::lock_guard lg(_fork_watchers_lock);
    _fork_watchers.push_back(w);
  }

  void notify_pre_fork();
  void notify_post_fork();

private:


  /* Stop and join the Ceph Context's service thread */
  void join_service_thread();

  uint32_t _module_type;

  int _init_flags;

  uid_t _set_uid; ///< uid to drop privs to
  gid_t _set_gid; ///< gid to drop privs to
  std::string _set_uid_string;
  std::string _set_gid_string;

  int _crypto_inited;

  /* libcommon service thread.
   * SIGHUP wakes this thread, which then reopens logfiles */
  friend class CephContextServiceThread;
  CephContextServiceThread *_service_thread;

  using md_config_obs_t = ceph::md_config_obs_impl<ConfigProxy>;

  md_config_obs_t *_log_obs;

  /* The admin socket associated with this context */
  AdminSocket *_admin_socket;

  /* lock which protects service thread creation, destruction, etc. */
  ceph::spinlock _service_thread_lock;

  /* The collection of profiling loggers associated with this context */
  PerfCountersCollection *_perf_counters_collection;

  md_config_obs_t *_perf_counters_conf_obs;

  CephContextHook *_admin_hook;

  ceph::HeartbeatMap *_heartbeat_map;

  ceph::spinlock associated_objs_lock;

  struct associated_objs_cmp {
    using is_transparent = std::true_type;
    template<typename T, typename U>
    bool operator ()(const std::pair<T, std::type_index>& l,
		     const std::pair<U, std::type_index>& r) const noexcept {
      return ((l.first < r.first)  ||
	      (l.first == r.first && l.second < r.second));
    }
  };

  std::map<std::pair<std::string, std::type_index>,
	   ceph::immobile_any<largest_singleton>,
	   associated_objs_cmp> associated_objs;
  std::set<std::string> associated_objs_drop_on_fork;

  ceph::spinlock _fork_watchers_lock;
  std::vector<ForkWatcher*> _fork_watchers;

  // crypto
  CryptoHandler *_crypto_none;
  CryptoHandler *_crypto_aes;
  std::unique_ptr<CryptoRandom> _crypto_random;

  // experimental
  CephContextObs *_cct_obs;
  ceph::spinlock _feature_lock;
  std::set<std::string> _experimental_features;

  ceph::PluginRegistry* _plugin_registry;

  md_config_obs_t *_lockdep_obs;

public:
  CrushLocation crush_location;
private:

  enum {
    l_cct_first,
    l_cct_total_workers,
    l_cct_unhealthy_workers,
    l_cct_last
  };
  enum {
    l_mempool_first = 873222,
    l_mempool_bytes,
    l_mempool_items,
    l_mempool_last
  };
  PerfCounters *_cct_perf = nullptr;
  PerfCounters* _mempool_perf = nullptr;
  std::vector<std::string> _mempool_perf_names, _mempool_perf_descriptions;

  /**
   * Enable the performance counters.
   */
  void _enable_perf_counter();

  /**
   * Disable the performance counter.
   */
  void _disable_perf_counter();

  /**
   * Refresh perf counter values.
   */
  void _refresh_perf_values();

  friend class CephContextObs;
};
#endif	// WITH_SEASTAR

#endif
