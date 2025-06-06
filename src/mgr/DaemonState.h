// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef DAEMON_STATE_H_
#define DAEMON_STATE_H_

#include <map>
#include <string>
#include <memory>
#include <shared_mutex> // for std::shared_lock
#include <set>
#include <boost/circular_buffer.hpp>

#include "include/str_map.h"

#include "msg/msg_types.h"

// For PerfCounterType
#include "messages/MMgrReport.h"
#include "DaemonKey.h"

namespace ceph {
  class Formatter;
}

// An instance of a performance counter type, within
// a particular daemon.
class PerfCounterInstance
{
  class DataPoint
  {
    public:
    utime_t t;
    uint64_t v;
    DataPoint(utime_t t_, uint64_t v_)
      : t(t_), v(v_)
    {}
  };

  class AvgDataPoint
  {
    public:
    utime_t t;
    uint64_t s;
    uint64_t c;
    AvgDataPoint(utime_t t_, uint64_t s_, uint64_t c_)
      : t(t_), s(s_), c(c_)
    {}
  };

  boost::circular_buffer<DataPoint> buffer;
  boost::circular_buffer<AvgDataPoint> avg_buffer;

  uint64_t get_current() const;

  public:
  const boost::circular_buffer<DataPoint> & get_data() const
  {
    return buffer;
  }
  const DataPoint& get_latest_data() const
  {
    return buffer.back();
  }
  const boost::circular_buffer<AvgDataPoint> & get_data_avg() const
  {
    return avg_buffer;
  }
  const AvgDataPoint& get_latest_data_avg() const
  {
    return avg_buffer.back();
  }
  void push(utime_t t, uint64_t const &v);
  void push_avg(utime_t t, uint64_t const &s, uint64_t const &c);

  PerfCounterInstance(enum perfcounter_type_d type)
  {
    if (type & PERFCOUNTER_LONGRUNAVG)
      avg_buffer = boost::circular_buffer<AvgDataPoint>(20);
    else
      buffer = boost::circular_buffer<DataPoint>(20);
  };
};


typedef std::map<std::string, PerfCounterType> PerfCounterTypes;

// Performance counters for one daemon
class DaemonPerfCounters
{
  public:
  // The record of perf stat types, shared between daemons
  PerfCounterTypes &types;

  explicit DaemonPerfCounters(PerfCounterTypes &types_)
    : types(types_)
  {}

  std::map<std::string, PerfCounterInstance> instances;

  void update(const MMgrReport& report);

  void clear()
  {
    instances.clear();
  }
};

// The state that we store about one daemon
class DaemonState
{
  public:
  ceph::mutex lock = ceph::make_mutex("DaemonState::lock");

  DaemonKey key;

  // The hostname where daemon was last seen running (extracted
  // from the metadata)
  std::string hostname;

  // The metadata (hostname, version, etc) sent from the daemon
  std::map<std::string, std::string> metadata;

  /// device ids -> devname, derived from metadata[device_ids]
  std::map<std::string,std::string> devices;

  /// device ids -> by-path, derived from metadata[device_ids]
  std::map<std::string,std::string> devices_bypath;

  // TODO: this can be generalized to other daemons
  std::vector<DaemonHealthMetric> daemon_health_metrics;

  // Ephemeral state
  bool service_daemon = false;
  utime_t service_status_stamp;
  std::map<std::string, std::string> service_status;
  utime_t last_service_beacon;

  // running config
  std::map<std::string,std::map<int32_t,std::string>> config;

  // mon config values we failed to set
  std::map<std::string,std::string> ignored_mon_config;

  // compiled-in config defaults (rarely used, so we leave them encoded!)
  bufferlist config_defaults_bl;
  std::map<std::string,std::string> config_defaults;

  // The perf counters received in MMgrReport messages
  DaemonPerfCounters perf_counters;

  explicit DaemonState(PerfCounterTypes &types_)
    : perf_counters(types_)
  {
  }
  void set_metadata(const std::map<std::string,std::string>& m);
  const std::map<std::string,std::string>& _get_config_defaults();
};

typedef std::shared_ptr<DaemonState> DaemonStatePtr;
typedef std::map<DaemonKey, DaemonStatePtr> DaemonStateCollection;


struct DeviceState : public RefCountedObject
{
  std::string devid;
  /// (server,devname,path)
  std::set<std::tuple<std::string,std::string,std::string>> attachments;
  std::set<DaemonKey> daemons;

  std::map<std::string,std::string> metadata;  ///< persistent metadata

  std::pair<utime_t,utime_t> life_expectancy;  ///< when device failure is expected
  utime_t life_expectancy_stamp;          ///< when life expectency was recorded
  float wear_level = -1;                  ///< SSD wear level (negative if unknown)

  void set_metadata(std::map<std::string,std::string>&& m);

  void set_life_expectancy(utime_t from, utime_t to, utime_t now);
  void rm_life_expectancy();

  void set_wear_level(float wear);

  std::string get_life_expectancy_str(utime_t now) const;

  /// true of we can be safely forgotten/removed from memory
  bool empty() const {
    return daemons.empty() && metadata.empty();
  }

  void dump(Formatter *f) const;
  void print(std::ostream& out) const;

private:
  FRIEND_MAKE_REF(DeviceState);
  DeviceState(const std::string& n) : devid(n) {}
};

/**
 * Fuse the collection of per-daemon metadata from Ceph into
 * a view that can be queried by service type, ID or also
 * by server (aka fqdn).
 */
class DaemonStateIndex
{
private:
  mutable ceph::shared_mutex lock =
    ceph::make_shared_mutex("DaemonStateIndex", true, true, true);

  std::map<std::string, DaemonStateCollection> by_server;
  DaemonStateCollection all;
  std::set<DaemonKey> updating;

  std::map<std::string,ceph::ref_t<DeviceState>> devices;

  void _erase(const DaemonKey& dmk);

  ceph::ref_t<DeviceState> _get_or_create_device(const std::string& dev) {
    auto em = devices.try_emplace(dev, nullptr);
    auto& d = em.first->second;
    if (em.second) {
      d = ceph::make_ref<DeviceState>(dev);
    }
    return d;
  }
  void _erase_device(const ceph::ref_t<DeviceState>& d) {
    devices.erase(d->devid);
  }

public:
  DaemonStateIndex() {}

  // FIXME: shouldn't really be public, maybe construct DaemonState
  // objects internally to avoid this.
  PerfCounterTypes types;

  void insert(DaemonStatePtr dm);
  void _insert(DaemonStatePtr dm);
  bool exists(const DaemonKey &key) const;
  DaemonStatePtr get(const DaemonKey &key);
  void rm(const DaemonKey &key);
  void _rm(const DaemonKey &key);

  // Note that these return by value rather than reference to avoid
  // callers needing to stay in lock while using result.  Callers must
  // still take the individual DaemonState::lock on each entry though.
  DaemonStateCollection get_by_server(const std::string &hostname) const;
  DaemonStateCollection get_by_service(const std::string &svc_name) const;
  DaemonStateCollection get_all() const {return all;}

  template<typename Callback, typename...Args>
  auto with_daemons_by_server(Callback&& cb, Args&&... args) const ->
    decltype(cb(by_server, std::forward<Args>(args)...)) {
    std::shared_lock l{lock};
    
    return std::forward<Callback>(cb)(by_server, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  bool with_device(const std::string& dev,
		   Callback&& cb, Args&&... args) const {
    std::shared_lock l{lock};
    auto p = devices.find(dev);
    if (p == devices.end()) {
      return false;
    }
    std::forward<Callback>(cb)(*p->second, std::forward<Args>(args)...);
    return true;
  }

  template<typename Callback, typename...Args>
  bool with_device_write(const std::string& dev,
			 Callback&& cb, Args&&... args) {
    std::unique_lock l{lock};
    auto p = devices.find(dev);
    if (p == devices.end()) {
      return false;
    }
    std::forward<Callback>(cb)(*p->second, std::forward<Args>(args)...);
    if (p->second->empty()) {
      _erase_device(p->second);
    }
    return true;
  }

  template<typename Callback, typename...Args>
  void with_device_create(const std::string& dev,
			  Callback&& cb, Args&&... args) {
    std::unique_lock l{lock};
    auto d = _get_or_create_device(dev);
    std::forward<Callback>(cb)(*d, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  void with_devices(Callback&& cb, Args&&... args) const {
    std::shared_lock l{lock};
    for (auto& i : devices) {
      std::forward<Callback>(cb)(*i.second, std::forward<Args>(args)...);
    }
  }

  template<typename CallbackInitial, typename Callback, typename...Args>
  void with_devices2(CallbackInitial&& cbi,  // with lock taken
		     Callback&& cb,          // for each device
		     Args&&... args) const {
    std::shared_lock l{lock};
    cbi();
    for (auto& i : devices) {
      std::forward<Callback>(cb)(*i.second, std::forward<Args>(args)...);
    }
  }

  void list_devids_by_server(const std::string& server,
			     std::set<std::string> *ls) {
    auto m = get_by_server(server);
    for (auto& i : m) {
      std::lock_guard l(i.second->lock);
      for (auto& j : i.second->devices) {
	ls->insert(j.first);
      }
    }
  }

  void notify_updating(const DaemonKey &k) {
    std::unique_lock l{lock};
    updating.insert(k);
  }
  void clear_updating(const DaemonKey &k) {
    std::unique_lock l{lock};
    updating.erase(k);
  }
  bool is_updating(const DaemonKey &k) {
    std::shared_lock l{lock};
    return updating.count(k) > 0;
  }

  void update_metadata(DaemonStatePtr state,
		       const std::map<std::string,std::string>& meta) {
    // remove and re-insert in case the device metadata changed
    std::unique_lock l{lock};
    _rm(state->key);
    {
      std::lock_guard l2{state->lock};
      state->set_metadata(meta);
    }
    _insert(state);
  }

  /**
   * Remove state for all daemons of this type whose names are
   * not present in `names_exist`.  Use this function when you have
   * a cluster map and want to ensure that anything absent in the map
   * is also absent in this class.
   */
  void cull(const std::string& svc_name,
	    const std::set<std::string>& names_exist);
  void cull_services(const std::set<std::string>& types_exist);
};

#endif

