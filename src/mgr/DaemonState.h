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
#include <set>
#include <boost/circular_buffer.hpp>

#include "common/RWLock.h"

#include "msg/msg_types.h"

// For PerfCounterType
#include "messages/MMgrReport.h"


// Unique reference to a daemon within a cluster
typedef std::pair<std::string, std::string> DaemonKey;

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

  boost::circular_buffer<DataPoint> buffer;
  uint64_t get_current() const;

  public:
  const boost::circular_buffer<DataPoint> & get_data() const
  {
    return buffer;
  }
  void push(utime_t t, uint64_t const &v);
  PerfCounterInstance()
    : buffer(20) {}
};


typedef std::map<std::string, PerfCounterType> PerfCounterTypes;

// Performance counters for one daemon
class DaemonPerfCounters
{
  public:
  // The record of perf stat types, shared between daemons
  PerfCounterTypes &types;

  DaemonPerfCounters(PerfCounterTypes &types_)
    : types(types_)
  {}

  std::map<std::string, PerfCounterInstance> instances;

  void update(MMgrReport *report);

  void clear()
  {
    instances.clear();
  }
};

// The state that we store about one daemon
class DaemonState
{
  public:
  Mutex lock = {"DaemonState::lock"};

  DaemonKey key;

  // The hostname where daemon was last seen running (extracted
  // from the metadata)
  std::string hostname;

  // The metadata (hostname, version, etc) sent from the daemon
  std::map<std::string, std::string> metadata;

  // Ephemeral state
  bool service_daemon = false;
  utime_t service_status_stamp;
  std::map<std::string, std::string> service_status;
  utime_t last_service_beacon;

  // The perf counters received in MMgrReport messages
  DaemonPerfCounters perf_counters;

  DaemonState(PerfCounterTypes &types_)
    : perf_counters(types_)
  {
  }
};

typedef std::shared_ptr<DaemonState> DaemonStatePtr;
typedef std::map<DaemonKey, DaemonStatePtr> DaemonStateCollection;




/**
 * Fuse the collection of per-daemon metadata from Ceph into
 * a view that can be queried by service type, ID or also
 * by server (aka fqdn).
 */
class DaemonStateIndex
{
  private:
  mutable RWLock lock = {"DaemonStateIndex", true, true, true};

  std::map<std::string, DaemonStateCollection> by_server;
  DaemonStateCollection all;
  std::set<DaemonKey> updating;

  void _erase(const DaemonKey& dmk);

  public:
  DaemonStateIndex() {}

  // FIXME: shouldn't really be public, maybe construct DaemonState
  // objects internally to avoid this.
  PerfCounterTypes types;

  void insert(DaemonStatePtr dm);
  bool exists(const DaemonKey &key) const;
  DaemonStatePtr get(const DaemonKey &key);

  // Note that these return by value rather than reference to avoid
  // callers needing to stay in lock while using result.  Callers must
  // still take the individual DaemonState::lock on each entry though.
  DaemonStateCollection get_by_server(const std::string &hostname) const;
  DaemonStateCollection get_by_service(const std::string &svc_name) const;
  DaemonStateCollection get_all() const {return all;}

  template<typename Callback, typename...Args>
  auto with_daemons_by_server(Callback&& cb, Args&&... args) const ->
    decltype(cb(by_server, std::forward<Args>(args)...)) {
    RWLock::RLocker l(lock);
    
    return std::forward<Callback>(cb)(by_server, std::forward<Args>(args)...);
  }

  void notify_updating(const DaemonKey &k) {
    RWLock::WLocker l(lock);
    updating.insert(k);
  }
  void clear_updating(const DaemonKey &k) {
    RWLock::WLocker l(lock);
    updating.erase(k);
  }
  bool is_updating(const DaemonKey &k) {
    RWLock::RLocker l(lock);
    return updating.count(k) > 0;
  }

  /**
   * Remove state for all daemons of this type whose names are
   * not present in `names_exist`.  Use this function when you have
   * a cluster map and want to ensure that anything absent in the map
   * is also absent in this class.
   */
  void cull(const std::string& svc_name,
	    const std::set<std::string>& names_exist);
};

#endif

