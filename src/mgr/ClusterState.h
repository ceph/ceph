// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef CLUSTER_STATE_H_
#define CLUSTER_STATE_H_

#include "mds/FSMap.h"
#include "mon/MgrMap.h"
#include "common/Mutex.h"

#include "osdc/Objecter.h"
#include "mon/MonClient.h"
#include "mon/PGMap.h"
#include "mgr/ServiceMap.h"

class MMgrDigest;
class MMonMgrReport;
class MPGStats;


/**
 * Cluster-scope state (things like cluster maps) as opposed
 * to daemon-level state (things like perf counters and smart)
 */
class ClusterState
{
protected:
  MonClient *monc;
  Objecter *objecter;
  FSMap fsmap;
  ServiceMap servicemap;
  mutable Mutex lock;

  MgrMap mgr_map;

  map<int64_t,unsigned> existing_pools; ///< pools that exist, and pg_num, as of PGMap epoch
  PGMap pg_map;
  PGMap::Incremental pending_inc;

  bufferlist health_json;
  bufferlist mon_status_json;

  class ClusterSocketHook *asok_hook;

public:

  void load_digest(MMgrDigest *m);
  void ingest_pgstats(MPGStats *stats);

  void update_delta_stats();

  const bufferlist &get_health() const {return health_json;}
  const bufferlist &get_mon_status() const {return mon_status_json;}

  ClusterState(MonClient *monc_, Objecter *objecter_, const MgrMap& mgrmap);

  void set_objecter(Objecter *objecter_);
  void set_fsmap(FSMap const &new_fsmap);
  void set_mgr_map(MgrMap const &new_mgrmap);
  void set_service_map(ServiceMap const &new_service_map);

  void notify_osdmap(const OSDMap &osd_map);

  bool have_fsmap() const {
    std::lock_guard l(lock);
    return fsmap.get_epoch() > 0;
  }

  template<typename Callback, typename...Args>
  void with_servicemap(Callback&& cb, Args&&...args) const
  {
    std::lock_guard l(lock);
    std::forward<Callback>(cb)(servicemap, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  void with_fsmap(Callback&& cb, Args&&...args) const
  {
    std::lock_guard l(lock);
    std::forward<Callback>(cb)(fsmap, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  void with_mgrmap(Callback&& cb, Args&&...args) const
  {
    std::lock_guard l(lock);
    std::forward<Callback>(cb)(mgr_map, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  auto with_pgmap(Callback&& cb, Args&&...args) const ->
    decltype(cb(pg_map, std::forward<Args>(args)...))
  {
    std::lock_guard l(lock);
    return std::forward<Callback>(cb)(pg_map, std::forward<Args>(args)...);
  }

  template<typename Callback, typename...Args>
  auto with_mutable_pgmap(Callback&& cb, Args&&...args) ->
    decltype(cb(pg_map, std::forward<Args>(args)...))
  {
    std::lock_guard l(lock);
    return std::forward<Callback>(cb)(pg_map, std::forward<Args>(args)...);
  }

  template<typename... Args>
  void with_monmap(Args &&... args) const
  {
    std::lock_guard l(lock);
    ceph_assert(monc != nullptr);
    monc->with_monmap(std::forward<Args>(args)...);
  }

  template<typename... Args>
  auto with_osdmap(Args &&... args) const ->
    decltype(objecter->with_osdmap(std::forward<Args>(args)...))
  {
    ceph_assert(objecter != nullptr);
    return objecter->with_osdmap(std::forward<Args>(args)...);
  }

  // call cb(osdmap, pg_map, ...args) with the appropriate locks
  template <typename Callback, typename ...Args>
  auto with_osdmap_and_pgmap(Callback&& cb, Args&& ...args) const {
    ceph_assert(objecter != nullptr);
    std::lock_guard l(lock);
    return objecter->with_osdmap(
      std::forward<Callback>(cb),
      pg_map,
      std::forward<Args>(args)...);
  }
  void final_init();
  void shutdown();
  bool asok_command(std::string_view admin_command, const cmdmap_t& cmdmap,
		       std::string_view format, ostream& ss);
};

#endif

