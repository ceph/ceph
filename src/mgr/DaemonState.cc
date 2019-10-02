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

#include "DaemonState.h"

#include "MgrSession.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

void DaemonStateIndex::insert(DaemonStatePtr dm)
{
  RWLock::WLocker l(lock);

  if (all.count(dm->key)) {
    _erase(dm->key);
  }

  by_server[dm->hostname][dm->key] = dm;
  all[dm->key] = dm;
}

void DaemonStateIndex::_erase(const DaemonKey& dmk)
{
  assert(lock.is_wlocked());

  const auto to_erase = all.find(dmk);
  assert(to_erase != all.end());
  const auto dm = to_erase->second;
  auto &server_collection = by_server[dm->hostname];
  server_collection.erase(dm->key);
  if (server_collection.empty()) {
    by_server.erase(dm->hostname);
  }

  all.erase(to_erase);
}

DaemonStateCollection DaemonStateIndex::get_by_service(
  const std::string& svc) const
{
  RWLock::RLocker l(lock);

  DaemonStateCollection result;

  for (const auto &i : all) {
    if (i.first.first == svc) {
      result[i.first] = i.second;
    }
  }

  return result;
}

DaemonStateCollection DaemonStateIndex::get_by_server(
  const std::string &hostname) const
{
  RWLock::RLocker l(lock);

  if (by_server.count(hostname)) {
    return by_server.at(hostname);
  } else {
    return {};
  }
}

bool DaemonStateIndex::exists(const DaemonKey &key) const
{
  RWLock::RLocker l(lock);

  return all.count(key) > 0;
}

DaemonStatePtr DaemonStateIndex::get(const DaemonKey &key)
{
  RWLock::RLocker l(lock);

  auto iter = all.find(key);
  if (iter != all.end()) {
    return iter->second;
  } else {
    return nullptr;
  }
}

void DaemonStateIndex::cull(const std::string& svc_name,
			    const std::set<std::string>& names_exist)
{
  std::vector<string> victims;

  RWLock::WLocker l(lock);
  auto begin = all.lower_bound({svc_name, ""});
  auto end = all.end();
  for (auto &i = begin; i != end; ++i) {
    const auto& daemon_key = i->first;
    if (daemon_key.first != svc_name)
      break;
    if (names_exist.count(daemon_key.second) == 0) {
      victims.push_back(daemon_key.second);
    }
  }

  for (auto &i : victims) {
    dout(4) << "Removing data for " << i << dendl;
    _erase({svc_name, i});
  }
}

void DaemonPerfCounters::update(MMgrReport *report)
{
  dout(20) << "loading " << report->declare_types.size() << " new types, "
	   << report->undeclare_types.size() << " old types, had "
	   << types.size() << " types, got "
           << report->packed.length() << " bytes of data" << dendl;

  // Retrieve session state
  auto priv = report->get_connection()->get_priv();
  auto session = static_cast<MgrSession*>(priv.get());

  // Load any newly declared types
  for (const auto &t : report->declare_types) {
    types.insert(std::make_pair(t.path, t));
    session->declared_types.insert(t.path);
  }
  // Remove any old types
  for (const auto &t : report->undeclare_types) {
    session->declared_types.erase(t);
  }

  const auto now = ceph_clock_now();

  // Parse packed data according to declared set of types
  bufferlist::iterator p = report->packed.begin();
  DECODE_START(1, p);
  for (const auto &t_path : session->declared_types) {
    const auto &t = types.at(t_path);
    auto instances_it = instances.find(t_path);
    // Always check the instance exists, as we don't prevent yet
    // multiple sessions from daemons with the same name, and one
    // session clearing stats created by another on open.
    if (instances_it == instances.end()) {
      instances_it = instances.insert({t_path, t.type}).first;
    }
    uint64_t val = 0;
    uint64_t avgcount = 0;
    uint64_t avgcount2 = 0;

    ::decode(val, p);
    if (t.type & PERFCOUNTER_LONGRUNAVG) {
      ::decode(avgcount, p);
      ::decode(avgcount2, p);
      instances_it->second.push_avg(now, val, avgcount);
    } else {
      instances_it->second.push(now, val);
    }
  }
  DECODE_FINISH(p);
}

uint64_t PerfCounterInstance::get_current() const
{
  return buffer.front().v;
}

void PerfCounterInstance::push(utime_t t, uint64_t const &v)
{
  buffer.push_back({t, v});
}

void PerfCounterInstance::push_avg(utime_t t, uint64_t const &s,
                                   uint64_t const &c)
{
  avg_buffer.push_back({t, s, c});
}
