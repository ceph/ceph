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

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

void DaemonStateIndex::insert(DaemonStatePtr dm)
{
  Mutex::Locker l(lock);

  if (all.count(dm->key)) {
    _erase(dm->key);
  }

  by_server[dm->hostname][dm->key] = dm;
  all[dm->key] = dm;
}

void DaemonStateIndex::_erase(DaemonKey dmk)
{
  assert(lock.is_locked_by_me());

  const auto dm = all.at(dmk);
  auto &server_collection = by_server[dm->hostname];
  server_collection.erase(dm->key);
  if (server_collection.empty()) {
    by_server.erase(dm->hostname);
  }

  all.erase(dmk);
}

DaemonStateCollection DaemonStateIndex::get_by_type(uint8_t type) const
{
  Mutex::Locker l(lock);

  DaemonStateCollection result;

  for (const auto &i : all) {
    if (i.first.first == type) {
      result[i.first] = i.second;
    }
  }

  return result;
}

DaemonStateCollection DaemonStateIndex::get_by_server(const std::string &hostname) const
{
  Mutex::Locker l(lock);

  if (by_server.count(hostname)) {
    return by_server.at(hostname);
  } else {
    return {};
  }
}

bool DaemonStateIndex::exists(const DaemonKey &key) const
{
  Mutex::Locker l(lock);

  return all.count(key) > 0;
}

DaemonStatePtr DaemonStateIndex::get(const DaemonKey &key)
{
  Mutex::Locker l(lock);

  return all.at(key);
}

void DaemonStateIndex::cull(entity_type_t daemon_type,
                               std::set<std::string> names_exist)
{
  Mutex::Locker l(lock);

  std::set<DaemonKey> victims;

  for (const auto &i : all) {
    if (i.first.first != daemon_type) {
      continue;
    }

    if (names_exist.count(i.first.second) == 0) {
      victims.insert(i.first);
    }
  }

  for (const auto &i : victims) {
    dout(4) << "Removing data for " << i << dendl;
    _erase(i);
  }
}

void DaemonPerfCounters::update(MMgrReport *report)
{
  dout(20) << "loading " << report->declare_types.size() << " new types, "
           << report->packed.length() << " bytes of data" << dendl;

  // Load any newly declared types
  for (const auto &t : report->declare_types) {
    types.insert(std::make_pair(t.path, t));
    declared_types.insert(t.path);
  }

  const auto now = ceph_clock_now(g_ceph_context);

  // Parse packed data according to declared set of types
  bufferlist::iterator p = report->packed.begin();
  DECODE_START(1, p);
  for (const auto &t_path : declared_types) {
    const auto &t = types.at(t_path);
    uint64_t val = 0;
    uint64_t avgcount = 0;
    uint64_t avgcount2 = 0;

    ::decode(val, p);
    if (t.type & PERFCOUNTER_LONGRUNAVG) {
      ::decode(avgcount, p);
      ::decode(avgcount2, p);
    }
    // TODO: interface for insertion of avgs
    instances[t_path].push(now, val);
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

