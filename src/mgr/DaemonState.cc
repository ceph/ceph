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
#include "include/stringify.h"
#include "common/Formatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

void DeviceState::set_metadata(map<string,string>&& m)
{
  metadata = std::move(m);
  auto p = metadata.find("expected_failure");
  if (p != metadata.end()) {
    expected_failure.parse(p->second);
  }
  p = metadata.find("expected_failure_stamp");
  if (p != metadata.end()) {
    expected_failure_stamp.parse(p->second);
  }
}

void DeviceState::set_expected_failure(utime_t when, utime_t now)
{
  expected_failure = when;
  expected_failure_stamp = now;
  metadata["expected_failure"] = stringify(expected_failure);
  metadata["expected_failure_stamp"] = stringify(expected_failure_stamp);
}

void DeviceState::rm_expected_failure()
{
  expected_failure = utime_t();
  expected_failure_stamp = utime_t();
  metadata.erase("expected_failure");
  metadata.erase("expected_failure_stamp");
}

void DeviceState::dump(Formatter *f) const
{
  f->dump_string("devid", devid);
  f->dump_string("host", server);
  f->open_array_section("daemons");
  for (auto& i : daemons) {
    f->dump_string("daemon", to_string(i));
  }
  f->close_section();
  if (expected_failure != utime_t()) {
    f->dump_stream("expected_failure") << expected_failure;
    f->dump_stream("expected_failure_stamp")
      << expected_failure_stamp;
  }
}

void DeviceState::print(ostream& out) const
{
  out << "device " << devid << "\n";
  out << "host " << server << "\n";
  set<string> d;
  for (auto& j : daemons) {
    d.insert(to_string(j));
  }
  out << "daemons " << d << "\n";
  if (expected_failure != utime_t()) {
    out << "expected_failure " << expected_failure
	<< " (as of " << expected_failure_stamp << ")\n";
  }
}

void DaemonStateIndex::insert(DaemonStatePtr dm)
{
  RWLock::WLocker l(lock);

  if (all.count(dm->key)) {
    _erase(dm->key);
  }

  by_server[dm->hostname][dm->key] = dm;
  all[dm->key] = dm;

  for (auto& devid : dm->devids) {
    auto d = _get_or_create_device(devid);
    d->daemons.insert(dm->key);
    d->server = dm->hostname;
  }
}

void DaemonStateIndex::_erase(const DaemonKey& dmk)
{
  assert(lock.is_wlocked());

  const auto to_erase = all.find(dmk);
  assert(to_erase != all.end());
  const auto dm = to_erase->second;

  for (auto& devid : dm->devids) {
    auto d = _get_or_create_device(devid);
    assert(d->daemons.count(dmk));
    d->daemons.erase(dmk);
    if (d->empty()) {
      _erase_device(d);
    }
  }

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

void DaemonStateIndex::rm(const DaemonKey &key)
{
  RWLock::WLocker l(lock);
  if (all.count(key)) {
    _erase(key);
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
    instances.insert(std::pair<std::string, PerfCounterInstance>(
                     t.path, PerfCounterInstance(t.type)));
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
    uint64_t val = 0;
    uint64_t avgcount = 0;
    uint64_t avgcount2 = 0;

    decode(val, p);
    if (t.type & PERFCOUNTER_LONGRUNAVG) {
      decode(avgcount, p);
      decode(avgcount2, p);
      instances.at(t_path).push_avg(now, val, avgcount);
    } else {
      instances.at(t_path).push(now, val);
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
