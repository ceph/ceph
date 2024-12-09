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

#include <experimental/iterator>

#include "MgrSession.h"
#include "include/stringify.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/debug.h"
#include "common/Formatter.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::ostringstream;
using std::string;
using std::stringstream;
using std::unique_ptr;

void DeviceState::set_metadata(map<string,string>&& m)
{
  metadata = std::move(m);
  auto p = metadata.find("life_expectancy_min");
  if (p != metadata.end()) {
    life_expectancy.first.parse(p->second);
  }
  p = metadata.find("life_expectancy_max");
  if (p != metadata.end()) {
    life_expectancy.second.parse(p->second);
  }
  p = metadata.find("life_expectancy_stamp");
  if (p != metadata.end()) {
    life_expectancy_stamp.parse(p->second);
  }
  p = metadata.find("wear_level");
  if (p != metadata.end()) {
    wear_level = atof(p->second.c_str());
  }
}

void DeviceState::set_life_expectancy(utime_t from, utime_t to, utime_t now)
{
  life_expectancy = make_pair(from, to);
  life_expectancy_stamp = now;
  if (from != utime_t()) {
    metadata["life_expectancy_min"] = stringify(from);
  } else {
    metadata["life_expectancy_min"] = "";
  }
  if (to != utime_t()) {
    metadata["life_expectancy_max"] = stringify(to);
  } else {
    metadata["life_expectancy_max"] = "";
  }
  if (now != utime_t()) {
    metadata["life_expectancy_stamp"] = stringify(now);
  } else {
    metadata["life_expectancy_stamp"] = "";
  }
}

void DeviceState::rm_life_expectancy()
{
  life_expectancy = make_pair(utime_t(), utime_t());
  life_expectancy_stamp = utime_t();
  metadata.erase("life_expectancy_min");
  metadata.erase("life_expectancy_max");
  metadata.erase("life_expectancy_stamp");
}

void DeviceState::set_wear_level(float wear)
{
  wear_level = wear;
  if (wear >= 0) {
    metadata["wear_level"] = stringify(wear);
  } else {
    metadata.erase("wear_level");
  }
}

string DeviceState::get_life_expectancy_str(utime_t now) const
{
  if (life_expectancy.first == utime_t()) {
    return string();
  }
  if (now >= life_expectancy.first) {
    return "now";
  }
  utime_t min = life_expectancy.first - now;
  utime_t max = life_expectancy.second - now;
  if (life_expectancy.second == utime_t()) {
    return string(">") + timespan_str(make_timespan(min));
  }
  string a = timespan_str(make_timespan(min));
  string b = timespan_str(make_timespan(max));
  if (a == b) {
    return a;
  }
  return a + " to " + b;
}

void DeviceState::dump(Formatter *f) const
{
  f->dump_string("devid", devid);
  f->open_array_section("location");
  for (auto& i : attachments) {
    f->open_object_section("attachment");
    f->dump_string("host", std::get<0>(i));
    f->dump_string("dev", std::get<1>(i));
    f->dump_string("path", std::get<2>(i));
    f->close_section();
  }
  f->close_section();
  f->open_array_section("daemons");
  for (auto& i : daemons) {
    f->dump_stream("daemon") << i;
  }
  f->close_section();
  if (life_expectancy.first != utime_t()) {
    f->dump_stream("life_expectancy_min") << life_expectancy.first;
    f->dump_stream("life_expectancy_max") << life_expectancy.second;
    f->dump_stream("life_expectancy_stamp")
      << life_expectancy_stamp;
  }
  if (wear_level >= 0) {
    f->dump_float("wear_level", wear_level);
  }
}

void DeviceState::print(ostream& out) const
{
  out << "device " << devid << "\n";
  for (auto& i : attachments) {
    out << "attachment " << std::get<0>(i) << " " << std::get<1>(i) << " "
	<< std::get<2>(i) << "\n";
    out << "\n";
  }
  std::copy(std::begin(daemons), std::end(daemons),
            std::experimental::make_ostream_joiner(out, ","));
  out << '\n';
  if (life_expectancy.first != utime_t()) {
    out << "life_expectancy " << life_expectancy.first << " to "
	<< life_expectancy.second
	<< " (as of " << life_expectancy_stamp << ")\n";
  }
  if (wear_level >= 0) {
    out << "wear_level " << wear_level << "\n";
  }
}

void DaemonState::set_metadata(const std::map<std::string,std::string>& m)
{
  devices.clear();
  devices_bypath.clear();
  metadata = m;
  if (auto found = m.find("device_ids"); found != m.end()) {
    auto& device_ids = found->second;
    std::map<std::string,std::string> paths; // devname -> id or path
    if (auto found = m.find("device_paths"); found != m.end()) {
      get_str_map(found->second, &paths, ",; ");
    }
    for_each_pair(
      device_ids, ",; ",
      [&paths, this](std::string_view devname, std::string_view id) {
	// skip blank ids
	if (id.empty()) {
	  return;
	}
	// id -> devname
	devices.emplace(id, devname);
	if (auto path = paths.find(std::string(id)); path != paths.end()) {
	  // id -> path
	  devices_bypath.emplace(id, path->second);
	}
      });
  }
  if (auto found = m.find("hostname"); found != m.end()) {
    hostname = found->second;
  }
}

const std::map<std::string,std::string>& DaemonState::_get_config_defaults()
{
  if (config_defaults.empty() &&
      config_defaults_bl.length()) {
    auto p = config_defaults_bl.cbegin();
    try {
      decode(config_defaults, p);
    } catch (buffer::error& e) {
    }
  }
  return config_defaults;
}

void DaemonStateIndex::insert(DaemonStatePtr dm)
{
  std::unique_lock l{lock};
  _insert(dm);
}

void DaemonStateIndex::_insert(DaemonStatePtr dm)
{
  if (all.count(dm->key)) {
    _erase(dm->key);
  }

  by_server[dm->hostname][dm->key] = dm;
  all[dm->key] = dm;

  for (auto& i : dm->devices) {
    auto d = _get_or_create_device(i.first);
    d->daemons.insert(dm->key);
    auto p = dm->devices_bypath.find(i.first);
    if (p != dm->devices_bypath.end()) {
      d->attachments.insert(std::make_tuple(dm->hostname, i.second, p->second));
    } else {
      d->attachments.insert(std::make_tuple(dm->hostname, i.second,
					    std::string()));
    }
  }
}

void DaemonStateIndex::_erase(const DaemonKey& dmk)
{
  ceph_assert(ceph_mutex_is_wlocked(lock));

  const auto to_erase = all.find(dmk);
  ceph_assert(to_erase != all.end());
  const auto dm = to_erase->second;

  for (auto& i : dm->devices) {
    auto d = _get_or_create_device(i.first);
    ceph_assert(d->daemons.count(dmk));
    d->daemons.erase(dmk);
    auto p = dm->devices_bypath.find(i.first);
    if (p != dm->devices_bypath.end()) {
      d->attachments.erase(make_tuple(dm->hostname, i.second, p->second));
    } else {
      d->attachments.erase(make_tuple(dm->hostname, i.second, std::string()));
    }
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
  std::shared_lock l{lock};

  DaemonStateCollection result;

  for (const auto& [key, state] : all) {
    if (key.type == svc) {
      result[key] = state;
    }
  }

  return result;
}

DaemonStateCollection DaemonStateIndex::get_by_server(
  const std::string &hostname) const
{
  std::shared_lock l{lock};

  if (auto found = by_server.find(hostname); found != by_server.end()) {
    return found->second;
  } else {
    return {};
  }
}

bool DaemonStateIndex::exists(const DaemonKey &key) const
{
  std::shared_lock l{lock};

  return all.count(key) > 0;
}

DaemonStatePtr DaemonStateIndex::get(const DaemonKey &key)
{
  std::shared_lock l{lock};

  auto iter = all.find(key);
  if (iter != all.end()) {
    return iter->second;
  } else {
    return nullptr;
  }
}

void DaemonStateIndex::rm(const DaemonKey &key)
{
  std::unique_lock l{lock};
  _rm(key);
}

void DaemonStateIndex::_rm(const DaemonKey &key)
{
  if (all.count(key)) {
    _erase(key);
  }
}

void DaemonStateIndex::cull(const std::string& svc_name,
			    const std::set<std::string>& names_exist)
{
  std::vector<string> victims;

  std::unique_lock l{lock};
  auto begin = all.lower_bound({svc_name, ""});
  auto end = all.end();
  for (auto &i = begin; i != end; ++i) {
    const auto& daemon_key = i->first;
    if (daemon_key.type != svc_name)
      break;
    if (names_exist.count(daemon_key.name) == 0) {
      victims.push_back(daemon_key.name);
    }
  }

  for (auto &i : victims) {
    DaemonKey daemon_key{svc_name, i};
    dout(4) << "Removing data for " << daemon_key << dendl;
    _erase(daemon_key);
  }
}

void DaemonStateIndex::cull_services(const std::set<std::string>& types_exist)
{
  std::set<DaemonKey> victims;

  std::unique_lock l{lock};
  for (auto it = all.begin(); it != all.end(); ++it) {
    const auto& daemon_key = it->first;
    if (it->second->service_daemon &&
        types_exist.count(daemon_key.type) == 0) {
      victims.insert(daemon_key);
    }
  }

  for (auto &i : victims) {
    dout(4) << "Removing data for " << i << dendl;
    _erase(i);
  }
}

void DaemonPerfCounters::update(const MMgrReport& report)
{
  dout(20) << "loading " << report.declare_types.size() << " new types, "
	   << report.undeclare_types.size() << " old types, had "
	   << types.size() << " types, got "
           << report.packed.length() << " bytes of data" << dendl;

  // Retrieve session state
  auto priv = report.get_connection()->get_priv();
  auto session = static_cast<MgrSession*>(priv.get());

  // Load any newly declared types
  for (const auto &t : report.declare_types) {
    types.insert(std::make_pair(t.path, t));
    session->declared_types.insert(t.path);
  }
  // Remove any old types
  for (const auto &t : report.undeclare_types) {
    session->declared_types.erase(t);
  }

  const auto now = ceph_clock_now();

  // Parse packed data according to declared set of types
  auto p = report.packed.cbegin();
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

    decode(val, p);
    if (t.type & PERFCOUNTER_LONGRUNAVG) {
      decode(avgcount, p);
      decode(avgcount2, p);
      instances_it->second.push_avg(now, val, avgcount);
    } else {
      instances_it->second.push(now, val);
    }
  }
  DECODE_FINISH(p);
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
