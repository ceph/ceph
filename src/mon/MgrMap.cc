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

#include "MgrMap.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"

#include <sstream>

void MgrMap::ModuleOption::encode(ceph::buffer::list& bl) const {
  ENCODE_START(1, 1, bl);
  encode(name, bl);
  encode(type, bl);
  encode(level, bl);
  encode(flags, bl);
  encode(default_value, bl);
  encode(min, bl);
  encode(max, bl);
  encode(enum_allowed, bl);
  encode(desc, bl);
  encode(long_desc, bl);
  encode(tags, bl);
  encode(see_also, bl);
  ENCODE_FINISH(bl);
}

void MgrMap::ModuleOption::decode(ceph::buffer::list::const_iterator& p) {
  DECODE_START(1, p);
  decode(name, p);
  decode(type, p);
  decode(level, p);
  decode(flags, p);
  decode(default_value, p);
  decode(min, p);
  decode(max, p);
  decode(enum_allowed, p);
  decode(desc, p);
  decode(long_desc, p);
  decode(tags, p);
  decode(see_also, p);
  DECODE_FINISH(p);
}

void MgrMap::ModuleOption::dump(ceph::Formatter *f) const
{
  f->dump_string("name", name);
  f->dump_string("type", Option::type_to_str(
    static_cast<Option::type_t>(type)));
  f->dump_string("level", Option::level_to_str(
    static_cast<Option::level_t>(level)));
  f->dump_unsigned("flags", flags);
  f->dump_string("default_value", default_value);
  f->dump_string("min", min);
  f->dump_string("max", max);
  f->open_array_section("enum_allowed");
  for (auto& i : enum_allowed) {
    f->dump_string("value", i);
  }
  f->close_section();
  f->dump_string("desc", desc);
  f->dump_string("long_desc", long_desc);
  f->open_array_section("tags");
  for (auto& i : tags) {
    f->dump_string("tag", i);
  }
  f->close_section();
  f->open_array_section("see_also");
  for (auto& i : see_also) {
    f->dump_string("option", i);
  }
  f->close_section();
}

std::list<MgrMap::ModuleOption> MgrMap::ModuleOption::generate_test_instances()
{
  std::list<ModuleOption> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().name = "name";
  ls.back().type = Option::TYPE_STR;
  ls.back().level = Option::LEVEL_ADVANCED;
  ls.back().flags = Option::FLAG_RUNTIME;
  ls.back().default_value = "default_value";
  ls.back().min = "min";
  ls.back().max = "max";
  ls.back().enum_allowed.insert("enum_allowed");
  ls.back().desc = "desc";
  ls.back().long_desc = "long_desc";
  ls.back().tags.insert("tag");
  ls.back().see_also.insert("see_also");
  return ls;
}

// We do not include the module's `failed` field in the beacon,
// because it is exposed via health checks.
void MgrMap::ModuleInfo::encode(ceph::buffer::list &bl) const {
  ENCODE_START(2, 1, bl);
  encode(name, bl);
  encode(can_run, bl);
  encode(error_string, bl);
  encode(module_options, bl);
  ENCODE_FINISH(bl);
}

void MgrMap::ModuleInfo::decode(ceph::buffer::list::const_iterator &bl) {
  DECODE_START(2, bl);
  decode(name, bl);
  decode(can_run, bl);
  decode(error_string, bl);
  if (struct_v >= 2) {
    decode(module_options, bl);
  }
  DECODE_FINISH(bl);
}

void MgrMap::ModuleInfo::dump(ceph::Formatter *f) const 
{
  f->open_object_section("module");
  f->dump_string("name", name);
  f->dump_bool("can_run", can_run);
  f->dump_string("error_string", error_string);
  f->open_object_section("module_options");
  for (auto& i : module_options) {
	f->dump_object(i.first.c_str(), i.second);
  }
  f->close_section();
  f->close_section();
}

std::list<MgrMap::ModuleInfo> MgrMap::ModuleInfo::generate_test_instances()
{
  std::list<ModuleInfo> ls;
  ls.emplace_back();
  ls.emplace_back();
  ls.back().name = "name";
  ls.back().can_run = true;
  ls.back().error_string = "error_string";
  ls.back().module_options["module_option"] = ModuleOption();
  return ls;
}

std::set<std::string> MgrMap::get_all_names() const {
  std::set<std::string> ls;
  if (active_name.size()) {
    ls.insert(active_name);
  }
  for (auto& p : standbys) {
    ls.insert(p.second.name);
  }
  return ls;
}

std::set<std::string> MgrMap::get_always_on_modules() const {
  unsigned rnum = to_integer<uint32_t>(ceph_release());
  auto it = always_on_modules.find(rnum);
  if (it == always_on_modules.end()) {
    // ok, try the most recent release
    if (always_on_modules.empty()) {
      return {}; // ugh
    }
    --it;
    if (it->first < rnum) {
      return it->second;
    }
    return {};      // wth
  }
  return it->second;
}

void MgrMap::StandbyInfo::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(4, 1, bl);
  encode(gid, bl);
  encode(name, bl);
  std::set<std::string> old_available_modules;
  for (const auto &i : available_modules) {
    old_available_modules.insert(i.name);
  }
  encode(old_available_modules, bl);  // version 2
  encode(available_modules, bl);  // version 3
  encode(mgr_features, bl); // v4
  ENCODE_FINISH(bl);
}

void MgrMap::StandbyInfo::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START(4, p);
  decode(gid, p);
  decode(name, p);
  if (struct_v >= 2) {
    std::set<std::string> old_available_modules;
    decode(old_available_modules, p);
    if (struct_v < 3) {
      for (const auto &name : old_available_modules) {
        MgrMap::ModuleInfo info;
        info.name = name;
        available_modules.push_back(std::move(info));
      }
    }
  }
  if (struct_v >= 3) {
    decode(available_modules, p);
  }
  if (struct_v >= 4) {
	decode(mgr_features, p);
  }
  DECODE_FINISH(p);
}

void MgrMap::StandbyInfo::dump(ceph::Formatter *f) const
{
  f->dump_unsigned("gid", gid);
  f->dump_string("name", name);
  encode_json("available_modules", available_modules, f);
  f->dump_unsigned("mgr_features", mgr_features);
}

std::list<MgrMap::StandbyInfo> MgrMap::StandbyInfo::generate_test_instances()
{
  std::list<StandbyInfo> ls;
  ls.push_back(StandbyInfo(1, "a", {}, 0));
  ls.push_back(StandbyInfo(2, "b", {}, 0));
  ls.push_back(StandbyInfo(3, "c", {}, 0));
  return ls;
}

bool MgrMap::StandbyInfo::have_module(const std::string &module_name) const
{
  auto it = std::find_if(available_modules.begin(),
      available_modules.end(),
      [module_name](const ModuleInfo &m) -> bool {
        return m.name == module_name;
      });

  return it != available_modules.end();
}

MgrMap::MgrMap() noexcept = default;
MgrMap::~MgrMap() noexcept = default;

MgrMap MgrMap::create_null_mgrmap() {
  MgrMap null_map;
  /* Use the largest epoch so it's always bigger than whatever the mgr has. */
  null_map.epoch = std::numeric_limits<decltype(epoch)>::max();
  return null_map;
}

bool MgrMap::all_support_module(const std::string& module) {
  if (!have_module(module)) {
    return false;
  }
  for (auto& p : standbys) {
    if (!p.second.have_module(module)) {
	return false;
    }
  }
  return true;
}

bool MgrMap::have_module(const std::string &module_name) const
{
  for (const auto &i : available_modules) {
    if (i.name == module_name) {
      return true;
    }
  }

  return false;
}

const MgrMap::ModuleInfo *MgrMap::get_module_info(const std::string &module_name) const {
  for (const auto &i : available_modules) {
    if (i.name == module_name) {
      return &i;
    }
  }
  return nullptr;
}

bool MgrMap::can_run_module(const std::string &module_name, std::string *error) const
{
  for (const auto &i : available_modules) {
    if (i.name == module_name) {
      *error = i.error_string;
      return i.can_run;
    }
  }

  std::ostringstream oss;
  oss << "Module '" << module_name << "' does not exist";
  throw std::logic_error(oss.str());
}

void MgrMap::encode(ceph::buffer::list& bl, uint64_t features) const
{
  if (!HAVE_FEATURE(features, SERVER_NAUTILUS)) {
    ENCODE_START(5, 1, bl);
    encode(epoch, bl);
    encode(active_addrs.legacy_addr(), bl, features);
    encode(active_gid, bl);
    encode(available, bl);
    encode(active_name, bl);
    encode(standbys, bl);
    encode(modules, bl);

    // Pre-version 4 std::string std::list of available modules
    // (replaced by direct encode of ModuleInfo below)
    std::set<std::string> old_available_modules;
    for (const auto &i : available_modules) {
	old_available_modules.insert(i.name);
    }
    encode(old_available_modules, bl);

    encode(services, bl);
    encode(available_modules, bl);
    ENCODE_FINISH(bl);
    return;
  }
  ENCODE_START(14, 6, bl);
  encode(epoch, bl);
  encode(active_addrs, bl, features);
  encode(active_gid, bl);
  encode(available, bl);
  encode(active_name, bl);
  encode(standbys, bl);
  encode(modules, bl);
  encode(services, bl);
  encode(available_modules, bl);
  encode(active_change, bl);
  encode(always_on_modules, bl);
  encode(active_mgr_features, bl);
  encode(last_failure_osd_epoch, bl);
  std::vector<std::string> clients_names;
  std::vector<entity_addrvec_t> clients_addrs;
  for (const auto& i : clients) {
    clients_names.push_back(i.first);
    clients_addrs.push_back(i.second);
  }
  // The address vector needs to be encoded first to produce a
  // backwards compatible messsage for older monitors.
  encode(clients_addrs, bl, features);
  encode(clients_names, bl, features);
  encode(flags, bl);
  encode(force_disabled_modules, bl);
  ENCODE_FINISH(bl);
  return;
}

void MgrMap::decode(ceph::buffer::list::const_iterator& p)
{
  DECODE_START(14, p);
  decode(epoch, p);
  decode(active_addrs, p);
  decode(active_gid, p);
  decode(available, p);
  decode(active_name, p);
  decode(standbys, p);
  if (struct_v >= 2) {
    decode(modules, p);

    if (struct_v < 6) {
	// Reconstitute ModuleInfos from names
	std::set<std::string> module_name_list;
	decode(module_name_list, p);
	// Only need to unpack this field if we won't have the full
	// MgrMap::ModuleInfo structures added in v4
	if (struct_v < 4) {
	  for (const auto &i : module_name_list) {
	    MgrMap::ModuleInfo info;
	    info.name = i;
	    available_modules.push_back(std::move(info));
	  }
	}
    }
  }
  if (struct_v >= 3) {
    decode(services, p);
  }
  if (struct_v >= 4) {
    decode(available_modules, p);
  }
  if (struct_v >= 7) {
    decode(active_change, p);
  } else {
    active_change = {};
  }
  if (struct_v >= 8) {
    decode(always_on_modules, p);
  }
  if (struct_v >= 9) {
    decode(active_mgr_features, p);
  }
  if (struct_v >= 10) {
    decode(last_failure_osd_epoch, p);
  }
  if (struct_v >= 11) {
    std::vector<entity_addrvec_t> clients_addrs;
    decode(clients_addrs, p);
    clients.clear();
    if (struct_v >= 12) {
	std::vector<std::string> clients_names;
	decode(clients_names, p);
	if (clients_names.size() != clients_addrs.size()) {
	  throw ceph::buffer::malformed_input(
	    "clients_names.size() != clients_addrs.size()");
	}
	auto cn = clients_names.begin();
	auto ca = clients_addrs.begin();
	for(; cn != clients_names.end(); ++cn, ++ca) {
	  clients.emplace(*cn, *ca);
	}
    } else {
	for (const auto& i : clients_addrs) {
	  clients.emplace("", i);
	}
    }
  }
  if (struct_v >= 13) {
    decode(flags, p);
  }

  if (struct_v >= 14) {
    decode(force_disabled_modules, p);
  }

  DECODE_FINISH(p);
}

void MgrMap::dump(ceph::Formatter *f) const
{
  f->dump_int("epoch", epoch);
  f->dump_int("flags", flags);
  f->dump_int("active_gid", get_active_gid());
  f->dump_string("active_name", get_active_name());
  f->dump_object("active_addrs", active_addrs);
  f->dump_stream("active_addr") << active_addrs.get_legacy_str();
  f->dump_stream("active_change") << active_change;
  f->dump_unsigned("active_mgr_features", active_mgr_features);
  f->dump_bool("available", available);
  f->open_array_section("standbys");
  for (const auto &i : standbys) {
    f->open_object_section("standby");
    f->dump_int("gid", i.second.gid);
    f->dump_string("name", i.second.name);
    f->dump_unsigned("mgr_features", i.second.mgr_features);
    f->open_array_section("available_modules");
    for (const auto& j : i.second.available_modules) {
      j.dump(f);
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();
  f->open_array_section("modules");
  for (auto& i : modules) {
    f->dump_string("module", i);
  }
  f->close_section();
  f->open_array_section("available_modules");
  for (const auto& j : available_modules) {
    j.dump(f);
  }
  f->close_section();

  f->open_object_section("services");
  for (const auto &i : services) {
    f->dump_string(i.first.c_str(), i.second);
  }
  f->close_section();

  f->open_object_section("always_on_modules");
  for (auto& v : always_on_modules) {
    f->open_array_section(ceph_release_name(v.first));
    for (auto& m : v.second) {
      f->dump_string("module", m);
    }
    f->close_section();
  }
  f->close_section(); // always_on_modules

  f->open_object_section("force_disabled_modules");
  for (auto& m : force_disabled_modules) {
      f->dump_string("module", m);
    }
  f->close_section();

  f->dump_int("last_failure_osd_epoch", last_failure_osd_epoch);
  f->open_array_section("active_clients");
  for (const auto& i : clients) {
    f->open_object_section("client");
    f->dump_string("name", i.first);
    i.second.dump(f);
    f->close_section();
  }
  f->close_section(); // active_clients
}

std::list<MgrMap> MgrMap::generate_test_instances()
{
  std::list<MgrMap> l;
  l.emplace_back();
  return l;
}

void MgrMap::print_summary(ceph::Formatter *f, std::ostream *ss) const
{
  // One or the other, not both
  ceph_assert((ss != nullptr) != (f != nullptr));
  if (f) {
    f->dump_bool("available", available);
    f->dump_int("num_standbys", standbys.size());
    f->open_array_section("modules");
    for (auto& i : modules) {
	f->dump_string("module", i);
    }
    f->close_section();
    f->open_object_section("services");
    for (const auto &i : services) {
	f->dump_string(i.first.c_str(), i.second);
    }
    f->close_section();
  } else {
    utime_t now = ceph_clock_now();
    if (get_active_gid() != 0) {
	*ss << get_active_name();
      if (!available) {
        // If the daemon hasn't gone active yet, indicate that.
        *ss << "(active, starting";
      } else {
        *ss << "(active";
      }
	if (active_change) {
	  *ss << ", since " << utimespan_str(now - active_change);
	}
	*ss << ")";
    } else {
	*ss << "no daemons active";
	if (active_change) {
	  *ss << " (since " << utimespan_str(now - active_change) << ")";
	}
    }
    if (standbys.size()) {
	*ss << ", standbys: ";
	bool first = true;
	for (const auto &i : standbys) {
	  if (!first) {
	    *ss << ", ";
	  }
	  *ss << i.second.name;
	  first = false;
	}
    }
  }
}

std::ostream& operator<<(std::ostream& out, const MgrMap& m) {
  std::ostringstream ss;
  m.print_summary(nullptr, &ss);
  return out << ss.str();
}

std::ostream& operator<<(std::ostream& out, const std::vector<MgrMap::ModuleInfo>& mi) {
  for (const auto &i : mi) {
    out << i.name << " ";
  }
  return out;
}
