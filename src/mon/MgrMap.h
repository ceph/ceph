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

#ifndef MGR_MAP_H_
#define MGR_MAP_H_

#include <sstream>
#include <set>

#include "msg/msg_types.h"
#include "include/encoding.h"
#include "include/utime.h"
#include "common/ceph_json.h"
#include "common/Formatter.h"
#include "common/ceph_releases.h"
#include "common/version.h"
#include "common/options.h"
#include "common/Clock.h"


class MgrMap
{
public:
  struct ModuleOption {
    std::string name;
    uint8_t type = Option::TYPE_STR;         // Option::type_t TYPE_*
    uint8_t level = Option::LEVEL_ADVANCED;  // Option::level_t LEVEL_*
    uint32_t flags = 0; // Option::flag_t FLAG_*
    std::string default_value;
    std::string min, max;
    std::set<std::string> enum_allowed;
    std::string desc, long_desc;
    std::set<std::string> tags;
    std::set<std::string> see_also;

    void encode(ceph::buffer::list& bl) const {
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
    void decode(ceph::buffer::list::const_iterator& p) {
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
    void dump(ceph::Formatter *f) const
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
    static void generate_test_instances(std::list<ModuleOption*>& ls)
    {
      ls.push_back(new ModuleOption);
      ls.push_back(new ModuleOption);
      ls.back()->name = "name";
      ls.back()->type = Option::TYPE_STR;
      ls.back()->level = Option::LEVEL_ADVANCED;
      ls.back()->flags = Option::FLAG_RUNTIME;
      ls.back()->default_value = "default_value";
      ls.back()->min = "min";
      ls.back()->max = "max";
      ls.back()->enum_allowed.insert("enum_allowed");
      ls.back()->desc = "desc";
      ls.back()->long_desc = "long_desc";
      ls.back()->tags.insert("tag");
      ls.back()->see_also.insert("see_also");
    }
  };

  class ModuleInfo
  {
    public:
    std::string name;
    bool can_run = true;
    std::string error_string;
    std::map<std::string,ModuleOption> module_options;

    // We do not include the module's `failed` field in the beacon,
    // because it is exposed via health checks.
    void encode(ceph::buffer::list &bl) const {
      ENCODE_START(2, 1, bl);
      encode(name, bl);
      encode(can_run, bl);
      encode(error_string, bl);
      encode(module_options, bl);
      ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &bl) {
      DECODE_START(1, bl);
      decode(name, bl);
      decode(can_run, bl);
      decode(error_string, bl);
      if (struct_v >= 2) {
	decode(module_options, bl);
      }
      DECODE_FINISH(bl);
    }

    bool operator==(const ModuleInfo &rhs) const
    {
      return (name == rhs.name) && (can_run == rhs.can_run);
    }

    void dump(ceph::Formatter *f) const 
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

    static void generate_test_instances(std::list<ModuleInfo*>& ls)
    {
      ls.push_back(new ModuleInfo);
      ls.push_back(new ModuleInfo);
      ls.back()->name = "name";
      ls.back()->can_run = true;
      ls.back()->error_string = "error_string";
      ls.back()->module_options["module_option"] = ModuleOption();
    }
  };

  class StandbyInfo
  {
  public:
    uint64_t gid = 0;
    std::string name;
    std::vector<ModuleInfo> available_modules;
    uint64_t mgr_features = 0;

    StandbyInfo(uint64_t gid_, const std::string &name_,
                const std::vector<ModuleInfo>& am,
		uint64_t feat)
      : gid(gid_), name(name_), available_modules(am),
	mgr_features(feat)
    {}

    StandbyInfo() {}

    void encode(ceph::buffer::list& bl) const
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

    void decode(ceph::buffer::list::const_iterator& p)
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
    void dump(ceph::Formatter *f) const
    {
      f->dump_unsigned("gid", gid);
      f->dump_string("name", name);
      encode_json("available_modules", available_modules, f);
      f->dump_unsigned("mgr_features", mgr_features);
    }
    static void generate_test_instances(std::list<StandbyInfo*>& ls)
    {
      ls.push_back(new StandbyInfo(1, "a", {}, 0));
      ls.push_back(new StandbyInfo(2, "b", {}, 0));
      ls.push_back(new StandbyInfo(3, "c", {}, 0));
    }

    bool have_module(const std::string &module_name) const
    {
      auto it = std::find_if(available_modules.begin(),
          available_modules.end(),
          [module_name](const ModuleInfo &m) -> bool {
            return m.name == module_name;
          });

      return it != available_modules.end();
    }
  };

  epoch_t epoch = 0;
  epoch_t last_failure_osd_epoch = 0;


  static const uint64_t FLAG_DOWN = (1<<0);
  uint64_t flags = 0;

  /// global_id of the ceph-mgr instance selected as a leader
  uint64_t active_gid = 0;
  /// server address reported by the leader once it is active
  entity_addrvec_t active_addrs;
  /// whether the nominated leader is active (i.e. has initialized its server)
  bool available = false;
  /// the name (foo in mgr.<foo>) of the active daemon
  std::string active_name;
  /// when the active mgr became active, or we lost the active mgr
  utime_t active_change;
  /// features
  uint64_t active_mgr_features = 0;

  std::multimap<std::string, entity_addrvec_t> clients; // for blocklist

  std::map<uint64_t, StandbyInfo> standbys;

  // Modules which are enabled
  std::set<std::string> modules;

  // Modules which should always be enabled. A manager daemon will enable
  // modules from the union of this set and the `modules` set above, latest
  // active version.
  std::map<uint32_t, std::set<std::string>> always_on_modules;

  // Modules which are always-on but have been force-disabled by user.
  std::set<std::string> force_disabled_modules;

  // Modules which are reported to exist
  std::vector<ModuleInfo> available_modules;

  // Map of module name to URI, indicating services exposed by
  // running modules on the active mgr daemon.
  std::map<std::string, std::string> services;

  static MgrMap create_null_mgrmap() {
    MgrMap null_map;
    /* Use the largest epoch so it's always bigger than whatever the mgr has. */
    null_map.epoch = std::numeric_limits<decltype(epoch)>::max();
    return null_map;
  }

  epoch_t get_epoch() const { return epoch; }
  epoch_t get_last_failure_osd_epoch() const { return last_failure_osd_epoch; }
  const entity_addrvec_t& get_active_addrs() const { return active_addrs; }
  uint64_t get_active_gid() const { return active_gid; }
  bool get_available() const { return available; }
  const std::string &get_active_name() const { return active_name; }
  const utime_t& get_active_change() const { return active_change; }
  int get_num_standby() const { return standbys.size(); }

  bool all_support_module(const std::string& module) {
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

  bool have_module(const std::string &module_name) const
  {
    for (const auto &i : available_modules) {
      if (i.name == module_name) {
        return true;
      }
    }

    return false;
  }

  const ModuleInfo *get_module_info(const std::string &module_name) const {
    for (const auto &i : available_modules) {
      if (i.name == module_name) {
        return &i;
      }
    }
    return nullptr;
  }

  bool can_run_module(const std::string &module_name, std::string *error) const
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

  bool module_enabled(const std::string& module_name) const
  {
    return modules.find(module_name) != modules.end();
  }

  bool any_supports_module(const std::string& module) const {
    if (have_module(module)) {
      return true;
    }
    for (auto& p : standbys) {
      if (p.second.have_module(module)) {
        return true;
      }
    }
    return false;
  }

  bool have_name(const std::string& name) const {
    if (active_name == name) {
      return true;
    }
    for (auto& p : standbys) {
      if (p.second.name == name) {
	return true;
      }
    }
    return false;
  }

  std::set<std::string> get_all_names() const {
    std::set<std::string> ls;
    if (active_name.size()) {
      ls.insert(active_name);
    }
    for (auto& p : standbys) {
      ls.insert(p.second.name);
    }
    return ls;
  }

  std::set<std::string> get_always_on_modules() const {
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

  void encode(ceph::buffer::list& bl, uint64_t features) const
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

  void decode(ceph::buffer::list::const_iterator& p)
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

  void dump(ceph::Formatter *f) const
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

  static void generate_test_instances(std::list<MgrMap*> &l)
  {
    l.push_back(new MgrMap);
  }

  void print_summary(ceph::Formatter *f, std::ostream *ss) const
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

  friend std::ostream& operator<<(std::ostream& out, const MgrMap& m) {
    std::ostringstream ss;
    m.print_summary(nullptr, &ss);
    return out << ss.str();
  }

  friend std::ostream& operator<<(std::ostream& out, const std::vector<ModuleInfo>& mi) {
    for (const auto &i : mi) {
      out << i.name << " ";
    }
    return out;
  }
};

WRITE_CLASS_ENCODER_FEATURES(MgrMap)
WRITE_CLASS_ENCODER(MgrMap::StandbyInfo)
WRITE_CLASS_ENCODER(MgrMap::ModuleInfo);
WRITE_CLASS_ENCODER(MgrMap::ModuleOption);

#endif

