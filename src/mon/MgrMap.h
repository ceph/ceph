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

#include "msg/msg_types.h"
#include "common/Formatter.h"
#include "include/encoding.h"


class MgrMap
{
public:
  class ModuleInfo
  {
    public:
    std::string name;
    bool can_run = true;
    std::string error_string;

    // We do not include the module's `failed` field in the beacon,
    // because it is exposed via health checks.
    void encode(bufferlist &bl) const {
      ENCODE_START(1, 1, bl);
      encode(name, bl);
      encode(can_run, bl);
      encode(error_string, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator &bl) {
      DECODE_START(1, bl);
      decode(name, bl);
      decode(can_run, bl);
      decode(error_string, bl);
      DECODE_FINISH(bl);
    }

    bool operator==(const ModuleInfo &rhs) const
    {
      return (name == rhs.name) && (can_run == rhs.can_run);
    }

    void dump(Formatter *f) const {
      f->open_object_section("module");
      f->dump_string("name", name);
      f->dump_bool("can_run", can_run);
      f->dump_string("error_string", error_string);
      f->close_section();
    }
  };

  class StandbyInfo
  {
  public:
    uint64_t gid;
    std::string name;
    std::vector<ModuleInfo> available_modules;

    StandbyInfo(uint64_t gid_, const std::string &name_,
                const std::vector<ModuleInfo>& am)
      : gid(gid_), name(name_), available_modules(am)
    {}

    StandbyInfo()
      : gid(0)
    {}

    void encode(bufferlist& bl) const
    {
      ENCODE_START(3, 1, bl);
      encode(gid, bl);
      encode(name, bl);
      std::set<std::string> old_available_modules;
      for (const auto &i : available_modules) {
        old_available_modules.insert(i.name);
      }
      encode(old_available_modules, bl);  // version 2
      encode(available_modules, bl);  // version 3
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& p)
    {
      DECODE_START(3, p);
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
      DECODE_FINISH(p);
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

  /// global_id of the ceph-mgr instance selected as a leader
  uint64_t active_gid = 0;
  /// server address reported by the leader once it is active
  entity_addr_t active_addr;
  /// whether the nominated leader is active (i.e. has initialized its server)
  bool available = false;
  /// the name (foo in mgr.<foo>) of the active daemon
  std::string active_name;

  std::map<uint64_t, StandbyInfo> standbys;

  // Modules which are enabled
  std::set<std::string> modules;

  // Modules which are reported to exist
  std::vector<ModuleInfo> available_modules;

  // Map of module name to URI, indicating services exposed by
  // running modules on the active mgr daemon.
  std::map<std::string, std::string> services;

  epoch_t get_epoch() const { return epoch; }
  entity_addr_t get_active_addr() const { return active_addr; }
  uint64_t get_active_gid() const { return active_gid; }
  bool get_available() const { return available; }
  const std::string &get_active_name() const { return active_name; }

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

  bool have_name(const string& name) const {
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

  void encode(bufferlist& bl, uint64_t features) const
  {
    ENCODE_START(4, 1, bl);
    encode(epoch, bl);
    encode(active_addr, bl, features);
    encode(active_gid, bl);
    encode(available, bl);
    encode(active_name, bl);
    encode(standbys, bl);
    encode(modules, bl);

    // Pre-version 4 string list of available modules
    // (replaced by direct encode of ModuleInfo below)
    std::set<std::string> old_available_modules;
    for (const auto &i : available_modules) {
      old_available_modules.insert(i.name);
    }
    encode(old_available_modules, bl);

    encode(services, bl);
    encode(available_modules, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p)
  {
    DECODE_START(4, p);
    decode(epoch, p);
    decode(active_addr, p);
    decode(active_gid, p);
    decode(available, p);
    decode(active_name, p);
    decode(standbys, p);
    if (struct_v >= 2) {
      decode(modules, p);

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
    if (struct_v >= 3) {
      decode(services, p);
    }
    if (struct_v >= 4) {
      decode(available_modules, p);
    }
    DECODE_FINISH(p);
  }

  void dump(Formatter *f) const {
    f->dump_int("epoch", epoch);
    f->dump_int("active_gid", get_active_gid());
    f->dump_string("active_name", get_active_name());
    f->dump_stream("active_addr") << active_addr;
    f->dump_bool("available", available);
    f->open_array_section("standbys");
    for (const auto &i : standbys) {
      f->open_object_section("standby");
      f->dump_int("gid", i.second.gid);
      f->dump_string("name", i.second.name);
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
  }

  static void generate_test_instances(list<MgrMap*> &l) {
    l.push_back(new MgrMap);
  }

  void print_summary(Formatter *f, std::ostream *ss) const
  {
    // One or the other, not both
    assert((ss != nullptr) != (f != nullptr));
    if (f) {
      dump(f);
    } else {
      if (get_active_gid() != 0) {
	*ss << get_active_name();
        if (!available) {
          // If the daemon hasn't gone active yet, indicate that.
          *ss << "(active, starting)";
        } else {
          *ss << "(active)";
        }
      } else {
	*ss << "no daemons active";
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

  friend ostream& operator<<(ostream& out, const MgrMap& m) {
    ostringstream ss;
    m.print_summary(nullptr, &ss);
    return out << ss.str();
  }

  friend ostream& operator<<(ostream& out, const std::vector<ModuleInfo>& mi) {
    for (const auto &i : mi) {
      out << i.name << " ";
    }
    return out;
  }
};

WRITE_CLASS_ENCODER_FEATURES(MgrMap)
WRITE_CLASS_ENCODER(MgrMap::StandbyInfo)
WRITE_CLASS_ENCODER(MgrMap::ModuleInfo);

#endif

