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

class StandbyInfo
{
public:
  uint64_t gid;
  std::string name;
  std::set<std::string> available_modules;

  StandbyInfo(uint64_t gid_, const std::string &name_,
	      std::set<std::string>& am)
    : gid(gid_), name(name_), available_modules(am)
  {}

  StandbyInfo()
    : gid(0)
  {}

  void encode(bufferlist& bl) const
  {
    ENCODE_START(2, 1, bl);
    ::encode(gid, bl);
    ::encode(name, bl);
    ::encode(available_modules, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p)
  {
    DECODE_START(2, p);
    ::decode(gid, p);
    ::decode(name, p);
    if (struct_v >= 2) {
      ::decode(available_modules, p);
    }
    DECODE_FINISH(p);
  }
};
WRITE_CLASS_ENCODER(StandbyInfo)

class MgrMap
{
public:
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

  std::set<std::string> modules;
  std::set<std::string> available_modules;

  // Map of module name to URI, indicating services exposed by
  // running modules on the active mgr daemon.
  std::map<std::string, std::string> services;

  epoch_t get_epoch() const { return epoch; }
  entity_addr_t get_active_addr() const { return active_addr; }
  uint64_t get_active_gid() const { return active_gid; }
  bool get_available() const { return available; }
  const std::string &get_active_name() const { return active_name; }

  bool all_support_module(const std::string& module) {
    if (!available_modules.count(module)) {
      return false;
    }
    for (auto& p : standbys) {
      if (!p.second.available_modules.count(module)) {
	return false;
      }
    }
    return true;
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
    ENCODE_START(3, 1, bl);
    ::encode(epoch, bl);
    ::encode(active_addr, bl, features);
    ::encode(active_gid, bl);
    ::encode(available, bl);
    ::encode(active_name, bl);
    ::encode(standbys, bl);
    ::encode(modules, bl);
    ::encode(available_modules, bl);
    ::encode(services, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::iterator& p)
  {
    DECODE_START(2, p);
    ::decode(epoch, p);
    ::decode(active_addr, p);
    ::decode(active_gid, p);
    ::decode(available, p);
    ::decode(active_name, p);
    ::decode(standbys, p);
    if (struct_v >= 2) {
      ::decode(modules, p);
      ::decode(available_modules, p);
    }
    if (struct_v >= 3) {
      ::decode(services, p);
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
      for (auto& j : i.second.available_modules) {
	f->dump_string("module", j);
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
    for (auto& j : available_modules) {
      f->dump_string("module", j);
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
};

WRITE_CLASS_ENCODER_FEATURES(MgrMap)

#endif

