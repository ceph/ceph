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

#include <map>
#include <set>
#include <string>
#include <vector>

#include "msg/msg_types.h"
#include "include/encoding.h"
#include "include/types.h" // for epoch_t
#include "include/utime.h"
#include "common/ceph_releases.h"
#include "common/version.h"
#include "common/options.h"
#include "common/Clock.h"

namespace ceph { class Formatter; }

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

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
    void dump(ceph::Formatter *f) const;
    static std::list<ModuleOption> generate_test_instances();
  };

  class ModuleInfo
  {
    public:
    std::string name;
    bool can_run = true;
    std::string error_string;
    std::map<std::string,ModuleOption> module_options;

    void encode(ceph::buffer::list &bl) const;
    void decode(ceph::buffer::list::const_iterator &bl);

    bool operator==(const ModuleInfo &rhs) const
    {
      return (name == rhs.name) && (can_run == rhs.can_run);
    }

    void dump(ceph::Formatter *f) const ;
    static std::list<ModuleInfo> generate_test_instances();
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

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& p);
    void dump(ceph::Formatter *f) const;
    static std::list<StandbyInfo> generate_test_instances();

    bool have_module(const std::string &module_name) const;
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

  MgrMap() noexcept;
  ~MgrMap() noexcept;

  static MgrMap create_null_mgrmap();

  epoch_t get_epoch() const { return epoch; }
  epoch_t get_last_failure_osd_epoch() const { return last_failure_osd_epoch; }
  const entity_addrvec_t& get_active_addrs() const { return active_addrs; }
  uint64_t get_active_gid() const { return active_gid; }
  bool get_available() const { return available; }
  const std::string &get_active_name() const { return active_name; }
  const utime_t& get_active_change() const { return active_change; }
  int get_num_standby() const { return standbys.size(); }

  bool all_support_module(const std::string& module);

  bool have_module(const std::string &module_name) const;
  const ModuleInfo *get_module_info(const std::string &module_name) const;

  bool can_run_module(const std::string &module_name, std::string *error) const;

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

  std::set<std::string> get_all_names() const;
  std::set<std::string> get_always_on_modules() const;

  void encode(ceph::buffer::list& bl, uint64_t features) const;
  void decode(ceph::buffer::list::const_iterator& p);

  void dump(ceph::Formatter *f) const;

  static std::list<MgrMap> generate_test_instances();
  void print_summary(ceph::Formatter *f, std::ostream *ss) const;

  friend std::ostream& operator<<(std::ostream& out, const MgrMap& m);
  friend std::ostream& operator<<(std::ostream& out, const std::vector<ModuleInfo>& mi);
};

WRITE_CLASS_ENCODER_FEATURES(MgrMap)
WRITE_CLASS_ENCODER(MgrMap::StandbyInfo)
WRITE_CLASS_ENCODER(MgrMap::ModuleInfo);
WRITE_CLASS_ENCODER(MgrMap::ModuleOption);

#endif

