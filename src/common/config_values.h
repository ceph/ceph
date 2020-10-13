// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#pragma once

#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>

#include "common/entity_name.h"
#include "common/options.h"
#include "log/SubsystemMap.h"
#include "msg/msg_types.h"

// @c ConfigValues keeps track of mappings from the config names to their values,
// debug logging settings, and some other "unnamed" settings, like entity name of
// the daemon.
class ConfigValues {
  using values_t = std::map<std::string, map<int32_t,Option::value_t>>;
  values_t values;
  // for populating md_config_impl::legacy_values in ctor
  friend struct md_config_t;

public:
  EntityName name;
  /// cluster name
  string cluster;
  ceph::logging::SubsystemMap subsys;
  bool no_mon_config = false;
  // Set of configuration options that have changed since the last
  // apply_changes
  using changed_set_t = std::set<std::string>;
  changed_set_t changed;

// This macro block defines C members of the md_config_t struct
// corresponding to the definitions in legacy_config_opts.h.
// These C members are consumed by code that was written before
// the new options.cc infrastructure: all newer code should
// be consume options via explicit get() rather than C members.
#define OPTION_OPT_INT(name) int64_t name;
#define OPTION_OPT_LONGLONG(name) int64_t name;
#define OPTION_OPT_STR(name) std::string name;
#define OPTION_OPT_DOUBLE(name) double name;
#define OPTION_OPT_FLOAT(name) double name;
#define OPTION_OPT_BOOL(name) bool name;
#define OPTION_OPT_ADDR(name) entity_addr_t name;
#define OPTION_OPT_ADDRVEC(name) entity_addrvec_t name;
#define OPTION_OPT_U32(name) uint64_t name;
#define OPTION_OPT_U64(name) uint64_t name;
#define OPTION_OPT_UUID(name) uuid_d name;
#define OPTION_OPT_SIZE(name) size_t name;
#define OPTION(name, ty)       \
  public:                      \
    OPTION_##ty(name)          
#define SAFE_OPTION(name, ty)       \
  protected:                        \
    OPTION_##ty(name)               
#include "common/legacy_config_opts.h"
#undef OPTION_OPT_INT
#undef OPTION_OPT_LONGLONG
#undef OPTION_OPT_STR
#undef OPTION_OPT_DOUBLE
#undef OPTION_OPT_FLOAT
#undef OPTION_OPT_BOOL
#undef OPTION_OPT_ADDR
#undef OPTION_OPT_ADDRVEC
#undef OPTION_OPT_U32
#undef OPTION_OPT_U64
#undef OPTION_OPT_UUID
#undef OPTION
#undef SAFE_OPTION

public:
  enum set_value_result_t {
    SET_NO_CHANGE,
    SET_NO_EFFECT,
    SET_HAVE_EFFECT,
  };
  /**
   * @return true if changed, false otherwise
   */
  set_value_result_t set_value(const std::string& key,
                               Option::value_t&& value,
                               int level);
  int rm_val(const std::string& key, int level);
  void set_logging(int which, const char* val);
  /**
   * @param level the level of the setting, -1 for the one with the 
   *              highest-priority
   */
  std::pair<Option::value_t, bool> get_value(const std::string& name,
                                             int level) const;
  template<typename Func> void for_each(Func&& func) const {
    for (const auto& [name,configs] : values) {
      func(name, configs);
    }
  }
  bool contains(const std::string& key) const;
};
