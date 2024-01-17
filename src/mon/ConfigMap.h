// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <optional>
#include <ostream>
#include <string>

#include "include/utime.h"
#include "common/options.h"
#include "common/entity_name.h"

class CrushWrapper;

// the precedence is thus:
//
//  global
//   crush location (coarse to fine, ordered by type id)
//  daemon type (e.g., osd)
//   device class (osd only)
//   crush location (coarse to fine, ordered by type id)
//  daemon name (e.g., mds.foo)
//
// Note that this means that if we have
//
//  config/host:foo/a = 1
//  config/osd/rack:foo/a = 2
//
// then we get a = 2.  The osd-level config wins, even though rack
// is less precise than host, because the crush limiters are only
// resolved within a section (global, per-daemon, per-instance).

struct OptionMask {
  std::string location_type, location_value; ///< matches crush_location
  std::string device_class;                  ///< matches device class

  bool empty() const {
    return location_type.size() == 0
      && location_value.size() == 0
      && device_class.size() == 0;
  }

  std::string to_str() const {
    std::string r;
    if (location_type.size()) {
      r += location_type + ":" + location_value;
    }
    if (device_class.size()) {
      if (r.size()) {
	r += "/";
      }
      r += "class:" + device_class;
    }
    return r;
  }
  void dump(ceph::Formatter *f) const;
};

struct MaskedOption {
  std::string raw_value;               ///< raw, unparsed, unvalidated value
  const Option *opt;              ///< the option
  OptionMask mask;
  std::unique_ptr<const Option> unknown_opt; ///< if fabricated for an unknown option
  std::string localized_name;     ///< localized name for the option

  MaskedOption(const Option *o, bool fab=false) : opt(o) {
    if (fab) {
      unknown_opt.reset(o);
    }
  }
  MaskedOption(MaskedOption&& o) {
    raw_value = std::move(o.raw_value);
    opt = o.opt;
    mask = std::move(o.mask);
    unknown_opt = std::move(o.unknown_opt);
    localized_name = std::move(o.localized_name);
  }
  const MaskedOption& operator=(const MaskedOption& o) = delete;
  const MaskedOption& operator=(MaskedOption&& o) = delete;

  /// return a precision metric (smaller is more precise)
  int get_precision(const CrushWrapper *crush);

  friend std::ostream& operator<<(std::ostream& out, const MaskedOption& o);

  void dump(ceph::Formatter *f) const;
};

struct Section {
  std::multimap<std::string,MaskedOption> options;

  void clear() {
    options.clear();
  }
  void dump(ceph::Formatter *f) const;
  std::string get_minimal_conf() const;
};

struct ConfigMap {
  Section global;
  std::map<std::string,Section, std::less<>> by_type;
  std::map<std::string,Section, std::less<>> by_id;
  std::list<std::unique_ptr<Option>> stray_options;

  Section *find_section(const std::string& name) {
    if (name == "global") {
      return &global;
    }
    auto i = by_type.find(name);
    if (i != by_type.end()) {
      return &i->second;
    }
    i = by_id.find(name);
    if (i != by_id.end()) {
      return &i->second;
    }
    return nullptr;
  }
  void clear() {
    global.clear();
    by_type.clear();
    by_id.clear();
    stray_options.clear();
  }
  void dump(ceph::Formatter *f) const;
  std::map<std::string,std::string,std::less<>> generate_entity_map(
    const EntityName& name,
    const std::map<std::string,std::string>& crush_location,
    const CrushWrapper *crush,
    const std::string& device_class,
    std::map<std::string,std::pair<std::string,const MaskedOption*>> *src=0);

  void parse_key(
    const std::string& key,
    std::string *name,
    std::string *who);
  static bool parse_mask(
    const std::string& in,
    std::string *section,
    OptionMask *mask);
};


struct ConfigChangeSet {
  version_t version;
  utime_t stamp;
  std::string name;

  // key -> (old value, new value)
  std::map<std::string,std::pair<std::optional<std::string>,std::optional<std::string>>> diff;

  void dump(ceph::Formatter *f) const;
  void print(std::ostream& out) const;
};
