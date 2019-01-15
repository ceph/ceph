// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
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
  void dump(Formatter *f) const;
};

struct MaskedOption {
  string raw_value;               ///< raw, unparsed, unvalidated value
  const Option *opt;              ///< the option
  OptionMask mask;
  unique_ptr<const Option> unknown_opt; ///< if fabricated for an unknown option

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
  }
  const MaskedOption& operator=(const MaskedOption& o) = delete;
  const MaskedOption& operator=(MaskedOption&& o) = delete;

  /// return a precision metric (smaller is more precise)
  int get_precision(const CrushWrapper *crush);

  friend ostream& operator<<(ostream& out, const MaskedOption& o);

  void dump(Formatter *f) const;
};

struct Section {
  std::multimap<std::string,MaskedOption> options;

  void clear() {
    options.clear();
  }
  void dump(Formatter *f) const;
  std::string get_minimal_conf() const;
};

struct ConfigMap {
  Section global;
  std::map<std::string,Section> by_type;
  std::map<std::string,Section> by_id;

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
  }
  void dump(Formatter *f) const;
  void generate_entity_map(
    const EntityName& name,
    const map<std::string,std::string>& crush_location,
    const CrushWrapper *crush,
    const std::string& device_class,
    std::map<std::string,std::string> *out,
    std::map<std::string,pair<std::string,const MaskedOption*>> *src=0);

  static bool parse_mask(
    const std::string& in,
    std::string *section,
    OptionMask *mask);
};


struct ConfigChangeSet {
  version_t version;
  utime_t stamp;
  string name;

  // key -> (old value, new value)
  map<string,pair<boost::optional<string>,boost::optional<string>>> diff;

  void dump(Formatter *f) const;
  void print(ostream& out) const;
};
