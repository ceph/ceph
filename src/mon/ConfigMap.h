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

//  See doc/rados/configuration/ceph-conf.rst

//  Configuration options stored by the monitor can be stored in:
//
//  * global section
//    - crush location (coarse to fine, ordered by type id)
//  * daemon type section (e.g., osd)
//    - device class (osd only)
//  * crush location (coarse to fine, ordered by type id)
//    - daemon name (e.g., mds.foo)
//
// Note that this means that if we have
//
//  config/host:foo/a = 1
//  config/osd/rack:foo/a = 2
//
// then we get a = 2.  The osd-level config wins, even though rack
// is less precise than host, because the crush limiters are only
// resolved within a section (global, per-daemon, per-instance).



// Options may have a mask associated with them to further restrict
// which daemons or clients the option applies to.
// Masks take two forms:
// * type:location - where type is a CRUSH property
//
// * class:device-class where device-class is the name of a CRUSH device class
//   - This mask has no effect on non-OSD daemons or clients.
struct MaskedOption {

  std::string raw_value;               ///< raw, unparsed, unvalidated value
  const Option *opt;              ///< the option
  struct OptionMask {
    std::string location_type, location_value; ///< matches crush_location
    std::string device_class;                  ///< matches device class

    bool empty() const;
    std::string to_str() const;
    void dump(ceph::Formatter *f) const;
  } mask;
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

// Indicates which daemons or clients the options apply to
struct Section {
  std::multimap<std::string,MaskedOption> options;

  void clear() {
    options.clear();
  }
  void dump(ceph::Formatter *f) const;
  std::string get_minimal_conf() const;
};

// TODO:
struct Profile {
  Option *opt;
  std::map<std::string, Section> profile;

  Profile(Option *o);

  int parse(
    CephContext *cct,
    const std::string& input,
    std::function<const Option *(const std::string&)> get_opt);
  void clear() {
    profile.clear();
  }
  void dump(ceph::Formatter *f) const;
};

struct ConfigMap {
  struct ValueSource {
    std::string section;
    const MaskedOption *option = nullptr;
    std::string profile_name;
    std::string profile_value;
    ValueSource() {}
    ValueSource(const std::string& s, const MaskedOption *o)
      : section(s), option(o) {}
    ValueSource(const std::string& s, const MaskedOption *o,
		const std::string& pn, const std::string& pv)
      : section(s), option(o), profile_name(pn), profile_value(pv) {}
  };

  Section global;
  std::map<std::string,Section, std::less<>> by_type;
  std::map<std::string,Section, std::less<>> by_id;
  std::map<std::string,Profile,std::less<>> profiles;
  std::list<std::unique_ptr<Option>> stray_options;

  const Option *get_option(
    CephContext *cct,
    const std::string& name,
    std::function<const Option *(const std::string&)> get_opt);

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
    profiles.clear();
    stray_options.clear();
  }
  void dump(ceph::Formatter *f) const;

  std::map<std::string,std::string,std::less<>> generate_entity_map(
    const EntityName& name,
    const std::map<std::string,std::string>& crush_location,
    const CrushWrapper *crush,
    const std::string& device_class,
    std::unordered_map<std::string,ValueSource> *src = nullptr);

  void parse_key(
    const std::string& key,
    std::string *name,
    std::string *who);
  static bool parse_mask(
    const std::string& in,
    std::string *section,
    MaskedOption::OptionMask *mask);

  int add_option(
    CephContext *cct,
    const std::string& name,
    const std::string& who,
    const std::string& value,
    std::function<const Option *(const std::string&)> get_opt);

  int add_profile(
    CephContext *cct,
    const std::string& name,
    const std::string& def,
    std::function<const Option *(const std::string&)> get_opt);
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
