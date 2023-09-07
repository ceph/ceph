// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string>

#include "include/types.h"

#include "common/Formatter.h"


static std::string RGW_STORAGE_CLASS_STANDARD = "STANDARD";

struct rgw_placement_rule {
  std::string name;
  std::string storage_class;

  rgw_placement_rule() {}
  rgw_placement_rule(const std::string& _n, const std::string& _sc) : name(_n), storage_class(_sc) {}
  rgw_placement_rule(const rgw_placement_rule& _r, const std::string& _sc) : name(_r.name) {
    if (!_sc.empty()) {
      storage_class = _sc;
    } else {
      storage_class = _r.storage_class;
    }
  }

  bool empty() const {
    return name.empty() && storage_class.empty();
  }

  void inherit_from(const rgw_placement_rule& r) {
    if (name.empty()) {
      name = r.name;
    }
    if (storage_class.empty()) {
      storage_class = r.storage_class;
    }
  }

  void clear() {
    name.clear();
    storage_class.clear();
  }

  void init(const std::string& n, const std::string& c) {
    name = n;
    storage_class = c;
  }

  static const std::string& get_canonical_storage_class(const std::string& storage_class) {
    if (storage_class.empty()) {
      return RGW_STORAGE_CLASS_STANDARD;
    }
    return storage_class;
  }

  const std::string& get_storage_class() const {
    return get_canonical_storage_class(storage_class);
  }

  int compare(const rgw_placement_rule& r) const {
    int c = name.compare(r.name);
    if (c != 0) {
      return c;
    }
    return get_storage_class().compare(r.get_storage_class());
  }

  bool operator==(const rgw_placement_rule& r) const {
    return (name == r.name &&
            get_storage_class() == r.get_storage_class());
  }

  bool operator!=(const rgw_placement_rule& r) const {
    return !(*this == r);
  }

  void encode(bufferlist& bl) const {
    /* no ENCODE_START/END due to backward compatibility */
    std::string s = to_str();
    ceph::encode(s, bl);
  }

  void decode(bufferlist::const_iterator& bl) {
    std::string s;
    ceph::decode(s, bl);
    from_str(s);
  }

  void dump(Formatter *f) const {
    f->dump_string("name", name);
    f->dump_string("storage_class", get_storage_class());
  }

  static void generate_test_instances(std::list<rgw_placement_rule*>& o) {
    o.push_back(new rgw_placement_rule);
    o.push_back(new rgw_placement_rule("name", "storage_class"));
  }

  std::string to_str() const {
    if (standard_storage_class()) {
      return name;
    }
    return to_str_explicit();
  }

  std::string to_str_explicit() const {
    return name + "/" + storage_class;
  }

  void from_str(const std::string& s) {
    size_t pos = s.find("/");
    if (pos == std::string::npos) {
      name = s;
      storage_class.clear();
      return;
    }
    name = s.substr(0, pos);
    storage_class = s.substr(pos + 1);
  }

  bool standard_storage_class() const {
    return storage_class.empty() || storage_class == RGW_STORAGE_CLASS_STANDARD;
  }
};
WRITE_CLASS_ENCODER(rgw_placement_rule)
