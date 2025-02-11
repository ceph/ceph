// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

/* N.B., this header defines fundamental serialized types.  Do not
 * introduce changes or include files which can only be compiled in
 * radosgw or OSD contexts (e.g., rgw_sal.h, rgw_common.h)
 */

#pragma once

#include <string>
#include <fmt/format.h>

#include "include/types.h"
#include "common/Formatter.h"

class JSONObj;

struct rgw_pool {
  std::string name;
  std::string ns;

  rgw_pool() = default;
  rgw_pool(const rgw_pool& _p) : name(_p.name), ns(_p.ns) {}
  rgw_pool(rgw_pool&&) = default;
  rgw_pool(const std::string& _s) {
    from_str(_s);
  }
  rgw_pool(const std::string& _name, const std::string& _ns) : name(_name), ns(_ns) {}

  std::string to_str() const;
  void from_str(const std::string& s);

  void init(const std::string& _s) {
    from_str(_s);
  }

  bool empty() const {
    return name.empty();
  }

  int compare(const rgw_pool& p) const {
    int r = name.compare(p.name);
    if (r != 0) {
      return r;
    }
    return ns.compare(p.ns);
  }

  void encode(ceph::buffer::list& bl) const {
     ENCODE_START(10, 10, bl);
    encode(name, bl);
    encode(ns, bl);
    ENCODE_FINISH(bl);
  }

  void decode_from_bucket(ceph::buffer::list::const_iterator& bl);

  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(10, 3, 3, bl);

    decode(name, bl);

    if (struct_v < 10) {

    /*
     * note that rgw_pool can be used where rgw_bucket was used before
     * therefore we inherit rgw_bucket's old versions. However, we only
     * need the first field from rgw_bucket. unless we add more fields
     * in which case we'll need to look at struct_v, and check the actual
     * version. Anything older than 10 needs to be treated as old rgw_bucket
     */

    } else {
      decode(ns, bl);
    }

    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter *f) const {
    f->dump_string("name", name);
    f->dump_string("ns", ns);
  }

  static void generate_test_instances(std::list<rgw_pool*>& o) {
    o.push_back(new rgw_pool);
    o.push_back(new rgw_pool("pool", "ns"));
  }

  rgw_pool& operator=(const rgw_pool&) = default;

  bool operator==(const rgw_pool& p) const {
    return (compare(p) == 0);
  }
  bool operator!=(const rgw_pool& p) const {
    return !(*this == p);
  }
  bool operator<(const rgw_pool& p) const {
    int r = name.compare(p.name);
    if (r == 0) {
      return (ns.compare(p.ns) < 0);
    }
    return (r < 0);
  }
};
WRITE_CLASS_ENCODER(rgw_pool)

inline std::ostream& operator<<(std::ostream& out, const rgw_pool& p) {
  out << p.to_str();
  return out;
}

struct rgw_data_placement_target {
  rgw_pool data_pool;
  rgw_pool data_extra_pool;
  rgw_pool index_pool;

  rgw_data_placement_target() = default;
  rgw_data_placement_target(const rgw_data_placement_target&) = default;
  rgw_data_placement_target(rgw_data_placement_target&&) = default;

  rgw_data_placement_target(const rgw_pool& data_pool,
                            const rgw_pool& data_extra_pool,
                            const rgw_pool& index_pool)
    : data_pool(data_pool),
      data_extra_pool(data_extra_pool),
      index_pool(index_pool) {
  }

  rgw_data_placement_target&
  operator=(const rgw_data_placement_target&) = default;

  const rgw_pool& get_data_extra_pool() const {
    if (data_extra_pool.empty()) {
      return data_pool;
    }
    return data_extra_pool;
  }

  int compare(const rgw_data_placement_target& t) {
    int c = data_pool.compare(t.data_pool);
    if (c != 0) {
      return c;
    }
    c = data_extra_pool.compare(t.data_extra_pool);
    if (c != 0) {
      return c;
    }
    return index_pool.compare(t.index_pool);
  };

  void dump(ceph::Formatter *f) const;
  void decode_json(JSONObj *obj);
};
