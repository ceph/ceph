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
 * include files which can only be compiled in radosgw or OSD
 * contexts (e.g., rgw_sal.h, rgw_common.h) */

#pragma once

#include <string_view>
#include <fmt/format.h>

#include "common/dout.h"
#include "common/Formatter.h"

struct rgw_user {
  std::string tenant;
  std::string id;
  std::string ns;

  rgw_user() {}
  explicit rgw_user(const std::string& s) {
    from_str(s);
  }
  rgw_user(const std::string& tenant, const std::string& id, const std::string& ns="")
    : tenant(tenant),
      id(id),
      ns(ns) {
  }
  rgw_user(std::string&& tenant, std::string&& id, std::string&& ns="")
    : tenant(std::move(tenant)),
      id(std::move(id)),
      ns(std::move(ns)) {
  }

  void encode(ceph::buffer::list& bl) const {
    ENCODE_START(2, 1, bl);
    encode(tenant, bl);
    encode(id, bl);
    encode(ns, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator& bl) {
    DECODE_START(2, bl);
    decode(tenant, bl);
    decode(id, bl);
    if (struct_v >= 2) {
      decode(ns, bl);
    }
    DECODE_FINISH(bl);
  }

  void to_str(std::string& str) const {
    if (!tenant.empty()) {
      if (!ns.empty()) {
        str = tenant + '$' + ns + '$' + id;
      } else {
        str = tenant + '$' + id;
      }
    } else if (!ns.empty()) {
      str = '$' + ns + '$' + id;
    } else {
      str = id;
    }
  }

  void clear() {
    tenant.clear();
    id.clear();
    ns.clear();
  }

  bool empty() const {
    return id.empty();
  }

  std::string to_str() const {
    std::string s;
    to_str(s);
    return s;
  }

  void from_str(const std::string& str) {
    size_t pos = str.find('$');
    if (pos != std::string::npos) {
      tenant = str.substr(0, pos);
      std::string_view sv = str;
      std::string_view ns_id = sv.substr(pos + 1);
      size_t ns_pos = ns_id.find('$');
      if (ns_pos != std::string::npos) {
        ns = std::string(ns_id.substr(0, ns_pos));
        id = std::string(ns_id.substr(ns_pos + 1));
      } else {
        ns.clear();
        id = std::string(ns_id);
      }
    } else {
      tenant.clear();
      ns.clear();
      id = str;
    }
  }

  rgw_user& operator=(const std::string& str) {
    from_str(str);
    return *this;
  }

  int compare(const rgw_user& u) const {
    int r = tenant.compare(u.tenant);
    if (r != 0)
      return r;
    r = ns.compare(u.ns);
    if (r != 0) {
      return r;
    }
    return id.compare(u.id);
  }
  int compare(const std::string& str) const {
    rgw_user u(str);
    return compare(u);
  }

  bool operator!=(const rgw_user& rhs) const {
    return (compare(rhs) != 0);
  }
  bool operator==(const rgw_user& rhs) const {
    return (compare(rhs) == 0);
  }
  bool operator<(const rgw_user& rhs) const {
    if (tenant < rhs.tenant) {
      return true;
    } else if (tenant > rhs.tenant) {
      return false;
    }
    if (ns < rhs.ns) {
      return true;
    } else if (ns > rhs.ns) {
      return false;
    }
    return (id < rhs.id);
  }
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_user*>& o);
};
WRITE_CLASS_ENCODER(rgw_user)
