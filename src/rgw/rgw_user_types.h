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

#include <iosfwd>
#include <string>
#include <variant>
#include <fmt/format.h>

#include "common/dout.h"
#include "common/Formatter.h"

// strong typedef to std::string
struct rgw_account_id : std::string {
  using std::string::string;
  using std::string::operator=;
  explicit rgw_account_id(const std::string& s) : std::string(s) {}
};
void encode_json_impl(const char* name, const rgw_account_id& id, Formatter* f);
void decode_json_obj(rgw_account_id& id, JSONObj* obj);

struct rgw_user {
  // note: order of member variables matches the sort order of operator<=>
  std::string tenant;
  std::string ns;
  std::string id;

  rgw_user() {}
  explicit rgw_user(const std::string& s) {
    from_str(s);
  }
  rgw_user(const std::string& tenant, const std::string& id, const std::string& ns="")
    : tenant(tenant),
      ns(ns),
      id(id) {
  }
  rgw_user(std::string&& tenant, std::string&& id, std::string&& ns="")
    : tenant(std::move(tenant)),
      ns(std::move(ns)),
      id(std::move(id)) {
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

  friend auto operator<=>(const rgw_user&, const rgw_user&) = default;

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<rgw_user*>& o);
};
WRITE_CLASS_ENCODER(rgw_user)


/// Resources are either owned by accounts, or by users or roles (represented as
/// rgw_user) that don't belong to an account.
///
/// This variant is present in binary encoding formats, so existing types cannot
/// be changed or removed. New types can only be added to the end.
using rgw_owner = std::variant<rgw_user, rgw_account_id>;

rgw_owner parse_owner(const std::string& str);
std::string to_string(const rgw_owner& o);

std::ostream& operator<<(std::ostream& out, const rgw_owner& o);

void encode_json_impl(const char *name, const rgw_owner& o, ceph::Formatter *f);
void decode_json_obj(rgw_owner& o, JSONObj *obj);
