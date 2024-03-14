// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <list>
#include <string>
#include "include/rados/librados_fwd.hpp"
#include "include/encoding.h"
#include "rgw_sal_fwd.h"

namespace ceph { class Formatter; }
class DoutPrefixProvider;
class optional_yield;
struct rgw_raw_obj;


namespace rgwrados::roles {

/// Add the given role to the list.
int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const rgw::sal::RGWRoleInfo& role,
        bool exclusive, uint32_t limit);

/// Look up a role's id by name in the list.
int get(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        std::string_view name,
        std::string& role_id);

/// Remove the given role from the list.
int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           librados::Rados& rados,
           const rgw_raw_obj& obj,
           std::string_view name);

/// Return a paginated listing of role ids.
int list(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw_raw_obj& obj,
         std::string_view marker,
         std::string_view path_prefix,
         uint32_t max_items,
         std::vector<std::string>& ids,
         std::string& next_marker);

// role-specific metadata for cls_user_account_resource
struct resource_metadata {
  std::string role_id;

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(role_id, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START(1, bl);
    decode(role_id, bl);
    DECODE_FINISH(bl);
  }

  void dump(ceph::Formatter* f) const;
  static void generate_test_instances(std::list<resource_metadata*>& o);
};
WRITE_CLASS_ENCODER(resource_metadata);

} // namespace rgwrados::roles
