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

#include "groups.h"

#include "include/rados/librados.hpp"
#include "common/ceph_json.h"
#include "common/dout.h"
#include "cls/user/cls_user_client.h"
#include "rgw_sal.h"

namespace rgwrados::groups {

int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const RGWGroupInfo& group,
        bool exclusive, uint32_t limit)
{
  resource_metadata meta;
  meta.group_id = group.id;

  cls_user_account_resource resource;
  resource.name = group.name;
  resource.path = group.path;
  encode(meta, resource.metadata);

  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_account_resource_add(op, resource, exclusive, limit);
  return ref.operate(dpp, &op, y);
}

int remove(const DoutPrefixProvider* dpp,
           optional_yield y,
           librados::Rados& rados,
           const rgw_raw_obj& obj,
           std::string_view name)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectWriteOperation op;
  ::cls_user_account_resource_rm(op, name);
  return ref.operate(dpp, &op, y);
}

int list(const DoutPrefixProvider* dpp,
         optional_yield y,
         librados::Rados& rados,
         const rgw_raw_obj& obj,
         std::string_view marker,
         std::string_view path_prefix,
         uint32_t max_items,
         std::vector<std::string>& ids,
         std::string& next_marker)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  std::vector<cls_user_account_resource> entries;
  bool truncated = false;
  int ret = 0;
  ::cls_user_account_resource_list(op, marker, path_prefix, max_items,
                                   entries, &truncated, &next_marker, &ret);

  r = ref.operate(dpp, &op, nullptr, y);
  if (r == -ENOENT) {
    next_marker.clear();
    return 0;
  }
  if (r < 0) {
    return r;
  }
  if (ret < 0) {
    return ret;
  }

  for (auto& resource : entries) {
    resource_metadata meta;
    try {
      auto p = resource.metadata.cbegin();
      decode(meta, p);
    } catch (const buffer::error&) {
      return -EIO;
    }
    ids.push_back(std::move(meta.group_id));
  }

  if (!truncated) {
    next_marker.clear();
  }
  return 0;
}


void resource_metadata::dump(ceph::Formatter* f) const
{
  encode_json("group_id", group_id, f);
}

void resource_metadata::generate_test_instances(std::list<resource_metadata*>& o)
{
  o.push_back(new resource_metadata);
  auto m = new resource_metadata;
  m->group_id = "id";
  o.push_back(m);
}

} // namespace rgwrados::groups
