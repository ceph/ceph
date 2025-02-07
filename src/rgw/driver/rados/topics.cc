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

#include "topics.h"

#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "cls/user/cls_user_client.h"
#include "rgw_pubsub.h"
#include "rgw_sal.h"

namespace rgwrados::topics {

int add(const DoutPrefixProvider* dpp,
        optional_yield y,
        librados::Rados& rados,
        const rgw_raw_obj& obj,
        const rgw_pubsub_topic& topic,
        bool exclusive, uint32_t limit)
{
  cls_user_account_resource resource;
  resource.name = topic.name;

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
         uint32_t max_items,
         std::vector<std::string>& names,
         std::string& next_marker)
{
  rgw_rados_ref ref;
  int r = rgw_get_rados_ref(dpp, &rados, obj, &ref);
  if (r < 0) {
    return r;
  }

  librados::ObjectReadOperation op;
  const std::string path_prefix; // unused
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
    names.push_back(std::move(resource.name));
  }

  if (!truncated) {
    next_marker.clear();
  }
  return 0;
}

} // namespace rgwrados::topics
