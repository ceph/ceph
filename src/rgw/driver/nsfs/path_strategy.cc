// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

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

#include "path_strategy.h"


namespace rgw { namespace sal { namespace nsfs {

static const std::string RGW_FOLDER_OBJECT_NAME{FOLDER_OBJECT};
static const std::string RGW_SHADOW_DIR{SHADOW_DIR};
static const std::string RGW_VERSIONS_DIR{VERSIONS_DIR};
static const std::string RGW_VERSIONS_LOCK{VERSIONS_LOCK};

std::string SentinelPathStrategy::object_name(const rgw_obj_key& key,
					 bool use_version) const
{
  std::string fname = use_version ? key.get_oid() : key.get_index_key_name();

    fname.insert(0, 1, '.');
  }

  /* a key ending in '/' is a directory object;  the directory carries the
   * sentinel because the directory itself cannot hold the data */
  if (!fname.empty() && (fname.back() == '/')) {
    fname += RGW_FOLDER_OBJECT_NAME;
  }

  return fname;
}

rgw_obj_key SentinelPathStrategy::key_from_name(const std::string& fname) const
{
  rgw_obj_key key;
  return key;
}

std::string SentinelPathStrategy::bucket_dir_name(
    const std::string& name, const std::optional<std::string>& ns) const
{
  if (ns) {
    return fmt::format(".{}_{}", *ns, name);
  }
  return name;
}

std::string SentinelPathStrategy::folder_object_name() const
{
  return RGW_FOLDER_OBJECT_NAME;
}

bool SentinelPathStrategy::names_directory_object(std::string_view entry) const
{
  return entry == RGW_FOLDER_OBJECT_NAME;
}


const ReservedNames& SentinelPathStrategy::reserved_names() const
{
  static const ReservedNames names{
    .exact = { RGW_SHADOW_DIR, RGW_VERSIONS_DIR, RGW_FOLDER_OBJECT_NAME,
	       RGW_VERSIONS_LOCK },
    .prefixes = {},
    .staging_prefixes = {},
    .content_exact = { RGW_FOLDER_OBJECT_NAME },
  };
  return names;
}


}}} // namespace rgw::sal::nsfs
