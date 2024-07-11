// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <array>
#include "cephfs_features.h"
#include "mdstypes.h"

#include <fmt/format.h>

static const std::array feature_names
{
  "reserved",
  "reserved",
  "reserved",
  "reserved",
  "reserved",
  "jewel",
  "kraken",
  "luminous",
  "mimic",
  "reply_encoding",
  "reclaim_client",
  "lazy_caps_wanted",
  "multi_reconnect",
  "deleg_ino",
  "metric_collect",
  "alternate_name",
  "notify_session_state",
  "op_getvxattr",
  "32bits_retry_fwd",
  "new_snaprealm_info",
  "has_owner_uidgid",
  "client_mds_auth_caps",
};
static_assert(feature_names.size() == CEPHFS_FEATURE_MAX + 1);

std::string_view cephfs_feature_name(size_t id)
{
  if (id > feature_names.size())
    return "unknown";
  return feature_names[id];
}

int cephfs_feature_from_name(std::string_view name)
{
  if (name == "reserved") {
    return -1;
  }
  for (size_t i = 0; i < feature_names.size(); ++i) {
    if (name == feature_names[i])
      return i;
  }
  return -1;
}

std::string cephfs_stringify_features(const feature_bitset_t& features)
{
  CachedStackStringStream css;
  bool first = true;
  *css << "{";
  for (size_t i = 0; i < feature_names.size(); ++i) {
    if (!features.test(i))
      continue;
    if (!first)
      *css << ",";
    *css << i << "=" << cephfs_feature_name(i);
    first = false;
  }
  *css << "}";
  return css->str();
}

void cephfs_dump_features(ceph::Formatter *f, const feature_bitset_t& features)
{
  for (size_t i = 0; i < feature_names.size(); ++i) {
    if (!features.test(i))
      continue;
    f->dump_string(fmt::format("feature_{}", i),
		   cephfs_feature_name(i));
  }
}
