// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_meta_sync_asio.h"

#include <string>
#include <fmt/format.h>

#include "rgw_meta_sync_common.h"

namespace rgw::sync::meta {
std::string_view Env::status_oid()
{
  return rgw::sync::meta::status_oid;
}


std::string Env::shard_obj_name(int shard_id)
{
  return fmt::format("{}.{}", status_shard_prefix, shard_id);
}
}
