// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <cinttypes>
#include <string>
#include <string_view>

#include "include/types.h"
#include "common/ceph_json.h"

namespace rgw::sync::meta {
using namespace std::literals;
inline constexpr auto status_oid = "mdlog.sync-status"sv;
inline constexpr auto status_shard_prefix = "mdlog.sync-status.shard"sv;
inline constexpr auto full_sync_index_prefix = "meta.full-sync.index"sv;
inline constexpr auto bids_oid = "meta-sync-bids"sv;

struct log_info {
  std::uint32_t num_shards = 0;
  std::string period; //< period id of the master's oldest metadata log
  epoch_t realm_epoch = 0; //< realm epoch of oldest metadata log

  void decode_json(JSONObj* obj);
};



}
