// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include <string_view>

namespace rgw::sync::meta {
using namespace std::literals;
inline constexpr auto status_oid = "mdlog.sync-status"sv;
inline constexpr auto status_shard_prefix = "mdlog.sync-status.shard"sv;
inline constexpr auto full_sync_index_prefix = "meta.full-sync.index"sv;
inline constexpr auto bids_oid = "meta-sync-bids"sv;
}
