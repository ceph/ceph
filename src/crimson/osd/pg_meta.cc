// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_meta.h"

#include <string_view>

#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"

using std::string;
using std::string_view;
// prefix pgmeta_oid keys with _ so that PGLog::read_log_and_missing() can
// easily skip them
using crimson::os::FuturizedStore;

PGMeta::PGMeta(FuturizedStore::Shard& store, spg_t pgid)
  : store{store},
    pgid{pgid}
{}

namespace {
  template<typename T>
  std::optional<T> find_value(const FuturizedStore::Shard::omap_values_t& values,
                              string_view key)
  {
    auto found = values.find(key);
    if (found == values.end()) {
      return {};
    }
    auto p = found->second.cbegin();
    T value;
    decode(value, p);
    return std::make_optional(std::move(value));
  }
}

seastar::future<epoch_t> PGMeta::get_epoch()
{
  return store.open_collection(coll_t{pgid}).then([this](auto ch) {
    return store.omap_get_values(ch,
                                 pgid.make_pgmeta_oid(),
                                 {string{infover_key},
                                  string{epoch_key}}).safe_then(
    [](auto&& values) {
      {
        // sanity check
        auto infover = find_value<__u8>(values, infover_key);
        assert(infover);
        if (*infover < 10) {
          throw std::runtime_error("incompatible pg meta");
        }
      }
      {
        auto epoch = find_value<epoch_t>(values, epoch_key);
        assert(epoch);
        return seastar::make_ready_future<epoch_t>(*epoch);
      }
    },
    FuturizedStore::Shard::read_errorator::assert_all{
      "PGMeta::get_epoch: unable to read pgmeta"
    });
  });
}

seastar::future<std::tuple<pg_info_t, PastIntervals>> PGMeta::load()
{
  return store.open_collection(coll_t{pgid}).then([this](auto ch) {
    return store.omap_get_values(ch,
                                 pgid.make_pgmeta_oid(),
                                 {string{infover_key},
                                  string{info_key},
                                  string{biginfo_key},
                                  string{fastinfo_key}});
  }).safe_then([](auto&& values) {
    {
      // sanity check
      auto infover = find_value<__u8>(values, infover_key);
      assert(infover);
      if (infover < 10) {
        throw std::runtime_error("incompatible pg meta");
      }
    }
    pg_info_t info;
    {
      auto found = find_value<pg_info_t>(values, info_key);
      assert(found);
      info = *std::move(found);
    }
    PastIntervals past_intervals;
    {
      using biginfo_t = std::pair<PastIntervals, decltype(info.purged_snaps)>;
      auto big_info = find_value<biginfo_t>(values, biginfo_key);
      assert(big_info);
      past_intervals = std::move(big_info->first);
      info.purged_snaps = std::move(big_info->second);
    }
    {
      auto fast_info = find_value<pg_fast_info_t>(values, fastinfo_key);
      if (fast_info) {
        fast_info->try_apply_to(&info);
      }
    }
    return seastar::make_ready_future<std::tuple<pg_info_t, PastIntervals>>(
      std::make_tuple(std::move(info), std::move(past_intervals)));
  },
  FuturizedStore::Shard::read_errorator::assert_all{
    "PGMeta::load: unable to read pgmeta"
  });
}
