// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <map>

#include "common/config_proxy.h"
#include "common/scrub_types.h"
#include "crimson/common/log.h"
#include "osd/ECUtil.h"
#include "osd/osd_types.h"

namespace crimson::osd::scrub {

struct chunk_validation_policy_t {
  pg_shard_t primary;
  std::optional<ECUtil::stripe_info_t> stripe_info;

  // osd_max_object_size
  size_t max_object_size;

  // osd_hit_set_namespace
  std::string hitset_namespace;

  // osd_deep_scrub_large_omap_object_key_threshold
  // osd_deep_scrub_large_omap_object_value_sum_threshold
  uint64_t omap_key_limit;
  size_t omap_bytes_limit;


  bool is_ec() const {
    return !!stripe_info;
  }

  size_t logical_to_ondisk_size(size_t size) const {
    return stripe_info ? stripe_info->logical_to_next_chunk_offset(size) : size;
  }
};

using scrub_map_set_t = std::map<pg_shard_t, ScrubMap>;

struct chunk_result_t {
  /* Scrub interacts with stats in two ways:
   * 1. scrub accumulates a subset of object_stat_sum_t members to
   *    to ultimately compare to the object_stat_sum_t value maintained
   *    by the OSD. These members will be referred to as
   *    *scrub_checked_stats*.
   *    See iterate_scrub_checked_stats() for the relevant members.
   * 2. scrub also updates some members that can't be maintained online
   *    (like num_omap_*, num_large_omap_objects) or that pertain
   *    specifically to scrub (like num_shallow_scrub_errors).
   *    Let these by referred to as *scrub_maintained_stats*.
   *    See iterate_scrub_maintained_stats() for the relevant members.
   *
   * The following stats member contains both, but the two sets are
   * disjoint and treated seperately.
   */
  object_stat_sum_t stats;

  // detected errors
  std::vector<inconsistent_snapset_wrapper> snapset_errors;
  std::vector<inconsistent_obj_wrapper> object_errors;

  bool has_errors() const {
    return !snapset_errors.empty() || !object_errors.empty();
  }
};

/**
 * validate_chunk
 *
 * Compares shard chunks and based on policy and returns a chunk_result_t
 * containing the results.  See chunk_result_t for details.
 */
chunk_result_t validate_chunk(
  DoutPrefixProvider &dpp,
  const chunk_validation_policy_t &policy, const scrub_map_set_t &in);

/**
 * iterate_scrub_checked_stats
 *
 * For each scrub_checked_stat member of object_stat_sum_t, invokes
 * op with three arguments:
 * - name of member (string_view)
 * - pointer to member (T object_stat_sum_t::*)
 * - function to corresponding pg_stat_t invalid member
 *   (bool func(const pg_stat_t &))
 *
 * Should be used to perform operations on all scrub_checked_stat members
 * such as checking the accumlated scrub stats against the maintained
 * pg stats.
 */
template <typename Func>
void foreach_scrub_checked_stat(Func &&op) {
  using namespace std::string_view_literals;
  op("num_objects"sv,
     &object_stat_sum_t::num_objects,
     [](const pg_stat_t &in) { return false; });
  op("num_bytes"sv,
     &object_stat_sum_t::num_bytes,
     [](const pg_stat_t &in) { return false; });
  op("num_object_clones"sv,
     &object_stat_sum_t::num_object_clones,
     [](const pg_stat_t &in) { return false; });
  op("num_whiteouts"sv,
     &object_stat_sum_t::num_whiteouts,
     [](const pg_stat_t &in) { return false; });
  op("num_objects_dirty"sv,
     &object_stat_sum_t::num_objects_dirty,
     [](const pg_stat_t &in) { return in.dirty_stats_invalid; });
  op("num_objects_omap"sv,
     &object_stat_sum_t::num_objects_omap,
     [](const pg_stat_t &in) { return in.omap_stats_invalid; });
  op("num_objects_pinned"sv,
     &object_stat_sum_t::num_objects_pinned,
     [](const pg_stat_t &in) { return in.pin_stats_invalid; });
  op("num_objects_hit_set_archive"sv,
     &object_stat_sum_t::num_objects_hit_set_archive,
     [](const pg_stat_t &in) { return in.hitset_stats_invalid; });
  op("num_bytes_hit_set_archive"sv,
     &object_stat_sum_t::num_bytes_hit_set_archive,
     [](const pg_stat_t &in) { return in.hitset_bytes_stats_invalid; });
  op("num_objects_manifest"sv,
     &object_stat_sum_t::num_objects_manifest,
     [](const pg_stat_t &in) { return in.manifest_stats_invalid; });
}

/**
 * iterate_scrub_maintained_stats
 *
 * For each scrub_maintained_stat member of object_stat_sum_t, invokes
 * op with three arguments:
 * - name of member (string_view)
 * - pointer to member (T object_stat_sum_t::*)
 * - skip for shallow (bool)
 *
 * Should be used to perform operations on all scrub_maintained_stat members
 * such as updating the pg maintained instance once scrub is complete.
 */
template <typename Func>
void foreach_scrub_maintained_stat(Func &&op) {
  using namespace std::string_view_literals;
  op("num_scrub_errors"sv, &object_stat_sum_t::num_scrub_errors, false);
  op("num_shallow_scrub_errors"sv,
     &object_stat_sum_t::num_shallow_scrub_errors,
     false);
  op("num_deep_scrub_errors"sv, &object_stat_sum_t::num_deep_scrub_errors, true);
  op("num_omap_bytes"sv, &object_stat_sum_t::num_omap_bytes, true);
  op("num_omap_keys"sv, &object_stat_sum_t::num_omap_keys, true);
  op("num_large_omap_objects"sv,
     &object_stat_sum_t::num_large_omap_objects,
     true);
}

}

template <>
struct fmt::formatter<crimson::osd::scrub::chunk_result_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(
    const crimson::osd::scrub::chunk_result_t &result, FormatContext& ctx) const
  {
    return fmt::format_to(
      ctx.out(),
      "chunk_result_t("
      "num_scrub_errors: {}, "
      "num_deep_scrub_errors: {}, "
      "snapset_errors: [{}], "
      "object_errors: [{}])",
      result.stats.num_scrub_errors,
      result.stats.num_deep_scrub_errors,
      result.snapset_errors,
      result.object_errors
    );
  }
};
