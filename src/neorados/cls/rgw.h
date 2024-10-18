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

#include <algorithm>
#include <concepts>
#include <iterator>
#include <ranges>
#include <string>

#include <neorados/RADOS.hpp>
#include "cls/rgw/cls_rgw_const.h"
#include "cls/rgw/cls_rgw_ops.h"

/// \namespace Neorados client interface for cls_rgw.
namespace neorados::cls::rgw {

/// \defgroup bi Bucket Index
/// @{

/// \brief Initialize a bucket index shard object.
///
/// Create a bucket index shard object and initialize its omap header.
///
/// Fails with -EEXIST if the object already exists.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_init()
{
  return ClsWriteOp{[] (WriteOp& op) {
    op.create(true);
    buffer::list in;
    op.exec(RGW_CLASS, RGW_BUCKET_INIT_INDEX, in);
  }};
}

/// \brief Initialize a bucket index shard object.
///
/// Create a bucket index shard object and initialize its omap header.
///
/// Fails with -EEXIST if the object already exists.
///
/// Fails with -EOPNOTSUPP if the osd version doesn't support the reshard log.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_init2()
{
  return ClsWriteOp{[] (WriteOp& op) {
    op.create(true);
    buffer::list in;
    op.exec(RGW_CLASS, RGW_BUCKET_INIT_INDEX2, in);
  }};
}

/// \brief Remove the bucket index shard object.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_clean()
{
  return ClsWriteOp{[] (WriteOp& op) {
    op.remove();
  }};
}

/// \brief Set the resharding status of a bucket index shard object.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_set_resharding(cls_rgw_reshard_status status)
{
  const auto call = cls_rgw_set_bucket_resharding_op{
    .entry = {.reshard_status = status}
  };
  buffer::list in;
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)] (WriteOp& op) {
    op.exec(RGW_CLASS, RGW_SET_BUCKET_RESHARDING, in);
  }};
}

/// \brief Write an entry directly to the bucket index shard object.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_put(rgw_cls_bi_entry entry)
{
  const auto call = rgw_cls_bi_put_op{.entry = std::move(entry)};
  buffer::list in;
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)] (WriteOp& op) {
    op.exec(RGW_CLASS, RGW_BI_PUT, in);
  }};
}

/// An input iterator whose value type is convertible to rgw_cls_bi_entry.
template <typename T>
concept bi_entry_iterator =
    std::input_iterator<T> &&
    std::convertible_to<std::iter_value_t<T>, rgw_cls_bi_entry>;

/// An input range whose value type is convertible to rgw_cls_bi_entry.
template <typename T>
concept bi_entry_range =
    std::ranges::input_range<T> &&
    bi_entry_iterator<std::ranges::iterator_t<T>>;

/// \brief Write entries directly to the bucket index shard object.
///
/// \param begin Beginning of a range of entries.
/// \param end End of a range of entries.
/// \param check_existing Subtract bucket stats for overwritten entries.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_put_entries(bi_entry_iterator auto begin,
                    bi_entry_iterator auto end,
                    bool check_existing)
{
  const auto call = rgw_cls_bi_put_entries_op{
    .entries = {begin, end},
    .check_existing = check_existing
  };
  bufferlist in;
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)] (WriteOp& op) {
    op.exec(RGW_CLASS, RGW_BI_PUT_ENTRIES, in);
  }};
}

/// \overload
[[nodiscard]] inline
auto bi_put_entries(bi_entry_range auto stats)
{
  // forward to iterator overload
  return bi_put_entries(std::ranges::begin(stats),
                        std::ranges::end(stats));
}

/// \brief List bucket index entries.
///
/// \param name_filter List entries whose key starts with the given prefix.
/// \param marker Resume listing after the given marker position.
/// \param max Maximum number of entries to return.
/// \param reshardlog When true, list entries from the reshard log.
/// \param out Output iterator to receive the entries.
/// \param truncated Output parameter set to true if more entries follow.
///
/// \return The ClsReadOp to be passed to ReadOp::exec
[[nodiscard]] inline
auto bi_list(std::string name_filter,
             std::string marker,
             uint32_t max,
             bool reshardlog,
             std::output_iterator<rgw_cls_bi_entry> auto out,
             bool& truncated)
{
  const auto call = rgw_cls_bi_list_op{
    .max = max,
    .name_filter = std::move(name_filter),
    .marker = std::move(marker),
    .reshardlog = reshardlog
  };
  buffer::list in;
  encode(call, in);
  return ClsReadOp{
    [in = std::move(in), out = std::move(out), &truncated] (ReadOp& op) {
      op.exec(RGW_CLASS, RGW_BI_LIST, in,
        [out = std::move(out), &truncated] (boost::system::error_code ec,
                                            const buffer::list& bl) {
          if (ec || bl.length() == 0) {
            truncated = false;
            return;
          }

          rgw_cls_bi_list_ret ret;
          auto iter = bl.cbegin();
          decode(ret, iter); // may throw, let it propagate

          std::move(ret.entries.begin(), ret.entries.end(), out);
          truncated = ret.is_truncated;
        });
    }
  };
}

/// \brief Remove all reshard log entries from a bucket index object.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_reshard_log_trim()
{
  return ClsWriteOp{[] (WriteOp& op) {
    buffer::list in;
    op.exec(RGW_CLASS, RGW_RESHARD_LOG_TRIM, in);
  }};
}

/// An input iterator whose value type is convertible to
/// std::pair<RGWObjCategory, rgw_bucket_category_stats>.
template <typename T>
concept category_stats_iterator =
    std::input_iterator<T> &&
    std::convertible_to<std::iter_value_t<T>,
        std::pair<RGWObjCategory, rgw_bucket_category_stats>>;

/// An input range whose value type is convertible to
/// std::pair<RGWObjCategory, rgw_bucket_category_stats>.
template <typename T>
concept category_stats_range =
    std::ranges::input_range<T> &&
    category_stats_iterator<std::ranges::iterator_t<T>>;

/// \brief Set stats for a bucket index object.
///
/// Set the bucket stats, replacing any existing categories.
///
/// \param begin Beginning of a range of category stats.
/// \param end End of a range of category stats.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_set_stats(category_stats_iterator auto begin,
                  category_stats_iterator auto end)
{
  const auto call = rgw_cls_bucket_update_stats_op{
    .absolute = true,
    .stats = {begin, end}
  };
  buffer::list in;
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)] (WriteOp& op) {
    op.exec(RGW_CLASS, RGW_BUCKET_UPDATE_STATS, in);
  }};
}

/// \overload
[[nodiscard]] inline
auto bi_set_stats(category_stats_range auto&& stats)
{
  // forward to iterator overload
  return bi_set_stats(std::ranges::begin(stats),
                      std::ranges::end(stats));
}

/// \brief Add stats to a bucket index object.
///
/// Add the bucket stats to any existing categories.
///
/// \param begin Beginning of a range of category stats.
/// \param end End of a range of category stats.
///
/// \return The ClsWriteOp to be passed to WriteOp::exec
[[nodiscard]] inline
auto bi_add_stats(category_stats_iterator auto begin,
                  category_stats_iterator auto end)
{
  const auto call = rgw_cls_bucket_update_stats_op{
    .absolute = false,
    .stats = {begin, end}
  };
  buffer::list in;
  encode(call, in);
  return ClsWriteOp{[in = std::move(in)] (WriteOp& op) {
    op.exec(RGW_CLASS, RGW_BUCKET_UPDATE_STATS, in);
  }};
}

/// \overload
[[nodiscard]] inline
auto bi_add_stats(category_stats_range auto stats)
{
  // forward to iterator overload
  return bi_add_stats(std::ranges::begin(stats),
                      std::ranges::end(stats));
}

/// @}

} // namespace neorados::cls::rgw
