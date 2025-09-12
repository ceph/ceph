// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {
class BackgroundListener;
class ExtentCallbackInterface;
class ExtentPlacementManager;

struct ExtentPinboard {
  virtual ~ExtentPinboard() = default;
  virtual void register_metrics() = 0;
  virtual void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t *p_src,
    extent_len_t load_start,
    extent_len_t load_length) = 0;
  virtual void remove(CachedExtent &extent) = 0;
  virtual void get_stats(
    cache_stats_t &stats,
    bool report_detail,
    double seconds) const = 0;
  virtual std::size_t get_current_size_bytes() const = 0;
  virtual std::size_t get_current_num_extents() const = 0;
  virtual void increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t *p_src) = 0;
  virtual void clear() = 0;
  virtual void set_background_callback(BackgroundListener *listener) = 0;
  virtual void set_extent_callback(ExtentCallbackInterface *cb) = 0;
  virtual std::size_t get_promotion_size() const = 0;
  virtual bool should_promote() const = 0;
  virtual seastar::future<> promote() = 0;
};
using ExtentPinboardRef = std::unique_ptr<ExtentPinboard>;
ExtentPinboardRef create_extent_pinboard(
  std::size_t capacity, ExtentPlacementManager *epm);

} // namespace crimson::os::seastore
