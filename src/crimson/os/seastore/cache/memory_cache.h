// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {
class BackgroundListener;
class ExtentCallbackInterface;
class ExtentPlacementManager;

struct MemoryCache {
  virtual ~MemoryCache() = default;
  virtual void move_to_top(CachedExtent &extent) = 0;
  virtual void remove_from_cache(CachedExtent &extent) = 0;
  virtual void clear() = 0;
  virtual void set_background_callback(BackgroundListener *listener) = 0;
  virtual void set_extent_callback(ExtentCallbackInterface *cb) = 0;
  virtual std::size_t get_promotion_size() const = 0;
  virtual bool should_promote() const = 0;
  virtual seastar::future<> promote() = 0;
};
using MemoryCacheRef = std::unique_ptr<MemoryCache>;
MemoryCacheRef create_memory_cache(
  std::size_t capacity,
  std::size_t promotioin_size,
  ExtentPlacementManager *epm);
}
