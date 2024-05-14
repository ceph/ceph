// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {
class BackgroundListener;
class ExtentCallbackInterface;

struct NonVolatileCache {
  virtual ~NonVolatileCache() = default;
  virtual void move_to_top(
    laddr_t laddr,
    extent_types_t type,
    bool create_if_absent) = 0;
  virtual void remove(
    laddr_t laddr,
    extent_types_t type) = 0;
  virtual bool is_cached(
    laddr_t laddr,
    extent_types_t type) = 0;
  virtual void clear() = 0;
  virtual void set_background_callback(BackgroundListener *listener) = 0;
  virtual void set_extent_callback(ExtentCallbackInterface *cb) = 0;
  virtual bool could_demote() const = 0;
  virtual bool should_demote() const = 0;
  virtual seastar::future<> demote() = 0;
};
using NonVolatileCacheRef = std::unique_ptr<NonVolatileCache>;
NonVolatileCacheRef create_non_volatile_cache(
  std::size_t memory_capacity,
  std::size_t demote_size_per_cycle
);
}
