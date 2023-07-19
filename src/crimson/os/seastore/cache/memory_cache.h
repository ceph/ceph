// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {
struct MemoryCache {
  virtual ~MemoryCache() = default;
  virtual void move_to_top(CachedExtent &extent) = 0;
  virtual void remove_from_cache(CachedExtent &extent) = 0;
  virtual void clear() = 0;
};
using MemoryCacheRef = std::unique_ptr<MemoryCache>;
MemoryCacheRef create_memory_cache(std::size_t capacity);
}
