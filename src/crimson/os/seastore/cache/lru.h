// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

struct CachePolicy {
  virtual ~CachePolicy() {}
  virtual std::size_t get_capacity() const = 0;
  virtual std::size_t get_current_contents_bytes() const = 0;
  virtual std::size_t get_current_contents_extents() const = 0;
  virtual void remove_from_cache(CachedExtent &extent) = 0;
  virtual void move_to_top(CachedExtent &extent) = 0;
  virtual void clear() = 0;
};

class LRUCachePolicy : public CachePolicy {
  // max size (bytes)
  const size_t capacity = 0;

  // current size (bytes)
  size_t contents = 0;

  CachedExtent::list lru;

  void trim_to_capacity() {
    while (contents > capacity) {
      assert(lru.size() > 0);
      remove_from_cache(lru.front());
    }
  }

  void add_to_lru(CachedExtent &extent) {
    assert(extent.is_clean() && !extent.is_placeholder());

    if (!extent.primary_ref_list_hook.is_linked()) {
      contents += extent.get_length();
      intrusive_ptr_add_ref(&extent);
      lru.push_back(extent);
    }
    trim_to_capacity();
  }

public:
  LRUCachePolicy(size_t capacity) : capacity(capacity) {}

  size_t get_capacity() const final {
    return capacity;
  }

  size_t get_current_contents_bytes() const final {
    return contents;
  }

  size_t get_current_contents_extents() const final {
    return lru.size();
  }

  void remove_from_cache(CachedExtent &extent) final {
    assert(extent.is_clean() && !extent.is_placeholder());

    if (extent.primary_ref_list_hook.is_linked()) {
      lru.erase(lru.s_iterator_to(extent));
      assert(contents >= extent.get_length());
      contents -= extent.get_length();
      intrusive_ptr_release(&extent);
    }
  }

  void move_to_top(CachedExtent &extent) final {
    assert(extent.is_clean() && !extent.is_placeholder());

    if (extent.primary_ref_list_hook.is_linked()) {
      lru.erase(lru.s_iterator_to(extent));
      intrusive_ptr_release(&extent);
      assert(contents >= extent.get_length());
      contents -= extent.get_length();
    }
    add_to_lru(extent);
  }

  void clear() final {
    LOG_PREFIX(LRUMemoryCache::clear);
    for (auto iter = lru.begin(); iter != lru.end();) {
      SUBDEBUG(seastore_cache, "clearing {}", *iter);
      remove_from_cache(*(iter++));
    }
  }

  ~LRUCachePolicy() {
    clear();
  }
};
}
