// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastar/core/metrics.hh"

#include "crimson/os/seastore/cache/memory_cache.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

/**
 * lru memory cache
 *
 * holds references to recently used extents
 */
class LRUMemoryCache : public MemoryCache {
  // max size (bytes)
  const size_t capacity = 0;

  // current size (bytes)
  size_t contents = 0;

  CachedExtent::list lru;

  seastar::metrics::metric_group metrics;

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

  void register_metrics() {
    namespace sm = seastar::metrics;
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "memory_cache_size_bytes",
          [this] { return contents; },
          sm::description("total bytes pinned by the memory cache")
        ),
        sm::make_counter(
          "memory_cache_size_extents",
          [this] { return lru.size(); },
          sm::description("total extents pinned by the memory cache")
        )
      }
    );
  }

public:
  LRUMemoryCache(size_t capacity) : capacity(capacity) {
    LOG_PREFIX(LRUMemoryCache::LRUMemoryCache);
    INFO("memory_cache_capacity={}", capacity);
    register_metrics();
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
    INFO("clear memory cache with {}({}B)", lru.size(), contents);
    for (auto iter = lru.begin(); iter != lru.end();) {
      SUBDEBUG(seastore_cache, "clearing {}", *iter);
      remove_from_cache(*(iter++));
    }
  }

  ~LRUMemoryCache() {
    clear();
  }
};

MemoryCacheRef create_memory_cache(std::size_t capacity) {
  return std::make_unique<LRUMemoryCache>(capacity);
}

}
