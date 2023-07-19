// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "seastar/core/metrics.hh"

#include "crimson/os/seastore/cache/memory_cache.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/transaction_manager.h"

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

enum class lru_extent_cache_state_t : uint8_t {
  FRESH = 0, // DEFAULT
  PENDING_PROMOTION,
  MAX
};
std::ostream &operator<<(std::ostream &out, lru_extent_cache_state_t state) {
  switch(state) {
  case lru_extent_cache_state_t::FRESH:
    return out << "cache_state(FRESH)";
  case lru_extent_cache_state_t::PENDING_PROMOTION:
    return out << "cache_state(PENDING_PROMOTION)";
  case lru_extent_cache_state_t::MAX:
    return out << "cache_state(INVALID)";
  }
};

lru_extent_cache_state_t get_cache_state(const CachedExtent &extent) {
  auto state = extent.get_cache_state();
  assert(state < static_cast<uint8_t>(lru_extent_cache_state_t::MAX));
  return static_cast<lru_extent_cache_state_t>(state);
}

void set_cache_state(CachedExtent &extent, lru_extent_cache_state_t state) {
  assert(state != lru_extent_cache_state_t::MAX);
  extent.set_cache_state(static_cast<uint8_t>(state));
}

/**
 * lru memory cache
 *
 * holds references to recently used extents
 */
class LRUMemoryCache : public MemoryCache {
  ExtentPlacementManager &epm;
  ExtentCallbackInterface *ecb = nullptr;
  BackgroundListener *listener = nullptr;

  // max size (bytes)
  const size_t capacity = 0;

  // current size (bytes)
  size_t contents = 0;

  CachedExtent::list lru;

  // max size of promotion list (bytes)
  const size_t promotion_size = 0;

  // current size of promotion list (bytes)
  size_t promote_contents = 0;

  CachedExtent::list promotion_list;

  size_t promoted_size = 0;

  seastar::metrics::metric_group metrics;

  void trim_to_capacity() {
    while (contents > capacity) {
      assert(lru.size() > 0);
      remove_impl(lru.front(), /*need_to_promote=*/ true);
    }
    if (should_promote()) {
      // TODO: wake up promote background process
    }
  }

  void add_to_lru(CachedExtent &extent) {
    assert(extent.is_clean() && !extent.is_placeholder());
    assert(get_cache_state(extent) == lru_extent_cache_state_t::FRESH);

    if (!extent.primary_ref_list_hook.is_linked()) {
      contents += extent.get_length();
      intrusive_ptr_add_ref(&extent);
      lru.push_back(extent);
    }
    trim_to_capacity();
  }

  bool should_promote_extnet(const CachedExtent &extent) {
    return epm.is_cold_device(extent.get_paddr().get_device_id());
  }

  void remove_impl(CachedExtent &extent, bool need_to_promote) {
    LOG_PREFIX(LRUMemoryCache::remove_impl);
    TRACE("release extent {} from memroy {}, need to promote: {},"
	  " promote_contents: {}",
	  extent, get_cache_state(extent), need_to_promote,
	  promote_contents);
    assert(extent.is_clean() && !extent.is_placeholder());

    if (extent.primary_ref_list_hook.is_linked()) {
      auto cache_state = get_cache_state(extent);

      if (cache_state == lru_extent_cache_state_t::FRESH) {
	assert(contents >= extent.get_length());
	contents -= extent.get_length();
	lru.erase(lru.s_iterator_to(extent));
	if (need_to_promote &&
	    promote_contents < promotion_size &&
	    should_promote_extnet(extent)) {
	  TRACE("move extent {} to pending promote list", extent);
	  promote_contents += extent.get_length();
	  promotion_list.push_back(extent);
	  set_cache_state(extent, lru_extent_cache_state_t::PENDING_PROMOTION);
	} else {
	  intrusive_ptr_release(&extent);
	}
      } else {
	assert(cache_state == lru_extent_cache_state_t::PENDING_PROMOTION);
	assert(promote_contents >= extent.get_length());
	promote_contents -= extent.get_length();
	set_cache_state(extent, lru_extent_cache_state_t::FRESH);
	promotion_list.erase(promotion_list.s_iterator_to(extent));
	intrusive_ptr_release(&extent);
      }
    }
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
        ),
        sm::make_counter(
          "promoted_bytes",
          [this] { return promoted_size; },
          sm::description("total bytes promoted to the hot tier")
        )
      }
    );
  }

public:
  LRUMemoryCache(size_t capacity, size_t promotion_size,
		 ExtentPlacementManager &epm)
    : epm(epm), ecb(nullptr), listener(nullptr),
      capacity(capacity), promotion_size(promotion_size) {
    LOG_PREFIX(LRUMemoryCache::LRUMemoryCache);
    INFO("memory_cache_capacity={}", capacity);
    register_metrics();
  }

  void remove_from_cache(CachedExtent &extent) final {
    remove_impl(extent, /*need_to_promote=*/ false);
  }

  void move_to_top(CachedExtent &extent) final {
    assert(extent.is_clean() && !extent.is_placeholder());

    remove_impl(extent, /*need_to_promote=*/ false);
    add_to_lru(extent);
  }

  void clear() final {
    LOG_PREFIX(LRUMemoryCache::clear);
    INFO("clear memory cache with {}({}B)",
	 lru.size() + promotion_list.size(),
	 contents + promote_contents);
    for (auto iter = lru.begin(); iter != lru.end();) {
      SUBDEBUG(seastore_cache, "clearing {}", *iter);
      remove_from_cache(*(iter++));
    }
    for (auto iter = promotion_list.begin(); iter != promotion_list.end();) {
      SUBDEBUG(seastore_cache, "clearing {}", *iter);
      remove_from_cache(*(iter++));
    }
  }

  void set_background_callback(BackgroundListener *l) final {
    listener = l;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) final {
    ecb = cb;
  }

  std::size_t get_promotion_size() const final {
    return promotion_size;
  }

  bool should_promote() const final {
    return promote_contents >= promotion_size / 2;
  }

  seastar::future<> promote() {
    LOG_PREFIX(LRUMemoryCache::promote);
    return repeat_eagain([this, FNAME] {
      return ecb->with_transaction_intr(
        Transaction::src_t::PROMOTE,
        "promote",
        [this, FNAME](auto &t) {
          std::list<CachedExtentRef> extents;
	  std::size_t promote_size = 0;
          for (auto &e : promotion_list) {
	    DEBUG("promote {} {} to the hot tier", e, get_cache_state(e));
	    promote_size += e.get_length();
            t.add_to_read_set(&e);
            extents.emplace_back(&e);
          }
          return seastar::do_with(
            std::move(extents),
            [this, promote_size, &t](auto &extents) {
              return trans_intr::do_for_each(extents, [this, &t](auto &extent) {
                ceph_assert(extent->is_clean());
                return ecb->promote_extent(t, extent);
              }).si_then([this, &t] {
                return ecb->submit_transaction_direct(t);
              }).si_then([this, promote_size] {
		promoted_size += promote_size;
	      });
            });
        });
    }).handle_error(crimson::ct_error::assert_all{"error occupied during promotion"});
  }

  ~LRUMemoryCache() {
    clear();
  }
};

MemoryCacheRef create_memory_cache(
  std::size_t capacity,
  std::size_t promotion_size,
  ExtentPlacementManager *epm)
{
  return std::make_unique<LRUMemoryCache>(capacity, promotion_size, *epm);
}

}

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::lru_extent_cache_state_t> : fmt::ostream_formatter {};
#endif
