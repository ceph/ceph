// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/extent_pinboard.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/transaction_manager.h"

#include <boost/unordered/unordered_flat_map.hpp>

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

/**
 * ExtentQueue
 *
 * A fixed-capacity queue for storing extents in memory. The implementation
 * holds references to stored extents(increasing their refcount) to prevent
 * release even when no transactions hold them. This queue serves as the
 * underlying implementation for ExtentPinboard.
 */
class ExtentQueue {
  // max size (bytes)
  const std::size_t capacity = 0;

  // current size (bytes)
  std::size_t current_size = 0;

  counter_by_extent_t<cache_size_stats_t> sizes_by_ext;
  cache_io_stats_t overall_io;
  counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
    trans_io_by_src_ext;

  mutable cache_io_stats_t last_overall_io;
  mutable counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
    last_trans_io_by_src_ext;

  CachedExtent::primary_ref_list list;

  void do_remove_from_list(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());
    assert(extent.is_linked_to_list());
    assert(list.size() > 0);
    auto extent_loaded_length = extent.get_loaded_length();
    assert(current_size >= extent_loaded_length);

    list.erase(list.s_iterator_to(extent));
    current_size -= extent_loaded_length;
    get_by_ext(sizes_by_ext, extent.get_type()).account_out(extent_loaded_length);
    overall_io.out_sizes.account_in(extent_loaded_length);
    if (p_src) {
      get_by_ext(
        get_by_src(trans_io_by_src_ext, *p_src),
        extent.get_type()
      ).out_sizes.account_in(extent_loaded_length);
    }
    intrusive_ptr_release(&extent);
  }

  std::list<CachedExtentRef> trim_to_capacity(
    const Transaction::src_t* p_src) {
    std::list<CachedExtentRef> ret;
    while (current_size > capacity) {
      ret.push_back(&list.front());
      do_remove_from_list(list.front(), p_src);
    }
    return ret;
  }

public:
  explicit ExtentQueue(std::size_t capacity) : capacity(capacity) {}

  std::size_t get_capacity_bytes() const {
    return capacity;
  }

  std::size_t get_current_size_bytes() const {
    return current_size;
  }

  std::size_t get_current_num_extents() const {
    return list.size();
  }

  void get_stats(
    std::string_view queue_name,
    cache_stats_t &stats,
    bool report_detail,
    double seconds) const ;

  void remove(CachedExtent &extent) {
    assert(extent.is_linked_to_list());

    do_remove_from_list(extent, nullptr);
  }

  std::list<CachedExtentRef> add_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());
    assert(!extent.is_linked_to_list());

    // absent, add to top (back)
    auto extent_loaded_length = extent.get_loaded_length();
    if (extent_loaded_length > 0) {
      current_size += extent_loaded_length;
      overall_io.in_sizes.account_in(extent_loaded_length);
      if (p_src) {
        get_by_ext(
          get_by_src(trans_io_by_src_ext, *p_src),
          extent.get_type()
        ).in_sizes.account_in(extent_loaded_length);
      }
    } // else: the extent isn't loaded upon touch_extent()/on_cache(),
      //       account the io later in increase_cached_size() upon read_extent()
    get_by_ext(sizes_by_ext, extent.get_type()).account_in(extent_loaded_length);
    intrusive_ptr_add_ref(&extent);
    list.push_back(extent);
    return trim_to_capacity(p_src);
  }

  void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());
    assert(extent.is_linked_to_list());

    // present, move to top (back)
    assert(list.size() > 0);
    assert(current_size >= extent.get_loaded_length());
    list.erase(list.s_iterator_to(extent));
    list.push_back(extent);
  }

  std::list<CachedExtentRef> increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t* p_src) {
    assert(extent.is_data_stable());
    assert(extent.is_linked_to_list());
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());

    // present, increase size
    assert(list.size() > 0);
    current_size += increased_length;
    get_by_ext(sizes_by_ext, extent.get_type()).account_parital_in(increased_length);
    overall_io.in_sizes.account_in(increased_length);
    if (p_src) {
      get_by_ext(
        get_by_src(trans_io_by_src_ext, *p_src),
        extent.get_type()
      ).in_sizes.account_in(increased_length);
    }

    return trim_to_capacity(p_src);
  }

  void clear() {
    LOG_PREFIX(ExtentQueue::clear);
    for (auto iter = list.begin(); iter != list.end();) {
      SUBDEBUG(seastore_cache, "clearing {}", *iter);
      do_remove_from_list(*(iter++), nullptr);
    }
  }

  ~ExtentQueue() {
    clear();
  }
};

void ExtentQueue::get_stats(
  std::string_view queue_name,
  cache_stats_t &stats,
  bool report_detail,
  double seconds) const
{
  LOG_PREFIX(ExtentQueue::get_stats);

  stats.pinboard_sizes = cache_size_stats_t{current_size, list.size()};
  stats.pinboard_io = overall_io;
  stats.pinboard_io.minus(last_overall_io);

  if (report_detail && seconds != 0) {
    counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
      _trans_io_by_src_ext = trans_io_by_src_ext;
    counter_by_src_t<cache_io_stats_t> trans_io_by_src;
    cache_io_stats_t trans_io;
    for (uint8_t _src=0; _src<TRANSACTION_TYPE_MAX; ++_src) {
      auto src = static_cast<transaction_type_t>(_src);
      auto& io_by_ext = get_by_src(_trans_io_by_src_ext, src);
      const auto& last_io_by_ext = get_by_src(last_trans_io_by_src_ext, src);
      auto& trans_io_per_src = get_by_src(trans_io_by_src, src);
      for (uint8_t _ext=0; _ext<EXTENT_TYPES_MAX; ++_ext) {
        auto ext = static_cast<extent_types_t>(_ext);
        auto& extent_io = get_by_ext(io_by_ext, ext);
        const auto& last_extent_io = get_by_ext(last_io_by_ext, ext);
        extent_io.minus(last_extent_io);
        trans_io_per_src.add(extent_io);
      }
      trans_io.add(trans_io_per_src);
    }
    cache_io_stats_t other_io = stats.pinboard_io;
    other_io.minus(trans_io);

    std::ostringstream oss;
    oss << "\n" << queue_name << " total" << stats.pinboard_sizes;
    cache_size_stats_t data_sizes;
    cache_size_stats_t mdat_sizes;
    cache_size_stats_t phys_sizes;
    for (uint8_t _ext=0; _ext<EXTENT_TYPES_MAX; ++_ext) {
      auto ext = static_cast<extent_types_t>(_ext);
      const auto& extent_sizes = get_by_ext(sizes_by_ext, ext);
      if (is_data_type(ext)) {
        data_sizes.add(extent_sizes);
      } else if (is_logical_metadata_type(ext)) {
        mdat_sizes.add(extent_sizes);
      } else if (is_physical_type(ext)) {
        phys_sizes.add(extent_sizes);
      }
    }
    oss << "\n  data" << data_sizes
        << "\n  mdat" << mdat_sizes
        << "\n  phys" << phys_sizes;

    oss << "\n" << queue_name << " io: trans-"
        << cache_io_stats_printer_t{seconds, trans_io}
        << "; other-"
        << cache_io_stats_printer_t{seconds, other_io};
    for (uint8_t _src=0; _src<TRANSACTION_TYPE_MAX; ++_src) {
      auto src = static_cast<transaction_type_t>(_src);
      const auto& trans_io_per_src = get_by_src(trans_io_by_src, src);
      if (trans_io_per_src.is_empty()) {
        continue;
      }
      cache_io_stats_t data_io;
      cache_io_stats_t mdat_io;
      cache_io_stats_t phys_io;
      const auto& io_by_ext = get_by_src(_trans_io_by_src_ext, src);
      for (uint8_t _ext=0; _ext<EXTENT_TYPES_MAX; ++_ext) {
        auto ext = static_cast<extent_types_t>(_ext);
        const auto extent_io = get_by_ext(io_by_ext, ext);
        if (is_data_type(ext)) {
          data_io.add(extent_io);
        } else if (is_logical_metadata_type(ext)) {
          mdat_io.add(extent_io);
        } else if (is_physical_type(ext)) {
          phys_io.add(extent_io);
        }
      }
      oss << "\n  " << src << ": "
          << cache_io_stats_printer_t{seconds, trans_io_per_src}
          << "\n    data: "
          << cache_io_stats_printer_t{seconds, data_io}
          << "\n    mdat: "
          << cache_io_stats_printer_t{seconds, mdat_io}
          << "\n    phys: "
          << cache_io_stats_printer_t{seconds, phys_io};
    }

    INFO("{}", oss.str());

    last_trans_io_by_src_ext = trans_io_by_src_ext;
  }

  last_overall_io = overall_io;
}

class ExtentPromoter {
public:
  ExtentPromoter(size_t promotion_size, ExtentPlacementManager &epm)
      : promotion_size(promotion_size), epm(epm) {}

  ~ExtentPromoter() {
    clear();
  }

  bool enabled() const {
    return ecb != nullptr;
  }

  bool should_promote_extent(const CachedExtent &extent) {
    return enabled() && epm.is_cold_device(extent.get_paddr().get_device_id());
  }

  size_t get_promotion_size() const {
    return current_contents;
  }

  void set_background_callback(BackgroundListener *l) {
    listener = l;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    ecb = cb;
  }

  bool should_run_promote() const {
    return enabled() && current_contents >= promotion_size;
  }

  std::size_t get_promoted_size() const {
    return promoted_size;
  }

  std::size_t get_promoted_count() const {
    return promoted_count;
  }

  void add_extent(CachedExtent &extent) {
    assert(!extent.is_linked_to_list());
    assert(extent.is_stable_clean());
    extent.set_pin_state(extent_pin_state_t::PendingPromote);
    list.push_back(extent);
    current_contents += extent.get_length();
    intrusive_ptr_add_ref(&extent);
    while (current_contents > promotion_size) {
      remove_extent(list.front(), extent_pin_state_t::Fresh);
    }
    if (should_run_promote()) {
      assert(listener);
      listener->maybe_wake_promote();
    }
  }

  void remove_extent(CachedExtent &extent, extent_pin_state_t new_state) {
    assert(extent.is_linked_to_list());
    assert(extent.get_pin_state() == extent_pin_state_t::PendingPromote);
    assert(current_contents >= extent.get_length());
    extent.set_pin_state(new_state);
    list.erase(list.s_iterator_to(extent));
    current_contents -= extent.get_length();
    intrusive_ptr_release(&extent);
  }

  void clear() {
    for (auto iter = list.begin(); iter != list.end();) {
      remove_extent(*(iter++), extent_pin_state_t::Fresh);
    }
  }

  using run_promote_ret = base_iertr::future<>;
  run_promote_ret run_promote(Transaction &t) {
    LOG_PREFIX(ExtentPromoter::run_promote);
    std::size_t promote_size = 0;
    std::list<CachedExtentRef> extents;
    DEBUGT("start promote", t);
    for (auto &extent : list) {
      DEBUGT("promote {} to the hot tier", t, extent);
      ceph_assert(extent.is_stable_clean());
      ceph_assert(extent.get_pin_state() == extent_pin_state_t::PendingPromote);
      promote_size += extent.get_length();
      t.add_to_read_set(&extent);
      extents.emplace_back(&extent);
    }
    for (auto &extent : extents) {
      co_await trans_intr::make_interruptible(extent->wait_io());
      co_await ecb->promote_extent(t, extent);
    }
    // existing extents in lru will be retired after transaction submitted
    co_await ecb->submit_transaction_direct(t);
    promoted_count += extents.size();
    promoted_size += promote_size;
    DEBUGT("finish promoting {} {}B extents", t, extents.size(), promote_size);
    co_return;
  }

  seastar::future<> promote() {
    assert(enabled());
    return repeat_eagain([this] {
      return ecb->with_transaction_intr(
        Transaction::src_t::PROMOTE,
	"promote", cache_hint_t::get_nocache(),
        [this](auto &t) {
	  return run_promote(t);
	});
    }).handle_error(crimson::ct_error::assert_all{"error occupied during promotion"});
  }

private:
  const size_t promotion_size;
  ExtentPlacementManager &epm;
  ExtentCallbackInterface *ecb = nullptr;
  BackgroundListener *listener = nullptr;
  CachedExtent::primary_ref_list list;
  size_t current_contents;

  size_t promoted_count;
  size_t promoted_size;
};

class ExtentPinboardLRU : public ExtentPinboard {
  ExtentQueue lru;
  ExtentPromoter promoter;
  seastar::metrics::metric_group metrics;

  // hit and miss indicates if an extent is linked when touching it
  uint64_t hit = 0;
  uint64_t miss = 0;

public:
  ExtentPinboardLRU(std::size_t capacity, size_t promotion_size, ExtentPlacementManager &epm)
      : lru(capacity), promoter(promotion_size, epm) {
    LOG_PREFIX(ExtentPinboardLRU::ExtentPinboardLRU);
    INFO("created, lru_capacity=0x{:x}B, promotion_size=0x{:x}B",
	 capacity, promotion_size);
  }

  std::size_t get_capacity_bytes() const {
    return lru.get_capacity_bytes();
  }

  std::size_t get_current_size_bytes() const final {
    return lru.get_current_size_bytes();
  }

  std::size_t get_current_num_extents() const final {
    return lru.get_current_num_extents();
  }

  void register_metrics() final {
    namespace sm = seastar::metrics;
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "lru_size_bytes",
          [this] {
            return get_current_size_bytes();
          },
          sm::description("total bytes pinned by the lru")
        ),
        sm::make_counter(
          "lru_num_extents",
          [this] {
            return get_current_num_extents();
          },
          sm::description("total extents pinned by the lru")
        ),
        sm::make_counter(
          "lru_hit", hit,
          sm::description("total count of the extents that are linked to lru when touching them")
        ),
        sm::make_counter(
          "lru_miss", miss,
          sm::description("total count of the extents that are not linked to lru when touching them")
        ),
      }
    );
    if (promoter.enabled()) {
      metrics.add_group(
	"cache",
	{
	  sm::make_counter(
	    "promoted_size",
	    [this] {
	      return promoter.get_promoted_size();
	    },
	    sm::description("total bytes promoted by the lru")),
	  sm::make_counter(
	    "promoted_count",
	    [this] {
	      return promoter.get_promoted_count();
	    },
	    sm::description("total extents promoted by the lru")),
	});
    }
  }

  void get_stats(
    cache_stats_t &stats,
    bool report_detail,
    double seconds) const final {
    lru.get_stats("LRU", stats, report_detail, seconds);
  }

  void remove(CachedExtent &extent) final {
    auto s = extent.get_pin_state();
    if (extent.is_linked_to_list()) {
      if (s == extent_pin_state_t::Fresh) {
	lru.remove(extent);
      } else {
	promoter.remove_extent(extent, extent_pin_state_t::Fresh);
      }
    } else {
      ceph_assert(s == extent_pin_state_t::Fresh);
    }
  }

  void add_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    auto trimmed = lru.add_to_top(extent, p_src);
    if (promoter.enabled()) {
      for (auto &extent : trimmed) {
	if (promoter.should_promote_extent(*extent)) {
	  promoter.add_extent(*extent);
	}
      }
    }
  }

  void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src,
    extent_len_t /*load_start*/,
    extent_len_t /*load_length*/) final {
    if (extent.is_linked_to_list()) {
      auto s = extent.get_pin_state();
      assert(s <= extent_pin_state_t::PendingPromote);
      if (s == extent_pin_state_t::Fresh) {
	lru.move_to_top(extent, p_src);
      } else {
	promoter.remove_extent(extent, extent_pin_state_t::Fresh);
	add_to_top(extent, p_src);
      }
      hit++;
    } else {
      add_to_top(extent, p_src);
      miss++;
    }
  }

  void increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t* p_src) final {
    if (extent.is_linked_to_list() &&
	extent.get_pin_state() == extent_pin_state_t::Fresh) {
      lru.increase_cached_size(extent, increased_length, p_src);
    } else {
      // promoter take the complete extent size for content size calculation
      assert(extent.get_pin_state() <= extent_pin_state_t::PendingPromote);
    }
  }

  void clear() final {
    lru.clear();
    promoter.clear();
  }

  void set_background_callback(BackgroundListener *listener) final {
    promoter.set_background_callback(listener);
  }

  void set_extent_callback(ExtentCallbackInterface *cb) final {
    promoter.set_extent_callback(cb);
  }

  std::size_t get_promotion_size() const final {
    return promoter.get_promotion_size();
  }

  bool should_promote() const final {
    return promoter.should_run_promote();
  }

  seastar::future<> promote() final {
    return promoter.promote();
  }

  ~ExtentPinboardLRU() {
    clear();
  }
};

// For A1_out queue(warm_out in ExtentPinboardTwoQ) in 2q algorithm
class IndexedFifoQueue {
public:
  explicit IndexedFifoQueue(std::size_t capacity)
      : capacity(capacity), current_size(0) {
    index.reserve(capacity >> 12);
  }

  ~IndexedFifoQueue() {
    clear();
  }

  enum class AccessMode {
    Missing,
    ContinueFromLastEnd,
    Again
  };

  AccessMode accessed_recently(laddr_t laddr, extent_len_t load_start) {
    auto iter = index.find(laddr);
    if (iter == index.end()) {
      return AccessMode::Missing;
    }
    // Current read start offset is same as the last access end offset,
    // we treat this access as sequential read.
    auto &last_end = iter->second->last_access_end;
    assert(last_end != 0);
    auto ret = last_end == load_start
	? AccessMode::ContinueFromLastEnd
	: AccessMode::Again;
    remove(iter);
    return ret;
  }

  void add(laddr_t laddr,
	   extent_len_t loaded_length,
	   extent_len_t access_end) {
    assert(laddr != L_ADDR_NULL);
    assert(loaded_length != 0);
    assert(access_end != 0);
    assert(!index.contains(laddr));
    index[laddr] = queue.emplace(
      queue.end(), laddr, loaded_length, access_end);
    current_size += loaded_length;
    trim_to(capacity);
  }

  std::size_t get_tracked_num_extents() const {
    return index.size();
  }

  std::size_t get_tracked_size_bytes() const {
    return current_size;
  }

  void clear() {
    trim_to(0);
  }

private:
  struct entry_t {
    entry_t(laddr_t laddr,
	    extent_len_t loaded_length,
	    extent_len_t access_end)
	: laddr(laddr),
	  loaded_length(loaded_length),
	  last_access_end(access_end) {}

    laddr_t laddr;
    extent_len_t loaded_length;
    extent_len_t last_access_end;
  };

  using entry_queue_t = std::list<entry_t>;
  using entry_index_t = boost::unordered_flat_map<
    laddr_t, entry_queue_t::iterator>;

  void remove(entry_index_t::iterator iter) {
    assert(iter != index.end());
    assert(iter->second != queue.end());
    assert(current_size >= iter->second->loaded_length);
    current_size -= iter->second->loaded_length;
    queue.erase(iter->second);
    index.erase(iter);
  }

  void trim_to(std::size_t target) {
    while (current_size > target) {
      assert(!queue.empty());
      assert(queue.size() == index.size());
      remove(index.find(queue.front().laddr));
    }
    if (target == 0) {
      assert(current_size == 0);
      assert(queue.empty());
      assert(index.empty());
    }
  }

  const std::size_t capacity;
  std::size_t current_size;
  entry_queue_t queue;
  entry_index_t index;
};

class ExtentPinboardTwoQ : public ExtentPinboard {
public:
  ExtentPinboardTwoQ(
    std::size_t warm_in_capacity,
    std::size_t warm_out_capacity,
    std::size_t hot_capacity,
    std::size_t promotion_size,
    ExtentPlacementManager &epm)
      : warm_in(warm_in_capacity),
	warm_out(warm_out_capacity),
	hot(hot_capacity),
	promoter(promotion_size, epm)
  {
    LOG_PREFIX(ExtentPinboardTwoQ::ExtentPinboardTwoQ);
    INFO("created, warm_in_capacity=0x{:x}B, warm_out_capacity=0x{:x}B, "
	 "hot_capacity=0x{:x}B, promotion_size=0x{:x}B",
	 warm_in_capacity, warm_out_capacity, hot_capacity, promotion_size);
  }

  std::size_t get_capacity_bytes() const {
    return warm_in.get_capacity_bytes() + hot.get_capacity_bytes();
  }

  std::size_t get_current_size_bytes() const final {
    return warm_in.get_current_size_bytes() + hot.get_current_size_bytes();
  }

  std::size_t get_current_num_extents() const final {
    return warm_in.get_current_num_extents() + hot.get_current_num_extents();
  }

  void register_metrics() final;

  void get_stats(
    cache_stats_t &stats,
    bool report_detail,
    double seconds) const final;

  void remove(CachedExtent &extent) final {
    auto s = extent.get_pin_state();
    if (extent.is_linked_to_list()) {
      if (s == extent_pin_state_t::WarmIn) {
	warm_in.remove(extent);
      } else if (s == extent_pin_state_t::PendingPromote) {
	promoter.remove_extent(extent, extent_pin_state_t::Fresh);
      } else {
	ceph_assert(s == extent_pin_state_t::Hot);
	hot.remove(extent);
      }
    } else {
      ceph_assert(s == extent_pin_state_t::Fresh);
    }
  }

  void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src,
    extent_len_t load_start,
    extent_len_t load_length) final {
    auto state = extent.get_pin_state();
    auto type = extent.get_type();
    if (extent.is_linked_to_list()) {
      if (state == extent_pin_state_t::Hot) {
	hot.move_to_top(extent, p_src);
	hit_queue(overall_hits.hot_hits, p_src, type);
      } else if (state == extent_pin_state_t::PendingPromote) {
	promoter.remove_extent(extent, extent_pin_state_t::Hot);
	auto trimmed_extents = hot.add_to_top(extent, p_src);
	on_update_hot(trimmed_extents);
	hit_queue(overall_hits.hot_hits, p_src, type);
      } else {
	ceph_assert(state == extent_pin_state_t::WarmIn);
	hit_queue(overall_hits.warm_in_hits, p_src, type);
	// warm_in is a FIFO queue, do nothing here
	// In the standard 2Q algorithm, the extent won't be considerred
	// hot until it is evicted to the warm out queue and accessed once
	// again.
      }
      hit++;
    } else if (!is_logical_type(extent.get_type())) {
      // put physical extents to hot queue directly
      ceph_assert(state == extent_pin_state_t::Fresh);
      extent.set_pin_state(extent_pin_state_t::Hot);
      auto trimmed_extents = hot.add_to_top(extent, p_src);
      on_update_hot(trimmed_extents);
      hit_queue(overall_hits.absent, p_src, type);
      miss++;
    } else { // the logical extent which is not in warm_in and not in hot
      ceph_assert(state == extent_pin_state_t::Fresh);
      auto lext = extent.cast<LogicalCachedExtent>();
      auto m = warm_out.accessed_recently(lext->get_laddr(), load_start);
      using AccessMode = IndexedFifoQueue::AccessMode;
      if (m == AccessMode::Again) {
	// This extent was accessed recently, consider it's hot enough to
	// promote to hot queue.
	extent.set_pin_state(extent_pin_state_t::Hot);
	auto trimmed_extents = hot.add_to_top(extent, p_src);
	on_update_hot(trimmed_extents);
	hit_queue(overall_hits.hot_absent, p_src, type);
      } else {
	// This extent didn't be accessed recently, put it warm_in queue
	// by default.
	extent.set_pin_state(extent_pin_state_t::WarmIn);
	auto trimmed_extents = warm_in.add_to_top(extent, p_src);
	on_update_warm_in(trimmed_extents);
	if (m == AccessMode::Missing) {
	  hit_queue(overall_hits.absent, p_src, type);
	} else { // m == AccessMode::ContinueFromLastEnd
	  hit_queue(overall_hits.sequential_absent, p_src, type);
	}
      }
      miss++;
    }
    auto end = load_start + load_length;
    assert(end != 0);
    // The last end update should be in the same continuation with touching
    // extent, subsequent touch will cover the prior touch end.
    extent.set_last_touch_end(end);
  }

  void increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t* p_src) final {
    if (extent.is_linked_to_list()) {
      auto state = extent.get_pin_state();
      if (state == extent_pin_state_t::WarmIn) {
	auto trimmed_extents = warm_in.increase_cached_size(
	  extent, increased_length, p_src);
	on_update_warm_in(trimmed_extents);
      } else if (state == extent_pin_state_t::Hot) {
	auto trimmed_extents = hot.increase_cached_size(
	  extent, increased_length, p_src);
	on_update_hot(trimmed_extents);
      } else {
	ceph_assert(extent.get_pin_state() ==
		    extent_pin_state_t::PendingPromote);
      }
    }
  }

  void clear() final {
    LOG_PREFIX(ExtentPinboardTwoQ::clear);
    INFO("close with warm_in: {}({}B), traced by warm_out: {}({}B), hot: {}({}B)",
	 warm_in.get_current_num_extents(), warm_in.get_current_size_bytes(),
	 warm_out.get_tracked_num_extents(), warm_out.get_tracked_size_bytes(),
	 hot.get_current_num_extents(), hot.get_current_size_bytes());
    warm_in.clear();
    warm_out.clear();
    hot.clear();
    promoter.clear();
  }

  void set_background_callback(BackgroundListener *listener) final {
    promoter.set_background_callback(listener);
  }
  void set_extent_callback(ExtentCallbackInterface *cb) final {
    promoter.set_extent_callback(cb);
  }
  std::size_t get_promotion_size() const final {
    return promoter.get_promotion_size();
  }
  bool should_promote() const final {
    return promoter.should_run_promote();
  }
  seastar::future<> promote() final {
    return promoter.promote();
  }

  ~ExtentPinboardTwoQ() {
    clear();
  }
private:
  void on_update_hot(std::list<CachedExtentRef> &extents) {
    for (auto extent : extents) {
      if (promoter.should_promote_extent(*extent)) {
	promoter.add_extent(*extent);
      } else {
	extent->set_pin_state(extent_pin_state_t::Fresh);
      }
    }
  }
  void on_update_warm_in(std::list<CachedExtentRef> &extents) {
    for (auto extent : extents) {
      ceph_assert(is_logical_type(extent->get_type()));
      extent->set_pin_state(extent_pin_state_t::Fresh);
      auto len = extent->get_loaded_length();
      if (len == 0) {
        // The extent is possibly empty after being initially split/remapped
        // by the ObjectDataHandler, we should only record non-empty extents
        // to the warm out queue.
        continue;
      }
      auto lext = extent->cast<LogicalCachedExtent>();
      auto laddr = lext->get_laddr();
      auto end = extent->get_last_touch_end();
      // the extents evicted from warm_in queue will be recorded
      // in warm_out FIFO queue as recently accessed extents.
      warm_out.add(laddr, len, end);
    }
  }
  // 2Q cache algorithm:
  // - warm_in: FIFO queue for new logical extents (for first insertion)
  // - warm_out: FIFO queue for tracking recently evicted extents from warm_in
  // - hot: LRU queue for frequently accessed extents
  //
  // Workflow:
  // 1. New non-logical extents enter warm_in first, physical extents
  //    are placed into hot queue directly
  // 2. On warm_in eviction, add extent's metadata(laddr, loaded length and
  //    last access end) to warm_out queue
  // 3. If the extent in warm_out is accessed again and the load start doesn't
  //    match the last accessed end recorded in the warm_out, promote extent
  //    to the hot queue, otherwise add add extent to warm_in queue
  // 4. Hot queue manages extents using LRU algorithm
  ExtentQueue warm_in;
  IndexedFifoQueue warm_out;
  ExtentQueue hot;
  ExtentPromoter promoter;
  seastar::metrics::metric_group metrics;

  struct QueueCounter {
    struct summary_t {
      uint64_t data;
      uint64_t mdat;
      uint64_t phys;

      bool empty() const {
	return data == 0 && mdat == 0 && phys == 0;
      }

      void minus(const summary_t &o) {
	data -= o.data;
	mdat -= o.mdat;
	phys -= o.phys;
      }
    };

    summary_t &get_summary_via_src(Transaction::src_t src) {
      if (src == Transaction::src_t::MAX) {
	return other_hits;
      }
      return get_by_src(trans_hits, src);
    }
    counter_by_src_t<summary_t> trans_hits;
    summary_t other_hits;
  };
  void hit_queue(
    QueueCounter &hits,
    const Transaction::src_t *p_src,
    extent_types_t type)
  {
    auto &summary =
	(p_src == nullptr)
	? hits.other_hits
	: get_by_src(hits.trans_hits, *p_src);
    if (is_data_type(type)) {
      summary.data++;
    } else if (is_logical_metadata_type(type)) {
      summary.mdat++;
    } else if (is_physical_type(type)) {
      summary.phys++;
    } else {
      ceph_abort("invalid extent type: {}", type);
    }
  }
  struct hit_stats_t {
    QueueCounter warm_in_hits;
    QueueCounter hot_hits;
    QueueCounter absent;
    QueueCounter hot_absent;
    QueueCounter sequential_absent;
  };
  mutable hit_stats_t overall_hits;
  mutable hit_stats_t last_hits;

  // hit and miss indicates if an extent is linked when touching it
  uint64_t hit = 0;
  uint64_t miss = 0;
};

void ExtentPinboardTwoQ::get_stats(
  cache_stats_t &stats,
  bool report_detail,
  double seconds) const
{
  LOG_PREFIX(ExtentPinboardTwoQ::get_stats);
  hot.get_stats("2Q_Hot", stats, report_detail, seconds);
  cache_stats_t warm;
  warm_in.get_stats("2Q_WarmIn", warm, report_detail, seconds);
  stats.add(warm);

  if (!report_detail || seconds == 0) {
    return;
  }

  std::ostringstream oss;
  bool output_src_type = true;

  auto header = [&output_src_type, &oss](Transaction::src_t src) {
    if (!output_src_type) {
      return;
    }
    if (src == Transaction::src_t::MAX) {
      oss << "\nOTHER:";
    } else {
      oss << '\n' << src << ':';
    }
    output_src_type = false;
  };

  auto handle_queue_counter = [&oss, &header, seconds]
      (QueueCounter &cur_qc, QueueCounter &other_qc,
       std::string_view name, Transaction::src_t src)
  {
    auto cur = cur_qc.get_summary_via_src(src);
    auto &last = other_qc.get_summary_via_src(src);
    cur.minus(last);
    if (cur.empty()) {
      return;
    }
    header(src);
    oss << "\n  " << name << "\n   ";
    if (cur.data != 0) {
      oss << " data: " << double(cur.data) / seconds << "ps";
    }
    if (cur.mdat != 0) {
      oss << " mdat: " << double(cur.mdat) / seconds << "ps";
    }
    if (cur.phys != 0) {
      oss << " phys: " << double(cur.phys) / seconds << "ps";
    }
  };

  // TRANSACTION_TYPE_MAX refers to QueueCounter::other_hits
  for (uint8_t _src = 0; _src <= TRANSACTION_TYPE_MAX; _src++) {
    auto src = static_cast<Transaction::src_t>(_src);
    output_src_type = true;
    handle_queue_counter(
      overall_hits.hot_hits, last_hits.hot_hits,
      "2Q_hot", src);
    handle_queue_counter(
      overall_hits.warm_in_hits, last_hits.warm_in_hits,
      "2Q_warm_in", src);
    handle_queue_counter(
      overall_hits.absent, last_hits.absent,
      "2Q_absent", src);
    handle_queue_counter(
      overall_hits.hot_absent, last_hits.hot_absent,
      "2Q_hot_absent", src);
    handle_queue_counter(
      overall_hits.sequential_absent, last_hits.sequential_absent,
      "2Q_sequential_absent", src);
  }

  INFO("{}", oss.str());
  last_hits = overall_hits;
}

void ExtentPinboardTwoQ::register_metrics() {
  namespace sm = seastar::metrics;
  metrics.add_group(
    "cache",
    {
      sm::make_counter(
        "2q_warm_in_size_bytes",
        [this] {
          return warm_in.get_current_size_bytes();
        },
        sm::description("total bytes pinned by the 2q warm_in queue")
      ),
      sm::make_counter(
        "2q_warm_in_num_extents",
        [this] {
          return warm_in.get_current_num_extents();
        },
        sm::description("total extents pinned by the 2q warm_in queue")
      ),
      sm::make_counter(
        "2q_hot_size_bytes",
        [this] {
          return hot.get_current_size_bytes();
        },
        sm::description("total bytes pinned by the 2q hot queue")
      ),
      sm::make_counter(
        "2q_hot_num_extents",
        [this] {
          return hot.get_current_num_extents();
        },
        sm::description("total extents pinned by the 2q hot queue")
      ),
      sm::make_counter(
        "2q_hit", hit,
        sm::description("total count of the extents that are linked to 2Q when touching them")
      ),
      sm::make_counter(
        "2q_miss", miss,
        sm::description("total count of the extents that are not linked to 2Q when touching them")
      ),
    }
  );
  if (promoter.enabled()) {
    metrics.add_group(
      "cache",
      {
	sm::make_counter(
	  "promoted_size",
	  [this] {
	    return promoter.get_promoted_size();
	  },
	  sm::description("total bytes promoted by the lru")),
	sm::make_counter(
	  "promoted_count",
	  [this] {
	    return promoter.get_promoted_count();
	  },
	  sm::description("total extents promoted by the lru")),
      });
  }
}

ExtentPinboardRef create_extent_pinboard(std::size_t capacity, ExtentPlacementManager *epm) {
  using crimson::common::get_conf;
  size_t promotion_size = get_conf<Option::size_t>("seastore_cache_promotion_size");
  auto algorithm = get_conf<std::string>("seastore_cachepin_type");
  if (algorithm == "LRU") {
    return std::make_unique<ExtentPinboardLRU>(capacity, promotion_size, *epm);
  } else if (algorithm == "2Q") {
    auto warm_in_ratio = get_conf<double>("seastore_cachepin_2q_in_ratio");
    auto warm_out_ratio = get_conf<double>("seastore_cachepin_2q_out_ratio");
    ceph_assert(0 < warm_in_ratio && warm_in_ratio < 1);
    ceph_assert(0 < warm_out_ratio && warm_out_ratio < 1);
    return std::make_unique<ExtentPinboardTwoQ>(
      capacity * warm_in_ratio,
      capacity * warm_out_ratio,
      capacity * (1 - warm_in_ratio),
      promotion_size, *epm);
  } else {
    ceph_abort("invalid seastore_cachepin_type(LRU or 2Q)");
    return nullptr;
  }
}

} // namespace crimson::os::seastore
