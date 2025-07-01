// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/extent_pinboard.h"
#include "crimson/os/seastore/transaction.h"

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
  mutable cache_io_stats_t last_trans_io;
  mutable counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
    last_trans_io_by_src_ext;

  CachedExtent::primary_ref_list list;

  void do_remove_from_list(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());
    assert(extent.primary_ref_list_hook.is_linked());
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

  void trim_to_capacity(
    const Transaction::src_t* p_src) {
    while (current_size > capacity) {
      do_remove_from_list(list.front(), p_src);
    }
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
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());

    if (extent.primary_ref_list_hook.is_linked()) {
      do_remove_from_list(extent, nullptr);
    }
  }

  void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());

    auto extent_loaded_length = extent.get_loaded_length();
    if (extent.primary_ref_list_hook.is_linked()) {
      // present, move to top (back)
      assert(list.size() > 0);
      assert(current_size >= extent_loaded_length);
      list.erase(list.s_iterator_to(extent));
      list.push_back(extent);
    } else {
      // absent, add to top (back)
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

      trim_to_capacity(p_src);
    }
  }

  void increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t* p_src) {
    assert(extent.is_data_stable());

    if (extent.primary_ref_list_hook.is_linked()) {
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

      trim_to_capacity(nullptr);
    }
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

class ExtentPinboardLRU : public ExtentPinboard {
  ExtentQueue lru;
  seastar::metrics::metric_group metrics;

public:
  ExtentPinboardLRU(std::size_t capacity) : lru(capacity) {
    LOG_PREFIX(ExtentPinboardLRU::ExtentPinboardLRU);
    INFO("created, lru_capacity=0x{:x}B", capacity);
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
      }
    );
  }

  void get_stats(
    cache_stats_t &stats,
    bool report_detail,
    double seconds) const final {
    lru.get_stats("LRU", stats, report_detail, seconds);
  }

  void remove(CachedExtent &extent) final {
    lru.remove(extent);
  }

  void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src) final {
    lru.move_to_top(extent, p_src);
  }

  void increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t* p_src) final {
    lru.increase_cached_size(extent, increased_length, p_src);
  }

  void clear() final {
    lru.clear();
  }

  ~ExtentPinboardLRU() {
    clear();
  }
};

ExtentPinboardRef create_extent_pinboard(std::size_t capacity) {
  return std::make_unique<ExtentPinboardLRU>(capacity);
}

} // namespace crimson::os::seastore
