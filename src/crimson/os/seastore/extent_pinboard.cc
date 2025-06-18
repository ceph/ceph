// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/extent_pinboard.h"
#include "crimson/os/seastore/transaction.h"

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {

class ExtentPinboardLRU : public ExtentPinboard {
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

  CachedExtent::primary_ref_list lru;

  seastar::metrics::metric_group metrics;

  void do_remove_from_lru(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());
    assert(extent.primary_ref_list_hook.is_linked());
    assert(lru.size() > 0);
    auto extent_loaded_length = extent.get_loaded_length();
    assert(current_size >= extent_loaded_length);

    lru.erase(lru.s_iterator_to(extent));
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
      do_remove_from_lru(lru.front(), p_src);
    }
  }

public:
  ExtentPinboardLRU(std::size_t capacity) : capacity(capacity) {
    LOG_PREFIX(ExtentPinboardLRU::ExtentPinboardLRU);
    INFO("created, lru_capacity=0x{:x}B", capacity);
  }

  std::size_t get_capacity_bytes() const {
    return capacity;
  }

  std::size_t get_current_size_bytes() const final {
    return current_size;
  }

  std::size_t get_current_num_extents() const final {
    return lru.size();
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
    double seconds) const final;

  void remove(CachedExtent &extent) {
    assert(extent.is_stable_clean());
    assert(!extent.is_placeholder());

    if (extent.primary_ref_list_hook.is_linked()) {
      do_remove_from_lru(extent, nullptr);
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
      assert(lru.size() > 0);
      assert(current_size >= extent_loaded_length);
      lru.erase(lru.s_iterator_to(extent));
      lru.push_back(extent);
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
      lru.push_back(extent);

      trim_to_capacity(p_src);
    }
  }

  void increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t* p_src) final {
    assert(extent.is_data_stable());

    if (extent.primary_ref_list_hook.is_linked()) {
      assert(extent.is_stable_clean());
      assert(!extent.is_placeholder());
      // present, increase size
      assert(lru.size() > 0);
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

  void clear() final {
    LOG_PREFIX(ExtentPinboardLRU::clear);
    for (auto iter = lru.begin(); iter != lru.end();) {
      SUBDEBUG(seastore_cache, "clearing {}", *iter);
      do_remove_from_lru(*(iter++), nullptr);
    }
  }

  ~ExtentPinboardLRU() {
    clear();
  }
};

ExtentPinboardRef create_extent_pinboard(std::size_t capacity) {
  return std::make_unique<ExtentPinboardLRU>(capacity);
}

void ExtentPinboardLRU::get_stats(
  cache_stats_t &stats,
  bool report_detail,
  double seconds) const
{
  LOG_PREFIX(Cache::LRU::get_stats);

  stats.lru_sizes = cache_size_stats_t{current_size, lru.size()};
  stats.lru_io = overall_io;
  stats.lru_io.minus(last_overall_io);

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
    cache_io_stats_t other_io = stats.lru_io;
    other_io.minus(trans_io);

    std::ostringstream oss;
    oss << "\nlru total" << stats.lru_sizes;
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

    oss << "\nlru io: trans-"
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

} // namespace crimson::os::seastore
