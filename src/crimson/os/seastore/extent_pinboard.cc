// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/extent_pinboard.h"
#include "crimson/os/seastore/transaction.h"

SET_SUBSYS(seastore_cache);

namespace crimson::os::seastore {
enum class extent_2q_state_t {
  Fresh = 0,
  WarmIn,
  Hot,
  Max
};
} // namespace crimson::os::seastore

template <>
struct fmt::formatter<crimson::os::seastore::extent_2q_state_t>
    : public fmt::formatter<std::string_view> {
  using State = crimson::os::seastore::extent_2q_state_t;
  auto format(const State &s, auto &ctx) const {
    switch (s) {
    case State::Fresh:
      return fmt::format_to(ctx.out(), "Fresh");
    case State::WarmIn:
      return fmt::format_to(ctx.out(), "WarmIn");
    case State::Hot:
      return fmt::format_to(ctx.out(), "Hot");
    case State::Max:
      return fmt::format_to(ctx.out(), "Max");
    default:
      __builtin_unreachable();
      return ctx.out();
    }
  }
};

namespace crimson::os::seastore {

class ExtentQueue {
  // max size (bytes)
  const size_t capacity = 0;

  // current size (bytes)
  size_t current_size = 0;

  counter_by_extent_t<cache_size_stats_t> sizes_by_ext;
  cache_io_stats_t overall_io;
  counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
    trans_io_by_src_ext;

  mutable cache_io_stats_t last_overall_io;
  mutable cache_io_stats_t last_trans_io;
  mutable counter_by_src_t<counter_by_extent_t<cache_io_stats_t> >
    last_trans_io_by_src_ext;

  CachedExtent::primary_ref_list list;

  void do_remove_from_queue(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean() && !extent.is_placeholder());
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
      do_remove_from_queue(list.front(), p_src);
    }
  }

public:
  explicit ExtentQueue(size_t capacity) : capacity(capacity) {}

  size_t get_capacity_bytes() const {
    return capacity;
  }

  size_t get_current_size_bytes() const {
    return current_size;
  }

  size_t get_current_num_extents() const {
    return list.size();
  }

  void get_stats(
    std::string_view queue_name,
    cache_stats_t &stats,
    bool report_detail,
    double seconds) const;

  void remove(CachedExtent &extent) {
    assert(extent.is_stable_clean() && !extent.is_placeholder());

    if (extent.primary_ref_list_hook.is_linked()) {
      do_remove_from_queue(extent, nullptr);
    }
  }

  void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src) {
    assert(extent.is_stable_clean() && !extent.is_placeholder());

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
      assert(extent.is_stable_clean() && !extent.is_placeholder());
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

  template <typename Func>
  void for_each_to_be_trimmed_extent(size_t size, Func &&func) {
    auto fake_cur_size = current_size + size;
    auto iter = list.begin();
    // keep consistent with do_remove_from_queue()
    while (fake_cur_size > capacity) {
      func(*iter);
      fake_cur_size -= iter->get_loaded_length();
      iter++;
    }
  }

  void clear() {
    LOG_PREFIX(ExtentPinboardLRU::clear);
    INFO("close with {}({}B)",
         get_current_num_extents(),
         get_current_size_bytes());
    for (auto iter = list.begin(); iter != list.end();) {
      SUBDEBUG(seastore_cache, "clearing {}", *iter);
      do_remove_from_queue(*(iter++), nullptr);
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

  stats.queue_sizes = cache_size_stats_t{current_size, list.size()};
  stats.queue_io = overall_io;
  stats.queue_io.minus(last_overall_io);

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
    cache_io_stats_t other_io = stats.queue_io;
    other_io.minus(trans_io);

    std::ostringstream oss;
    oss << "\n" << queue_name << " total" << stats.queue_sizes;
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
  ExtentPinboardLRU(size_t capacity) : lru(capacity) {
    LOG_PREFIX(ExtentPinboardLRU::ExtentPinboardLRU);
    INFO("created, lru_capacity=0x{:x}B", capacity);
  }

  size_t get_capacity_bytes() const {
    return lru.get_capacity_bytes();
  }

  size_t get_current_size_bytes() const {
    return lru.get_current_size_bytes();
  }

  size_t get_current_num_extents() const {
    return lru.get_current_num_extents();
  }

  void register_metrics() final {
    namespace sm = seastar::metrics;
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "cache_lru_size_bytes",
          [this] {
            return get_current_size_bytes();
          },
          sm::description("total bytes pinned by the lru")
        ),
        sm::make_counter(
          "cache_lru_num_extents",
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

namespace {
extent_2q_state_t get_2q_state(CachedExtent &extent) {
  auto s = extent.get_cache_state();
  assert(s <= static_cast<uint8_t>(extent_2q_state_t::Max));
  return static_cast<extent_2q_state_t>(s);
}
void set_2q_state(CachedExtent &extent, extent_2q_state_t s) {
  assert(s != extent_2q_state_t::Max);
  extent.set_cache_state(static_cast<uint8_t>(s));
}
}

// For A1_out queue in 2q algorithm
class IndexedFifoQueue {
public:
  explicit IndexedFifoQueue(size_t capacity)
      : capacity(capacity), current_size(0) {}

  ~IndexedFifoQueue() {
    clear();
  }

  bool remove_if_contains(laddr_t laddr) {
    auto iter = index.find(laddr);
    if (iter == index.end()) {
      return false;
    }
    remove(&*iter);
    return true;
  }

  void add(laddr_t laddr, extent_len_t length, extent_types_t type) noexcept {
    // assume the allocation won't fail
    auto e = new entry_t(laddr, length, type);
    current_size += length;
    queue.push_back(*e);
    index.insert(*e);
    trim_to(capacity);
  }

  void clear() {
    trim_to(0);
  }

private:
  struct entry_t :
      boost::intrusive::list_base_hook<>,
      boost::intrusive::set_base_hook<> {
    entry_t(laddr_t laddr, extent_len_t length, extent_types_t type)
	: laddr(laddr), length(length), type(type) {}

    laddr_t laddr;
    extent_len_t length;
    extent_types_t type;

    struct key_t {
      using type = laddr_t;
      const type &operator()(const entry_t &e) const {
	return e.laddr;
      }
    };
  };

  using entry_queue_t = boost::intrusive::list<entry_t>;
  using entry_index_t = boost::intrusive::set<
    entry_t, boost::intrusive::key_of_value<entry_t::key_t>>;

  void remove(entry_t *e) {
    current_size -= e->length;
    queue.erase(queue.s_iterator_to(*e));
    index.erase(index.s_iterator_to(*e));
    delete e;
  }

  void trim_to(size_t target) {
    while (current_size > target && !queue.empty()) {
      remove(&queue.front());
    }
  }

  const size_t capacity;
  size_t current_size;
  entry_queue_t queue;
  entry_index_t index;
};

class ExtentPinboardTwoQ : public ExtentPinboard {
public:
  ExtentPinboardTwoQ(
    size_t warm_in_capacity,
    size_t warm_out_capacity,
    size_t hot_capacity)
      : warm_in(warm_in_capacity),
	warm_out(warm_out_capacity),
	hot(hot_capacity)
  {
    LOG_PREFIX(ExtentPinboardTwoQ::ExtentPinboardTwoQ);
    INFO("created, warm_in_capacity=0x{:x}B, "
	 "warm_out_capacity=0x{:x}B, hot_capacity=0x{:x}B",
	 warm_in_capacity, warm_out_capacity, hot_capacity);
  }

  size_t get_capacity_bytes() const {
    return warm_in.get_capacity_bytes() + hot.get_capacity_bytes();
  }

  size_t get_current_size_bytes() const {
    return warm_in.get_current_size_bytes() + hot.get_current_size_bytes();
  }

  size_t get_current_num_extents() const {
    return warm_in.get_current_num_extents() + hot.get_current_num_extents();
  }

  void register_metrics() final {
    namespace sm = seastar::metrics;
    metrics.add_group(
      "cache",
      {
        sm::make_counter(
          "cache_2q_size_bytes",
          [this] {
            return get_current_size_bytes();
          },
          sm::description("total bytes pinned by the 2q")
        ),
        sm::make_counter(
          "cache_2q_num_extents",
          [this] {
            return get_current_num_extents();
          },
          sm::description("total extents pinned by the 2q")
        ),
      }
    );
  }

  void get_stats(
    cache_stats_t &stats,
    bool report_detail,
    double seconds) const final {
    hot.get_stats("Hot", stats, report_detail, seconds);
    cache_stats_t warm;
    warm_in.get_stats("WarmIn", warm, report_detail, seconds);
    stats.add(warm);
  }

  void remove(CachedExtent &extent) final {
    auto s = get_2q_state(extent);
    if (extent.primary_ref_list_hook.is_linked()) {
      if (s == extent_2q_state_t::WarmIn) {
	warm_in.remove(extent);
      } else {
	ceph_assert(s == extent_2q_state_t::Hot);
	hot.remove(extent);
      }
    }
  }

  void move_to_top(
    CachedExtent &extent,
    const Transaction::src_t* p_src) final {
    auto state = get_2q_state(extent);
    if (extent.primary_ref_list_hook.is_linked()) {
      if (state == extent_2q_state_t::Hot) {
	hot.move_to_top(extent, p_src);
      } else {
	ceph_assert(state == extent_2q_state_t::WarmIn);
	// warm_in is a FIFO queue, do nothing here
      }
    } else if (!is_logical_type(extent.get_type())) {
      // put physical extents to hot queue directly
      ceph_assert(state == extent_2q_state_t::Hot ||
		  state == extent_2q_state_t::Fresh);
      set_2q_state(extent, extent_2q_state_t::Hot);
      hot.move_to_top(extent, p_src);
    } else { // not in two queue
      ceph_assert(state == extent_2q_state_t::Fresh);
      auto queue = &warm_in;
      auto new_state = extent_2q_state_t::WarmIn;
      auto lext = extent.cast<LogicalCachedExtent>();
      if (warm_out.remove_if_contains(lext->get_laddr())) {
	queue = &hot;
	new_state = extent_2q_state_t::Hot;
	on_update_hot(extent.get_loaded_length());
      } else {
	on_update_warm_out(extent.get_loaded_length());
      }
      queue->move_to_top(extent, p_src);
      set_2q_state(extent, new_state);
    }
  }

  void increase_cached_size(
    CachedExtent &extent,
    extent_len_t increased_length,
    const Transaction::src_t* p_src) final {
    if (extent.primary_ref_list_hook.is_linked()) {
      auto state = get_2q_state(extent);
      if (state == extent_2q_state_t::WarmIn) {
	on_update_warm_out(increased_length);
	warm_in.increase_cached_size(extent, increased_length, p_src);
      } else {
	ceph_assert(state == extent_2q_state_t::Hot);
	on_update_hot(increased_length);
	hot.increase_cached_size(extent, increased_length, p_src);
      }
    }
  }

  void clear() final {
    warm_in.clear();
    warm_out.clear();
    hot.clear();
  }

  ~ExtentPinboardTwoQ() {
    clear();
  }
private:
  void on_update_hot(size_t size) {
    hot.for_each_to_be_trimmed_extent(size, [](CachedExtent &extent) {
      set_2q_state(extent, extent_2q_state_t::Fresh);
    });
  }
  void on_update_warm_out(size_t size) {
    warm_in.for_each_to_be_trimmed_extent(
      size,
      [this](CachedExtent &extent) {
	ceph_assert(is_logical_type(extent.get_type()));
	set_2q_state(extent, extent_2q_state_t::Fresh);
	auto lext = extent.cast<LogicalCachedExtent>();
	warm_out.add(
	  lext->get_laddr(),
	  lext->get_loaded_length(),
	  lext->get_type());
      });
  }
  ExtentQueue warm_in;
  IndexedFifoQueue warm_out;
  ExtentQueue hot;
  seastar::metrics::metric_group metrics;
};

ExtentPinboardRef create_extent_pinboard(std::size_t capacity) {
  using crimson::common::get_conf;
  auto algorithm = get_conf<std::string>("seastore_extent_pinboard_algorithm");
  if (algorithm == "LRU") {
    return std::make_unique<ExtentPinboardLRU>(capacity);
  } else if (algorithm == "TwoQ") {
    auto warm_in_ratio = get_conf<double>("seastore_extent_pinboard_2q_in_ratio");
    auto warm_out_ratio = get_conf<double>("seastore_extent_pinboard_2q_out_ratio");
    ceph_assert(0 < warm_in_ratio && warm_in_ratio < 1);
    ceph_assert(0 < warm_out_ratio && warm_out_ratio < 1);
    return std::make_unique<ExtentPinboardTwoQ>(
      capacity * warm_in_ratio,
      capacity * warm_out_ratio,
      capacity * (1 - warm_in_ratio));
  } else {
    ceph_abort("invalid seastore_extent_pinboard_algorithm (LRU or TwoQ)");
    return nullptr;
  }
}

} // namespace crimson::os::seastore
