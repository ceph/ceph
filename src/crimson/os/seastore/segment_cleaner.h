// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>
#include <seastar/core/metrics_types.hh>

#include "common/ceph_time.h"

#include "osd/osd_types.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager_group.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/segment_seq_allocator.h"

namespace crimson::os::seastore {

/*
 * segment_info_t
 *
 * Maintains the tracked information for a segment.
 * It is read-only outside segments_info_t.
 */
struct segment_info_t {
  using time_point = seastar::lowres_system_clock::time_point;

  // segment_info_t is initiated as set_empty()
  Segment::segment_state_t state = Segment::segment_state_t::EMPTY;

  // Will be non-null for any segments in the current journal
  segment_seq_t seq = NULL_SEG_SEQ;

  segment_type_t type = segment_type_t::NULL_SEG;

  time_point last_modified;
  time_point last_rewritten;

  std::size_t open_avail_bytes = 0;

  bool is_in_journal(journal_seq_t tail_committed) const {
    return type == segment_type_t::JOURNAL &&
           tail_committed.segment_seq <= seq;
  }

  bool is_empty() const {
    return state == Segment::segment_state_t::EMPTY;
  }

  bool is_closed() const {
    return state == Segment::segment_state_t::CLOSED;
  }

  bool is_open() const {
    return state == Segment::segment_state_t::OPEN;
  }

  void init_closed(segment_seq_t, segment_type_t);

  void set_open(segment_seq_t, segment_type_t, std::size_t segment_size);

  void set_empty();

  void set_closed();

  void update_last_modified_rewritten(
      time_point _last_modified, time_point _last_rewritten) {
    if (_last_modified != time_point() && last_modified < _last_modified) {
      last_modified = _last_modified;
    }
    if (_last_rewritten != time_point() && last_rewritten < _last_rewritten) {
      last_rewritten = _last_rewritten;
    }
  }
};

std::ostream& operator<<(std::ostream&, const segment_info_t&);

/*
 * segments_info_t
 *
 * Keep track of all segments and related information.
 */
class segments_info_t {
public:
  using time_point = seastar::lowres_system_clock::time_point;

  segments_info_t() {
    reset();
  }

  const segment_info_t& operator[](segment_id_t id) const {
    return segments[id];
  }

  auto begin() const {
    return segments.begin();
  }

  auto end() const {
    return segments.end();
  }

  std::size_t get_num_segments() const {
    assert(segments.size() > 0);
    return segments.size();
  }
  std::size_t get_segment_size() const {
    assert(segment_size > 0);
    return segment_size;
  }
  std::size_t get_num_in_journal() const {
    return num_in_journal;
  }
  std::size_t get_num_open() const {
    return num_open;
  }
  std::size_t get_num_empty() const {
    return num_empty;
  }
  std::size_t get_num_closed() const {
    return num_closed;
  }
  std::size_t get_count_open() const {
    return count_open;
  }
  std::size_t get_count_release() const {
    return count_release;
  }
  std::size_t get_count_close() const {
    return count_close;
  }
  size_t get_total_bytes() const {
    return total_bytes;
  }
  size_t get_available_bytes() const {
    return avail_bytes;
  }

  void reset();

  void add_segment_manager(SegmentManager &segment_manager);

  // initiate non-empty segments, the others are by default empty
  void init_closed(segment_id_t, segment_seq_t, segment_type_t);

  void mark_open(segment_id_t, segment_seq_t, segment_type_t);

  void mark_empty(segment_id_t);

  void mark_closed(segment_id_t);

  void update_written_to(paddr_t offset);

  void update_last_modified_rewritten(
      segment_id_t id, time_point last_modified, time_point last_rewritten) {
    segments[id].update_last_modified_rewritten(last_modified, last_rewritten);
  }

private:
  // See reset() for member initialization
  segment_map_t<segment_info_t> segments;

  std::size_t segment_size;

  std::size_t num_in_journal;
  std::size_t num_open;
  std::size_t num_empty;
  std::size_t num_closed;

  std::size_t count_open;
  std::size_t count_release;
  std::size_t count_close;

  std::size_t total_bytes;
  std::size_t avail_bytes;
};

/**
 * Callback interface for managing available segments
 */
class SegmentProvider {
public:
  virtual journal_seq_t get_journal_tail_target() const = 0;

  virtual const segment_info_t& get_seg_info(segment_id_t id) const = 0;

  virtual segment_id_t allocate_segment(
      segment_seq_t seq, segment_type_t type) = 0;

  virtual void close_segment(segment_id_t) = 0;

  virtual void update_journal_tail_committed(journal_seq_t tail_committed) = 0;

  virtual void update_segment_avail_bytes(paddr_t offset) = 0;

  virtual SegmentManagerGroup* get_segment_manager_group() = 0;

  virtual ~SegmentProvider() {}
};

class SpaceTrackerI {
public:
  virtual int64_t allocate(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) = 0;

  virtual int64_t release(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) = 0;

  virtual int64_t get_usage(
    segment_id_t segment) const = 0;

  virtual bool equals(const SpaceTrackerI &other) const = 0;

  virtual std::unique_ptr<SpaceTrackerI> make_empty() const = 0;

  virtual void dump_usage(segment_id_t) const = 0;

  virtual double calc_utilization(segment_id_t segment) const = 0;

  virtual void reset() = 0;

  virtual ~SpaceTrackerI() = default;
};
using SpaceTrackerIRef = std::unique_ptr<SpaceTrackerI>;

class SpaceTrackerSimple : public SpaceTrackerI {
  struct segment_bytes_t {
    int64_t live_bytes = 0;
    seastore_off_t total_bytes = 0;
  };
  // Tracks live space for each segment
  segment_map_t<segment_bytes_t> live_bytes_by_segment;

  int64_t update_usage(segment_id_t segment, int64_t delta) {
    live_bytes_by_segment[segment].live_bytes += delta;
    assert(live_bytes_by_segment[segment].live_bytes >= 0);
    return live_bytes_by_segment[segment].live_bytes;
  }
public:
  SpaceTrackerSimple(const SpaceTrackerSimple &) = default;
  SpaceTrackerSimple(const std::vector<SegmentManager*> &sms) {
    for (auto sm : sms) {
      live_bytes_by_segment.add_device(
	sm->get_device_id(),
	sm->get_num_segments(),
	{0, sm->get_segment_size()});
    }
  }

  int64_t allocate(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return update_usage(segment, len);
  }

  int64_t release(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return update_usage(segment, -(int64_t)len);
  }

  int64_t get_usage(segment_id_t segment) const final {
    return live_bytes_by_segment[segment].live_bytes;
  }

  double calc_utilization(segment_id_t segment) const final {
    auto& seg_bytes = live_bytes_by_segment[segment];
    return (double)seg_bytes.live_bytes / (double)seg_bytes.total_bytes;
  }

  void dump_usage(segment_id_t) const final;

  void reset() final {
    for (auto &i : live_bytes_by_segment) {
      i.second = {0, 0};
    }
  }

  SpaceTrackerIRef make_empty() const final {
    auto ret = SpaceTrackerIRef(new SpaceTrackerSimple(*this));
    ret->reset();
    return ret;
  }

  bool equals(const SpaceTrackerI &other) const;
};

class SpaceTrackerDetailed : public SpaceTrackerI {
  class SegmentMap {
    int64_t used = 0;
    seastore_off_t total_bytes = 0;
    std::vector<bool> bitmap;

  public:
    SegmentMap(
      size_t blocks,
      seastore_off_t total_bytes)
    : total_bytes(total_bytes),
      bitmap(blocks, false) {}

    int64_t update_usage(int64_t delta) {
      used += delta;
      return used;
    }

    int64_t allocate(
      device_segment_id_t segment,
      seastore_off_t offset,
      extent_len_t len,
      const extent_len_t block_size);

    int64_t release(
      device_segment_id_t segment,
      seastore_off_t offset,
      extent_len_t len,
      const extent_len_t block_size);

    int64_t get_usage() const {
      return used;
    }

    void dump_usage(extent_len_t block_size) const;

    double calc_utilization() const {
      return (double)used / (double)total_bytes;
    }

    void reset() {
      used = 0;
      for (auto &&i: bitmap) {
	i = false;
      }
    }
  };

  // Tracks live space for each segment
  segment_map_t<SegmentMap> segment_usage;
  std::vector<size_t> block_size_by_segment_manager;

public:
  SpaceTrackerDetailed(const SpaceTrackerDetailed &) = default;
  SpaceTrackerDetailed(const std::vector<SegmentManager*> &sms)
  {
    block_size_by_segment_manager.resize(DEVICE_ID_MAX, 0);
    for (auto sm : sms) {
      segment_usage.add_device(
	sm->get_device_id(),
	sm->get_num_segments(),
	SegmentMap(
	  sm->get_segment_size() / sm->get_block_size(),
	  sm->get_segment_size()));
      block_size_by_segment_manager[sm->get_device_id()] = sm->get_block_size();
    }
  }

  int64_t allocate(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return segment_usage[segment].allocate(
      segment.device_segment_id(),
      offset,
      len,
      block_size_by_segment_manager[segment.device_id()]);
  }

  int64_t release(
    segment_id_t segment,
    seastore_off_t offset,
    extent_len_t len) final {
    return segment_usage[segment].release(
      segment.device_segment_id(),
      offset,
      len,
      block_size_by_segment_manager[segment.device_id()]);
  }

  int64_t get_usage(segment_id_t segment) const final {
    return segment_usage[segment].get_usage();
  }

  double calc_utilization(segment_id_t segment) const final {
    return segment_usage[segment].calc_utilization();
  }

  void dump_usage(segment_id_t seg) const final;

  void reset() final {
    for (auto &i: segment_usage) {
      i.second.reset();
    }
  }

  SpaceTrackerIRef make_empty() const final {
    auto ret = SpaceTrackerIRef(new SpaceTrackerDetailed(*this));
    ret->reset();
    return ret;
  }

  bool equals(const SpaceTrackerI &other) const;
};


class SegmentCleaner : public SegmentProvider {
public:
  using time_point = seastar::lowres_system_clock::time_point;
  using duration = seastar::lowres_system_clock::duration;

  /// Config
  struct config_t {
    size_t target_journal_segments = 0;
    size_t max_journal_segments = 0;

    double available_ratio_gc_max = 0;
    double reclaim_ratio_hard_limit = 0;
    double reclaim_ratio_gc_threshhold = 0;

    double available_ratio_hard_limit = 0;

    /// Number of bytes to reclaim on each cycle
    size_t reclaim_bytes_stride = 0;

    /// Number of bytes of journal entries to rewrite per cycle
    size_t journal_rewrite_per_cycle = 0;

    static config_t get_default() {
      return config_t{
	  2,    // target_journal_segments
	  4,    // max_journal_segments
	  .9,   // available_ratio_gc_max
	  .8,   // reclaim_ratio_hard_limit
	  .6,   // reclaim_ratio_gc_threshhold
	  .2,   // available_ratio_hard_limit
	  1<<25,// reclaim 64MB per gc cycle
	  1<<25 // rewrite 64MB of journal entries per gc cycle
	};
    }
  };

  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    virtual ~ExtentCallbackInterface() = default;

    virtual TransactionRef create_transaction(
        Transaction::src_t, const char*) = 0;

    /// Creates empty transaction with interruptible context
    template <typename Func>
    auto with_transaction_intr(
        Transaction::src_t src,
        const char* name,
        Func &&f) {
      return seastar::do_with(
        create_transaction(src, name),
        [f=std::forward<Func>(f)](auto &ref_t) mutable {
          return with_trans_intr(
            *ref_t,
            [f=std::forward<Func>(f)](auto& t) mutable {
              return f(t);
            }
          );
        }
      );
    }

    /// See Cache::get_next_dirty_extents
    using get_next_dirty_extents_iertr = trans_iertr<
      crimson::errorator<
        crimson::ct_error::input_output_error>
      >;
    using get_next_dirty_extents_ret = get_next_dirty_extents_iertr::future<
      std::vector<CachedExtentRef>>;
    virtual get_next_dirty_extents_ret get_next_dirty_extents(
      Transaction &t,     ///< [in] current transaction
      journal_seq_t bound,///< [in] return extents with dirty_from < bound
      size_t max_bytes    ///< [in] return up to max_bytes of extents
    ) = 0;

    using extent_mapping_ertr = crimson::errorator<
      crimson::ct_error::input_output_error,
      crimson::ct_error::eagain>;
    using extent_mapping_iertr = trans_iertr<
      crimson::errorator<
	crimson::ct_error::input_output_error>
      >;

    /**
     * rewrite_extent
     *
     * Updates t with operations moving the passed extents to a new
     * segment.  extent may be invalid, implementation must correctly
     * handle finding the current instance if it is still alive and
     * otherwise ignore it.
     */
    using rewrite_extent_iertr = extent_mapping_iertr;
    using rewrite_extent_ret = rewrite_extent_iertr::future<>;
    virtual rewrite_extent_ret rewrite_extent(
      Transaction &t,
      CachedExtentRef extent) = 0;

    /**
     * get_extent_if_live
     *
     * Returns extent at specified location if still referenced by
     * lba_manager and not removed by t.
     *
     * See TransactionManager::get_extent_if_live and
     * LBAManager::get_physical_extent_if_live.
     */
    using get_extent_if_live_iertr = extent_mapping_iertr;
    using get_extent_if_live_ret = get_extent_if_live_iertr::future<
      CachedExtentRef>;
    virtual get_extent_if_live_ret get_extent_if_live(
      Transaction &t,
      extent_types_t type,
      paddr_t addr,
      laddr_t laddr,
      seastore_off_t len) = 0;

    /**
     * submit_transaction_direct
     *
     * Submits transaction without any space throttling.
     */
    using submit_transaction_direct_iertr = trans_iertr<
      crimson::errorator<
        crimson::ct_error::input_output_error>
      >;
    using submit_transaction_direct_ret =
      submit_transaction_direct_iertr::future<>;
    virtual submit_transaction_direct_ret submit_transaction_direct(
      Transaction &t) = 0;
  };

private:
  const bool detailed;
  const config_t config;

  SegmentManagerGroupRef sm_group;

  SpaceTrackerIRef space_tracker;
  segments_info_t segments;
  bool init_complete = false;

  struct {
    uint64_t used_bytes = 0;
    /**
     * projected_used_bytes
     *
     * Sum of projected bytes used by each transaction between throttle
     * acquisition and commit completion.  See await_throttle()
     */
    uint64_t projected_used_bytes = 0;

    uint64_t accumulated_blocked_ios = 0;
    int64_t ios_blocking = 0;
    uint64_t reclaim_rewrite_bytes = 0;
    uint64_t reclaiming_bytes = 0;
    seastar::metrics::histogram segment_util;
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();

  /// target journal_tail for next fresh segment
  journal_seq_t journal_tail_target;

  /// most recently committed journal_tail
  journal_seq_t journal_tail_committed;

  /// head of journal
  journal_seq_t journal_head;

  ExtentCallbackInterface *ecb = nullptr;

  /// populated if there is an IO blocked on hard limits
  std::optional<seastar::promise<>> blocked_io_wake;

  SegmentSeqAllocatorRef ool_segment_seq_allocator;

public:
  SegmentCleaner(
    config_t config,
    SegmentManagerGroupRef&& sm_group,
    bool detailed = false);

  SegmentSeqAllocator& get_ool_segment_seq_allocator() {
    return *ool_segment_seq_allocator;
  }

  using mount_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using mount_ret = mount_ertr::future<>;
  mount_ret mount();

  /*
   * SegmentProvider interfaces
   */
  journal_seq_t get_journal_tail_target() const final {
    return journal_tail_target;
  }

  const segment_info_t& get_seg_info(segment_id_t id) const final {
    return segments[id];
  }

  segment_id_t allocate_segment(
      segment_seq_t seq, segment_type_t type) final;

  void close_segment(segment_id_t segment) final;

  void update_journal_tail_committed(journal_seq_t committed) final;

  void update_segment_avail_bytes(paddr_t offset) final {
    segments.update_written_to(offset);
  }

  SegmentManagerGroup* get_segment_manager_group() final {
    return sm_group.get();
  }

  void update_journal_tail_target(journal_seq_t target);

  void init_mkfs(journal_seq_t head) {
    journal_tail_target = head;
    journal_tail_committed = head;
    journal_head = head;
  }

  void set_journal_head(journal_seq_t head) {
    assert(journal_head == JOURNAL_SEQ_NULL || head >= journal_head);
    journal_head = head;
    segments.update_written_to(head.offset);
    gc_process.maybe_wake_on_space_used();
  }

  using release_ertr = SegmentManagerGroup::release_ertr;
  release_ertr::future<> maybe_release_segment(Transaction &t);

  void adjust_segment_util(double old_usage, double new_usage) {
    assert(stats.segment_util.buckets[std::floor(old_usage * 10)].count > 0);
    stats.segment_util.buckets[std::floor(old_usage * 10)].count--;
    stats.segment_util.buckets[std::floor(new_usage * 10)].count++;
  }

  void mark_space_used(
    paddr_t addr,
    extent_len_t len,
    time_point last_modified = time_point(),
    time_point last_rewritten = time_point(),
    bool init_scan = false) {
    auto& seg_addr = addr.as_seg_paddr();

    if (!init_scan && !init_complete)
      return;

    stats.used_bytes += len;
    auto old_usage = space_tracker->calc_utilization(seg_addr.get_segment_id());
    [[maybe_unused]] auto ret = space_tracker->allocate(
      seg_addr.get_segment_id(),
      seg_addr.get_segment_off(),
      len);
    auto new_usage = space_tracker->calc_utilization(seg_addr.get_segment_id());
    adjust_segment_util(old_usage, new_usage);

    // use the last extent's last modified time for the calculation of the projected
    // time the segments' live extents are to stay unmodified; this is an approximation
    // of the sprite lfs' segment "age".

    segments.update_last_modified_rewritten(
        seg_addr.get_segment_id(), last_modified, last_rewritten);

    gc_process.maybe_wake_on_space_used();
    assert(ret > 0);
  }

  void mark_space_free(
    paddr_t addr,
    extent_len_t len) {
    if (!init_complete)
      return;

    ceph_assert(stats.used_bytes >= len);
    stats.used_bytes -= len;
    auto& seg_addr = addr.as_seg_paddr();

    auto old_usage = space_tracker->calc_utilization(seg_addr.get_segment_id());
    [[maybe_unused]] auto ret = space_tracker->release(
      seg_addr.get_segment_id(),
      seg_addr.get_segment_off(),
      len);
    auto new_usage = space_tracker->calc_utilization(seg_addr.get_segment_id());
    adjust_segment_util(old_usage, new_usage);
    maybe_wake_gc_blocked_io();
    assert(ret >= 0);
  }

  SpaceTrackerIRef get_empty_space_tracker() const {
    return space_tracker->make_empty();
  }

  void start() {
    gc_process.start();
  }

  void complete_init() {
    init_complete = true;
    start();
  }

  store_statfs_t stat() const {
    store_statfs_t st;
    st.total = get_total_bytes();
    st.available = get_total_bytes() - get_used_bytes();
    st.allocated = get_used_bytes();
    st.data_stored = get_used_bytes();

    // TODO add per extent type counters for omap_allocated and
    // internal metadata
    return st;
  }

  seastar::future<> stop() {
    return gc_process.stop();
  }

  seastar::future<> run_until_halt() {
    return gc_process.run_until_halt();
  }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    ecb = cb;
  }

  bool debug_check_space(const SpaceTrackerI &tracker) {
    return space_tracker->equals(tracker);
  }

  using work_ertr = ExtentCallbackInterface::extent_mapping_ertr;
  using work_iertr = ExtentCallbackInterface::extent_mapping_iertr;

private:

  // journal status helpers

  double calc_gc_benefit_cost(segment_id_t id) const {
    double util = space_tracker->calc_utilization(id);
    auto cur_time = seastar::lowres_system_clock::now();
    auto segment = segments[id];
    assert(cur_time >= segment.last_modified);
    auto segment_age =
      cur_time - std::max(segment.last_modified, segment.last_rewritten);
    uint64_t age = segment_age.count();
    return (1 - util) * age / (1 + util);
  }

  journal_seq_t get_next_gc_target() const {
    segment_id_t id = NULL_SEG_ID;
    segment_seq_t seq = NULL_SEG_SEQ;
    double max_benefit_cost = 0;
    for (auto it = segments.begin();
	 it != segments.end();
	 ++it) {
      auto _id = it->first;
      const auto& segment_info = it->second;
      double benefit_cost = calc_gc_benefit_cost(_id);
      if (segment_info.is_closed() &&
	  !segment_info.is_in_journal(journal_tail_committed) &&
	  benefit_cost > max_benefit_cost) {
	id = _id;
	seq = segment_info.seq;
	max_benefit_cost = benefit_cost;
      }
    }
    if (id != NULL_SEG_ID) {
      crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
	"SegmentCleaner::get_next_gc_target: segment {} seq {}, benefit_cost {}",
	id,
	seq,
	max_benefit_cost);
      return journal_seq_t{seq, paddr_t::make_seg_paddr(id, 0)};
    } else {
      return JOURNAL_SEQ_NULL;
    }
  }

  /**
   * rewrite_dirty
   *
   * Writes out dirty blocks dirtied earlier than limit.
   */
  using rewrite_dirty_iertr = work_iertr;
  using rewrite_dirty_ret = rewrite_dirty_iertr::future<>;
  rewrite_dirty_ret rewrite_dirty(
    Transaction &t,
    journal_seq_t limit);

  journal_seq_t get_dirty_tail() const {
    auto ret = journal_head;
    ret.segment_seq -= std::min(
      static_cast<size_t>(ret.segment_seq),
      config.target_journal_segments);
    return ret;
  }

  journal_seq_t get_dirty_tail_limit() const {
    auto ret = journal_head;
    ret.segment_seq -= std::min(
      static_cast<size_t>(ret.segment_seq),
      config.max_journal_segments);
    return ret;
  }

  // GC status helpers
  std::unique_ptr<
    SegmentManagerGroup::scan_extents_cursor
    > scan_cursor;

  /**
   * GCProcess
   *
   * Background gc process.
   */
  using gc_cycle_ret = seastar::future<>;
  class GCProcess {
    std::optional<gc_cycle_ret> process_join;

    SegmentCleaner &cleaner;

    bool stopping = false;

    std::optional<seastar::promise<>> blocking;

    gc_cycle_ret run();

    void wake() {
      if (blocking) {
	blocking->set_value();
	blocking = std::nullopt;
      }
    }

    seastar::future<> maybe_wait_should_run() {
      return seastar::do_until(
	[this] {
	  cleaner.log_gc_state("GCProcess::maybe_wait_should_run");
	  return stopping || cleaner.gc_should_run();
	},
	[this] {
	  ceph_assert(!blocking);
	  blocking = seastar::promise<>();
	  return blocking->get_future();
	});
    }
  public:
    GCProcess(SegmentCleaner &cleaner) : cleaner(cleaner) {}

    void start() {
      ceph_assert(!process_join);
      process_join = run();
    }

    gc_cycle_ret stop() {
      if (!process_join)
	return seastar::now();
      stopping = true;
      wake();
      ceph_assert(process_join);
      auto ret = std::move(*process_join);
      process_join = std::nullopt;
      return ret.then([this] { stopping = false; });
    }

    gc_cycle_ret run_until_halt() {
      ceph_assert(!process_join);
      return seastar::do_until(
	[this] {
	  cleaner.log_gc_state("GCProcess::run_until_halt");
	  return !cleaner.gc_should_run();
	},
	[this] {
	  return cleaner.do_gc_cycle();
	});
    }

    void maybe_wake_on_space_used() {
      if (cleaner.gc_should_run()) {
	wake();
      }
    }
  } gc_process;

  using gc_ertr = work_ertr::extend_ertr<
    SegmentManagerGroup::scan_extents_ertr
    >;

  gc_cycle_ret do_gc_cycle();

  using gc_trim_journal_ertr = gc_ertr;
  using gc_trim_journal_ret = gc_trim_journal_ertr::future<>;
  gc_trim_journal_ret gc_trim_journal();

  using gc_reclaim_space_ertr = gc_ertr;
  using gc_reclaim_space_ret = gc_reclaim_space_ertr::future<>;
  gc_reclaim_space_ret gc_reclaim_space();

  size_t get_bytes_used_current_segment() const {
    auto& seg_addr = journal_head.offset.as_seg_paddr();
    return seg_addr.get_segment_off();
  }

  size_t get_bytes_available_current_segment() const {
    auto segment_size = segments.get_segment_size();
    return segment_size - get_bytes_used_current_segment();
  }

  /**
   * get_bytes_scanned_current_segment
   *
   * Returns the number of bytes from the current gc segment that
   * have been scanned.
   */
  size_t get_bytes_scanned_current_segment() const {
    if (!scan_cursor)
      return 0;
    return scan_cursor->get_segment_offset();
  }

  /// Returns free space available for writes
  size_t get_available_bytes() const {
    return segments.get_available_bytes();
  }
  size_t get_projected_available_bytes() const {
    return (get_available_bytes() > stats.projected_used_bytes) ?
      get_available_bytes() - stats.projected_used_bytes:
      0;
  }

  /// Returns total space available
  size_t get_total_bytes() const {
    return segments.get_total_bytes();
  }

  /// Returns total space not free
  size_t get_unavailable_bytes() const {
    return segments.get_total_bytes() - segments.get_available_bytes();
  }
  size_t get_projected_unavailable_bytes() const {
    return (get_total_bytes() > get_projected_available_bytes()) ?
      (get_total_bytes() - get_projected_available_bytes()) :
      0;
  }

  /// Returns bytes currently occupied by live extents (not journal)
  size_t get_used_bytes() const {
    return stats.used_bytes;
  }
  size_t get_projected_used_bytes() const {
    return stats.used_bytes + stats.projected_used_bytes;
  }

  /// Return bytes contained in segments in journal
  size_t get_journal_segment_bytes() const {
    if (journal_head == JOURNAL_SEQ_NULL) {
      // this for calculating journal bytes in the journal
      // replay phase in which journal_head is not set
      return segments.get_num_in_journal() * segments.get_segment_size();
    } else {
      assert(journal_head >= journal_tail_committed);
      auto segment_size = segments.get_segment_size();
      return (journal_head.segment_seq - journal_tail_committed.segment_seq + 1) *
	segment_size;
    }
  }

  /**
   * get_reclaimable_bytes
   *
   * Returns the number of bytes in unavailable segments that can be
   * reclaimed.
   */
  size_t get_reclaimable_bytes() const {
    auto ret = get_unavailable_bytes() - get_used_bytes();
    if (ret > get_journal_segment_bytes())
      return ret - get_journal_segment_bytes();
    else
      return 0;
  }
  size_t get_projected_reclaimable_bytes() const {
    auto ret = get_projected_unavailable_bytes() - get_projected_used_bytes();
    if (ret > get_journal_segment_bytes())
      return ret - get_journal_segment_bytes();
    else
      return 0;
  }

  /**
   * get_reclaim_ratio
   *
   * Returns the ratio of space reclaimable unavailable space to
   * total unavailable space.
   */
  double get_reclaim_ratio() const {
    if (get_unavailable_bytes() == 0) return 0;
    return (double)get_reclaimable_bytes() / (double)get_unavailable_bytes();
  }
  double get_projected_reclaim_ratio() const {
    if (get_projected_unavailable_bytes() == 0) return 0;
    return (double)get_reclaimable_bytes() /
      (double)get_projected_unavailable_bytes();
  }

  /**
   * get_available_ratio
   *
   * Returns ratio of available space to write to total space
   */
  double get_available_ratio() const {
    return (double)get_available_bytes() / (double)get_total_bytes();
  }
  double get_projected_available_ratio() const {
    return (double)get_projected_available_bytes() /
      (double)get_total_bytes();
  }

  /**
   * should_block_on_gc
   *
   * Encapsulates whether block pending gc.
   */
  bool should_block_on_gc() const {
    // TODO: probably worth projecting journal usage as well
    auto aratio = get_projected_available_ratio();
    return (
      ((aratio < config.available_ratio_gc_max) &&
       ((get_projected_reclaim_ratio() >
	 config.reclaim_ratio_hard_limit) ||
	(aratio < config.available_ratio_hard_limit))) ||
      (get_dirty_tail_limit() > journal_tail_target)
    );
  }

  void log_gc_state(const char *caller) const {
    auto &logger = crimson::get_logger(ceph_subsys_seastore_cleaner);
    if (logger.is_enabled(seastar::log_level::debug)) {
      logger.debug(
	"SegmentCleaner::log_gc_state({}): "
	"total {}, "
	"available {}, "
	"unavailable {}, "
	"used {}, "
	"reclaimable {}, "
	"reclaim_ratio {}, "
	"available_ratio {}, "
	"should_block_on_gc {}, "
	"gc_should_reclaim_space {}, "
	"journal_head {}, "
	"journal_tail_target {}, "
	"journal_tail_commit {}, "
	"dirty_tail {}, "
	"dirty_tail_limit {}, "
	"gc_should_trim_journal {}, ",
	caller,
	get_total_bytes(),
	get_available_bytes(),
	get_unavailable_bytes(),
	get_used_bytes(),
	get_reclaimable_bytes(),
	get_reclaim_ratio(),
	get_available_ratio(),
	should_block_on_gc(),
	gc_should_reclaim_space(),
	journal_head,
	journal_tail_target,
	journal_tail_committed,
	get_dirty_tail(),
	get_dirty_tail_limit(),
	gc_should_trim_journal()
      );
    }
  }

public:
  seastar::future<> reserve_projected_usage(size_t projected_usage) {
    // The pipeline configuration prevents another IO from entering
    // prepare until the prior one exits and clears this.
    ceph_assert(!blocked_io_wake);
    stats.ios_blocking++;
    return seastar::do_until(
      [this] {
	log_gc_state("await_hard_limits");
	return !should_block_on_gc();
      },
      [this] {
	stats.accumulated_blocked_ios++;
	blocked_io_wake = seastar::promise<>();
	return blocked_io_wake->get_future();
      }
    ).then([this, projected_usage] {
      ceph_assert(!blocked_io_wake);
      assert(stats.ios_blocking > 0);
      stats.ios_blocking--;
      stats.projected_used_bytes += projected_usage;
    });
  }

  void release_projected_usage(size_t projected_usage) {
    ceph_assert(stats.projected_used_bytes >= projected_usage);
    stats.projected_used_bytes -= projected_usage;
    return maybe_wake_gc_blocked_io();
  }
private:
  void maybe_wake_gc_blocked_io() {
    if (!should_block_on_gc() && blocked_io_wake) {
      blocked_io_wake->set_value();
      blocked_io_wake = std::nullopt;
    }
  }

  using scan_extents_ret_bare =
    std::vector<std::pair<segment_id_t, segment_header_t>>;
  using scan_extents_ertr = SegmentManagerGroup::scan_extents_ertr;
  using scan_extents_ret = scan_extents_ertr::future<>;
  scan_extents_ret scan_nonfull_segment(
    const segment_header_t& header,
    scan_extents_ret_bare& segment_set,
    segment_id_t segment_id);

  /**
   * gc_should_reclaim_space
   *
   * Encapsulates logic for whether gc should be reclaiming segment space.
   */
  bool gc_should_reclaim_space() const {
    auto aratio = get_available_ratio();
    return (
      (aratio < config.available_ratio_gc_max) &&
      (get_reclaim_ratio() > config.reclaim_ratio_gc_threshhold ||
       aratio < config.available_ratio_hard_limit)
    );
  }

  /**
   * gc_should_trim_journal
   *
   * Encapsulates logic for whether gc should be reclaiming segment space.
   */
  bool gc_should_trim_journal() const {
    return get_dirty_tail() > journal_tail_target;
  }

  /**
   * gc_should_run
   *
   * True if gc should be running.
   */
  bool gc_should_run() const {
    return gc_should_reclaim_space() || gc_should_trim_journal();
  }

  void init_mark_segment_closed(
      segment_id_t segment,
      segment_seq_t seq,
      segment_type_t s_type) {
    ceph_assert(!init_complete);
    segments.init_closed(segment, seq, s_type);
    if (s_type == segment_type_t::OOL) {
      ool_segment_seq_allocator->set_next_segment_seq(seq);
    }
  }
};
using SegmentCleanerRef = std::unique_ptr<SegmentCleaner>;

}
