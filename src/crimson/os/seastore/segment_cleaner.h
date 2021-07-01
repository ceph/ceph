// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "common/ceph_time.h"

#include "osd/osd_types.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

struct segment_info_t {
  Segment::segment_state_t state = Segment::segment_state_t::EMPTY;

  // Will be non-null for any segments in the current journal
  segment_seq_t journal_segment_seq = NULL_SEG_SEQ;


  bool is_in_journal(journal_seq_t tail_committed) const {
    return journal_segment_seq != NULL_SEG_SEQ &&
      tail_committed.segment_seq <= journal_segment_seq;
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
};

class SpaceTrackerI {
public:
  virtual int64_t allocate(
    segment_id_t segment,
    segment_off_t offset,
    extent_len_t len) = 0;

  virtual int64_t release(
    segment_id_t segment,
    segment_off_t offset,
    extent_len_t len) = 0;

  virtual int64_t get_usage(
    segment_id_t segment) const = 0;

  virtual bool equals(const SpaceTrackerI &other) const = 0;

  virtual std::unique_ptr<SpaceTrackerI> make_empty() const = 0;

  virtual void dump_usage(segment_id_t) const = 0;

  virtual void reset() = 0;

  virtual ~SpaceTrackerI() = default;
};
using SpaceTrackerIRef = std::unique_ptr<SpaceTrackerI>;

class SpaceTrackerSimple : public SpaceTrackerI {
  // Tracks live space for each segment
  std::vector<int64_t> live_bytes_by_segment;

  int64_t update_usage(segment_id_t segment, int64_t delta) {
    assert(segment < live_bytes_by_segment.size());
    live_bytes_by_segment[segment] += delta;
    assert(live_bytes_by_segment[segment] >= 0);
    return live_bytes_by_segment[segment];
  }
public:
  SpaceTrackerSimple(size_t num_segments)
    : live_bytes_by_segment(num_segments, 0) {}

  int64_t allocate(
    segment_id_t segment,
    segment_off_t offset,
    extent_len_t len) final {
    return update_usage(segment, len);
  }

  int64_t release(
    segment_id_t segment,
    segment_off_t offset,
    extent_len_t len) final {
    return update_usage(segment, -len);
  }

  int64_t get_usage(segment_id_t segment) const final {
    assert(segment < live_bytes_by_segment.size());
    return live_bytes_by_segment[segment];
  }

  void dump_usage(segment_id_t) const final {}

  void reset() final {
    for (auto &i: live_bytes_by_segment)
      i = 0;
  }

  SpaceTrackerIRef make_empty() const final {
    return SpaceTrackerIRef(
      new SpaceTrackerSimple(live_bytes_by_segment.size()));
  }

  bool equals(const SpaceTrackerI &other) const;
};

class SpaceTrackerDetailed : public SpaceTrackerI {
  class SegmentMap {
    int64_t used = 0;
    std::vector<bool> bitmap;

  public:
    SegmentMap(size_t blocks) : bitmap(blocks, false) {}

    int64_t update_usage(int64_t delta) {
      used += delta;
      return used;
    }

    int64_t allocate(
      segment_id_t segment,
      segment_off_t offset,
      extent_len_t len,
      const extent_len_t block_size);

    int64_t release(
      segment_id_t segment,
      segment_off_t offset,
      extent_len_t len,
      const extent_len_t block_size);

    int64_t get_usage() const {
      return used;
    }

    void dump_usage(extent_len_t block_size) const;

    void reset() {
      used = 0;
      for (auto &&i: bitmap) {
	i = false;
      }
    }
  };
  const size_t block_size;
  const size_t segment_size;

  // Tracks live space for each segment
  std::vector<SegmentMap> segment_usage;

public:
  SpaceTrackerDetailed(size_t num_segments, size_t segment_size, size_t block_size)
    : block_size(block_size),
      segment_size(segment_size),
      segment_usage(num_segments, segment_size / block_size) {}

  int64_t allocate(
    segment_id_t segment,
    segment_off_t offset,
    extent_len_t len) final {
    assert(segment < segment_usage.size());
    return segment_usage[segment].allocate(segment, offset, len, block_size);
  }

  int64_t release(
    segment_id_t segment,
    segment_off_t offset,
    extent_len_t len) final {
    assert(segment < segment_usage.size());
    return segment_usage[segment].release(segment, offset, len, block_size);
  }

  int64_t get_usage(segment_id_t segment) const final {
    assert(segment < segment_usage.size());
    return segment_usage[segment].get_usage();
  }

  void dump_usage(segment_id_t seg) const final;

  void reset() final {
    for (auto &i: segment_usage)
      i.reset();
  }

  SpaceTrackerIRef make_empty() const final {
    return SpaceTrackerIRef(
      new SpaceTrackerDetailed(
	segment_usage.size(),
	segment_size,
	block_size));
  }

  bool equals(const SpaceTrackerI &other) const;
};


class SegmentCleaner : public JournalSegmentProvider {
public:
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
	  .1,   // available_ratio_hard_limit
	  1<<20,// reclaim 1MB per gc cycle
	  1<<20 // rewrite 1MB of journal entries per gc cycle
	};
    }
  };

  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    virtual ~ExtentCallbackInterface() = default;

    virtual TransactionRef create_transaction() = 0;

    /**
     * get_next_dirty_extent
     *
     * returns all extents with dirty_from < bound
     */
    using get_next_dirty_extents_iertr = crimson::errorator<>;
    using get_next_dirty_extents_ret = get_next_dirty_extents_iertr::future<
      std::vector<CachedExtentRef>>;
    virtual get_next_dirty_extents_ret get_next_dirty_extents(
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
      segment_off_t len) = 0;

    /**
     * scan_extents
     *
     * Interface shim for Journal::scan_extents
     */
    using scan_extents_cursor = Journal::scan_valid_records_cursor;
    using scan_extents_ertr = Journal::scan_extents_ertr;
    using scan_extents_ret = Journal::scan_extents_ret;
    virtual scan_extents_ret scan_extents(
      scan_extents_cursor &cursor,
      extent_len_t bytes_to_read) = 0;

    /**
     * release_segment
     *
     * Release segment.
     */
    using release_segment_ertr = SegmentManager::release_ertr;
    using release_segment_ret = release_segment_ertr::future<>;
    virtual release_segment_ret release_segment(
      segment_id_t id) = 0;

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

  size_t num_segments = 0;
  size_t segment_size = 0;
  size_t block_size = 0;

  SpaceTrackerIRef space_tracker;
  std::vector<segment_info_t> segments;
  size_t empty_segments;
  int64_t used_bytes = 0;
  bool init_complete = false;

  struct {
    uint64_t segments_released = 0;
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

public:
  SegmentCleaner(config_t config, bool detailed = false);

  void mount(SegmentManager &sm) {
    init_complete = false;
    used_bytes = 0;
    journal_tail_target = journal_seq_t{};
    journal_tail_committed = journal_seq_t{};
    journal_head = journal_seq_t{};

    num_segments = sm.get_num_segments();
    segment_size = static_cast<size_t>(sm.get_segment_size());
    block_size = static_cast<size_t>(sm.get_block_size());

    space_tracker.reset(
      detailed ?
      (SpaceTrackerI*)new SpaceTrackerDetailed(
	num_segments,
	segment_size,
	block_size) :
      (SpaceTrackerI*)new SpaceTrackerSimple(
	num_segments));

    segments.clear();
    segments.resize(num_segments);
    empty_segments = num_segments;
  }

  get_segment_ret get_segment() final;

  void close_segment(segment_id_t segment) final;

  void set_journal_segment(
    segment_id_t segment, segment_seq_t seq) final {
    assert(segment < segments.size());
    segments[segment].journal_segment_seq = seq;
    assert(segments[segment].is_open());
  }

  journal_seq_t get_journal_tail_target() const final {
    return journal_tail_target;
  }

  void update_journal_tail_committed(journal_seq_t committed) final;

  void update_journal_tail_target(journal_seq_t target);

  void init_journal_tail(journal_seq_t tail) {
    journal_tail_target = journal_tail_committed = tail;
  }

  void init_mkfs(journal_seq_t head) {
    journal_tail_target = head;
    journal_tail_committed = head;
    journal_head = head;
  }

  void set_journal_head(journal_seq_t head) {
    assert(journal_head == journal_seq_t() || head >= journal_head);
    journal_head = head;
    gc_process.maybe_wake_on_space_used();
  }

  journal_seq_t get_journal_head() const {
    return journal_head;
  }

  void init_mark_segment_closed(segment_id_t segment, segment_seq_t seq) final {
    crimson::get_logger(ceph_subsys_seastore).debug(
      "SegmentCleaner::init_mark_segment_closed: segment {}, seq {}",
      segment,
      seq);
    mark_closed(segment);
    segments[segment].journal_segment_seq = seq;
  }

  segment_seq_t get_seq(segment_id_t id) final {
    return segments[id].journal_segment_seq;
  }

  void mark_segment_released(segment_id_t segment) {
    stats.segments_released++;
    return mark_empty(segment);
  }

  void mark_space_used(
    paddr_t addr,
    extent_len_t len,
    bool init_scan = false) {
    assert(addr.segment < segments.size());

    if (!init_scan && !init_complete)
      return;

    used_bytes += len;
    [[maybe_unused]] auto ret = space_tracker->allocate(
      addr.segment,
      addr.offset,
      len);
    gc_process.maybe_wake_on_space_used();
    assert(ret > 0);
  }

  void mark_space_free(
    paddr_t addr,
    extent_len_t len) {
    if (!init_complete)
      return;

    used_bytes -= len;
    assert(addr.segment < segments.size());

    [[maybe_unused]] auto ret = space_tracker->release(
      addr.segment,
      addr.offset,
      len);
    maybe_wake_gc_blocked_io();
    assert(ret >= 0);
  }

  segment_id_t get_next_gc_target() const {
    segment_id_t ret = NULL_SEG_ID;
    int64_t least_live_bytes = std::numeric_limits<int64_t>::max();
    for (segment_id_t i = 0; i < segments.size(); ++i) {
      if (segments[i].is_closed() &&
	  !segments[i].is_in_journal(journal_tail_committed) &&
	  space_tracker->get_usage(i) < least_live_bytes) {
	ret = i;
	least_live_bytes = space_tracker->get_usage(i);
      }
    }
    if (ret != NULL_SEG_ID) {
      crimson::get_logger(ceph_subsys_seastore).debug(
	"SegmentCleaner::get_next_gc_target: segment {} seq {}",
	ret,
	segments[ret].journal_segment_seq);
    }
    return ret;
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
    ExtentCallbackInterface::scan_extents_cursor
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
    ExtentCallbackInterface::scan_extents_ertr
    >;

  gc_cycle_ret do_gc_cycle();

  using gc_trim_journal_ertr = gc_ertr;
  using gc_trim_journal_ret = gc_trim_journal_ertr::future<>;
  gc_trim_journal_ret gc_trim_journal();

  using gc_reclaim_space_ertr = gc_ertr;
  using gc_reclaim_space_ret = gc_reclaim_space_ertr::future<>;
  gc_reclaim_space_ret gc_reclaim_space();

  size_t get_bytes_used_current_segment() const {
    return journal_head.offset.offset;
  }

  size_t get_bytes_available_current_segment() const {
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

    return scan_cursor->get_offset().offset;
  }

  /// Returns free space available for writes
  size_t get_available_bytes() const {
    return (empty_segments * segment_size) +
      get_bytes_available_current_segment() +
      get_bytes_scanned_current_segment();
  }

  /// Returns total space available
  size_t get_total_bytes() const {
    return segment_size * num_segments;
  }

  /// Returns total space not free
  size_t get_unavailable_bytes() const {
    return get_total_bytes() - get_available_bytes();
  }

  /// Returns bytes currently occupied by live extents (not journal)
  size_t get_used_bytes() const {
    return used_bytes;
  }

  /// Return bytes contained in segments in journal
  size_t get_journal_segment_bytes() const {
    assert(journal_head >= journal_tail_committed);
    return (journal_head.segment_seq - journal_tail_committed.segment_seq + 1) *
      segment_size;
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

  /**
   * get_available_ratio
   *
   * Returns ratio of available space to write to total space
   */
  double get_available_ratio() const {
    return (double)get_available_bytes() / (double)get_total_bytes();
  }

  /**
   * should_block_on_gc
   *
   * Encapsulates whether block pending gc.
   */
  bool should_block_on_gc() const {
    auto aratio = get_available_ratio();
    return (
      ((aratio < config.available_ratio_gc_max) &&
       (get_reclaim_ratio() > config.reclaim_ratio_hard_limit ||
	aratio < config.available_ratio_hard_limit)) ||
      (get_dirty_tail_limit() > journal_tail_target)
    );
  }

  void log_gc_state(const char *caller) const {
    auto &logger = crimson::get_logger(ceph_subsys_seastore);
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
	get_dirty_tail(),
	get_dirty_tail_limit(),
	gc_should_trim_journal()
      );
    }
  }

public:
  seastar::future<> await_hard_limits() {
    // The pipeline configuration prevents another IO from entering
    // prepare until the prior one exits and clears this.
    ceph_assert(!blocked_io_wake);
    return seastar::do_until(
      [this] {
	log_gc_state("await_hard_limits");
	return !should_block_on_gc();
      },
      [this] {
	blocked_io_wake = seastar::promise<>();
	return blocked_io_wake->get_future();
      });
  }
private:
  void maybe_wake_gc_blocked_io() {
    if (!should_block_on_gc() && blocked_io_wake) {
      blocked_io_wake->set_value();
      blocked_io_wake = std::nullopt;
    }
  }

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

  void mark_closed(segment_id_t segment) {
    assert(segments.size() > segment);
    if (init_complete) {
      assert(segments[segment].is_open());
    } else {
      assert(segments[segment].is_empty());
      assert(empty_segments > 0);
      --empty_segments;
    }
    crimson::get_logger(ceph_subsys_seastore).debug(
      "mark_closed: empty_segments: {}",
      empty_segments);
    segments[segment].state = Segment::segment_state_t::CLOSED;
  }

  void mark_empty(segment_id_t segment) {
    assert(segments.size() > segment);
    assert(segments[segment].is_closed());
    assert(segments.size() > empty_segments);
    ++empty_segments;
    if (space_tracker->get_usage(segment) != 0) {
      space_tracker->dump_usage(segment);
      assert(space_tracker->get_usage(segment) == 0);
    }
    segments[segment].state = Segment::segment_state_t::EMPTY;
    maybe_wake_gc_blocked_io();
  }

  void mark_open(segment_id_t segment) {
    assert(segments.size() > segment);
    assert(segments[segment].is_empty());
    assert(empty_segments > 0);
    --empty_segments;
    segments[segment].state = Segment::segment_state_t::OPEN;
  }
};
using SegmentCleanerRef = std::unique_ptr<SegmentCleaner>;

}
