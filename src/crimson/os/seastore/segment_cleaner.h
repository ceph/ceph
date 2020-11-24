// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "common/ceph_time.h"

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {
class Transaction;

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
    size_t num_segments = 0;
    size_t segment_size = 0;
    size_t block_size = 0;
    size_t target_journal_segments = 0;
    size_t max_journal_segments = 0;

    double reclaim_ratio_hard_limit = 0;
    // don't apply reclaim ratio with available space below this
    double reclaim_ratio_usage_min = 0;

    double available_ratio_hard_limit = 0;

    static config_t default_from_segment_manager(
      SegmentManager &manager) {
      return config_t{
	manager.get_num_segments(),
	static_cast<size_t>(manager.get_segment_size()),
	(size_t)manager.get_block_size(),
	2,
	4,
	.5,
	.95,
	.2
	};
    }
  };

  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    virtual ~ExtentCallbackInterface() = default;
    /**
     * get_next_dirty_extent
     *
     * returns all extents with dirty_from < bound
     */
    using get_next_dirty_extents_ertr = crimson::errorator<>;
    using get_next_dirty_extents_ret = get_next_dirty_extents_ertr::future<
      std::vector<CachedExtentRef>>;
    virtual get_next_dirty_extents_ret get_next_dirty_extents(
      journal_seq_t bound ///< [in] return extents with dirty_from < bound
    ) = 0;

    /**
     * rewrite_extent
     *
     * Updates t with operations moving the passed extents to a new
     * segment.  extent may be invalid, implementation must correctly
     * handle finding the current instance if it is still alive and
     * otherwise ignore it.
     */
    using rewrite_extent_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    using rewrite_extent_ret = rewrite_extent_ertr::future<>;
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
    using get_extent_if_live_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
    using get_extent_if_live_ret = get_extent_if_live_ertr::future<
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
  };

private:
  const config_t config;

  SpaceTrackerIRef space_tracker;
  std::vector<segment_info_t> segments;
  size_t empty_segments;
  int64_t used_bytes = 0;
  bool init_complete = false;

  journal_seq_t journal_tail_target;
  journal_seq_t journal_tail_committed;
  journal_seq_t journal_head;

  ExtentCallbackInterface *ecb = nullptr;

public:
  SegmentCleaner(config_t config, bool detailed = false)
    : config(config),
      space_tracker(
	detailed ?
	(SpaceTrackerI*)new SpaceTrackerDetailed(
	  config.num_segments,
	  config.segment_size,
	  config.block_size) :
	(SpaceTrackerI*)new SpaceTrackerSimple(
	  config.num_segments)),
      segments(config.num_segments),
      empty_segments(config.num_segments) {}

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

  void set_journal_head(journal_seq_t head) {
    assert(journal_head == journal_seq_t() || head >= journal_head);
    journal_head = head;
  }

  void init_mark_segment_closed(segment_id_t segment, segment_seq_t seq) final {
    crimson::get_logger(ceph_subsys_filestore).debug(
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
    return mark_empty(segment);
  }

  void mark_space_used(
    paddr_t addr,
    extent_len_t len,
    bool init_scan = false) {
    assert(addr.segment < segments.size());

    if (!init_scan && !init_complete)
      return;

    if (!init_scan) {
      assert(segments[addr.segment].state == Segment::segment_state_t::OPEN);
    }

    used_bytes += len;
    [[maybe_unused]] auto ret = space_tracker->allocate(
      addr.segment,
      addr.offset,
      len);
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
      crimson::get_logger(ceph_subsys_filestore).debug(
	"SegmentCleaner::get_next_gc_target: segment {} seq {}",
	ret,
	segments[ret].journal_segment_seq);
    }
    return ret;
  }

  SpaceTrackerIRef get_empty_space_tracker() const {
    return space_tracker->make_empty();
  }

  void complete_init() { init_complete = true; }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    ecb = cb;
  }

  bool debug_check_space(const SpaceTrackerI &tracker) {
    return space_tracker->equals(tracker);
  }

  /**
   * do_immediate_work
   *
   * Should be invoked prior to submission of any transaction,
   * will piggy-back work required to maintain deferred work
   * constraints.
   */
  using do_immediate_work_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using do_immediate_work_ret = do_immediate_work_ertr::future<>;
  do_immediate_work_ret do_immediate_work(
    Transaction &t);


  /**
   * do_deferred_work
   *
   * Should be called at idle times -- will perform background
   * operations based on deferred work constraints.
   *
   * If returned timespan is non-zero, caller should pause calling
   * back into do_deferred_work before returned timespan has elapsed,
   * or a foreground operation occurs.
   */
  using do_deferred_work_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using do_deferred_work_ret = do_deferred_work_ertr::future<
    ceph::timespan
    >;
  do_deferred_work_ret do_deferred_work(
    Transaction &t);

private:

  // journal status helpers

  /**
   * rewrite_dirty
   *
   * Writes out dirty blocks dirtied earlier than limit.
   */
  using rewrite_dirty_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using rewrite_dirty_ret = rewrite_dirty_ertr::future<>;
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
  std::unique_ptr<ExtentCallbackInterface::scan_extents_cursor> scan_cursor;

  /**
   * do_gc
   *
   * Performs bytes worth of gc work on t.
   */
  using do_gc_ertr = SegmentManager::read_ertr;
  using do_gc_ret = do_gc_ertr::future<>;
  do_gc_ret do_gc(
    Transaction &t,
    size_t bytes);

  size_t get_bytes_used_current_segment() const {
    assert(journal_head != journal_seq_t());
    return journal_head.offset.offset;
  }

  size_t get_bytes_available_current_segment() const {
    return config.segment_size - get_bytes_used_current_segment();
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

  size_t get_available_bytes() const {
    return (empty_segments * config.segment_size) +
      get_bytes_available_current_segment() +
      get_bytes_scanned_current_segment();
  }

  size_t get_total_bytes() const {
    return config.segment_size * config.num_segments;
  }

  size_t get_unavailable_bytes() const {
    return get_total_bytes() - get_available_bytes();
  }

  /// Returns bytes currently occupied by live extents (not journal)
  size_t get_used_bytes() const {
    return used_bytes;
  }

  /// Returns the number of bytes in unavailable segments that are not live
  size_t get_reclaimable_bytes() const {
    return get_unavailable_bytes() - get_used_bytes();
  }

  /**
   * get_reclaim_ratio
   *
   * Returns the ratio of unavailable space that is not currently used.
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
   * get_immediate_bytes_to_gc_for_reclaim
   *
   * Returns the number of bytes to gc in order to bring the
   * reclaim ratio below reclaim_ratio_usage_min.
   */
  size_t get_immediate_bytes_to_gc_for_reclaim() const {
    if (get_reclaim_ratio() < config.reclaim_ratio_hard_limit)
      return 0;

    const size_t unavailable_target = std::max(
      get_used_bytes() / (1.0 - config.reclaim_ratio_hard_limit),
      (1 - config.reclaim_ratio_usage_min) * get_total_bytes());

    if (unavailable_target > get_unavailable_bytes())
      return 0;

    return (get_unavailable_bytes() - unavailable_target) / get_reclaim_ratio();
  }

  /**
   * get_immediate_bytes_to_gc_for_available
   *
   * Returns the number of bytes to gc in order to bring the
   * the ratio of available disk space to total disk space above
   * available_ratio_hard_limit.
   */
  size_t get_immediate_bytes_to_gc_for_available() const {
    if (get_available_ratio() > config.available_ratio_hard_limit) {
      return 0;
    }

    const double ratio_to_make_available = config.available_ratio_hard_limit -
      get_available_ratio();
    return ratio_to_make_available * (double)get_total_bytes()
      / get_reclaim_ratio();
  }

  /**
   * get_immediate_bytes_to_gc
   *
   * Returns number of bytes to gc in order to restore any strict
   * limits.
   */
  size_t get_immediate_bytes_to_gc() const {
    // number of bytes to gc in order to correct reclaim ratio
    size_t for_reclaim = get_immediate_bytes_to_gc_for_reclaim();

    // number of bytes to gc in order to correct available_ratio
    size_t for_available = get_immediate_bytes_to_gc_for_available();

    return std::max(for_reclaim, for_available);
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
    crimson::get_logger(ceph_subsys_filestore).debug(
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
  }

  void mark_open(segment_id_t segment) {
    assert(segments.size() > segment);
    assert(segments[segment].is_empty());
    assert(empty_segments > 0);
    --empty_segments;
    segments[segment].state = Segment::segment_state_t::OPEN;
  }
};

}
