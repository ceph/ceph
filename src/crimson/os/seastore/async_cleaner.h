// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>
#include <seastar/core/metrics_types.hh>

#include "common/ceph_time.h"

#include "osd/osd_types.h"

#include "crimson/os/seastore/backref_manager.h"
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
  segment_id_t id = NULL_SEG_ID;

  // segment_info_t is initiated as set_empty()
  Segment::segment_state_t state = Segment::segment_state_t::EMPTY;

  // Will be non-null for any segments in the current journal
  segment_seq_t seq = NULL_SEG_SEQ;

  segment_type_t type = segment_type_t::NULL_SEG;

  data_category_t category = data_category_t::NUM;

  reclaim_gen_t generation = NULL_GENERATION;

  sea_time_point modify_time = NULL_TIME;

  std::size_t num_extents = 0;

  std::size_t written_to = 0;

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

  void init_closed(segment_seq_t, segment_type_t,
                   data_category_t, reclaim_gen_t,
                   std::size_t);

  void set_open(segment_seq_t, segment_type_t,
                data_category_t, reclaim_gen_t);

  void set_empty();

  void set_closed();

  void update_modify_time(sea_time_point _modify_time, std::size_t _num_extents) {
    ceph_assert(!is_closed());
    assert(_modify_time != NULL_TIME);
    assert(_num_extents != 0);
    if (modify_time == NULL_TIME) {
      modify_time = _modify_time;
      num_extents = _num_extents;
    } else {
      modify_time = get_average_time(
          modify_time, num_extents, _modify_time, _num_extents);
      num_extents += _num_extents;
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
  std::size_t get_num_in_journal_open() const {
    return num_in_journal_open;
  }
  std::size_t get_num_type_journal() const {
    return num_type_journal;
  }
  std::size_t get_num_type_ool() const {
    return num_type_ool;
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
  std::size_t get_count_open_journal() const {
    return count_open_journal;
  }
  std::size_t get_count_open_ool() const {
    return count_open_ool;
  }
  std::size_t get_count_release_journal() const {
    return count_release_journal;
  }
  std::size_t get_count_release_ool() const {
    return count_release_ool;
  }
  std::size_t get_count_close_journal() const {
    return count_close_journal;
  }
  std::size_t get_count_close_ool() const {
    return count_close_ool;
  }

  std::size_t get_total_bytes() const {
    return total_bytes;
  }
  /// the available space that is writable, including in open segments
  std::size_t get_available_bytes() const {
    return num_empty * get_segment_size() + avail_bytes_in_open;
  }
  /// the unavailable space that is not writable
  std::size_t get_unavailable_bytes() const {
    assert(total_bytes >= get_available_bytes());
    return total_bytes - get_available_bytes();
  }
  std::size_t get_available_bytes_in_open() const {
    return avail_bytes_in_open;
  }
  double get_available_ratio() const {
    return (double)get_available_bytes() / (double)total_bytes;
  }

  journal_seq_t get_submitted_journal_head() const {
    if (unlikely(journal_segment_id == NULL_SEG_ID)) {
      return JOURNAL_SEQ_NULL;
    }
    auto &segment_info = segments[journal_segment_id];
    assert(!segment_info.is_empty());
    assert(segment_info.type == segment_type_t::JOURNAL);
    assert(segment_info.seq != NULL_SEG_SEQ);
    return journal_seq_t{
      segment_info.seq,
      paddr_t::make_seg_paddr(
        journal_segment_id,
        segment_info.written_to)
    };
  }

  sea_time_point get_time_bound() const {
    if (!modify_times.empty()) {
      return *modify_times.begin();
    } else {
      return NULL_TIME;
    }
  }

  void reset();

  void add_segment_manager(SegmentManager &segment_manager);

  void assign_ids() {
    for (auto &item : segments) {
      item.second.id = item.first;
    }
  }

  // initiate non-empty segments, the others are by default empty
  void init_closed(segment_id_t, segment_seq_t, segment_type_t,
                   data_category_t, reclaim_gen_t);

  void mark_open(segment_id_t, segment_seq_t, segment_type_t,
                 data_category_t, reclaim_gen_t);

  void mark_empty(segment_id_t);

  void mark_closed(segment_id_t);

  void update_written_to(segment_type_t, paddr_t);

  void update_modify_time(
      segment_id_t id, sea_time_point tp, std::size_t num) {
    if (num == 0) {
      return;
    }

    assert(tp != NULL_TIME);
    segments[id].update_modify_time(tp, num);
  }

private:
  // See reset() for member initialization
  segment_map_t<segment_info_t> segments;

  std::size_t segment_size;

  segment_id_t journal_segment_id;
  std::size_t num_in_journal_open;
  std::size_t num_type_journal;
  std::size_t num_type_ool;

  std::size_t num_open;
  std::size_t num_empty;
  std::size_t num_closed;

  std::size_t count_open_journal;
  std::size_t count_open_ool;
  std::size_t count_release_journal;
  std::size_t count_release_ool;
  std::size_t count_close_journal;
  std::size_t count_close_ool;

  std::size_t total_bytes;
  std::size_t avail_bytes_in_open;

  std::multiset<sea_time_point> modify_times;
};

std::ostream &operator<<(std::ostream &, const segments_info_t &);

/**
 * Callback interface for managing available segments
 */
class SegmentProvider {
public:
  // get the committed journal head
  virtual journal_seq_t get_journal_head() const = 0;

  // set the committed journal head
  virtual void set_journal_head(journal_seq_t) = 0;

  // get the committed journal tail
  journal_seq_t get_journal_tail() const {
    return std::min(get_alloc_tail(), get_dirty_tail());
  }

  // get the committed journal dirty tail
  virtual journal_seq_t get_dirty_tail() const = 0;

  // get the committed journal alloc tail
  virtual journal_seq_t get_alloc_tail() const = 0;

  // set the committed journal tails
  virtual void update_journal_tails(
      journal_seq_t dirty_tail, journal_seq_t alloc_tail) = 0;

  virtual const segment_info_t& get_seg_info(segment_id_t id) const = 0;

  virtual segment_id_t allocate_segment(
      segment_seq_t, segment_type_t, data_category_t, reclaim_gen_t) = 0;

  virtual void close_segment(segment_id_t) = 0;

  // set the submitted segment writes in order
  virtual void update_segment_avail_bytes(segment_type_t, paddr_t) = 0;

  virtual void update_modify_time(
      segment_id_t, sea_time_point, std::size_t) = 0;

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


class AsyncCleaner : public SegmentProvider {
public:
  /// Config
  struct config_t {
    /// Number of minimum journal segments to stop trimming dirty.
    size_t target_journal_dirty_segments = 0;
    /// Number of maximum journal segments to block user transactions.
    size_t max_journal_segments = 0;

    /// Number of minimum journal segments to stop trimming allocation
    /// (having the corresponding backrefs unmerged)
    size_t target_journal_alloc_segments = 0;

    /// Ratio of maximum available space to disable reclaiming.
    double available_ratio_gc_max = 0;
    /// Ratio of minimum available space to force reclaiming.
    double available_ratio_hard_limit = 0;

    /// Ratio of minimum reclaimable space to stop reclaiming.
    double reclaim_ratio_gc_threshold = 0;

    /// Number of bytes to reclaim per cycle
    size_t reclaim_bytes_per_cycle = 0;

    /// Number of bytes to rewrite dirty per cycle
    size_t rewrite_dirty_bytes_per_cycle = 0;

    /// Number of bytes to rewrite backref per cycle
    size_t rewrite_backref_bytes_per_cycle = 0;

    void validate() const {
      ceph_assert(max_journal_segments > target_journal_dirty_segments);
      ceph_assert(max_journal_segments > target_journal_alloc_segments);
      ceph_assert(available_ratio_gc_max > available_ratio_hard_limit);
      ceph_assert(reclaim_bytes_per_cycle > 0);
      ceph_assert(rewrite_dirty_bytes_per_cycle > 0);
      ceph_assert(rewrite_backref_bytes_per_cycle > 0);
    }

    static config_t get_default() {
      return config_t{
	  12,   // target_journal_dirty_segments
	  16,   // max_journal_segments
	  2,	// target_journal_alloc_segments
	  .15,  // available_ratio_gc_max
	  .1,   // available_ratio_hard_limit
	  .1,   // reclaim_ratio_gc_threshold
	  1<<20,// reclaim_bytes_per_cycle
	  1<<17,// rewrite_dirty_bytes_per_cycle
	  1<<24 // rewrite_backref_bytes_per_cycle
	};
    }

    static config_t get_test() {
      return config_t{
	  2,    // target_journal_dirty_segments
	  4,    // max_journal_segments
	  2,	// target_journal_alloc_segments
	  .99,  // available_ratio_gc_max
	  .2,   // available_ratio_hard_limit
	  .6,   // reclaim_ratio_gc_threshold
	  1<<20,// reclaim_bytes_per_cycle
	  1<<17,// rewrite_dirty_bytes_per_cycle
	  1<<24 // rewrite_backref_bytes_per_cycle
	};
    }
  };

  /// Callback interface for querying and operating on segments
  class ExtentCallbackInterface {
  public:
    virtual ~ExtentCallbackInterface() = default;

    /// Creates empty transaction
    /// weak transaction should be type READ
    virtual TransactionRef create_transaction(
        Transaction::src_t, const char *name, bool is_weak=false) = 0;

    /// Creates empty transaction with interruptible context
    template <typename Func>
    auto with_transaction_intr(
        Transaction::src_t src,
        const char* name,
        Func &&f) {
      return do_with_transaction_intr<Func, false>(
          src, name, std::forward<Func>(f));
    }

    template <typename Func>
    auto with_transaction_weak(
        const char* name,
        Func &&f) {
      return do_with_transaction_intr<Func, true>(
          Transaction::src_t::READ, name, std::forward<Func>(f)
      ).handle_error(
        crimson::ct_error::eagain::handle([] {
          ceph_assert(0 == "eagain impossible");
        }),
        crimson::ct_error::pass_further_all{}
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
      CachedExtentRef extent,
      reclaim_gen_t target_generation,
      sea_time_point modify_time) = 0;

    /**
     * get_extent_if_live
     *
     * Returns extent at specified location if still referenced by
     * lba_manager and not removed by t.
     *
     * See TransactionManager::get_extent_if_live and
     * LBAManager::get_physical_extent_if_live.
     */
    using get_extents_if_live_iertr = extent_mapping_iertr;
    using get_extents_if_live_ret = get_extents_if_live_iertr::future<
      std::list<CachedExtentRef>>;
    virtual get_extents_if_live_ret get_extents_if_live(
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
      Transaction &t,
      std::optional<journal_seq_t> seq_to_trim = std::nullopt) = 0;

  private:
    template <typename Func, bool IsWeak>
    auto do_with_transaction_intr(
        Transaction::src_t src,
        const char* name,
        Func &&f) {
      return seastar::do_with(
        create_transaction(src, name, IsWeak),
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
  };

private:
  const bool detailed;
  const config_t config;

  SegmentManagerGroupRef sm_group;
  BackrefManager &backref_manager;

  SpaceTrackerIRef space_tracker;
  segments_info_t segments;
  bool init_complete = false;

  struct {
    /**
     * used_bytes
     *
     * Bytes occupied by live extents
     */
    uint64_t used_bytes = 0;

    /**
     * projected_used_bytes
     *
     * Sum of projected bytes used by each transaction between throttle
     * acquisition and commit completion.  See reserve_projected_usage()
     */
    uint64_t projected_used_bytes = 0;
    uint64_t projected_count = 0;
    uint64_t projected_used_bytes_sum = 0;

    uint64_t closed_journal_used_bytes = 0;
    uint64_t closed_journal_total_bytes = 0;
    uint64_t closed_ool_used_bytes = 0;
    uint64_t closed_ool_total_bytes = 0;

    uint64_t io_blocking_num = 0;
    uint64_t io_count = 0;
    uint64_t io_blocked_count = 0;
    uint64_t io_blocked_count_trim = 0;
    uint64_t io_blocked_count_reclaim = 0;
    uint64_t io_blocked_sum = 0;

    uint64_t reclaiming_bytes = 0;
    uint64_t reclaimed_bytes = 0;
    uint64_t reclaimed_segment_bytes = 0;

    seastar::metrics::histogram segment_util;
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();

  journal_seq_t journal_alloc_tail;

  journal_seq_t journal_dirty_tail;

  /// the committed journal head
  journal_seq_t journal_head;

  ExtentCallbackInterface *ecb = nullptr;

  /// populated if there is an IO blocked on hard limits
  std::optional<seastar::promise<>> blocked_io_wake;

  SegmentSeqAllocatorRef ool_segment_seq_allocator;

  /**
   * disable_trim
   *
   * added to enable unit testing of CircularBoundedJournal before
   * proper support is added to AsyncCleaner.
   * Should be removed once proper support is added. TODO
   */
  bool disable_trim = false;
public:
  AsyncCleaner(
    config_t config,
    SegmentManagerGroupRef&& sm_group,
    BackrefManager &backref_manager,
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
  journal_seq_t get_journal_head() const final {
    return journal_head;
  }

  const segment_info_t& get_seg_info(segment_id_t id) const final {
    return segments[id];
  }

  void set_journal_head(journal_seq_t head) final {
    ceph_assert(head != JOURNAL_SEQ_NULL);
    ceph_assert(journal_head == JOURNAL_SEQ_NULL ||
                head >= journal_head);
    ceph_assert(journal_alloc_tail == JOURNAL_SEQ_NULL ||
                head >= journal_alloc_tail);
    ceph_assert(journal_dirty_tail == JOURNAL_SEQ_NULL ||
                head >= journal_dirty_tail);

    if (head.offset.get_addr_type() == paddr_types_t::SEGMENT) {
      auto submitted_journal_head = segments.get_submitted_journal_head();
      ceph_assert(submitted_journal_head != JOURNAL_SEQ_NULL &&
                  head <= submitted_journal_head);
    }

    journal_head = head;
    gc_process.maybe_wake_on_space_used();
  }

  segment_id_t allocate_segment(
      segment_seq_t, segment_type_t, data_category_t, reclaim_gen_t) final;

  void close_segment(segment_id_t segment) final;

  void update_segment_avail_bytes(segment_type_t type, paddr_t offset) final {
    segments.update_written_to(type, offset);
    gc_process.maybe_wake_on_space_used();
  }

  void update_modify_time(
      segment_id_t id, sea_time_point tp, std::size_t num_extents) final {
    ceph_assert(num_extents == 0 || tp != NULL_TIME);
    segments.update_modify_time(id, tp, num_extents);
  }

  SegmentManagerGroup* get_segment_manager_group() final {
    return sm_group.get();
  }

  journal_seq_t get_dirty_tail() const final {
    return journal_dirty_tail;
  }

  journal_seq_t get_alloc_tail() const final {
    return journal_alloc_tail;
  }

  void update_journal_tails(
      journal_seq_t dirty_tail, journal_seq_t alloc_tail) final;

  void adjust_segment_util(double old_usage, double new_usage) {
    auto old_index = get_bucket_index(old_usage);
    auto new_index = get_bucket_index(new_usage);
    assert(stats.segment_util.buckets[old_index].count > 0);
    stats.segment_util.buckets[old_index].count--;
    stats.segment_util.buckets[new_index].count++;
  }

  void mark_space_used(
    paddr_t addr,
    extent_len_t len,
    bool init_scan = false);

  void mark_space_free(
    paddr_t addr,
    extent_len_t len,
    bool init_scan = false);

  SpaceTrackerIRef get_empty_space_tracker() const {
    return space_tracker->make_empty();
  }

  void complete_init();

  store_statfs_t stat() const {
    store_statfs_t st;
    st.total = segments.get_total_bytes();
    st.available = segments.get_total_bytes() - stats.used_bytes;
    st.allocated = stats.used_bytes;
    st.data_stored = stats.used_bytes;

    // TODO add per extent type counters for omap_allocated and
    // internal metadata
    return st;
  }

  seastar::future<> stop();

  seastar::future<> run_until_halt() {
    return gc_process.run_until_halt();
  }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    ecb = cb;
  }

  bool debug_check_space(const SpaceTrackerI &tracker) {
    return space_tracker->equals(tracker);
  }

  void set_disable_trim(bool val) {
    disable_trim = val;
  }

  using work_ertr = ExtentCallbackInterface::extent_mapping_ertr;
  using work_iertr = ExtentCallbackInterface::extent_mapping_iertr;

private:
  /*
   * 10 buckets for the number of closed segments by usage
   * 2 extra buckets for the number of open and empty segments
   */
  static constexpr double UTIL_STATE_OPEN = 1.05;
  static constexpr double UTIL_STATE_EMPTY = 1.15;
  static constexpr std::size_t UTIL_BUCKETS = 12;
  static std::size_t get_bucket_index(double util) {
    auto index = std::floor(util * 10);
    assert(index < UTIL_BUCKETS);
    return index;
  }
  double calc_utilization(segment_id_t id) const {
    auto& info = segments[id];
    if (info.is_open()) {
      return UTIL_STATE_OPEN;
    } else if (info.is_empty()) {
      return UTIL_STATE_EMPTY;
    } else {
      auto ret = space_tracker->calc_utilization(id);
      assert(ret >= 0 && ret < 1);
      return ret;
    }
  }

  // journal status helpers

  double calc_gc_benefit_cost(
      segment_id_t id,
      const sea_time_point &now_time,
      const sea_time_point &bound_time) const;

  segment_id_t get_next_reclaim_segment() const;

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

  using trim_alloc_iertr = work_iertr;
  using trim_alloc_ret = trim_alloc_iertr::future<journal_seq_t>;
  trim_alloc_ret trim_alloc(
    Transaction &t,
    journal_seq_t limit);

  journal_seq_t get_dirty_tail_target() const {
    assert(init_complete);
    auto ret = journal_head;
    ceph_assert(ret != JOURNAL_SEQ_NULL);
    if (ret.segment_seq >= config.target_journal_dirty_segments) {
      ret.segment_seq -= config.target_journal_dirty_segments;
    } else {
      ret.segment_seq = 0;
      ret.offset = P_ADDR_MIN;
    }
    return ret;
  }

  journal_seq_t get_tail_limit() const {
    assert(init_complete);
    auto ret = journal_head;
    ceph_assert(ret != JOURNAL_SEQ_NULL);
    if (ret.segment_seq >= config.max_journal_segments) {
      ret.segment_seq -= config.max_journal_segments;
    } else {
      ret.segment_seq = 0;
      ret.offset = P_ADDR_MIN;
    }
    return ret;
  }

  journal_seq_t get_alloc_tail_target() const {
    assert(init_complete);
    auto ret = journal_head;
    ceph_assert(ret != JOURNAL_SEQ_NULL);
    if (ret.segment_seq >= config.target_journal_alloc_segments) {
      ret.segment_seq -= config.target_journal_alloc_segments;
    } else {
      ret.segment_seq = 0;
      ret.offset = P_ADDR_MIN;
    }
    return ret;
  }

  struct reclaim_state_t {
    reclaim_gen_t generation;
    reclaim_gen_t target_generation;
    std::size_t segment_size;
    paddr_t start_pos;
    paddr_t end_pos;

    static reclaim_state_t create(
        segment_id_t segment_id,
        reclaim_gen_t generation,
        std::size_t segment_size) {
      ceph_assert(generation < RECLAIM_GENERATIONS);
      return {generation,
              (reclaim_gen_t)(generation == RECLAIM_GENERATIONS - 1 ?
                              generation : generation + 1),
              segment_size,
              P_ADDR_NULL,
              paddr_t::make_seg_paddr(segment_id, 0)};
    }

    segment_id_t get_segment_id() const {
      return end_pos.as_seg_paddr().get_segment_id();
    }

    bool is_complete() const {
      return (std::size_t)end_pos.as_seg_paddr().get_segment_off() >= segment_size;
    }

    void advance(std::size_t bytes) {
      assert(!is_complete());
      start_pos = end_pos;
      auto &end_seg_paddr = end_pos.as_seg_paddr();
      auto next_off = end_seg_paddr.get_segment_off() + bytes;
      if (next_off > segment_size) {
        end_seg_paddr.set_segment_off(segment_size);
      } else {
        end_seg_paddr.set_segment_off(next_off);
      }
    }
  };
  std::optional<reclaim_state_t> reclaim_state;

  /**
   * GCProcess
   *
   * Background gc process.
   */
  using gc_cycle_ret = seastar::future<>;
  class GCProcess {
    std::optional<gc_cycle_ret> process_join;

    AsyncCleaner &cleaner;

    std::optional<seastar::promise<>> blocking;

    bool is_stopping() const {
      return !process_join;
    }

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
	  return is_stopping() || cleaner.gc_should_run();
	},
	[this] {
	  ceph_assert(!blocking);
	  blocking = seastar::promise<>();
	  return blocking->get_future();
	});
    }
  public:
    GCProcess(AsyncCleaner &cleaner) : cleaner(cleaner) {}

    void start() {
      ceph_assert(is_stopping());
      process_join = seastar::now(); // allow run()
      process_join = run();
      assert(!is_stopping());
    }

    gc_cycle_ret stop() {
      if (is_stopping()) {
        return seastar::now();
      }
      auto ret = std::move(*process_join);
      process_join.reset();
      assert(is_stopping());
      wake();
      return ret;
    }

    gc_cycle_ret run_until_halt() {
      ceph_assert(is_stopping());
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
      if (is_stopping()) {
        return;
      }
      if (cleaner.gc_should_run()) {
	wake();
      }
    }
  } gc_process;

  using gc_ertr = work_ertr::extend_ertr<
    SegmentManagerGroup::scan_valid_records_ertr
    >;

  gc_cycle_ret do_gc_cycle();

  using gc_trim_dirty_ertr = gc_ertr;
  using gc_trim_dirty_ret = gc_trim_dirty_ertr::future<>;
  gc_trim_dirty_ret gc_trim_dirty();

  using gc_trim_alloc_ertr = gc_ertr;
  using gc_trim_alloc_ret = gc_trim_alloc_ertr::future<>;
  gc_trim_alloc_ret gc_trim_alloc();

  using gc_reclaim_space_ertr = gc_ertr;
  using gc_reclaim_space_ret = gc_reclaim_space_ertr::future<>;
  gc_reclaim_space_ret gc_reclaim_space();


  using retrieve_live_extents_iertr = work_iertr;
  using retrieve_live_extents_ret =
    retrieve_live_extents_iertr::future<>;
  retrieve_live_extents_ret _retrieve_live_extents(
    Transaction &t,
    std::set<
      backref_entry_t,
      backref_entry_t::cmp_t> &&backrefs,
    std::vector<CachedExtentRef> &extents);

  using retrieve_backref_mappings_ertr = work_ertr;
  using retrieve_backref_mappings_ret =
    retrieve_backref_mappings_ertr::future<backref_pin_list_t>;
  retrieve_backref_mappings_ret retrieve_backref_mappings(
    paddr_t start_paddr,
    paddr_t end_paddr);

  /*
   * Segments calculations
   */
  std::size_t get_segments_in_journal() const {
    auto journal_tail = get_journal_tail();
    if (journal_tail == JOURNAL_SEQ_NULL ||
        journal_head == JOURNAL_SEQ_NULL) {
      return 0;
    }
    assert(journal_head.segment_seq >= journal_tail.segment_seq);
    return journal_head.segment_seq + 1 - journal_tail.segment_seq;
  }
  std::size_t get_segments_in_journal_closed() const {
    auto in_journal = get_segments_in_journal();
    auto in_journal_open = segments.get_num_in_journal_open();
    if (in_journal >= in_journal_open) {
      return in_journal - in_journal_open;
    } else {
      return 0;
    }
  }
  std::size_t get_segments_reclaimable() const {
    assert(segments.get_num_closed() >= get_segments_in_journal_closed());
    return segments.get_num_closed() - get_segments_in_journal_closed();
  }

  /*
   * Space calculations
   */
  /// the unavailable space that is not reclaimable yet
  std::size_t get_unavailable_unreclaimable_bytes() const {
    auto ret = (segments.get_num_open() + get_segments_in_journal_closed()) *
               segments.get_segment_size();
    assert(ret >= segments.get_available_bytes_in_open());
    return ret - segments.get_available_bytes_in_open();
  }
  /// the unavailable space that can be reclaimed
  std::size_t get_unavailable_reclaimable_bytes() const {
    auto ret = get_segments_reclaimable() * segments.get_segment_size();
    ceph_assert(ret + get_unavailable_unreclaimable_bytes() == segments.get_unavailable_bytes());
    return ret;
  }
  /// the unavailable space that is not alive
  std::size_t get_unavailable_unused_bytes() const {
    assert(segments.get_unavailable_bytes() > stats.used_bytes);
    return segments.get_unavailable_bytes() - stats.used_bytes;
  }
  double get_reclaim_ratio() const {
    if (segments.get_unavailable_bytes() == 0) return 0;
    return (double)get_unavailable_unused_bytes() / (double)segments.get_unavailable_bytes();
  }
  double get_alive_ratio() const {
    return stats.used_bytes / (double)segments.get_total_bytes();
  }

  /*
   * Space calculations (projected)
   */
  std::size_t get_projected_available_bytes() const {
    return (segments.get_available_bytes() > stats.projected_used_bytes) ?
      segments.get_available_bytes() - stats.projected_used_bytes:
      0;
  }
  double get_projected_available_ratio() const {
    return (double)get_projected_available_bytes() /
      (double)segments.get_total_bytes();
  }

  /*
   * Journal sizes
   */
  std::size_t get_dirty_journal_size() const {
    if (journal_head == JOURNAL_SEQ_NULL ||
        journal_dirty_tail == JOURNAL_SEQ_NULL) {
      return 0;
    }
    return (journal_head.segment_seq - journal_dirty_tail.segment_seq) *
           segments.get_segment_size() +
           journal_head.offset.as_seg_paddr().get_segment_off() -
           segments.get_segment_size() -
           journal_dirty_tail.offset.as_seg_paddr().get_segment_off();
  }

  std::size_t get_alloc_journal_size() const {
    if (journal_head == JOURNAL_SEQ_NULL ||
        journal_alloc_tail == JOURNAL_SEQ_NULL) {
      return 0;
    }
    return (journal_head.segment_seq - journal_alloc_tail.segment_seq) *
           segments.get_segment_size() +
           journal_head.offset.as_seg_paddr().get_segment_off() -
           segments.get_segment_size() -
           journal_alloc_tail.offset.as_seg_paddr().get_segment_off();
  }

  /**
   * should_block_on_gc
   *
   * Encapsulates whether block pending gc.
   */
  bool should_block_on_trim() const {
    assert(init_complete);
    if (disable_trim) return false;
    return get_tail_limit() > get_journal_tail();
  }

  bool should_block_on_reclaim() const {
    assert(init_complete);
    if (disable_trim) return false;
    if (get_segments_reclaimable() == 0) {
      return false;
    }
    auto aratio = get_projected_available_ratio();
    return aratio < config.available_ratio_hard_limit;
  }

  bool should_block_on_gc() const {
    assert(init_complete);
    return should_block_on_trim() || should_block_on_reclaim();
  }

  void log_gc_state(const char *caller) const;

public:
  seastar::future<> reserve_projected_usage(std::size_t projected_usage);

  void release_projected_usage(size_t projected_usage);

private:
  void maybe_wake_gc_blocked_io() {
    if (!init_complete) {
      return;
    }
    if (!should_block_on_gc() && blocked_io_wake) {
      blocked_io_wake->set_value();
      blocked_io_wake = std::nullopt;
    }
  }

  using scan_extents_ertr = SegmentManagerGroup::scan_valid_records_ertr;
  using scan_extents_ret = scan_extents_ertr::future<>;
  scan_extents_ret scan_no_tail_segment(
    const segment_header_t& header,
    segment_id_t segment_id);

  /**
   * gc_should_reclaim_space
   *
   * Encapsulates logic for whether gc should be reclaiming segment space.
   */
  bool gc_should_reclaim_space() const {
    assert(init_complete);
    if (disable_trim) return false;
    if (get_segments_reclaimable() == 0) {
      return false;
    }
    auto aratio = segments.get_available_ratio();
    auto rratio = get_reclaim_ratio();
    return (
      (aratio < config.available_ratio_hard_limit) ||
      ((aratio < config.available_ratio_gc_max) &&
       (rratio > config.reclaim_ratio_gc_threshold))
    );
  }

  bool gc_should_trim_dirty() const {
    assert(init_complete);
    return get_dirty_tail_target() > journal_dirty_tail;
  }

  bool gc_should_trim_alloc() const {
    assert(init_complete);
    return get_alloc_tail_target() > journal_alloc_tail;
  }
  /**
   * gc_should_run
   *
   * True if gc should be running.
   */
  bool gc_should_run() const {
    if (disable_trim) return false;
    ceph_assert(init_complete);
    return gc_should_reclaim_space()
      || gc_should_trim_dirty()
      || gc_should_trim_alloc();
  }

  void init_mark_segment_closed(
      segment_id_t segment,
      segment_seq_t seq,
      segment_type_t s_type,
      data_category_t category,
      reclaim_gen_t generation) {
    ceph_assert(!init_complete);
    auto old_usage = calc_utilization(segment);
    segments.init_closed(segment, seq, s_type, category, generation);
    auto new_usage = calc_utilization(segment);
    adjust_segment_util(old_usage, new_usage);
    if (s_type == segment_type_t::OOL) {
      ool_segment_seq_allocator->set_next_segment_seq(seq);
    }
  }

  struct gc_stat_printer_t {
    const AsyncCleaner *cleaner;
    bool detailed = false;
  };
  friend std::ostream &operator<<(std::ostream &, gc_stat_printer_t);
};
using AsyncCleanerRef = std::unique_ptr<AsyncCleaner>;

std::ostream &operator<<(std::ostream &, AsyncCleaner::gc_stat_printer_t);

}
