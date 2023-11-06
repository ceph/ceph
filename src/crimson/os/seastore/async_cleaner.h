// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>
#include <seastar/core/metrics_types.hh>

#include "common/ceph_time.h"

#include "osd/osd_types.h"

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager_group.h"
#include "crimson/os/seastore/randomblock_manager_group.h"
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

  rewrite_gen_t generation = NULL_GENERATION;

  sea_time_point modify_time = NULL_TIME;

  std::size_t num_extents = 0;

  segment_off_t written_to = 0;

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
                   data_category_t, rewrite_gen_t,
                   segment_off_t);

  void set_open(segment_seq_t, segment_type_t,
                data_category_t, rewrite_gen_t);

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
  segment_off_t get_segment_size() const {
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
                   data_category_t, rewrite_gen_t);

  void mark_open(segment_id_t, segment_seq_t, segment_type_t,
                 data_category_t, rewrite_gen_t);

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

  segment_off_t segment_size;

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
 * Callback interface for querying extents and operating on transactions.
 */
class ExtentCallbackInterface {
public:
  using base_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using base_iertr = trans_iertr<base_ertr>;

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
  using get_next_dirty_extents_iertr = base_iertr;
  using get_next_dirty_extents_ret = get_next_dirty_extents_iertr::future<
    std::vector<CachedExtentRef>>;
  virtual get_next_dirty_extents_ret get_next_dirty_extents(
    Transaction &t,     ///< [in] current transaction
    journal_seq_t bound,///< [in] return extents with dirty_from < bound
    size_t max_bytes    ///< [in] return up to max_bytes of extents
  ) = 0;

  /**
   * rewrite_extent
   *
   * Updates t with operations moving the passed extents to a new
   * segment.  extent may be invalid, implementation must correctly
   * handle finding the current instance if it is still alive and
   * otherwise ignore it.
   */
  using rewrite_extent_iertr = base_iertr;
  using rewrite_extent_ret = rewrite_extent_iertr::future<>;
  virtual rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent,
    rewrite_gen_t target_generation,
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
  using get_extents_if_live_iertr = base_iertr;
  using get_extents_if_live_ret = get_extents_if_live_iertr::future<
    std::list<CachedExtentRef>>;
  virtual get_extents_if_live_ret get_extents_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    extent_len_t len) = 0;

  /**
   * submit_transaction_direct
   *
   * Submits transaction without any space throttling.
   */
  using submit_transaction_direct_iertr = base_iertr;
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

/**
 * Callback interface to wake up background works
 */
struct BackgroundListener {
  enum class state_t {
    STOP,
    MOUNT,
    SCAN_SPACE,
    RUNNING,
    HALT,
  };

  virtual ~BackgroundListener() = default;
  virtual void maybe_wake_background() = 0;
  virtual void maybe_wake_blocked_io() = 0;
  virtual state_t get_state() const = 0;

  bool is_ready() const {
    return get_state() >= state_t::RUNNING;
  }
};

/**
 * Callback interface for Journal
 */
class JournalTrimmer {
public:
  // get the committed journal head
  virtual journal_seq_t get_journal_head() const = 0;

  // set the committed journal head
  virtual void set_journal_head(journal_seq_t) = 0;

  // get the committed journal dirty tail
  virtual journal_seq_t get_dirty_tail() const = 0;

  // get the committed journal alloc tail
  virtual journal_seq_t get_alloc_tail() const = 0;

  // set the committed journal tails
  virtual void update_journal_tails(
      journal_seq_t dirty_tail, journal_seq_t alloc_tail) = 0;

  // try reserve the projected usage in journal
  // returns if the reservation is successful
  // if the reservation is successful, user should call
  // release_inline_usage to restore.
  virtual bool try_reserve_inline_usage(std::size_t usage) = 0;

  // release the projected usage in journal
  virtual void release_inline_usage(std::size_t usage) = 0;

  virtual ~JournalTrimmer() {}

  journal_seq_t get_journal_tail() const {
    return std::min(get_alloc_tail(), get_dirty_tail());
  }

  virtual std::size_t get_trim_size_per_cycle() const = 0;

  bool check_is_ready() const {
    return (get_journal_head() != JOURNAL_SEQ_NULL &&
            get_dirty_tail() != JOURNAL_SEQ_NULL &&
            get_alloc_tail() != JOURNAL_SEQ_NULL);
  }

  std::size_t get_num_rolls() const {
    if (!check_is_ready()) {
      return 0;
    }
    assert(get_journal_head().segment_seq >=
           get_journal_tail().segment_seq);
    return get_journal_head().segment_seq + 1 -
           get_journal_tail().segment_seq;
  }
};

class BackrefManager;
class JournalTrimmerImpl;
using JournalTrimmerImplRef = std::unique_ptr<JournalTrimmerImpl>;

/**
 * Journal trimming implementation
 */
class JournalTrimmerImpl : public JournalTrimmer {
public:
  struct config_t {
    /// Number of minimum bytes to stop trimming dirty.
    std::size_t target_journal_dirty_bytes = 0;
    /// Number of minimum bytes to stop trimming allocation
    /// (having the corresponding backrefs unmerged)
    std::size_t target_journal_alloc_bytes = 0;
    /// Number of maximum bytes to block user transactions.
    std::size_t max_journal_bytes = 0;
    /// Number of bytes to rewrite dirty per cycle
    std::size_t rewrite_dirty_bytes_per_cycle = 0;
    /// Number of bytes to rewrite backref per cycle
    std::size_t rewrite_backref_bytes_per_cycle = 0;

    void validate() const;

    static config_t get_default(
        std::size_t roll_size, journal_type_t type);

    static config_t get_test(
        std::size_t roll_size, journal_type_t type);
  };

  JournalTrimmerImpl(
    BackrefManager &backref_manager,
    config_t config,
    journal_type_t type,
    device_off_t roll_start,
    device_off_t roll_size);

  ~JournalTrimmerImpl() = default;

  /*
   * JournalTrimmer interfaces
   */

  journal_seq_t get_journal_head() const final {
    return journal_head;
  }

  void set_journal_head(journal_seq_t) final;

  journal_seq_t get_dirty_tail() const final {
    return journal_dirty_tail;
  }

  journal_seq_t get_alloc_tail() const final {
    return journal_alloc_tail;
  }

  void update_journal_tails(
      journal_seq_t dirty_tail, journal_seq_t alloc_tail) final;

  std::size_t get_trim_size_per_cycle() const final {
    return config.rewrite_backref_bytes_per_cycle +
      config.rewrite_dirty_bytes_per_cycle;
  }

  journal_type_t get_journal_type() const {
    return journal_type;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) {
    extent_callback = cb;
  }

  void set_background_callback(BackgroundListener *cb) {
    background_callback = cb;
  }

  void reset() {
    journal_head = JOURNAL_SEQ_NULL;
    journal_dirty_tail = JOURNAL_SEQ_NULL;
    journal_alloc_tail = JOURNAL_SEQ_NULL;
  }

  bool should_trim() const {
    return should_trim_alloc() || should_trim_dirty();
  }

  bool should_block_io_on_trim() const {
    return get_tail_limit() >
      get_journal_tail().add_offset(
        journal_type, reserved_usage, roll_start, roll_size);
  }

  bool try_reserve_inline_usage(std::size_t usage) final {
    reserved_usage += usage;
    if (should_block_io_on_trim()) {
      reserved_usage -= usage;
      return false;
    } else {
      return true;
    }
  }

  void release_inline_usage(std::size_t usage) final {
    ceph_assert(reserved_usage >= usage);
    reserved_usage -= usage;
  }

  seastar::future<> trim();

  static JournalTrimmerImplRef create(
      BackrefManager &backref_manager,
      config_t config,
      journal_type_t type,
      device_off_t roll_start,
      device_off_t roll_size) {
    return std::make_unique<JournalTrimmerImpl>(
        backref_manager, config, type, roll_start, roll_size);
  }

  struct stat_printer_t {
    const JournalTrimmerImpl &trimmer;
    bool detailed = false;
  };
  friend std::ostream &operator<<(std::ostream &, const stat_printer_t &);

private:
  bool should_trim_dirty() const {
    return get_dirty_tail_target() > journal_dirty_tail;
  }

  bool should_trim_alloc() const {
    return get_alloc_tail_target() > journal_alloc_tail;
  }

  using trim_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  trim_ertr::future<> trim_dirty();

  trim_ertr::future<> trim_alloc();

  journal_seq_t get_tail_limit() const;
  journal_seq_t get_dirty_tail_target() const;
  journal_seq_t get_alloc_tail_target() const;
  std::size_t get_dirty_journal_size() const;
  std::size_t get_alloc_journal_size() const;
  void register_metrics();

  ExtentCallbackInterface *extent_callback = nullptr;
  BackgroundListener *background_callback = nullptr;
  BackrefManager &backref_manager;

  config_t config;
  journal_type_t journal_type;
  device_off_t roll_start;
  device_off_t roll_size;

  journal_seq_t journal_head;
  journal_seq_t journal_dirty_tail;
  journal_seq_t journal_alloc_tail;

  std::size_t reserved_usage;

  seastar::metrics::metric_group metrics;
};

std::ostream &operator<<(
    std::ostream &, const JournalTrimmerImpl::stat_printer_t &);

/**
 * Callback interface for managing available segments
 */
class SegmentProvider {
public:
  virtual const segment_info_t& get_seg_info(segment_id_t id) const = 0;

  virtual segment_id_t allocate_segment(
      segment_seq_t, segment_type_t, data_category_t, rewrite_gen_t) = 0;

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

  virtual double calc_utilization(segment_id_t segment) const = 0;

  virtual void reset() = 0;

  virtual ~SpaceTrackerI() = default;
};
using SpaceTrackerIRef = std::unique_ptr<SpaceTrackerI>;

class SpaceTrackerSimple : public SpaceTrackerI {
  struct segment_bytes_t {
    int64_t live_bytes = 0;
    segment_off_t total_bytes = 0;
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
    segment_off_t offset,
    extent_len_t len) final {
    return update_usage(segment, len);
  }

  int64_t release(
    segment_id_t segment,
    segment_off_t offset,
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
    segment_off_t total_bytes = 0;
    std::vector<bool> bitmap;

  public:
    SegmentMap(
      size_t blocks,
      segment_off_t total_bytes)
    : total_bytes(total_bytes),
      bitmap(blocks, false) {}

    int64_t update_usage(int64_t delta) {
      used += delta;
      return used;
    }

    int64_t allocate(
      device_segment_id_t segment,
      segment_off_t offset,
      extent_len_t len,
      const extent_len_t block_size);

    int64_t release(
      device_segment_id_t segment,
      segment_off_t offset,
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
    segment_off_t offset,
    extent_len_t len) final {
    return segment_usage[segment].allocate(
      segment.device_segment_id(),
      offset,
      len,
      block_size_by_segment_manager[segment.device_id()]);
  }

  int64_t release(
    segment_id_t segment,
    segment_off_t offset,
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

template <typename T>
class block_map_t {
public:
  block_map_t() {
    device_to_blocks.resize(DEVICE_ID_MAX_VALID);
    device_block_size.resize(DEVICE_ID_MAX_VALID);
  }
  void add_device(device_id_t device, std::size_t blocks, const T& init,
		  size_t block_size) {
    ceph_assert(device <= DEVICE_ID_MAX_VALID);
    ceph_assert(device_to_blocks[device].size() == 0);
    ceph_assert(blocks > 0);
    device_to_blocks[device].resize(blocks, init);
    total_blocks += blocks;
    device_block_size[device] = block_size;
  }
  void clear() {
    device_to_blocks.clear();
    device_to_blocks.resize(DEVICE_ID_MAX_VALID);
    total_blocks = 0;
  }

  T& operator[](paddr_t block) {
    ceph_assert(device_to_blocks[block.get_device_id()].size() != 0);
    auto &blk = block.as_blk_paddr();
    auto block_id = get_block_id(block.get_device_id(), blk.get_device_off());
    return device_to_blocks[block.get_device_id()][block_id];
  }
  const T& operator[](paddr_t block) const {
    ceph_assert(device_to_blocks[block.get_device_id()].size() != 0);
    auto &blk = block.as_blk_paddr();
    auto block_id = get_block_id(block.get_device_id(), blk.get_device_off());
    return device_to_blocks[block.get_device_id()][block_id];
  }

  auto begin() {
    return iterator<false>::lower_bound(*this, 0, 0);
  }
  auto begin() const {
    return iterator<true>::lower_bound(*this, 0, 0);
  }

  auto end() {
    return iterator<false>::end_iterator(*this);
  }
  auto end() const {
    return iterator<true>::end_iterator(*this);
  }

  size_t size() const {
    return total_blocks;
  }

  uint64_t get_block_size(device_id_t device_id) {
    return device_block_size[device_id];
  }

  uint32_t get_block_id(device_id_t device_id, device_off_t blk_off) const {
    auto block_size = device_block_size[device_id];
    return blk_off == 0 ? 0 : blk_off/block_size;
  }

  template <bool is_const = false>
  class iterator {
    /// points at set being iterated over
    std::conditional_t<
      is_const,
      const block_map_t &,
      block_map_t &> parent;

    /// points at current device, or DEVICE_ID_MAX_VALID if is_end()
    device_id_t device_id;

    /// segment at which we are pointing, 0 if is_end()
    device_off_t blk_off;

    /// holds referent for operator* and operator-> when !is_end()
    std::optional<
      std::pair<
        const device_off_t,
	std::conditional_t<is_const, const T&, T&>
	>> current;

    bool is_end() const {
      return device_id == DEVICE_ID_MAX_VALID;
    }

    uint32_t get_block_id() {
      return parent.get_block_id(device_id, blk_off);
    }

    void find_valid() {
      assert(!is_end());
      auto &device_vec = parent.device_to_blocks[device_id];
      if (device_vec.size() == 0 ||
	  get_block_id() == device_vec.size()) {
	while (++device_id < DEVICE_ID_MAX_VALID&&
	       parent.device_to_blocks[device_id].size() == 0);
	blk_off = 0;
      }
      if (is_end()) {
	current = std::nullopt;
      } else {
	current.emplace(
	  blk_off,
	  parent.device_to_blocks[device_id][get_block_id()]
	);
      }
    }

    iterator(
      decltype(parent) &parent,
      device_id_t device_id,
      device_off_t device_block_off)
      : parent(parent), device_id(device_id),
	blk_off(device_block_off) {}

  public:
    static iterator lower_bound(
      decltype(parent) &parent,
      device_id_t device_id,
      device_off_t block_off) {
      if (device_id == DEVICE_ID_MAX_VALID) {
	return end_iterator(parent);
      } else {
	auto ret = iterator{parent, device_id, block_off};
	ret.find_valid();
	return ret;
      }
    }

    static iterator end_iterator(
      decltype(parent) &parent) {
      return iterator{parent, DEVICE_ID_MAX_VALID, 0};
    }

    iterator<is_const>& operator++() {
      assert(!is_end());
      auto block_size = parent.device_block_size[device_id];
      blk_off += block_size;
      find_valid();
      return *this;
    }

    bool operator==(iterator<is_const> rit) {
      return (device_id == rit.device_id &&
	      blk_off == rit.blk_off);
    }

    bool operator!=(iterator<is_const> rit) {
      return !(*this == rit);
    }
    template <bool c = is_const, std::enable_if_t<c, int> = 0>
    const std::pair<const device_off_t, const T&> *operator->() {
      assert(!is_end());
      return &*current;
    }
    template <bool c = is_const, std::enable_if_t<!c, int> = 0>
    std::pair<const device_off_t, T&> *operator->() {
      assert(!is_end());
      return &*current;
    }
    template <bool c = is_const, std::enable_if_t<c, int> = 0>
    const std::pair<const device_off_t, const T&> &operator*() {
      assert(!is_end());
      return *current;
    }
    template <bool c = is_const, std::enable_if_t<!c, int> = 0>
    std::pair<const device_off_t, T&> &operator*() {
      assert(!is_end());
      return *current;
    }
  };
  std::vector<std::vector<T>> device_to_blocks;
  std::vector<size_t> device_block_size;
  size_t total_blocks = 0;
};

class RBMSpaceTracker {
  struct random_block_t {
    bool used = false;
    void allocate() {
      used = true;
    }
    void release() {
      used = false;
    }
  };
  block_map_t<random_block_t> block_usage;

public:
  RBMSpaceTracker(const RBMSpaceTracker &) = default;
  RBMSpaceTracker(const std::vector<RandomBlockManager*> &rbms) {
    for (auto rbm : rbms) {
      block_usage.add_device(
	rbm->get_device_id(),
	rbm->get_device()->get_available_size() / rbm->get_block_size(),
	{false},
	rbm->get_block_size());
    }
  }

  void allocate(
    paddr_t addr,
    extent_len_t len) {
    paddr_t cursor = addr;
    paddr_t end = addr.add_offset(len);
    do {
      block_usage[cursor].allocate();
      cursor = cursor.add_offset(
	block_usage.get_block_size(addr.get_device_id()));
    } while (cursor < end);
  }

  void release(
    paddr_t addr,
    extent_len_t len) {
    paddr_t cursor = addr;
    paddr_t end = addr.add_offset(len);
    do {
      block_usage[cursor].release();
      cursor = cursor.add_offset(
	block_usage.get_block_size(addr.get_device_id()));
    } while (cursor < end);
  }

  void reset() {
    for (auto &i : block_usage) {
      i.second = {false};
    }
  }

  std::unique_ptr<RBMSpaceTracker> make_empty() const {
    auto ret = std::make_unique<RBMSpaceTracker>(*this);
    ret->reset();
    return ret;
  }
  friend class RBMCleaner;
};
using RBMSpaceTrackerRef = std::unique_ptr<RBMSpaceTracker>;

/*
 * AsyncCleaner
 *
 * Interface for ExtentPlacementManager::BackgroundProcess
 * to do background cleaning.
 */
class AsyncCleaner {
public:
  using state_t = BackgroundListener::state_t;
  using base_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;

  virtual void set_background_callback(BackgroundListener *) = 0;

  virtual void set_extent_callback(ExtentCallbackInterface *) = 0;

  virtual store_statfs_t get_stat() const = 0;

  virtual void print(std::ostream &, bool is_detailed) const = 0;

  virtual bool check_usage_is_empty() const = 0;

  using mount_ertr = base_ertr;
  using mount_ret = mount_ertr::future<>;
  virtual mount_ret mount() = 0;

  virtual void mark_space_used(paddr_t, extent_len_t) = 0;

  virtual void mark_space_free(paddr_t, extent_len_t) = 0;

  virtual void commit_space_used(paddr_t, extent_len_t) = 0;

  // try reserve the projected usage in cleaner
  // returns if the reservation is successful
  // if the reservation is successful, user should call
  // release_projected_usage to restore.
  virtual bool try_reserve_projected_usage(std::size_t) = 0;

  virtual void release_projected_usage(std::size_t) = 0;

  virtual bool should_block_io_on_clean() const = 0;

  virtual bool can_clean_space() const = 0;

  virtual bool should_clean_space() const = 0;

  using clean_space_ertr = base_ertr;
  using clean_space_ret = clean_space_ertr::future<>;
  virtual clean_space_ret clean_space() = 0;

  virtual const std::set<device_id_t>& get_device_ids() const = 0;

  virtual std::size_t get_reclaim_size_per_cycle() const = 0;

#ifdef UNIT_TESTS_BUILT
  virtual void prefill_fragmented_devices() {}
#endif

  // test only
  virtual bool check_usage() = 0;

  struct stat_printer_t {
    const AsyncCleaner &cleaner;
    bool detailed = false;
  };

  virtual ~AsyncCleaner() {}
};

using AsyncCleanerRef = std::unique_ptr<AsyncCleaner>;

std::ostream &operator<<(
    std::ostream &, const AsyncCleaner::stat_printer_t &);

class SegmentCleaner;
using SegmentCleanerRef = std::unique_ptr<SegmentCleaner>;

class SegmentCleaner : public SegmentProvider, public AsyncCleaner {
public:
  /// Config
  struct config_t {
    /// Ratio of maximum available space to disable reclaiming.
    double available_ratio_gc_max = 0;
    /// Ratio of minimum available space to force reclaiming.
    double available_ratio_hard_limit = 0;
    /// Ratio of minimum reclaimable space to stop reclaiming.
    double reclaim_ratio_gc_threshold = 0;
    /// Number of bytes to reclaim per cycle
    std::size_t reclaim_bytes_per_cycle = 0;

    void validate() const {
      ceph_assert(available_ratio_gc_max > available_ratio_hard_limit);
      ceph_assert(reclaim_bytes_per_cycle > 0);
    }

    static config_t get_default() {
      return config_t{
        .15,  // available_ratio_gc_max
        .1,   // available_ratio_hard_limit
        .1,   // reclaim_ratio_gc_threshold
        1<<20 // reclaim_bytes_per_cycle
      };
    }

    static config_t get_test() {
      return config_t{
        .99,  // available_ratio_gc_max
        .2,   // available_ratio_hard_limit
        .6,   // reclaim_ratio_gc_threshold
        1<<20 // reclaim_bytes_per_cycle
      };
    }
  };

  SegmentCleaner(
    config_t config,
    SegmentManagerGroupRef&& sm_group,
    BackrefManager &backref_manager,
    SegmentSeqAllocator &segment_seq_allocator,
    bool detailed,
    bool is_cold);

  void set_journal_trimmer(JournalTrimmer &_trimmer) {
    trimmer = &_trimmer;
  }

  static SegmentCleanerRef create(
      config_t config,
      SegmentManagerGroupRef&& sm_group,
      BackrefManager &backref_manager,
      SegmentSeqAllocator &ool_seq_allocator,
      bool detailed,
      bool is_cold = false) {
    return std::make_unique<SegmentCleaner>(
        config, std::move(sm_group), backref_manager,
        ool_seq_allocator, detailed, is_cold);
  }

  /*
   * SegmentProvider interfaces
   */

  const segment_info_t& get_seg_info(segment_id_t id) const final {
    return segments[id];
  }

  segment_id_t allocate_segment(
      segment_seq_t, segment_type_t, data_category_t, rewrite_gen_t) final;

  void close_segment(segment_id_t segment) final;

  void update_segment_avail_bytes(segment_type_t type, paddr_t offset) final {
    assert(type == segment_type_t::OOL ||
           trimmer != nullptr); // segment_type_t::JOURNAL
    segments.update_written_to(type, offset);
    background_callback->maybe_wake_background();
  }

  void update_modify_time(
      segment_id_t id, sea_time_point tp, std::size_t num_extents) final {
    ceph_assert(num_extents == 0 || tp != NULL_TIME);
    segments.update_modify_time(id, tp, num_extents);
  }

  SegmentManagerGroup* get_segment_manager_group() final {
    return sm_group.get();
  }

  /*
   * AsyncCleaner interfaces
   */

  void set_background_callback(BackgroundListener *cb) final {
    background_callback = cb;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) final {
    extent_callback = cb;
  }

  store_statfs_t get_stat() const final {
    store_statfs_t st;
    st.total = segments.get_total_bytes();
    st.available = segments.get_total_bytes() - stats.used_bytes;
    st.allocated = stats.used_bytes;
    st.data_stored = stats.used_bytes;

    // TODO add per extent type counters for omap_allocated and
    // internal metadata
    return st;
  }

  void print(std::ostream &, bool is_detailed) const final;

  bool check_usage_is_empty() const final {
    return space_tracker->equals(*space_tracker->make_empty());
  }

  mount_ret mount() final;

  void mark_space_used(paddr_t, extent_len_t) final;

  void mark_space_free(paddr_t, extent_len_t) final;
  
  void commit_space_used(paddr_t addr, extent_len_t len) final {
    mark_space_used(addr, len);
  }

  bool try_reserve_projected_usage(std::size_t) final;

  void release_projected_usage(size_t) final;

  bool should_block_io_on_clean() const final {
    assert(background_callback->is_ready());
    if (get_segments_reclaimable() == 0) {
      return false;
    }
    auto aratio = get_projected_available_ratio();
    return aratio < config.available_ratio_hard_limit;
  }

  bool can_clean_space() const final {
    assert(background_callback->is_ready());
    return get_segments_reclaimable() > 0;
  }

  bool should_clean_space() const final {
    assert(background_callback->is_ready());
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

  clean_space_ret clean_space() final;

  const std::set<device_id_t>& get_device_ids() const final {
    return sm_group->get_device_ids();
  }

  std::size_t get_reclaim_size_per_cycle() const final {
    return config.reclaim_bytes_per_cycle;
  }

  // Testing interfaces

  bool check_usage() final;

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

  struct reclaim_state_t {
    rewrite_gen_t generation;
    rewrite_gen_t target_generation;
    segment_off_t segment_size;
    paddr_t start_pos;
    paddr_t end_pos;

    static reclaim_state_t create(
        segment_id_t segment_id,
        rewrite_gen_t generation,
        segment_off_t segment_size) {
      ceph_assert(is_rewrite_generation(generation));

      rewrite_gen_t target_gen;
      if (generation < MIN_REWRITE_GENERATION) {
        target_gen = MIN_REWRITE_GENERATION;
      } else {
        // tolerate the target_gen to exceed MAX_REWRETE_GENERATION to make EPM
        // aware of its original generation for the decisions.
        target_gen = generation + 1;
      }

      assert(is_target_rewrite_generation(target_gen));
      return {generation,
              target_gen,
              segment_size,
              P_ADDR_NULL,
              paddr_t::make_seg_paddr(segment_id, 0)};
    }

    segment_id_t get_segment_id() const {
      return end_pos.as_seg_paddr().get_segment_id();
    }

    bool is_complete() const {
      return end_pos.as_seg_paddr().get_segment_off() >= segment_size;
    }

    void advance(std::size_t bytes) {
      assert(!is_complete());
      start_pos = end_pos;
      auto &end_seg_paddr = end_pos.as_seg_paddr();
      auto next_off = end_seg_paddr.get_segment_off() + bytes;
      if (next_off > (std::size_t)segment_size) {
        end_seg_paddr.set_segment_off(segment_size);
      } else {
        end_seg_paddr.set_segment_off(next_off);
      }
    }
  };
  std::optional<reclaim_state_t> reclaim_state;

  using do_reclaim_space_ertr = base_ertr;
  using do_reclaim_space_ret = do_reclaim_space_ertr::future<>;
  do_reclaim_space_ret do_reclaim_space(
    const std::vector<CachedExtentRef> &backref_extents,
    const backref_pin_list_t &pin_list,
    std::size_t &reclaimed,
    std::size_t &runs);

  /*
   * Segments calculations
   */
  std::size_t get_segments_in_journal() const {
    if (trimmer != nullptr) {
      return trimmer->get_num_rolls();
    } else {
      return 0;
    }
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

  using scan_extents_ertr = SegmentManagerGroup::scan_valid_records_ertr;
  using scan_extents_ret = scan_extents_ertr::future<>;
  scan_extents_ret scan_no_tail_segment(
    const segment_header_t& header,
    segment_id_t segment_id);

  void adjust_segment_util(double old_usage, double new_usage) {
    auto old_index = get_bucket_index(old_usage);
    auto new_index = get_bucket_index(new_usage);
    assert(stats.segment_util.buckets[old_index].count > 0);
    stats.segment_util.buckets[old_index].count--;
    stats.segment_util.buckets[new_index].count++;
  }

  void init_mark_segment_closed(
      segment_id_t segment,
      segment_seq_t seq,
      segment_type_t s_type,
      data_category_t category,
      rewrite_gen_t generation) {
    assert(background_callback->get_state() == state_t::MOUNT);
    ceph_assert(s_type == segment_type_t::OOL ||
                trimmer != nullptr); // segment_type_t::JOURNAL
    auto old_usage = calc_utilization(segment);
    segments.init_closed(segment, seq, s_type, category, generation);
    auto new_usage = calc_utilization(segment);
    adjust_segment_util(old_usage, new_usage);
    if (s_type == segment_type_t::OOL) {
      ool_segment_seq_allocator.set_next_segment_seq(seq);
    }
  }

  const bool detailed;
  const bool is_cold;
  const config_t config;

  SegmentManagerGroupRef sm_group;
  BackrefManager &backref_manager;

  SpaceTrackerIRef space_tracker;
  segments_info_t segments;

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
     * acquisition and commit completion.  See try_reserve_projected_usage()
     */
    uint64_t projected_used_bytes = 0;
    uint64_t projected_count = 0;
    uint64_t projected_used_bytes_sum = 0;

    uint64_t closed_journal_used_bytes = 0;
    uint64_t closed_journal_total_bytes = 0;
    uint64_t closed_ool_used_bytes = 0;
    uint64_t closed_ool_total_bytes = 0;

    uint64_t reclaiming_bytes = 0;
    uint64_t reclaimed_bytes = 0;
    uint64_t reclaimed_segment_bytes = 0;

    seastar::metrics::histogram segment_util;
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();

  // optional, set if this cleaner is assigned to SegmentedJournal
  JournalTrimmer *trimmer = nullptr;

  ExtentCallbackInterface *extent_callback = nullptr;

  BackgroundListener *background_callback = nullptr;

  // TODO: drop once paddr->journal_seq_t is introduced
  SegmentSeqAllocator &ool_segment_seq_allocator;
};

class RBMCleaner;
using RBMCleanerRef = std::unique_ptr<RBMCleaner>;

class RBMCleaner : public AsyncCleaner {
public:
  RBMCleaner(
    RBMDeviceGroupRef&& rb_group,
    BackrefManager &backref_manager,
    bool detailed);

  static RBMCleanerRef create(
      RBMDeviceGroupRef&& rb_group,
      BackrefManager &backref_manager,
      bool detailed) {
    return std::make_unique<RBMCleaner>(
      std::move(rb_group), backref_manager, detailed);
  }

  RBMDeviceGroup* get_rb_group() {
    return rb_group.get();
  }

  /*
   * AsyncCleaner interfaces
   */

  void set_background_callback(BackgroundListener *cb) final {
    background_callback = cb;
  }

  void set_extent_callback(ExtentCallbackInterface *cb) final {
    extent_callback = cb;
  }

  store_statfs_t get_stat() const final {
    store_statfs_t st;
    st.total = get_total_bytes();
    st.available = get_total_bytes() - get_journal_bytes() - stats.used_bytes;
    st.allocated = get_journal_bytes() + stats.used_bytes;
    st.data_stored = get_journal_bytes() + stats.used_bytes;
    return st;
  }

  void print(std::ostream &, bool is_detailed) const final;

  mount_ret mount() final;

  void mark_space_used(paddr_t, extent_len_t) final;

  void mark_space_free(paddr_t, extent_len_t) final;

  void commit_space_used(paddr_t, extent_len_t) final;

  bool try_reserve_projected_usage(std::size_t) final;

  void release_projected_usage(size_t) final;

  bool should_block_io_on_clean() const final {
    return false;
  }

  bool can_clean_space() const final {
    return false;
  }

  bool should_clean_space() const final {
    return false;
  }

  clean_space_ret clean_space() final;

  const std::set<device_id_t>& get_device_ids() const final {
    return rb_group->get_device_ids();
  }

  std::size_t get_reclaim_size_per_cycle() const final {
    return 0;
  }

#ifdef UNIT_TESTS_BUILT
  void prefill_fragmented_devices() final {
    LOG_PREFIX(RBMCleaner::prefill_fragmented_devices);
    SUBDEBUG(seastore_cleaner, "");
    auto rbs = rb_group->get_rb_managers();
    for (auto p : rbs) {
      p->prefill_fragmented_device();
    }
  }
#endif

  RandomBlockManager* get_rbm(paddr_t paddr) {
    auto rbs = rb_group->get_rb_managers();
    for (auto p : rbs) {
      if (p->get_device_id() == paddr.get_device_id()) {
	return p;
      }
    }
    return nullptr;
  }

  paddr_t alloc_paddr(extent_len_t length) {
    // TODO: implement allocation strategy (dirty metadata and multiple devices)
    auto rbs = rb_group->get_rb_managers();
    auto paddr = rbs[0]->alloc_extent(length);
    stats.used_bytes += length;
    return paddr;
  }

  std::list<alloc_paddr_result> alloc_paddrs(extent_len_t length) {
    // TODO: implement allocation strategy (dirty metadata and multiple devices)
    auto rbs = rb_group->get_rb_managers();
    auto ret = rbs[0]->alloc_extents(length);
    stats.used_bytes += length;
    return ret;
  }

  size_t get_total_bytes() const {
    auto rbs = rb_group->get_rb_managers();
    size_t total = 0;
    for (auto p : rbs) {
      total += p->get_device()->get_available_size();
    }
    return total;
  }

  size_t get_journal_bytes() const {
    auto rbs = rb_group->get_rb_managers();
    size_t total = 0;
    for (auto p : rbs) {
      total += p->get_journal_size();
    }
    return total;
  }

  // Testing interfaces

  bool check_usage() final;

  bool check_usage_is_empty() const final {
    // TODO
    return true;
  }

private:
  bool equals(const RBMSpaceTracker &other) const;

  const bool detailed;
  RBMDeviceGroupRef rb_group;
  BackrefManager &backref_manager;

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
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();

  ExtentCallbackInterface *extent_callback = nullptr;
  BackgroundListener *background_callback = nullptr;
};
}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::segment_info_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::segments_info_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::AsyncCleaner::stat_printer_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::JournalTrimmerImpl::stat_printer_t> : fmt::ostream_formatter {};
#endif
