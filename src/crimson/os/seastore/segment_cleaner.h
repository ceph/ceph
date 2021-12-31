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

class SegmentCleaner;

// for keeping track of segment managers' various information,
// like empty segments, opened segments and so on.
class segment_info_set_t {
  struct segment_manager_info_t {
    segment_manager_info_t() = default;
    segment_manager_info_t(
      device_id_t device_id,
      device_segment_id_t num_segments,
      segment_off_t segment_size,
      segment_off_t block_size,
      size_t empty_segments,
      size_t size)
      : device_id(device_id),
	num_segments(num_segments),
	segment_size(segment_size),
	block_size(block_size),
	empty_segments(empty_segments),
	size(size),
	avail_bytes(size)
    {}

    device_id_t device_id = 0;
    device_segment_id_t num_segments = 0;
    segment_off_t segment_size = 0;
    segment_off_t block_size = 0;
    size_t empty_segments = 0;
    size_t size = 0;
    size_t avail_bytes = 0;
    std::map<segment_id_t, segment_off_t> open_segment_avails;
  };

  struct segment_info_t {
    Segment::segment_state_t state = Segment::segment_state_t::EMPTY;

    // Will be non-null for any segments in the current journal
    segment_seq_t journal_segment_seq = NULL_SEG_SEQ;

    bool out_of_line = false;

    void set_open();
    void set_empty();
    void set_closed();

    bool is_in_journal(journal_seq_t tail_committed) const {
      return !out_of_line &&
	journal_segment_seq != NULL_SEG_SEQ &&
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
public:
  segment_info_set_t() {
    sm_infos.resize(DEVICE_ID_MAX);
  }

  segment_info_t& operator[](segment_id_t id) {
    return segments[id];
  }
  const segment_info_t& operator[](segment_id_t id) const {
    return segments[id];
  }

  std::optional<segment_manager_info_t> &
  operator[](device_id_t id) {
    auto& sm_info = sm_infos[id];
    assert(sm_info && sm_info->device_id == id);
    return sm_info;
  }
  const std::optional<segment_manager_info_t> &
  operator[](device_id_t id) const {
    auto& sm_info = sm_infos[id];
    assert(sm_info && sm_info->device_id == id);
    return sm_info;
  }

  void clear() {
    segments.clear();
    total_bytes = 0;
    journal_segments = 0;
    avail_bytes = 0;
    opened_segments = 0;
  }

  void add_segment_manager(SegmentManager& segment_manager)
  {
    device_id_t d_id = segment_manager.get_device_id();
    segments.add_device(
      d_id,
      segment_manager.get_num_segments(),
      segment_info_t{});
    sm_infos[segment_manager.get_device_id()].emplace(
      d_id,
      segment_manager.get_num_segments(),
      segment_manager.get_segment_size(),
      segment_manager.get_block_size(),
      segment_manager.get_num_segments(),
      segment_manager.get_size());

    total_bytes += segment_manager.get_size();
    avail_bytes += segment_manager.get_size();
  }

  device_segment_id_t size() const {
    return segments.size();
  }

  auto begin() {
    return segments.begin();
  }
  auto begin() const {
    return segments.begin();
  }

  auto end() {
    return segments.end();
  }
  auto end() const {
    return segments.end();
  }

  auto device_begin(device_id_t id) {
    return segments.device_begin(id);
  }
  auto device_end(device_id_t id) {
    return segments.device_end(id);
  }

  // the following methods are used for keeping track of
  // seastore disk space usage
  void segment_opened(segment_id_t segment) {
    auto& sm_info = sm_infos[segment.device_id()];
    sm_info->empty_segments--;
    ceph_assert(segments[segment].is_empty());
    // must be opening a new segment
    auto [iter, inserted] = sm_info->open_segment_avails.emplace(
      segment, sm_info->segment_size);
    opened_segments++;
    ceph_assert(inserted);
  }
  void segment_emptied(segment_id_t segment) {
    auto& sm_info = sm_infos[segment.device_id()];
    sm_info->empty_segments++;
    sm_info->avail_bytes += sm_info->segment_size;
    avail_bytes += sm_info->segment_size;
  }
  void segment_closed(segment_id_t segment) {
    assert(segments.contains(segment));
    auto& segment_info = segments[segment];
    auto& sm_info = sm_infos[segment.device_id()];
    if (segment_info.is_open()) {
      auto iter = sm_info->open_segment_avails.find(segment);
      ceph_assert(iter != sm_info->open_segment_avails.end());
      assert(sm_info->avail_bytes >= (size_t)iter->second);
      assert(avail_bytes >= (size_t)iter->second);
      sm_info->avail_bytes -= iter->second;
      avail_bytes -= iter->second;
      sm_info->open_segment_avails.erase(iter);
      opened_segments--;
    } else {
      ceph_assert(segment_info.is_empty());
      assert(sm_info->avail_bytes >= (size_t)sm_info->segment_size);
      assert(avail_bytes >= (size_t)sm_info->segment_size);
      assert(sm_info->empty_segments > 0);
      sm_info->avail_bytes -= sm_info->segment_size;
      avail_bytes -= sm_info->segment_size;
      sm_info->empty_segments--;
    }
    segment_info.set_closed();
  }
  void update_segment_avail_bytes(paddr_t offset) {
    auto segment_id = offset.as_seg_paddr().get_segment_id();
    auto& sm_info = sm_infos[segment_id.device_id()];
    auto iter = sm_info->open_segment_avails.find(segment_id);
    if (iter == sm_info->open_segment_avails.end()) {
      crimson::get_logger(ceph_subsys_seastore_cleaner).error(
	"SegmentCleaner::update_segment_avail_bytes:"
	":segment closed {}, not updating",
	offset);
      return;
    }
    auto new_avail_bytes = sm_info->segment_size - offset.as_seg_paddr().get_segment_off();
    if (iter->second < new_avail_bytes) {
      crimson::get_logger(ceph_subsys_seastore_cleaner).error(
	"SegmentCleaner::update_segment_avail_bytes:"
	" avail_bytes increased? , {}, {}",
	iter->second,
	new_avail_bytes);
      ceph_assert(iter->second >= new_avail_bytes);
    }
    assert(sm_info->avail_bytes >= (size_t)(iter->second - new_avail_bytes));
    assert(avail_bytes >= (size_t)(iter->second - new_avail_bytes));
    sm_info->avail_bytes -= iter->second - new_avail_bytes;
    avail_bytes -= iter->second - new_avail_bytes;
    iter->second = new_avail_bytes;
  }
  size_t get_empty_segments(device_id_t d_id) {
    return sm_infos[d_id]->empty_segments;
  }
  size_t get_opened_segments(device_id_t d_id) {
    return sm_infos[d_id]->open_segment_avails.size();
  }
  size_t get_opened_segments() {
    return opened_segments;
  }
  size_t get_total_bytes() const {
    return total_bytes;
  }
  size_t get_available_bytes(device_id_t d_id) const {
    auto& sm_info = sm_infos[d_id];
    return sm_info->avail_bytes;
  }
  size_t get_available_bytes() const {
    return avail_bytes;
  }
  void new_journal_segment() {
    ++journal_segments;
  }
  void journal_segment_emptied() {
    --journal_segments;
  }
  device_segment_id_t get_journal_segments() const {
    return journal_segments;
  }
private:
  std::vector<std::optional<segment_manager_info_t>> sm_infos;
  segment_map_t<segment_info_t> segments;

  device_segment_id_t journal_segments = 0;
  size_t total_bytes = 0;
  size_t avail_bytes = 0;
  size_t opened_segments = 0;

  friend class SegmentCleaner;
};

/**
 * Callback interface for managing available segments
 */
class SegmentProvider {
public:
  using get_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using get_segment_ret = get_segment_ertr::future<segment_id_t>;
  virtual get_segment_ret get_segment(device_id_t id) = 0;

  virtual void close_segment(segment_id_t) {}

  virtual void set_journal_segment(
    segment_id_t segment,
    segment_seq_t seq) {}

  virtual journal_seq_t get_journal_tail_target() const = 0;
  virtual void update_journal_tail_committed(journal_seq_t tail_committed) = 0;

  virtual void init_mark_segment_closed(
    segment_id_t segment,
    segment_seq_t seq,
    bool out_of_line) {}

  virtual segment_seq_t get_seq(segment_id_t id) { return 0; }

  virtual void update_segment_avail_bytes(paddr_t offset) = 0;

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

  virtual void reset() = 0;

  virtual ~SpaceTrackerI() = default;
};
using SpaceTrackerIRef = std::unique_ptr<SpaceTrackerI>;

class SpaceTrackerSimple : public SpaceTrackerI {
  // Tracks live space for each segment
  segment_map_t<int64_t> live_bytes_by_segment;

  int64_t update_usage(segment_id_t segment, int64_t delta) {
    live_bytes_by_segment[segment] += delta;
    assert(live_bytes_by_segment[segment] >= 0);
    return live_bytes_by_segment[segment];
  }
public:
  SpaceTrackerSimple(const SpaceTrackerSimple &) = default;
  SpaceTrackerSimple(std::vector<SegmentManager*> sms) {
    for (auto sm : sms) {
      if (!sm) {
	// sms is a vector that is indexed by device id and
	// always has "max_device" elements, some of which
	// may be null.
	continue;
      }
      live_bytes_by_segment.add_device(
	sm->get_device_id(),
	sm->get_num_segments(),
	0);
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
    return live_bytes_by_segment[segment];
  }

  void dump_usage(segment_id_t) const final {}

  void reset() final {
    for (auto &i : live_bytes_by_segment) {
      i.second = 0;
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
    std::vector<bool> bitmap;

  public:
    SegmentMap(size_t blocks) : bitmap(blocks, false) {}

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
  SpaceTrackerDetailed(std::vector<SegmentManager*> sms)
  {
    block_size_by_segment_manager.resize(DEVICE_ID_MAX, 0);
    for (auto sm : sms) {
      // sms is a vector that is indexed by device id and
      // always has "max_device" elements, some of which
      // may be null.
      if (!sm) {
	continue;
      }
      segment_usage.add_device(
	sm->get_device_id(),
	sm->get_num_segments(),
	SegmentMap(
	  sm->get_segment_size() / sm->get_block_size()));
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
      segment_off_t len) = 0;

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

  ExtentReaderRef scanner;

  SpaceTrackerIRef space_tracker;
  segment_info_set_t segments;
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

    uint64_t segments_released = 0;
    uint64_t accumulated_blocked_ios = 0;
    uint64_t empty_segments = 0;
    int64_t ios_blocking = 0;
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();

  /// target journal_tail for next fresh segment
  journal_seq_t journal_tail_target;

  /// most recently committed journal_tail
  journal_seq_t journal_tail_committed;

  /// head of journal
  journal_seq_t journal_head;

  device_id_t journal_device_id;

  ExtentCallbackInterface *ecb = nullptr;

  /// populated if there is an IO blocked on hard limits
  std::optional<seastar::promise<>> blocked_io_wake;

  std::vector<device_id_t> effective_devices;

public:
  SegmentCleaner(
    config_t config,
    ExtentReaderRef&& scanner,
    bool detailed = false);

  void mount(device_id_t pdevice_id, std::vector<SegmentManager*>& sms) {
    crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
      "SegmentCleaner::mount: {} segment managers", sms.size());
    init_complete = false;
    stats = {};
    journal_tail_target = journal_seq_t{};
    journal_tail_committed = journal_seq_t{};
    journal_head = journal_seq_t{};
    journal_device_id = pdevice_id;

    for (auto& sm : sms) {
      if (sm)
	effective_devices.push_back(sm->get_device_id());
    }

    space_tracker.reset(
      detailed ?
      (SpaceTrackerI*)new SpaceTrackerDetailed(
	sms) :
      (SpaceTrackerI*)new SpaceTrackerSimple(
	sms));

    segments.clear();
    for (auto sm : sms) {
      // sms is a vector that is indexed by device id and
      // always has "max_device" elements, some of which
      // may be null.
      if (!sm) {
	continue;
      }
      segments.add_segment_manager(*sm);
      stats.empty_segments += sm->get_num_segments();
    }
    metrics.clear();
    register_metrics();
  }

  using init_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using init_segments_ret_bare =
    std::vector<std::pair<segment_id_t, segment_header_t>>;
  using init_segments_ret = init_segments_ertr::future<init_segments_ret_bare>;
  init_segments_ret init_segments();

  get_segment_ret get_segment(device_id_t id) final;

  void close_segment(segment_id_t segment) final;

  void set_journal_segment(
    segment_id_t segment, segment_seq_t seq) final {
    assert(segment.device_id() ==
      segments[segment.device_id()]->device_id);
    assert(segment.device_segment_id() <
      segments[segment.device_id()]->num_segments);
    segments[segment].journal_segment_seq = seq;
    segments[segment].out_of_line = false;
    segments.new_journal_segment();
    assert(segments[segment].is_open());
  }

  journal_seq_t get_journal_tail_target() const final {
    return journal_tail_target;
  }

  void update_journal_tail_committed(journal_seq_t committed) final;

  void update_journal_tail_target(journal_seq_t target);

  void init_mkfs(journal_seq_t head) {
    journal_tail_target = head;
    journal_tail_committed = head;
    journal_head = head;
  }

  void set_journal_head(journal_seq_t head) {
    assert(journal_head == journal_seq_t() || head >= journal_head);
    journal_head = head;
    segments.update_segment_avail_bytes(head.offset);
    gc_process.maybe_wake_on_space_used();
  }

  void update_segment_avail_bytes(paddr_t offset) final {
    segments.update_segment_avail_bytes(offset);
  }

  journal_seq_t get_journal_head() const {
    return journal_head;
  }

  void init_mark_segment_closed(
    segment_id_t segment,
    segment_seq_t seq,
    bool out_of_line) final
  {
    crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
      "SegmentCleaner::init_mark_segment_closed: segment {}, seq {}",
      segment,
      seq);
    mark_closed(segment);
    segments[segment].journal_segment_seq = seq;
    segments[segment].out_of_line = out_of_line;
    if (!segments[segment].out_of_line) {
      assert(journal_device_id == segment.device_id());
      segments.new_journal_segment();
    }
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
    auto& seg_addr = addr.as_seg_paddr();
    assert(seg_addr.get_segment_id().device_id() ==
      segments[seg_addr.get_segment_id().device_id()]->device_id);
    assert(seg_addr.get_segment_id().device_segment_id() <
      segments[seg_addr.get_segment_id().device_id()]->num_segments);

    if (!init_scan && !init_complete)
      return;

    stats.used_bytes += len;
    [[maybe_unused]] auto ret = space_tracker->allocate(
      seg_addr.get_segment_id(),
      seg_addr.get_segment_off(),
      len);
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
    assert(addr.get_device_id() ==
      segments[seg_addr.get_segment_id().device_id()]->device_id);
    assert(seg_addr.get_segment_id().device_segment_id() <
      segments[seg_addr.get_segment_id().device_id()]->num_segments);

    [[maybe_unused]] auto ret = space_tracker->release(
      seg_addr.get_segment_id(),
      seg_addr.get_segment_off(),
      len);
    maybe_wake_gc_blocked_io();
    assert(ret >= 0);
  }

  journal_seq_t get_next_gc_target() const {
    segment_id_t id = NULL_SEG_ID;
    segment_seq_t seq = NULL_SEG_SEQ;
    int64_t least_live_bytes = std::numeric_limits<int64_t>::max();
    for (auto it = segments.begin();
	 it != segments.end();
	 ++it) {
      auto _id = it->first;
      const auto& segment_info = it->second;
      if (segment_info.is_closed() &&
	  !segment_info.is_in_journal(journal_tail_committed) &&
	  space_tracker->get_usage(_id) < least_live_bytes) {
	id = _id;
	seq = segment_info.journal_segment_seq;
	least_live_bytes = space_tracker->get_usage(id);
      }
    }
    if (id != NULL_SEG_ID) {
      crimson::get_logger(ceph_subsys_seastore_cleaner).debug(
	"SegmentCleaner::get_next_gc_target: segment {} seq {}",
	id,
	seq);
      return journal_seq_t{seq, paddr_t::make_seg_paddr(id, 0)};
    } else {
      return journal_seq_t();
    }
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
    ExtentReader::scan_extents_cursor
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
    ExtentReader::scan_extents_ertr
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
    auto& seg_addr = journal_head.offset.as_seg_paddr();
    auto segment_size =
      segments[seg_addr.get_segment_id().device_id()]->segment_size;
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
    if (journal_head == journal_seq_t()) {
      // this for calculating journal bytes in the journal
      // replay phase in which journal_head is not set
      return segments.get_journal_segments() * segments[journal_device_id]->segment_size;
    } else {
      assert(journal_head >= journal_tail_committed);
      auto& seg_addr = journal_head.offset.as_seg_paddr();
      auto segment_size =
	segments[seg_addr.get_segment_id().device_id()]->segment_size;
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
    assert(segment.device_id() ==
      segments[segment.device_id()]->device_id);
    assert(segment.device_segment_id() <
      segments[segment.device_id()]->num_segments);
    if (init_complete) {
      assert(segments[segment].is_open());
    } else {
      assert(segments[segment].is_empty());
      assert(segments.get_empty_segments(segment.device_id()) > 0);
      assert(stats.empty_segments > 0);
      stats.empty_segments--;
    }
    segments.segment_closed(segment);
    crimson::get_logger(ceph_subsys_seastore_cleaner).info(
      "mark closed: {} empty_segments: {}"
      ", opened_segments {}, should_block_on_gc {}"
      ", projected_avail_ratio {}, projected_reclaim_ratio {}",
      segment,
      segments.get_empty_segments(segment.device_id()),
      segments.get_opened_segments(),
      should_block_on_gc(),
      get_projected_available_ratio(),
      get_projected_reclaim_ratio());
  }

  void mark_empty(segment_id_t segment) {
    auto& segment_info = segments[segment];
    assert(segment.device_id() ==
      segments[segment.device_id()]->device_id);
    assert(segment.device_segment_id() <
      segments[segment.device_id()]->num_segments);
    assert(segment_info.is_closed());
    segments.segment_emptied(segment);
    if (space_tracker->get_usage(segment) != 0) {
      space_tracker->dump_usage(segment);
      assert(space_tracker->get_usage(segment) == 0);
    }
    segment_info.set_empty();
    stats.empty_segments++;
    crimson::get_logger(ceph_subsys_seastore_cleaner
      ).info("mark empty: {}, empty_segments {}"
	", opened_segments {}, should_block_on_gc {}"
	", projected_avail_ratio {}, projected_reclaim_ratio {}",
	segment,
	stats.empty_segments,
	segments.get_opened_segments(),
	should_block_on_gc(),
	get_projected_available_ratio(),
	get_projected_reclaim_ratio());
    if (!segment_info.out_of_line) {
      segments.journal_segment_emptied();
    }
    maybe_wake_gc_blocked_io();
  }

  void mark_open(segment_id_t segment) {
    assert(segment.device_id() ==
      segments[segment.device_id()]->device_id);
    assert(segment.device_segment_id() <
      segments[segment.device_id()]->num_segments);
    assert(segments[segment].is_empty());
    assert(segments.get_empty_segments(segment.device_id()) > 0);
    segments.segment_opened(segment);
    segments[segment].set_open();
    assert(stats.empty_segments > 0);
    stats.empty_segments--;
    crimson::get_logger(ceph_subsys_seastore_cleaner
      ).info("mark open: {}, empty_segments {}"
	", opened_segments {}, should_block_on_gc {}"
	", projected_avail_ratio {}, projected_reclaim_ratio {}",
	segment,
	stats.empty_segments,
	segments.get_opened_segments(),
	should_block_on_gc(),
	get_projected_available_ratio(),
	get_projected_reclaim_ratio());
  }
};
using SegmentCleanerRef = std::unique_ptr<SegmentCleaner>;

}
