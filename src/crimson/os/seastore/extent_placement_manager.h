// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>

#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/journal/segment_allocator.h"
#include "crimson/os/seastore/journal/record_submitter.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/extent_pinboard.h"
#include "crimson/os/seastore/logical_bucket.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/block_rb_manager.h"
#include "crimson/os/seastore/randomblock_manager_group.h"

class transaction_manager_test_t;

namespace crimson::os::seastore {

class Cache;

class TokenBucket {
public:
  TokenBucket(uint64_t mt) :
    tokens(mt), max_tokens(mt), timer(), sp(std::nullopt) {}

  void start() {
    if (max_tokens != 0) {
      tokens = max_tokens;
      timer.set_callback([this] {
        tokens += max_tokens / 10;
        if (tokens > max_tokens) {
          tokens = max_tokens;
        }
        do_wake();
      });
      if (!timer.armed()) {
        timer.arm_periodic(std::chrono::milliseconds(100));
      }
    }
  }

  void stop() {
    if (max_tokens != 0) {
      timer.cancel();
      tokens = std::numeric_limits<uint64_t>::max();
      do_wake();
    }
  }

  seastar::future<> get(uint64_t size) {
    if (max_tokens == 0) {
      return seastar::now();
    }
    return seastar::repeat([this, size] {
      if (tokens < size) {
        if (!sp) {
          sp.emplace(seastar::shared_promise<>());
        }
        return sp->get_shared_future().then([] {
          return seastar::stop_iteration::no;
        });
      } else {
        tokens -= size;
        return seastar::make_ready_future<
          seastar::stop_iteration>(seastar::stop_iteration::yes);
      }
    });
  }

private:
  void do_wake() {
    if (sp) {
      sp->set_value();
      sp.reset();
    }
  }
  uint64_t tokens;
  const uint64_t max_tokens;
  seastar::timer<seastar::steady_clock_type> timer;
  std::optional<seastar::shared_promise<>> sp;
};

using TokenBucketRef = std::unique_ptr<TokenBucket>;

/**
 * ExtentOolWriter
 *
 * Write the extents as out-of-line and allocate the physical addresses.
 * Different writers write extents to different locations.
 */
class ExtentOolWriter {
public:
  virtual ~ExtentOolWriter() {}

  virtual backend_type_t get_type() const = 0;

  virtual writer_stats_t get_stats() const = 0;

  using open_ertr = base_ertr;
  virtual open_ertr::future<> open() = 0;

  virtual paddr_t alloc_paddr(extent_len_t length) = 0;

  virtual std::list<alloc_paddr_result> alloc_paddrs(
    extent_len_t length,
    paddr_t hint) = 0;

  using alloc_write_ertr = base_ertr;
  using alloc_write_iertr = trans_iertr<alloc_write_ertr>;
  virtual alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<CachedExtentRef> &extents) = 0;

  using close_ertr = base_ertr;
  virtual close_ertr::future<> close() = 0;

  virtual bool can_inplace_rewrite(Transaction& t,
    CachedExtentRef extent) = 0;

#ifdef UNIT_TESTS_BUILT
  virtual void prefill_fragmented_devices() {}
#endif
};
using ExtentOolWriterRef = std::unique_ptr<ExtentOolWriter>;

/**
 * SegmentedOolWriter
 *
 * Different writers write extents to different out-of-line segments provided
 * by the SegmentProvider.
 */
class SegmentedOolWriter : public ExtentOolWriter {
public:
  SegmentedOolWriter(data_category_t category,
                     rewrite_gen_t gen,
                     SegmentProvider &sp,
                     SegmentSeqAllocator &ssa,
                     TokenBucket &buckets);

  backend_type_t get_type() const final {
    return backend_type_t::SEGMENTED;
  }

  writer_stats_t get_stats() const final {
    return record_submitter.get_stats();
  }

  open_ertr::future<> open() final {
    return record_submitter.open(false).discard_result();
  }

  alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<CachedExtentRef> &extents) final;

  close_ertr::future<> close() final {
    return write_guard.close().then([this] {
      return record_submitter.close();
    }).safe_then([this] {
      write_guard = seastar::gate();
    });
  }

  paddr_t alloc_paddr(extent_len_t length) final {
    return make_delayed_temp_paddr(0);
  }

  std::list<alloc_paddr_result> alloc_paddrs(extent_len_t length, paddr_t) final {
    return {alloc_paddr_result{make_delayed_temp_paddr(0), length}};
  }

  bool can_inplace_rewrite(Transaction& t,
    CachedExtentRef extent) final {
    return false;
  }

private:
  alloc_write_iertr::future<> do_write(
    Transaction& t,
    std::list<CachedExtentRef> &extent);

  alloc_write_ertr::future<> write_record(
    Transaction& t,
    record_t&& record,
    std::list<LogicalCachedExtentRef> &&extents,
    bool with_atomic_roll_segment=false);

  journal::SegmentAllocator segment_allocator;
  journal::RecordSubmitter record_submitter;
  seastar::gate write_guard;
  TokenBucket &token_bucket;
};


class RandomBlockOolWriter : public ExtentOolWriter {
public:
  RandomBlockOolWriter(RBMCleaner* rb_cleaner, TokenBucket &bucket) :
    rb_cleaner(rb_cleaner), token_bucket(bucket) {}

  backend_type_t get_type() const final {
    return backend_type_t::RANDOM_BLOCK;
  }

  writer_stats_t get_stats() const final {
    writer_stats_t ret = w_stats;
    ret.minus(last_w_stats);
    last_w_stats = w_stats;
    return ret;
  }

  using open_ertr = ExtentOolWriter::open_ertr;
  open_ertr::future<> open() final {
    w_stats = {};
    last_w_stats = {};
    return open_ertr::now();
  }

  alloc_write_iertr::future<> alloc_write_ool_extents(
    Transaction &t,
    std::list<CachedExtentRef> &extents) final;

  close_ertr::future<> close() final {
    return write_guard.close().then([this] {
      write_guard = seastar::gate();
      return close_ertr::now();
    });
  }

  paddr_t alloc_paddr(extent_len_t length) final {
    assert(rb_cleaner);
    return rb_cleaner->alloc_paddr(length);
  }

  std::list<alloc_paddr_result> alloc_paddrs(
    extent_len_t length, paddr_t hint) final {
    assert(rb_cleaner);
    return rb_cleaner->alloc_paddrs(length, hint);
  }

  bool can_inplace_rewrite(Transaction& t,
    CachedExtentRef extent) final {
    if (!extent->is_stable_dirty()) {
      return false;
    }
    assert(t.get_src() == transaction_type_t::TRIM_DIRTY);
    ceph_assert_always(is_root_type(extent->get_type()) ||
	extent->get_paddr().is_absolute());
    return crimson::os::seastore::can_inplace_rewrite(extent->get_type());
  }

#ifdef UNIT_TESTS_BUILT
  void prefill_fragmented_devices() final {
    LOG_PREFIX(RandomBlockOolWriter::prefill_fragmented_devices);
    SUBDEBUG(seastore_epm, "");
    return rb_cleaner->prefill_fragmented_devices();
  }
#endif
private:
  struct write_info_t {
    paddr_t offset;
    ceph::bufferptr bp;
    RandomBlockManager* rbm;
    std::list<ceph::bufferptr> mergeable_bps;
  };
  alloc_write_iertr::future<> do_write(
    Transaction& t,
    std::list<CachedExtentRef> &extent);

  RBMCleaner* rb_cleaner;
  seastar::gate write_guard;
  writer_stats_t w_stats;
  mutable writer_stats_t last_w_stats;
  TokenBucket &token_bucket;
};

struct cleaner_usage_t {
  // The size of all extents write to the main devices, including inline extents
  // and out-of-line extents.
  std::size_t main_usage = 0;
  // The size of extents write to the cold devices
  std::size_t cold_ool_usage = 0;
};

struct reserve_cleaner_result_t {
  bool reserve_main_success = true;
  bool reserve_cold_success = true;

  bool is_successful() const {
    return reserve_main_success &&
      reserve_cold_success;
  }
};

/**
 * io_usage_t
 *
 * io_usage_t describes the space usage consumed by client IO.
 */
struct io_usage_t {
  // The total size of all inlined extents, not including deltas and other metadata
  // produced by Cache::prepare_record.
  std::size_t inline_usage = 0;
  cleaner_usage_t cleaner_usage;
  friend std::ostream &operator<<(std::ostream &out, const io_usage_t &usage) {
    return out << "io_usage_t("
               << "inline_usage=0x" << std::hex << usage.inline_usage
               << ", main_cleaner_usage=0x" << usage.cleaner_usage.main_usage
               << ", cold_cleaner_usage=0x" << usage.cleaner_usage.cold_ool_usage << std::dec
               << ")";
  }
};

struct reserve_io_result_t {
  bool reserve_inline_success = true;
  reserve_cleaner_result_t cleaner_result;

  bool is_successful() const {
    return reserve_inline_success &&
      cleaner_result.is_successful();
  }
};

class ExtentPlacementManager {
public:
  ExtentPlacementManager(
    rewrite_gen_t hot_tier_generations,
    rewrite_gen_t cold_tier_generations)
    : hot_tier_generations(hot_tier_generations),
      cold_tier_generations(cold_tier_generations),
      dynamic_max_rewrite_generation(cold_tier_generations),
      ool_segment_seq_allocator(
          std::make_unique<SegmentSeqAllocator>(segment_type_t::OOL)),
      max_data_allocation_size(crimson::common::get_conf<Option::size_t>(
	  "seastore_max_data_allocation_size")),
      write_through_size(crimson::common::get_conf<Option::size_t>(
	  "seastore_write_through_size"))
  {
    LOG_PREFIX(ExtentPlacementManager::ExtentPlacementManager);
    devices_by_id.resize(DEVICE_ID_MAX, nullptr);
    SUBINFO(seastore_epm, "cold_tier_generations={}, hot_tier_generations={}",
      cold_tier_generations, hot_tier_generations);
  }

  void init(JournalTrimmerImplRef &&, AsyncCleanerRef &&, AsyncCleanerRef &&,
            ExtentPinboard *pinboard);

  SegmentSeqAllocator &get_ool_segment_seq_allocator() const {
    return *ool_segment_seq_allocator;
  }

  void set_primary_device(Device *device);

  void set_extent_callback(ExtentCallbackInterface *cb) {
    background_process.set_extent_callback(cb);
  }

  bool can_inplace_rewrite(Transaction& t, CachedExtentRef extent) {
    auto writer = get_writer(placement_hint_t::REWRITE,
      get_extent_category(extent->get_type()),
      OOL_GENERATION);
    return writer->can_inplace_rewrite(t, extent);
  }

  backend_type_t get_backend_type() const {
    return background_process.get_backend_type();
  }

  extent_len_t get_block_size() const {
    assert(primary_device != nullptr);
    // assume all the devices have the same block size
    return primary_device->get_block_size();
  }

  Device& get_primary_device() {
    assert(primary_device != nullptr);
    return *primary_device;
  }

  store_statfs_t get_stat() const {
    return background_process.get_stat();
  }

  device_stats_t get_device_stats(
    const writer_stats_t &journal_stats,
    bool report_detail,
    double seconds) const;

  LogicalBucket *get_logical_bucket() {
    return background_process.get_logical_bucket();
  }

  using mount_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
  using mount_ret = mount_ertr::future<>;
  mount_ret mount() {
    return background_process.mount();
  }

  using open_ertr = ExtentOolWriter::open_ertr;
  open_ertr::future<> open_for_write();

  void start_scan_space() {
    return background_process.start_scan_space();
  }

  void start_background() {
    return background_process.start_background();
  }

  struct alloc_option_t {
    placement_hint_t hint;
    rewrite_gen_t gen;
    bool is_tracked;
    paddr_t paddr_hint = P_ADDR_NULL;
    write_policy_t write_policy = write_policy_t::WRITE_BACK;
#ifdef UNIT_TESTS_BUILT
    std::optional<paddr_t> external_paddr = std::nullopt;
#endif
  };
  struct alloc_result_t {
    paddr_t paddr;
    bufferptr bp;
    rewrite_gen_t gen;
  };
  std::optional<alloc_result_t> alloc_new_non_data_extent(
    Transaction& t,
    extent_types_t type,
    extent_len_t length,
    alloc_option_t opt
  ) {
    assert(opt.hint < placement_hint_t::NUM_HINTS);
    assert(is_target_rewrite_generation(opt.gen, dynamic_max_rewrite_generation));
    assert(opt.gen == INIT_GENERATION || opt.hint == placement_hint_t::REWRITE);

    data_category_t category = get_extent_category(type);
    opt.gen = adjust_generation(
      category, type, opt.hint, opt.gen, opt.write_policy, opt.is_tracked);

    paddr_t addr;
#ifdef UNIT_TESTS_BUILT
    if (unlikely(opt.external_paddr.has_value())) {
      assert(opt.external_paddr->is_fake());
      addr = *opt.external_paddr;
    } else
#endif
      if (opt.gen == INLINE_GENERATION) {
      addr = make_record_relative_paddr(0);
    } else {
      assert(category == data_category_t::METADATA);
      addr = get_writer(opt.hint, category, opt.gen)->alloc_paddr(length);
    }
    assert(!(category == data_category_t::DATA));

    if (addr.is_null()) {
      return std::nullopt;
    }

    // XXX: bp might be extended to point to different memory (e.g. PMem)
    // according to the allocator.
    auto bp = create_extent_ptr_zero(length);

    return alloc_result_t{addr, std::move(bp), opt.gen};
  }

  std::list<alloc_result_t> alloc_new_data_extents(
    Transaction& t,
    extent_types_t type,
    extent_len_t length,
    alloc_option_t opt
  ) {
    LOG_PREFIX(ExtentPlacementManager::alloc_new_data_extents);
    assert(opt.hint < placement_hint_t::NUM_HINTS);
    assert(is_target_rewrite_generation(opt.gen, dynamic_max_rewrite_generation));
    assert(opt.gen == INIT_GENERATION || opt.hint == placement_hint_t::REWRITE);

    data_category_t category = get_extent_category(type);
    opt.gen = adjust_generation(
      category, type, opt.hint, opt.gen, opt.write_policy, opt.is_tracked);
    assert(opt.gen != INLINE_GENERATION);

    // XXX: bp might be extended to point to different memory (e.g. PMem)
    // according to the allocator.
    std::list<alloc_result_t> allocs;
#ifdef UNIT_TESTS_BUILT
    if (unlikely(opt.external_paddr.has_value())) {
      assert(opt.external_paddr->is_fake());
      auto bp = create_extent_ptr_zero(length);
      allocs.emplace_back(alloc_result_t{*opt.external_paddr, std::move(bp), opt.gen});
    } else
#endif
    {
      assert(category == data_category_t::DATA);
      auto addrs = get_writer(opt.hint, category, opt.gen)->alloc_paddrs(
        length, opt.paddr_hint);
      for (auto &ext : addrs) {
        auto left = ext.len;
        while (left > 0) {
          auto len = left;
          if (max_data_allocation_size) {
            len = std::min(max_data_allocation_size, len);
          }
          auto bp = create_extent_ptr_zero(len);
          auto start = ext.start.is_delayed()
                        ? ext.start
                        : ext.start + (ext.len - left);
          allocs.emplace_back(alloc_result_t{start, std::move(bp), opt.gen});
          SUBDEBUGT(seastore_epm,
                    "allocated {} 0x{:x}B extent at {}, opt.hint={}, opt.gen={}",
                    t, type, len, start, opt.hint, opt.gen);
          left -= len;
        }
      }
    }
    return allocs;
  }

  write_policy_t get_write_policy(extent_types_t type, extent_len_t length) const {
    if (has_cold_tier() &&  is_data_type(type)) {
      if (length >= write_through_size
#ifdef CRIMSON_TEST_WORKLOAD
          || (crimson::common::get_conf<bool>("crimson_test_workload")
              && (double(std::rand() % 100) / 100.0) <= crimson::common::get_conf<double>(
                "seastore_test_workload_write_through_probability"))
#endif
          ) {
        return write_policy_t::WRITE_THROUGH;
      }
    }
    return write_policy_t::WRITE_BACK;
  }

#ifdef UNIT_TESTS_BUILT
  void prefill_fragmented_devices() {
    LOG_PREFIX(ExtentPlacementManager::prefill_fragmented_devices);
    SUBDEBUG(seastore_epm, "");
    for (auto &writer : writer_refs) {
      writer->prefill_fragmented_devices();
    }
  }

  void set_max_extent_size(extent_len_t len) {
    max_data_allocation_size = len;
  }

  extent_len_t get_max_extent_size() const {
    return max_data_allocation_size;
  }
#endif

  /**
   * dispatch_result_t
   *
   * ool extents are placed in alloc_map and passed to
   * EPM::write_delayed_ool_extents,
   * delayed_extents is used to update lba mapping.
   * usage is used to reserve projected space
   */
  using extents_by_writer_t =
    std::map<ExtentOolWriter*, std::list<CachedExtentRef>>;
  struct dispatch_result_t {
    extents_by_writer_t alloc_map;
    std::list<CachedExtentRef> delayed_extents;
    io_usage_t usage;
  };

  /**
   * dispatch_delayed_extents
   *
   * Performs delayed allocation
   */
  dispatch_result_t dispatch_delayed_extents(Transaction& t);

  /**
   * write_delayed_ool_extents
   *
   * Do writes for out-of-line extents.
   */
  using alloc_paddr_iertr = ExtentOolWriter::alloc_write_iertr;
  alloc_paddr_iertr::future<> write_delayed_ool_extents(
    Transaction& t,
    extents_by_writer_t& alloc_map);

  /**
   * write_preallocated_ool_extents
   *
   * Performs ool writes for extents with pre-allocated addresses.
   * See Transaction::pre_alloc_list
   */
  alloc_paddr_iertr::future<> write_preallocated_ool_extents(
    Transaction &t,
    std::list<CachedExtentRef> extents);

  seastar::future<> stop_background() {
    return background_process.stop_background();
  }

  using close_ertr = ExtentOolWriter::close_ertr;
  close_ertr::future<> close();

  using read_ertr = Device::read_ertr;
  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out
  ) {
    assert(devices_by_id[addr.get_device_id()] != nullptr);
    return devices_by_id[addr.get_device_id()]->read(addr, len, out);
  }

  void mark_space_used(paddr_t addr, extent_len_t len) {
    background_process.mark_space_used(addr, len);
  }

  void mark_space_free(paddr_t addr, extent_len_t len) {
    background_process.mark_space_free(addr, len);
  }

  void commit_space_used(paddr_t addr, extent_len_t len) {
    return background_process.commit_space_used(addr, len);
  }

  seastar::future<> reserve_projected_usage(io_usage_t usage) {
    return background_process.reserve_projected_usage(usage);
  }

  void release_projected_usage(const io_usage_t &usage) {
    background_process.release_projected_usage(usage);
  }

  backend_type_t get_main_backend_type() const {
    if (!background_process.is_no_background()) {
      return background_process.get_main_backend_type();
    } 
    // for test
    assert(primary_device);
    return primary_device->get_backend_type();
  }

  bool has_cold_tier() const {
    return background_process.has_cold_tier();
  }

  bool is_cold_device(device_id_t id) const {
    return background_process.is_cold_device(id);
  }

  // Testing interfaces

  void test_init_no_background(Device *test_device) {
    assert(test_device->get_backend_type() == backend_type_t::SEGMENTED);
    add_device(test_device);
    set_primary_device(test_device);
  }

  bool check_usage() {
    return background_process.check_usage();
  }

  seastar::future<> run_background_work_until_halt() {
    return background_process.run_until_halt();
  }

  bool get_checksum_needed(paddr_t addr) {
    // checksum offloading only for blocks physically stored in the device
#ifdef UNIT_TESTS_BUILT
    if (addr.is_fake()) {
      return true;
    }
#endif
    assert(addr.is_absolute());
    return !devices_by_id[addr.get_device_id()]->is_end_to_end_data_protection();
  }

  rewrite_gen_t get_max_hot_gen() const {
    return hot_tier_generations - 1;
  }

#ifdef CRIMSON_TEST_WORKLOAD
  device_id_t get_cold_device_id() const {
    return background_process.get_cold_device_id();
  }
#endif

private:
  rewrite_gen_t adjust_generation(
      data_category_t category,
      extent_types_t type,
      placement_hint_t hint,
      rewrite_gen_t gen,
      write_policy_t policy,
      bool is_tracked) {
    assert(is_real_type(type));
    if (is_root_type(type)) {
      gen = INLINE_GENERATION;
    } else if (get_main_backend_type() == backend_type_t::SEGMENTED &&
               is_lba_backref_node(type)) {
      gen = INLINE_GENERATION;
    } else if (hint == placement_hint_t::COLD) {
      assert(gen == INIT_GENERATION);
      if (background_process.has_cold_tier()) {
        gen = hot_tier_generations;
      } else {
        gen = MIN_REWRITE_GENERATION;
      }
    } else if (gen == INIT_GENERATION) {
      if (category == data_category_t::METADATA) {
        if (get_main_backend_type() == backend_type_t::SEGMENTED) {
          // with SEGMENTED, default not to ool metadata extents to reduce
          // padding overhead.
          // TODO: improve padding so we can default to the ool path.
          gen = INLINE_GENERATION;
        } else {
          // with RBM, all extents must be OOL
          assert(get_main_backend_type() ==
                 backend_type_t::RANDOM_BLOCK);
          gen = OOL_GENERATION;
        }
      } else {
        assert(category == data_category_t::DATA);
        if (background_process.has_cold_tier() &&
            policy == write_policy_t::WRITE_THROUGH) {
          gen = hot_tier_generations;
        } else {
          assert(policy != write_policy_t::WRITE_THROUGH);
          gen = OOL_GENERATION;
        }
      }
    } else if (background_process.has_cold_tier()) {
      gen = background_process.adjust_generation(gen);
      if (gen <= hot_tier_generations &&
          policy == write_policy_t::WRITE_THROUGH) {
        gen = hot_tier_generations;
      }
    }

    if (is_tracked && gen >= hot_tier_generations &&
        hint != placement_hint_t::REWRITE) {
      gen = hot_tier_generations - 1;
    }

    if (gen > dynamic_max_rewrite_generation) {
      gen = dynamic_max_rewrite_generation;
    }

    return gen;
  }

  void add_device(Device *device) {
    auto device_id = device->get_device_id();
    ceph_assert(devices_by_id[device_id] == nullptr);
    devices_by_id[device_id] = device;
    ++num_devices;
  }

  /**
   * dispatch_delayed_extent
   *
   * Specify the extent inline or ool
   * return true indicates inline otherwise ool
   */
  bool dispatch_delayed_extent(CachedExtentRef& extent) {
    // TODO: all delayed extents are ool currently
    boost::ignore_unused(extent);
    return false;
  }

  ExtentOolWriter* get_writer(placement_hint_t hint,
                              data_category_t category,
                              rewrite_gen_t gen) {
    assert(hint < placement_hint_t::NUM_HINTS);
    // TODO: might worth considering the hint
    return get_writer(category, gen);
  }

  ExtentOolWriter* get_writer(data_category_t category,
                              rewrite_gen_t gen) {
    assert(is_rewrite_generation(gen, dynamic_max_rewrite_generation));
    assert(gen != INLINE_GENERATION);
    assert(gen <= dynamic_max_rewrite_generation);
    ExtentOolWriter* ret = nullptr;
    if (category == data_category_t::DATA) {
      ret = data_writers_by_gen[generation_to_writer(gen)];
    } else {
      assert(category == data_category_t::METADATA);
      ret = md_writers_by_gen[generation_to_writer(gen)];
    }
    assert(ret != nullptr);
    return ret;
  }

  const ExtentOolWriter* get_writer(data_category_t category,
                                    rewrite_gen_t gen) const {
    assert(is_rewrite_generation(gen, dynamic_max_rewrite_generation));
    assert(gen != INLINE_GENERATION);
    assert(gen <= dynamic_max_rewrite_generation);
    ExtentOolWriter* ret = nullptr;
    if (category == data_category_t::DATA) {
      ret = data_writers_by_gen[generation_to_writer(gen)];
    } else {
      assert(category == data_category_t::METADATA);
      ret = md_writers_by_gen[generation_to_writer(gen)];
    }
    assert(ret != nullptr);
    return ret;
  }

  /**
   * BackgroundProcess
   *
   * Background process to schedule background transactions.
   *
   * TODO: device tiering
   */
  class BackgroundProcess : public BackgroundListener {
  public:
    BackgroundProcess() = default;

    void init(JournalTrimmerImplRef &&_trimmer,
              AsyncCleanerRef &&_cleaner,
              AsyncCleanerRef &&_cold_cleaner,
              rewrite_gen_t hot_tier_generations,
              ExtentPinboard *_pinboard) {
      trimmer = std::move(_trimmer);
      trimmer->set_background_callback(this);
      main_cleaner = std::move(_cleaner);
      main_cleaner->set_background_callback(this);
      if (_cold_cleaner) {
        cold_cleaner = std::move(_cold_cleaner);
        cold_cleaner->set_background_callback(this);

        cleaners_by_device_id.resize(DEVICE_ID_MAX, nullptr);
        for (auto id : main_cleaner->get_device_ids()) {
          cleaners_by_device_id[id] = main_cleaner.get();
        }
        for (auto id : cold_cleaner->get_device_ids()) {
          cleaners_by_device_id[id] = cold_cleaner.get();
        }

        using crimson::common::get_conf;
        eviction_state.init(
          crimson::common::get_conf<double>(
            "seastore_multiple_tiers_stop_evict_ratio"),
          crimson::common::get_conf<double>(
            "seastore_multiple_tiers_default_evict_ratio"),
          crimson::common::get_conf<double>(
            "seastore_multiple_tiers_fast_evict_ratio"),
          hot_tier_generations);

        pinboard = _pinboard;
        ceph_assert(pinboard != nullptr);
        pinboard->set_background_callback(this);

        logical_bucket = create_logical_bucket(
          get_conf<Option::size_t>("seastore_logical_bucket_capacity"),
          get_conf<Option::size_t>("seastore_logical_bucket_proceed_size_per_cycle"));
      }
#ifdef CRIMSON_TEST_WORKLOAD
      LOG_PREFIX(BackgroundProcess::init);
      test_workload = crimson::common::get_conf<bool>("crimson_test_workload");
      force_process_half_life = crimson::common::get_conf<uint64_t>(
        "seastore_test_workload_force_prcess_background_tasks_period");
      force_background_timer.set_callback([this] { wake_half_life(); });
      SUBINFO(seastore_epm, "crimson test workload supported, enabled: {}", test_workload);
      if (test_workload) {
        set_next_arm_timepoint();
      }
#endif
    }

    LogicalBucket *get_logical_bucket() {
      return logical_bucket.get();
    }

    backend_type_t get_backend_type() const {
      return trimmer->get_backend_type();
    }

    bool has_cold_tier() const {
      return cold_cleaner.get() != nullptr;
    }

    bool is_cold_device(device_id_t id) const {
      if (!has_cold_tier()) {
        return false;
      }
      assert(cleaners_by_device_id[id]);
      return cleaners_by_device_id[id] != main_cleaner.get();
    }

    void set_extent_callback(ExtentCallbackInterface *cb) {
      trimmer->set_extent_callback(cb);
      main_cleaner->set_extent_callback(cb);
      if (has_cold_tier()) {
        cold_cleaner->set_extent_callback(cb);
        pinboard->set_extent_callback(cb);
        logical_bucket->set_extent_callback(cb);
      }
    }

    store_statfs_t get_stat() const {
      auto stat = main_cleaner->get_stat();
      if (has_cold_tier()) {
        stat.add(cold_cleaner->get_stat());
      }
      return stat;
    }

    using mount_ret = ExtentPlacementManager::mount_ret;
    mount_ret mount() {
      ceph_assert(state == state_t::STOP);
      state = state_t::MOUNT;
      trimmer->reset();
      stats = {};
      register_metrics();
      return main_cleaner->mount(
      ).safe_then([this] {
        return has_cold_tier() ? cold_cleaner->mount() : mount_ertr::now();
      });
    }

    void start_scan_space() {
      ceph_assert(state == state_t::MOUNT);
      state = state_t::SCAN_SPACE;
      ceph_assert(main_cleaner->check_usage_is_empty());
      ceph_assert(!has_cold_tier() ||
                  cold_cleaner->check_usage_is_empty());
    }

    void start_background();

    void mark_space_used(paddr_t addr, extent_len_t len) {
      if (state < state_t::SCAN_SPACE) {
        return;
      }

      if (!has_cold_tier()) {
        assert(main_cleaner);
        main_cleaner->mark_space_used(addr, len);
      } else {
        auto id = addr.get_device_id();
        assert(id < cleaners_by_device_id.size());
        auto cleaner = cleaners_by_device_id[id];
        assert(cleaner);
        cleaner->mark_space_used(addr, len);
      }
    }

    void mark_space_free(paddr_t addr, extent_len_t len) {
      if (state < state_t::SCAN_SPACE) {
        return;
      }

      if (!has_cold_tier()) {
        assert(main_cleaner);
        main_cleaner->mark_space_free(addr, len);
      } else {
        auto id = addr.get_device_id();
        assert(id < cleaners_by_device_id.size());
        auto cleaner = cleaners_by_device_id[id];
        assert(cleaner);
        cleaner->mark_space_free(addr, len);
      }
    }

    void commit_space_used(paddr_t addr, extent_len_t len) {
      if (state < state_t::SCAN_SPACE) {
        return;
      }

      if (!has_cold_tier()) {
        assert(main_cleaner);
        main_cleaner->commit_space_used(addr, len);
      } else {
        auto id = addr.get_device_id();
        assert(id < cleaners_by_device_id.size());
        auto cleaner = cleaners_by_device_id[id];
        assert(cleaner);
        cleaner->commit_space_used(addr, len);
      }
    }

    rewrite_gen_t adjust_generation(rewrite_gen_t gen) {
      if (has_cold_tier()) {
        return eviction_state.adjust_generation_with_eviction(gen);
      } else {
        return gen;
      }
    }


#ifdef CRIMSON_TEST_WORKLOAD
    device_id_t get_cold_device_id() const {
      assert(has_cold_tier());
      return *cold_cleaner->get_device_ids().begin();
    }
#endif

    seastar::future<> reserve_projected_usage(io_usage_t usage);

    void release_projected_usage(const io_usage_t &usage) {
      if (is_ready()) {
        trimmer->release_inline_usage(usage.inline_usage);
        main_cleaner->release_projected_usage(usage.cleaner_usage.main_usage);
        if (has_cold_tier()) {
          cold_cleaner->release_projected_usage(usage.cleaner_usage.cold_ool_usage);
        }
      }
    }

    seastar::future<> stop_background();
    backend_type_t get_main_backend_type() const {
      return get_backend_type();
    }

    // Testing interfaces

    bool check_usage() {
      return main_cleaner->check_usage() &&
        (!has_cold_tier() || cold_cleaner->check_usage());
    }

    seastar::future<> run_until_halt();
    
    bool is_no_background() const {
      return !trimmer || !main_cleaner;
    }

  protected:
    state_t get_state() const final {
      return state;
    }

    void maybe_wake_background() final {
      if (!is_running()) {
        return;
      }
      if (background_should_run()) {
        do_wake_background();
      }
    }

    void maybe_wake_blocked_io() final;

    void maybe_wake_promote() final {
      if (!is_ready()) {
        return;
      }
      if (pinboard && pinboard->should_promote()) {
        do_wake_promote();
      }
    }

  private:
    // reserve helpers
    bool try_reserve_cold(std::size_t usage);
    void abort_cold_usage(std::size_t usage, bool success);

    reserve_cleaner_result_t try_reserve_cleaner(const cleaner_usage_t &usage);
    void abort_cleaner_usage(const cleaner_usage_t &usage,
                             const reserve_cleaner_result_t &result);

    reserve_io_result_t try_reserve_io(const io_usage_t &usage);
    void abort_io_usage(const io_usage_t &usage,
                        const reserve_io_result_t &result);

    bool is_running() const {
      if (state == state_t::RUNNING) {
        assert(process_join);
        return true;
      } else {
        assert(!process_join);
        return false;
      }
    }

    void log_state(const char *caller) const;

    seastar::future<> run();

    void do_wake_background() {
      if (blocking_background) {
	blocking_background->set_value();
	blocking_background = std::nullopt;
      }
    }

    void do_wake_promote() {
      if (blocking_promote) {
        blocking_promote->set_value();
        blocking_promote = std::nullopt;
      }
    }

    // background_should_run() should be atomic with do_background_cycle()
    // to make sure the condition is consistent.
    bool background_should_run() {
      assert(is_ready());
      maybe_update_eviction_mode();
      return main_cleaner_should_run()
        || cold_cleaner_should_run()
        || trimmer->should_trim();
    }

    bool main_cleaner_should_fast_evict() const {
      return has_cold_tier() &&
          (main_cleaner->can_clean_space() ||
           (logical_bucket && logical_bucket->could_demote())) &&
         eviction_state.is_fast_mode();
    }

    bool main_cleaner_should_run() const {
      assert(is_ready());
      return main_cleaner->should_clean_space() ||
        main_cleaner_should_fast_evict();
    }

    bool cold_cleaner_should_run() const {
      assert(is_ready());
      return has_cold_tier() &&
        cold_cleaner->should_clean_space();
    }

    bool should_block_io() const {
      assert(is_ready());
      return trimmer->should_block_io_on_trim() ||
             main_cleaner->should_block_io_on_clean() ||
             (has_cold_tier() &&
              cold_cleaner->should_block_io_on_clean());
    }

    void maybe_update_eviction_mode() {
      if (has_cold_tier()) {
        auto main_alive_ratio = main_cleaner->get_stat().get_used_raw_ratio();
        eviction_state.maybe_update_eviction_mode(main_alive_ratio);
      }
    }

    struct eviction_state_t {
      enum class eviction_mode_t {
        STOP,     // generation greater than or equal to MIN_COLD_GENERATION
                  // will be set to MIN_COLD_GENERATION - 1, which means
                  // no extents will be evicted.
        DEFAULT,  // generation incremented with each rewrite. Extents will
                  // be evicted when generation reaches MIN_COLD_GENERATION.
        FAST,     // map all generations located in
                  // [MIN_REWRITE_GENERATION, MIN_COLD_GENERATIOIN) to
                  // MIN_COLD_GENERATION.
      };

      eviction_mode_t eviction_mode;
      double stop_evict_ratio;
      double default_evict_ratio;
      double fast_evict_ratio;
      rewrite_gen_t hot_tier_generations;

      void init(double stop_ratio,
                double default_ratio,
                double fast_ratio,
                rewrite_gen_t hot_tier_generations) {
        ceph_assert(0 <= stop_ratio);
        ceph_assert(stop_ratio < default_ratio);
        ceph_assert(default_ratio < fast_ratio);
        ceph_assert(fast_ratio <= 1);
        eviction_mode = eviction_mode_t::STOP;
        stop_evict_ratio = stop_ratio;
        default_evict_ratio = default_ratio;
        fast_evict_ratio = fast_ratio;
        this->hot_tier_generations = hot_tier_generations;
      }

      bool is_stop_mode() const {
        return eviction_mode == eviction_mode_t::STOP;
      }

      bool is_default_mode() const {
        return eviction_mode == eviction_mode_t::DEFAULT;
      }

      bool is_fast_mode() const {
        return eviction_mode == eviction_mode_t::FAST;
      }

      rewrite_gen_t adjust_generation_with_eviction(rewrite_gen_t gen) {
        rewrite_gen_t ret = gen;
        switch(eviction_mode) {
        case eviction_mode_t::STOP:
          if (gen == hot_tier_generations) {
            ret = hot_tier_generations - 1;
          }
          break;
        case eviction_mode_t::DEFAULT:
          break;
        case eviction_mode_t::FAST:
          if (gen >= MIN_REWRITE_GENERATION && gen < hot_tier_generations) {
            ret = hot_tier_generations;
          }
          break;
        default:
          ceph_abort_msg("impossible");
        }
        return ret;
      }

      // We change the state of eviction_mode according to the alive ratio
      // of the main cleaner.
      //
      // Use A, B, C, D to represent the state of alive ratio:
      //   A: alive ratio <= stop_evict_ratio
      //   B: alive ratio <= default_evict_ratio
      //   C: alive ratio <= fast_evict_ratio
      //   D: alive ratio >  fast_evict_ratio
      //
      // and use X, Y, Z to shorten the state of eviction_mode_t:
      //   X: STOP
      //   Y: DEFAULT
      //   Z: FAST
      //
      // Then we can use a form like (A && X) to describe the current state
      // of the main cleaner, which indicates the alive ratio is less than or
      // equal to stop_evict_ratio and current eviction mode is STOP.
      //
      // all valid state transitions show as follow:
      //   (A && X) => (B && X) => (C && Y) => (D && Z) =>
      //   (C && Z) => (B && Y) => (A && X)
      //                      `--> (C && Y) => ...
      //
      // when the system restarts, the init state is (_ && X), the
      // transitions should be:
      // (_ && X) -> (A && X) => normal transition
      //          -> (B && X) => normal transition
      //          -> (C && X) => (C && Y) => normal transition
      //          -> (D && X) => (D && Z) => normal transition
      void maybe_update_eviction_mode(double main_alive_ratio) {
        if (main_alive_ratio <= stop_evict_ratio) {
          eviction_mode = eviction_mode_t::STOP;
        } else if (main_alive_ratio <= default_evict_ratio) {
          if (eviction_mode > eviction_mode_t::DEFAULT) {
            eviction_mode = eviction_mode_t::DEFAULT;
          }
        } else if (main_alive_ratio <= fast_evict_ratio) {
          if (eviction_mode < eviction_mode_t::DEFAULT) {
            eviction_mode = eviction_mode_t::DEFAULT;
          }
        } else {
          assert(main_alive_ratio > fast_evict_ratio);
          eviction_mode = eviction_mode_t::FAST;
        }
      }
    };

    seastar::future<> do_background_cycle();
    seastar::future<> run_promote();

    void register_metrics();

    struct {
      uint64_t io_blocking_num = 0;
      uint64_t io_count = 0;
      uint64_t io_blocked_count = 0;
      uint64_t io_blocked_count_trim = 0;
      uint64_t io_blocked_count_clean = 0;
      uint64_t io_blocked_sum = 0;
      uint64_t io_blocked_time = 0;
    } stats;
    seastar::metrics::metric_group metrics;

    JournalTrimmerImplRef trimmer;
    AsyncCleanerRef main_cleaner;
    ExtentPinboard *pinboard = nullptr;
    LogicalBucketRef logical_bucket;

    /*
     * cold tier (optional, see has_cold_tier())
     */
    AsyncCleanerRef cold_cleaner;
    std::vector<AsyncCleaner*> cleaners_by_device_id;

    std::optional<seastar::future<>> process_join;
    std::optional<seastar::promise<>> blocking_background;
    std::optional<seastar::promise<>> blocking_io;
    std::optional<seastar::future<>> promote_process_join;
    std::optional<seastar::promise<>> blocking_promote;
    bool is_running_until_halt = false;
    state_t state = state_t::STOP;
    eviction_state_t eviction_state;

#ifdef CRIMSON_TEST_WORKLOAD
    enum class ForceProcessState {
      STOP,
      TRIM,
      CLEAN,
    };
    bool test_workload = false;
    ForceProcessState force_process_state = ForceProcessState::STOP;
    ForceProcessState last_process_state = ForceProcessState::STOP;
    seastar::timer<seastar::steady_clock_type> force_background_timer;
    int force_process_half_life;

    void set_next_arm_timepoint() {
      assert(test_workload);
      force_background_timer.arm(std::chrono::seconds(force_process_half_life));
    }

    void wake_half_life() {
      assert(test_workload);
      if (last_process_state == ForceProcessState::TRIM) {
        force_process_state = ForceProcessState::CLEAN;
      } else {
        force_process_state = ForceProcessState::TRIM;
      }

      do_wake_background();
    }

    bool force_run_background() const {
      return test_workload && force_process_state != ForceProcessState::STOP;
      return false;
    }
#endif
    friend class ::transaction_manager_test_t;
  };

  std::vector<ExtentOolWriterRef> writer_refs;
  std::vector<ExtentOolWriter*> data_writers_by_gen;
  // gen 0 METADATA writer is the journal writer
  std::vector<ExtentOolWriter*> md_writers_by_gen;
  std::vector<TokenBucketRef> token_buckets;

  std::vector<Device*> devices_by_id;
  Device* primary_device = nullptr;
  std::size_t num_devices = 0;

  // without cold tier, the largest generation is less than hot_tier_generations
  const rewrite_gen_t hot_tier_generations = NULL_GENERATION;
  const rewrite_gen_t cold_tier_generations = NULL_GENERATION;
  rewrite_gen_t dynamic_max_rewrite_generation = NULL_GENERATION;
  BackgroundProcess background_process;
  // TODO: drop once paddr->journal_seq_t is introduced
  SegmentSeqAllocatorRef ool_segment_seq_allocator;
  extent_len_t max_data_allocation_size = 0;
  std::size_t write_through_size;

  friend class ::transaction_manager_test_t;
  friend class Cache;
};

using ExtentPlacementManagerRef = std::unique_ptr<ExtentPlacementManager>;

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::io_usage_t> : fmt::ostream_formatter {};
#endif
