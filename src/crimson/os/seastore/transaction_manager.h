// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <optional>
#include <vector>
#include <utility>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_placement_manager.h"

namespace crimson::os::seastore {
class Journal;

template <typename F>
auto repeat_eagain(F &&f) {
  LOG_PREFIX("repeat_eagain");
  return seastar::do_with(
    std::forward<F>(f),
    [FNAME](auto &f) {
      return crimson::repeat(
	[FNAME, &f] {
	  return std::invoke(f
	  ).safe_then([] {
	    return seastar::stop_iteration::yes;
	  }).handle_error(
	    [FNAME](const crimson::ct_error::eagain &e) {
	      SUBDEBUG(seastore_tm, "hit eagain, restarting");
	      return seastar::stop_iteration::no;
	    },
	    crimson::ct_error::pass_further_all{}
	  );
	});
    });
}

/**
 * TransactionManager
 *
 * Abstraction hiding reading and writing to persistence.
 * Exposes transaction based interface with read isolation.
 */
class TransactionManager : public SegmentCleaner::ExtentCallbackInterface {
public:
  using base_ertr = Cache::base_ertr;
  using base_iertr = Cache::base_iertr;

  TransactionManager(
    SegmentManager &segment_manager,
    SegmentCleanerRef segment_cleaner,
    JournalRef journal,
    CacheRef cache,
    LBAManagerRef lba_manager,
    ExtentPlacementManagerRef&& epm,
    ExtentReader& scanner);

  /// Writes initial metadata to disk
  using mkfs_ertr = base_ertr;
  mkfs_ertr::future<> mkfs();

  /// Reads initial metadata from disk
  using mount_ertr = base_ertr;
  mount_ertr::future<> mount();

  /// Closes transaction_manager
  using close_ertr = base_ertr;
  close_ertr::future<> close();

  /// Creates empty transaction
  TransactionRef create_transaction(
      Transaction::src_t src,
      const char* name) final {
    return cache->create_transaction(src, name, false);
  }

  /// Creates empty weak transaction
  TransactionRef create_weak_transaction(
      Transaction::src_t src,
      const char* name) {
    return cache->create_transaction(src, name, true);
  }

  /// Resets transaction
  void reset_transaction_preserve_handle(Transaction &t) {
    return cache->reset_transaction_preserve_handle(t);
  }

  /**
   * get_pin
   *
   * Get the logical pin at offset
   */
  using get_pin_iertr = LBAManager::get_mapping_iertr;
  using get_pin_ret = LBAManager::get_mapping_iertr::future<LBAPinRef>;
  get_pin_ret get_pin(
    Transaction &t,
    laddr_t offset) {
    return lba_manager->get_mapping(t, offset);
  }

  /**
   * get_pins
   *
   * Get logical pins overlapping offset~length
   */
  using get_pins_iertr = LBAManager::get_mappings_iertr;
  using get_pins_ret = get_pins_iertr::future<lba_pin_list_t>;
  get_pins_ret get_pins(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    return lba_manager->get_mappings(
      t, offset, length);
  }

  /**
   * pin_to_extent
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_iertr = get_pin_iertr::extend_ertr<
    SegmentManager::read_ertr>;
  template <typename T>
  using pin_to_extent_ret = pin_to_extent_iertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  pin_to_extent_ret<T> pin_to_extent(
    Transaction &t,
    LBAPinRef pin) {
    LOG_PREFIX(TransactionManager::pin_to_extent);
    using ret = pin_to_extent_ret<T>;
    SUBDEBUGT(seastore_tm, "getting extent {}", t, *pin);
    auto &pref = *pin;
    return cache->get_extent<T>(
      t,
      pref.get_paddr(),
      pref.get_length(),
      [this, pin=std::move(pin)](T &extent) mutable {
	assert(!extent.has_pin());
	assert(!extent.has_been_invalidated());
	assert(!pin->has_been_invalidated());
	extent.set_pin(std::move(pin));
	lba_manager->add_pin(extent.get_pin());
      }
    ).si_then([FNAME, &t](auto ref) mutable -> ret {
      SUBDEBUGT(seastore_tm, "got extent {}", t, *ref);
      return pin_to_extent_ret<T>(
	interruptible::ready_future_marker{},
	std::move(ref));
    });
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset~length
   */
  using read_extent_iertr = get_pin_iertr::extend_ertr<
    SegmentManager::read_ertr>;
  template <typename T>
  using read_extent_ret = read_extent_iertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::read_extent);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset, length] (auto pin) {
      if (length != pin->get_length() || !pin->get_paddr().is_real()) {
        SUBERRORT(seastore_tm,
            "offset {} len {} got wrong pin {}",
            t, offset, length, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->pin_to_extent<T>(t, std::move(pin));
    });
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset
   */
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset) {
    LOG_PREFIX(TransactionManager::read_extent);
    return get_pin(
      t, offset
    ).si_then([this, FNAME, &t, offset] (auto pin) {
      if (!pin->get_paddr().is_real()) {
        SUBERRORT(seastore_tm,
            "offset {} got wrong pin {}",
            t, offset, *pin);
        ceph_assert(0 == "Should be impossible");
      }
      return this->pin_to_extent<T>(t, std::move(pin));
    });
  }

  /// Obtain mutable copy of extent
  LogicalCachedExtentRef get_mutable_extent(Transaction &t, LogicalCachedExtentRef ref) {
    LOG_PREFIX(TransactionManager::get_mutable_extent);
    auto ret = cache->duplicate_for_write(
      t,
      ref)->cast<LogicalCachedExtent>();
    stats.extents_mutated_total++;
    stats.extents_mutated_bytes += ret->get_length();
    if (!ret->has_pin()) {
      SUBDEBUGT(seastore_tm,
	"duplicating {} for write: {}",
	t,
	*ref,
	*ret);
      ret->set_pin(ref->get_pin().duplicate());
    } else {
      SUBDEBUGT(seastore_tm,
	"{} already pending",
	t,
	*ref);
      assert(ref->is_pending());
      assert(&*ref == &*ret);
    }
    return ret;
  }


  using ref_iertr = LBAManager::ref_iertr;
  using ref_ret = ref_iertr::future<unsigned>;

  /// Add refcount for ref
  ref_ret inc_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  /// Add refcount for offset
  ref_ret inc_ref(
    Transaction &t,
    laddr_t offset);

  /// Remove refcount for ref
  ref_ret dec_ref(
    Transaction &t,
    LogicalCachedExtentRef &ref);

  /// Remove refcount for offset
  ref_ret dec_ref(
    Transaction &t,
    laddr_t offset);

  /// remove refcount for list of offset
  using refs_ret = ref_iertr::future<std::vector<unsigned>>;
  refs_ret dec_ref(
    Transaction &t,
    std::vector<laddr_t> offsets);

  /**
   * alloc_extent
   *
   * Allocates a new block of type T with the minimum lba range of size len
   * greater than laddr_hint.
   */
  using alloc_extent_iertr = LBAManager::alloc_extent_iertr;
  template <typename T>
  using alloc_extent_ret = alloc_extent_iertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_extent(
    Transaction &t,
    laddr_t laddr_hint,
    extent_len_t len) {
    placement_hint_t placement_hint;
    if constexpr (T::TYPE == extent_types_t::OBJECT_DATA_BLOCK ||
                  T::TYPE == extent_types_t::COLL_BLOCK) {
      placement_hint = placement_hint_t::COLD;
    } else {
      placement_hint = placement_hint_t::HOT;
    }
    auto ext = epm->alloc_new_extent<T>(
      t,
      len,
      placement_hint);
    return lba_manager->alloc_extent(
      t,
      laddr_hint,
      len,
      ext->get_paddr()
    ).si_then([ext=std::move(ext), len, laddr_hint, &t, this](auto &&ref) mutable {
      LOG_PREFIX(TransactionManager::alloc_extent);
      ext->set_pin(std::move(ref));
      stats.extents_allocated_total++;
      stats.extents_allocated_bytes += len;
      SUBDEBUGT(seastore_tm, "new extent: {}, laddr_hint: {}", t, *ext, laddr_hint);
      return alloc_extent_iertr::make_ready_future<TCachedExtentRef<T>>(
	std::move(ext));
    });
  }

  using reserve_extent_iertr = alloc_extent_iertr;
  using reserve_extent_ret = reserve_extent_iertr::future<LBAPinRef>;
  reserve_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    return lba_manager->alloc_extent(
      t,
      hint,
      len,
      P_ADDR_ZERO);
  }

  /* alloc_extents
   *
   * allocates more than one new blocks of type T.
   */
   using alloc_extents_iertr = alloc_extent_iertr;
   template<class T>
   alloc_extents_iertr::future<std::vector<TCachedExtentRef<T>>>
   alloc_extents(
     Transaction &t,
     laddr_t hint,
     extent_len_t len,
     int num) {
     return seastar::do_with(std::vector<TCachedExtentRef<T>>(),
       [this, &t, hint, len, num] (auto &extents) {
       return trans_intr::do_for_each(
                       boost::make_counting_iterator(0),
                       boost::make_counting_iterator(num),
         [this, &t, len, hint, &extents] (auto i) {
         return alloc_extent<T>(t, hint, len).si_then(
           [&extents](auto &&node) {
           extents.push_back(node);
         });
       }).si_then([&extents] {
         return alloc_extents_iertr::make_ready_future
                <std::vector<TCachedExtentRef<T>>>(std::move(extents));
       });
     });
  }

  /**
   * submit_transaction
   *
   * Atomically submits transaction to persistence
   */
  using submit_transaction_iertr = base_iertr;
  submit_transaction_iertr::future<> submit_transaction(Transaction &);

  /// SegmentCleaner::ExtentCallbackInterface
  using SegmentCleaner::ExtentCallbackInterface::submit_transaction_direct_ret;
  submit_transaction_direct_ret submit_transaction_direct(
    Transaction &t) final;

  using SegmentCleaner::ExtentCallbackInterface::get_next_dirty_extents_ret;
  get_next_dirty_extents_ret get_next_dirty_extents(
    Transaction &t,
    journal_seq_t seq,
    size_t max_bytes) final;

  using SegmentCleaner::ExtentCallbackInterface::rewrite_extent_ret;
  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final;

  using SegmentCleaner::ExtentCallbackInterface::get_extent_if_live_ret;
  get_extent_if_live_ret get_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    segment_off_t len) final;

  using release_segment_ret =
    SegmentCleaner::ExtentCallbackInterface::release_segment_ret;
  release_segment_ret release_segment(
    segment_id_t id) final {
    return segment_manager.release(id);
  }

  /**
   * read_root_meta
   *
   * Read root block meta entry for key.
   */
  using read_root_meta_iertr = base_iertr;
  using read_root_meta_bare = std::optional<std::string>;
  using read_root_meta_ret = read_root_meta_iertr::future<
    read_root_meta_bare>;
  read_root_meta_ret read_root_meta(
    Transaction &t,
    const std::string &key) {
    return cache->get_root(
      t
    ).si_then([&key](auto root) {
      auto meta = root->root.get_meta();
      auto iter = meta.find(key);
      if (iter == meta.end()) {
	return seastar::make_ready_future<read_root_meta_bare>(std::nullopt);
      } else {
	return seastar::make_ready_future<read_root_meta_bare>(iter->second);
      }
    });
  }

  /**
   * update_root_meta
   *
   * Update root block meta entry for key to value.
   */
  using update_root_meta_iertr = base_iertr;
  using update_root_meta_ret = update_root_meta_iertr::future<>;
  update_root_meta_ret update_root_meta(
    Transaction& t,
    const std::string& key,
    const std::string& value) {
    return cache->get_root(
      t
    ).si_then([this, &t, &key, &value](RootBlockRef root) {
      root = cache->duplicate_for_write(t, root)->cast<RootBlock>();

      auto meta = root->root.get_meta();
      meta[key] = value;

      root->root.set_meta(meta);
      return seastar::now();
    });
  }

  /**
   * read_onode_root
   *
   * Get onode-tree root logical address
   */
  using read_onode_root_iertr = base_iertr;
  using read_onode_root_ret = read_onode_root_iertr::future<laddr_t>;
  read_onode_root_ret read_onode_root(Transaction &t) {
    return cache->get_root(t).si_then([](auto croot) {
      laddr_t ret = croot->get_root().onode_root;
      return ret;
    });
  }

  /**
   * write_onode_root
   *
   * Write onode-tree root logical address, must be called after read.
   */
  void write_onode_root(Transaction &t, laddr_t addr) {
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().onode_root = addr;
  }

  /**
   * read_collection_root
   *
   * Get collection root addr
   */
  using read_collection_root_iertr = base_iertr;
  using read_collection_root_ret = read_collection_root_iertr::future<
    coll_root_t>;
  read_collection_root_ret read_collection_root(Transaction &t) {
    return cache->get_root(t).si_then([](auto croot) {
      return croot->get_root().collection_root.get();
    });
  }

  /**
   * write_collection_root
   *
   * Update collection root addr
   */
  void write_collection_root(Transaction &t, coll_root_t cmroot) {
    auto croot = cache->get_root_fast(t);
    croot = cache->duplicate_for_write(t, croot)->cast<RootBlock>();
    croot->get_root().collection_root.update(cmroot);
  }

  extent_len_t get_block_size() const {
    return segment_manager.get_block_size();
  }

  store_statfs_t store_stat() const {
    return segment_cleaner->stat();
  }

  void add_segment_manager(SegmentManager* sm) {
    LOG_PREFIX(TransactionManager::add_segment_manager);
    SUBDEBUG(seastore_tm, "adding segment manager {}", sm->get_device_id());
    scanner.add_segment_manager(sm);
    epm->add_allocator(
      device_type_t::SEGMENTED,
      std::make_unique<SegmentedAllocator>(
	*segment_cleaner,
	*sm,
	*lba_manager,
	*journal,
	*cache));
  }

  ~TransactionManager();

private:
  friend class Transaction;

  // although there might be multiple devices backing seastore,
  // only one of them are supposed to hold the journal. This
  // segment manager is that device
  SegmentManager &segment_manager;
  SegmentCleanerRef segment_cleaner;
  CacheRef cache;
  LBAManagerRef lba_manager;
  JournalRef journal;
  ExtentPlacementManagerRef epm;
  ExtentReader& scanner;

  WritePipeline write_pipeline;

  struct {
    uint64_t extents_retired_total = 0;
    uint64_t extents_retired_bytes = 0;
    uint64_t extents_mutated_total = 0;
    uint64_t extents_mutated_bytes = 0;
    uint64_t extents_allocated_total = 0;
    uint64_t extents_allocated_bytes = 0;
  } stats;
  seastar::metrics::metric_group metrics;
  void register_metrics();

  rewrite_extent_ret rewrite_logical_extent(
    Transaction& t,
    LogicalCachedExtentRef extent);
public:
  // Testing interfaces
  auto get_segment_cleaner() {
    return segment_cleaner.get();
  }

  auto get_lba_manager() {
    return lba_manager.get();
  }
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

}
