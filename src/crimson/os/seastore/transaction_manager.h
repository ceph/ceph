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
  return seastar::do_with(
    std::forward<F>(f),
    [](auto &f)
  {
    return crimson::repeat([&f] {
      return std::invoke(f
      ).safe_then([] {
        return seastar::stop_iteration::yes;
      }).handle_error(
        [](const crimson::ct_error::eagain &e) {
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
    LOG_PREFIX(TransactionManager::get_pin);
    SUBTRACET(seastore_tm, "{}", t, offset);
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
    LOG_PREFIX(TransactionManager::get_pins);
    SUBDEBUGT(seastore_tm, "{}~{}", t, offset, length);
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
    SUBTRACET(seastore_tm, "getting extent {}", t, *pin);
    using ret = pin_to_extent_ret<T>;
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
      SUBTRACET(seastore_tm, "got extent -- {}", t, *ref);
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
    SUBTRACET(seastore_tm, "{}~{}", t, offset, length);
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
    SUBTRACET(seastore_tm, "{}", t, offset);
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
    if (!ret->has_pin()) {
      SUBDEBUGT(seastore_tm,
	"duplicating extent for write -- {} -> {}",
	t,
	*ref,
	*ret);
      ret->set_pin(ref->get_pin().duplicate());
    } else {
      SUBTRACET(seastore_tm,
	"extent is already duplicated -- {}",
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
    LOG_PREFIX(TransactionManager::alloc_extent);
    SUBTRACET(seastore_tm, "{} len={}, placement_hint={}, laddr_hint={}",
              t, T::TYPE, len, placement_hint, laddr_hint);
    auto ext = cache->alloc_new_extent<T>(
      t,
      len,
      placement_hint);
    return lba_manager->alloc_extent(
      t,
      laddr_hint,
      len,
      ext->get_paddr()
    ).si_then([ext=std::move(ext), laddr_hint, &t, FNAME](auto &&ref) mutable {
      ext->set_pin(std::move(ref));
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
    LOG_PREFIX(TransactionManager::reserve_region);
    SUBDEBUGT(seastore_tm, "len={}, laddr_hint={}", t, len, hint);
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
     LOG_PREFIX(TransactionManager::alloc_extents);
     SUBDEBUGT(seastore_tm, "len={}, laddr_hint={}, num={}",
               t, len, hint, num);
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

  /**
   * flush
   *
   * Block until all outstanding IOs on handle are committed.
   * Note, flush() machinery must go through the same pipeline
   * stages and locks as submit_transaction.
   */
  seastar::future<> flush(OrderingHandle &handle);

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
    seastore_off_t len) final;

  using release_segment_ret =
    SegmentCleaner::ExtentCallbackInterface::release_segment_ret;
  release_segment_ret release_segment(
    segment_id_t id) final {
    LOG_PREFIX(TransactionManager::release_segment);
    SUBDEBUG(seastore_tm, "{}", id);
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
    ).si_then([&key, &t](auto root) {
      LOG_PREFIX(TransactionManager::read_root_meta);
      auto meta = root->root.get_meta();
      auto iter = meta.find(key);
      if (iter == meta.end()) {
        SUBDEBUGT(seastore_tm, "{} -> nullopt", t, key);
	return seastar::make_ready_future<read_root_meta_bare>(std::nullopt);
      } else {
        SUBDEBUGT(seastore_tm, "{} -> {}", t, key, iter->second);
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
    LOG_PREFIX(TransactionManager::update_root_meta);
    SUBDEBUGT(seastore_tm, "seastore_tm, {} -> {}", t, key, value);
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
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_onode_root);
      laddr_t ret = croot->get_root().onode_root;
      SUBTRACET(seastore_tm, "{}", t, ret);
      return ret;
    });
  }

  /**
   * write_onode_root
   *
   * Write onode-tree root logical address, must be called after read.
   */
  void write_onode_root(Transaction &t, laddr_t addr) {
    LOG_PREFIX(TransactionManager::write_onode_root);
    SUBDEBUGT(seastore_tm, "{}", t, addr);
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
    return cache->get_root(t).si_then([&t](auto croot) {
      LOG_PREFIX(TransactionManager::read_collection_root);
      auto ret = croot->get_root().collection_root.get();
      SUBTRACET(seastore_tm, "{}~{}",
                t, ret.get_location(), ret.get_size());
      return ret;
    });
  }

  /**
   * write_collection_root
   *
   * Update collection root addr
   */
  void write_collection_root(Transaction &t, coll_root_t cmroot) {
    LOG_PREFIX(TransactionManager::write_collection_root);
    SUBDEBUGT(seastore_tm, "{}~{}",
              t, cmroot.get_location(), cmroot.get_size());
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
	*sm));
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
