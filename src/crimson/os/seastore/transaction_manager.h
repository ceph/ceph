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

namespace crimson::os::seastore {
class Journal;

template <typename F>
auto repeat_eagain(F &&f) {
  LOG_PREFIX("repeat_eagain");
  return seastar::do_with(
    std::forward<F>(f),
    [FNAME](auto &f) {
      return crimson::do_until(
	[FNAME, &f] {
	  return std::invoke(f
	  ).safe_then([] {
	    return true;
	  }).handle_error(
	    [FNAME](const crimson::ct_error::eagain &e) {
	      DEBUG("hit eagain, restarting");
	      return seastar::make_ready_future<bool>(false);
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

  TransactionManager(
    SegmentManager &segment_manager,
    SegmentCleanerRef segment_cleaner,
    JournalRef journal,
    CacheRef cache,
    LBAManagerRef lba_manager);

  /// Writes initial metadata to disk
  using mkfs_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  mkfs_ertr::future<> mkfs();

  /// Reads initial metadata from disk
  using mount_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  mount_ertr::future<> mount();

  /// Closes transaction_manager
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  close_ertr::future<> close();

  /// Creates empty transaction
  TransactionRef create_transaction() final {
    return cache->create_transaction();
  }

  /// Creates empty weak transaction
  TransactionRef create_weak_transaction() {
    return cache->create_weak_transaction();
  }

  /**
   * get_pins
   *
   * Get logical pins overlapping offset~length
   */
  using get_pins_ertr = LBAManager::get_mapping_ertr;
  using get_pins_ret = get_pins_ertr::future<lba_pin_list_t>;
  get_pins_ret get_pins(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    return lba_manager->get_mapping(
      t, offset, length);
  }

  /**
   * pin_to_extent
   *
   * Get extent mapped at pin.
   */
  using pin_to_extent_ertr = get_pins_ertr::extend_ertr<
    SegmentManager::read_ertr>;
  template <typename T>
  using pin_to_extent_ret = pin_to_extent_ertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  pin_to_extent_ret<T> pin_to_extent(
    Transaction &t,
    LBAPinRef pin) {
    LOG_PREFIX(TransactionManager::pin_to_extent);
    using ret = pin_to_extent_ret<T>;
    DEBUGT("getting extent {}", t, *pin);
    return cache->get_extent<T>(
      t,
      pin->get_paddr(),
      pin->get_length()
    ).safe_then([this, FNAME, &t, pin=std::move(pin)](auto ref) mutable -> ret {
      if (!ref->has_pin()) {
	if (pin->has_been_invalidated() || ref->has_been_invalidated()) {
	  return crimson::ct_error::eagain::make();
	} else {
	  ref->set_pin(std::move(pin));
	  lba_manager->add_pin(ref->get_pin());
	}
      }
      DEBUGT("got extent {}", t, *ref);
      return pin_to_extent_ret<T>(
	pin_to_extent_ertr::ready_future_marker{},
	std::move(ref));
    });
  }

  /**
   * read_extent
   *
   * Read extent of type T at offset~length
   */
  using read_extent_ertr = get_pins_ertr::extend_ertr<
    SegmentManager::read_ertr>;
  template <typename T>
  using read_extent_ret = read_extent_ertr::future<
    TCachedExtentRef<T>>;
  template <typename T>
  read_extent_ret<T> read_extent(
    Transaction &t,
    laddr_t offset,
    extent_len_t length) {
    LOG_PREFIX(TransactionManager::read_extent);
    return get_pins(
      t, offset, length
    ).safe_then([this, FNAME, &t, offset, length](auto pins) {
      if (pins.size() != 1 || !pins.front()->get_paddr().is_real()) {
	ERRORT(
	  "offset {} len {} got {} extents:",
	  t, offset, length, pins.size());
	for (auto &i: pins) {
	  ERRORT("\t{}", t, *i);
	}
	ceph_assert(0 == "Should be impossible");
      }
      return this->pin_to_extent<T>(t, std::move(pins.front()));
    });
  }

  /// Obtain mutable copy of extent
  LogicalCachedExtentRef get_mutable_extent(Transaction &t, LogicalCachedExtentRef ref) {
    LOG_PREFIX(TransactionManager::get_mutable_extent);
    auto ret = cache->duplicate_for_write(
      t,
      ref)->cast<LogicalCachedExtent>();
    if (!ret->has_pin()) {
      DEBUGT(
	"duplicating {} for write: {}",
	t,
	*ref,
	*ret);
      ret->set_pin(ref->get_pin().duplicate());
    } else {
      DEBUGT(
	"{} already pending",
	t,
	*ref);
      assert(ref->is_pending());
      assert(&*ref == &*ret);
    }
    return ret;
  }


  using ref_ertr = LBAManager::ref_ertr;
  using ref_ret = ref_ertr::future<unsigned>;

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
  using refs_ret = ref_ertr::future<std::vector<unsigned>>;
  refs_ret dec_ref(
    Transaction &t,
    std::vector<laddr_t> offsets);

  /**
   * alloc_extent
   *
   * Allocates a new block of type T with the minimum lba range of size len
   * greater than hint.
   */
  using alloc_extent_ertr = LBAManager::alloc_extent_ertr;
  template <typename T>
  using alloc_extent_ret = alloc_extent_ertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    auto ext = cache->alloc_new_extent<T>(
      t,
      len);
    return lba_manager->alloc_extent(
      t,
      hint,
      len,
      ext->get_paddr()
    ).safe_then([ext=std::move(ext)](auto &&ref) mutable {
      ext->set_pin(std::move(ref));
      return alloc_extent_ertr::make_ready_future<TCachedExtentRef<T>>(
	std::move(ext));
    });
  }

  using reserve_extent_ertr = alloc_extent_ertr;
  using reserve_extent_ret = reserve_extent_ertr::future<LBAPinRef>;
  reserve_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    return lba_manager->alloc_extent(
      t,
      hint,
      len,
      zero_paddr());
  }

  using find_hole_ertr = LBAManager::find_hole_ertr;
  using find_hole_ret = LBAManager::find_hole_ret;
  find_hole_ret find_hole(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    return lba_manager->find_hole(
      t,
      hint,
      len);
  }

  /* alloc_extents
   *
   * allocates more than one new blocks of type T.
   */
   using alloc_extents_ertr = alloc_extent_ertr;
   template<class T>
   alloc_extents_ertr::future<std::vector<TCachedExtentRef<T>>>
   alloc_extents(
     Transaction &t,
     laddr_t hint,
     extent_len_t len,
     int num) {
     return seastar::do_with(std::vector<TCachedExtentRef<T>>(),
       [this, &t, hint, len, num] (auto &extents) {
       return crimson::do_for_each(
                       boost::make_counting_iterator(0),
                       boost::make_counting_iterator(num),
         [this, &t, len, hint, &extents] (auto i) {
         return alloc_extent<T>(t, hint, len).safe_then(
           [&extents](auto &&node) {
           extents.push_back(node);
         });
       }).safe_then([&extents] {
         return alloc_extents_ertr::make_ready_future
                <std::vector<TCachedExtentRef<T>>>(std::move(extents));
       });
     });
  }

  /**
   * submit_transaction
   *
   * Atomically submits transaction to persistence
   */
  using submit_transaction_ertr = crimson::errorator<
    crimson::ct_error::eagain, // Caller should retry transaction from beginning
    crimson::ct_error::input_output_error // Media error
    >;
  submit_transaction_ertr::future<> submit_transaction(TransactionRef);

  /// SegmentCleaner::ExtentCallbackInterface
  using SegmentCleaner::ExtentCallbackInterface::submit_transaction_direct_ret;
  submit_transaction_direct_ret submit_transaction_direct(
    TransactionRef t) final;

  using SegmentCleaner::ExtentCallbackInterface::get_next_dirty_extents_ret;
  get_next_dirty_extents_ret get_next_dirty_extents(
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

  using scan_extents_cursor =
    SegmentCleaner::ExtentCallbackInterface::scan_extents_cursor;
  using scan_extents_ertr =
    SegmentCleaner::ExtentCallbackInterface::scan_extents_ertr;
  using scan_extents_ret =
    SegmentCleaner::ExtentCallbackInterface::scan_extents_ret;
  scan_extents_ret scan_extents(
    scan_extents_cursor &cursor,
    extent_len_t bytes_to_read) final {
    return journal->scan_extents(cursor, bytes_to_read);
  }

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
  using read_root_meta_ertr = base_ertr;
  using read_root_meta_bare = std::optional<std::string>;
  using read_root_meta_ret = read_root_meta_ertr::future<
    read_root_meta_bare>;
  read_root_meta_ret read_root_meta(
    Transaction &t,
    const std::string &key) {
    return cache->get_root(
      t
    ).safe_then([&key](auto root) {
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
  using update_root_meta_ertr = base_ertr;
  using update_root_meta_ret = update_root_meta_ertr::future<>;
  update_root_meta_ret update_root_meta(
    Transaction& t,
    const std::string& key,
    const std::string& value) {
    return cache->get_root(
      t
    ).safe_then([this, &t, &key, &value](RootBlockRef root) {
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
  using read_onode_root_ertr = base_ertr;
  using read_onode_root_ret = read_onode_root_ertr::future<laddr_t>;
  read_onode_root_ret read_onode_root(Transaction &t) {
    return cache->get_root(t).safe_then([](auto croot) {
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
  using read_collection_root_ertr = base_ertr;
  using read_collection_root_ret = read_collection_root_ertr::future<
    coll_root_t>;
  read_collection_root_ret read_collection_root(Transaction &t) {
    return cache->get_root(t).safe_then([](auto croot) {
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

  ~TransactionManager();

private:
  friend class Transaction;

  SegmentManager &segment_manager;
  SegmentCleanerRef segment_cleaner;
  CacheRef cache;
  LBAManagerRef lba_manager;
  JournalRef journal;

  WritePipeline write_pipeline;

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
