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
      return crimson::repeat(
	[FNAME, &f] {
	  return std::invoke(f
	  ).safe_then([] {
	    return seastar::stop_iteration::yes;
	  }).handle_error(
	    [FNAME](const crimson::ct_error::eagain &e) {
	      DEBUG("hit eagain, restarting");
	      return seastar::stop_iteration::no;
	    },
	    crimson::ct_error::pass_further_all{}
	  );
	});
    });
}

// non-errorated version
template <typename F>
auto repeat_eagain2(F &&f) {
  LOG_PREFIX("repeat_eagain");
  return seastar::do_with(
    std::forward<F>(f),
    [FNAME](auto &f) {
      return seastar::repeat(
	[FNAME, &f] {
	  return std::invoke(f
	  ).safe_then([] {
	    return seastar::stop_iteration::yes;
	  }).handle_error(
	    [FNAME](const crimson::ct_error::eagain &e) {
	      DEBUG("hit eagain, restarting");
	      return seastar::make_ready_future<seastar::stop_iteration>(
	          seastar::stop_iteration::no);
	    }
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
    LBAManagerRef lba_manager);

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
  TransactionRef create_transaction() final {
    return cache->create_transaction();
  }

  /// Creates empty weak transaction
  TransactionRef create_weak_transaction() {
    return cache->create_weak_transaction();
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
  using get_pin_ertr = LBAManager::get_mapping_ertr;
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
    DEBUGT("getting extent {}", t, *pin);
    return cache->get_extent<T>(
      t,
      pin->get_paddr(),
      pin->get_length()
    ).si_then([this, FNAME, &t, pin=std::move(pin)](auto ref) mutable -> ret {
      if (!ref->has_pin()) {
	assert(!(pin->has_been_invalidated() || ref->has_been_invalidated()));
	ref->set_pin(std::move(pin));
	lba_manager->add_pin(ref->get_pin());
      }
      DEBUGT("got extent {}", t, *ref);
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
        ERRORT("offset {} len {} got wrong pin {}",
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
        ERRORT("offset {} got wrong pin {}",
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
   * greater than hint.
   */
  using alloc_extent_iertr = LBAManager::alloc_extent_iertr;
  template <typename T>
  using alloc_extent_ret = alloc_extent_iertr::future<TCachedExtentRef<T>>;
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
    ).si_then([ext=std::move(ext), len, this](auto &&ref) mutable {
      ext->set_pin(std::move(ref));
      stats.extents_allocated_total++;
      stats.extents_allocated_bytes += len;
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
      zero_paddr());
  }

  using find_hole_iertr = LBAManager::find_hole_iertr;
  using find_hole_ret = LBAManager::find_hole_iertr::future<
    std::pair<laddr_t, extent_len_t>
    >;
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

  ~TransactionManager();

private:
  friend class Transaction;

  SegmentManager &segment_manager;
  SegmentCleanerRef segment_cleaner;
  CacheRef cache;
  LBAManagerRef lba_manager;
  JournalRef journal;

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

#define FORWARD(METHOD)					\
  template <typename... Args>				\
  auto METHOD(Args&&... args) const {			\
    return tm->METHOD(std::forward<Args>(args)...);	\
  }

#define PARAM_FORWARD(METHOD)					\
  template <typename T, typename... Args>			\
  auto METHOD(Args&&... args) const {				\
    return tm->METHOD<T>(std::forward<Args>(args)...);	\
  }

#define INT_FORWARD(METHOD)						\
  template <typename... Args>						\
  auto METHOD(Transaction &t, Args&&... args) const {			\
    return with_trans_intr(						\
      t,								\
      [this](auto&&... args) {						\
	return tm->METHOD(std::forward<decltype(args)>(args)...);	\
      },								\
      std::forward<Args>(args)...);					\
  }

#define PARAM_INT_FORWARD(METHOD)					\
  template <typename T, typename... Args>				\
  auto METHOD(Transaction &t, Args&&... args) const {			\
    return with_trans_intr(						\
      t,								\
      [this](auto&&... args) {						\
	return tm->METHOD<T>(std::forward<decltype(args)>(args)...);	\
      },								\
      std::forward<Args>(args)...);					\
  }

/// Temporary translator to non-interruptible futures
class InterruptedTransactionManager {
  TransactionManager *tm = nullptr;
public:
  InterruptedTransactionManager() = default;
  InterruptedTransactionManager(const InterruptedTransactionManager &) = default;
  InterruptedTransactionManager(InterruptedTransactionManager &&) = default;
  InterruptedTransactionManager(TransactionManager &tm) : tm(&tm) {}

  InterruptedTransactionManager &operator=(
    const InterruptedTransactionManager &) = default;
  InterruptedTransactionManager &operator=(
    InterruptedTransactionManager &&) = default;

  TransactionManager &get_tm() const { return *tm; }

  FORWARD(mkfs)
  FORWARD(mount)
  FORWARD(close)
  FORWARD(create_transaction)
  FORWARD(create_weak_transaction)
  FORWARD(reset_transaction_preserve_handle)
  INT_FORWARD(get_pin)
  INT_FORWARD(get_pins)
  PARAM_INT_FORWARD(pin_to_extent)
  PARAM_INT_FORWARD(read_extent)
  FORWARD(get_mutable_extent)
  INT_FORWARD(inc_ref)
  INT_FORWARD(dec_ref)
  PARAM_INT_FORWARD(alloc_extent)
  INT_FORWARD(reserve_region)
  INT_FORWARD(find_hole)
  PARAM_INT_FORWARD(alloc_extents)
  INT_FORWARD(submit_transaction)

  INT_FORWARD(read_root_meta)
  INT_FORWARD(update_root_meta)
  INT_FORWARD(read_onode_root)
  FORWARD(write_onode_root)
  INT_FORWARD(read_collection_root)
  FORWARD(write_collection_root)
  FORWARD(get_block_size)
  FORWARD(store_stat)

  FORWARD(get_segment_cleaner)
  FORWARD(get_lba_manager)

  void reset() { tm = nullptr; }
};

class InterruptedTMRef {
  std::unique_ptr<TransactionManager> ref;
  std::optional<InterruptedTransactionManager> itm;
public:
  InterruptedTMRef() {}

  template <typename... T>
  InterruptedTMRef(T&&... args)
    : ref(std::make_unique<TransactionManager>(std::forward<T>(args)...)),
      itm(*ref) {}

  InterruptedTMRef(std::unique_ptr<TransactionManager> tm)
    : ref(std::move(tm)), itm(*ref) {}

  InterruptedTMRef(InterruptedTMRef &&itmr)
    : ref(std::move(itmr.ref)), itm(*ref) {}

  InterruptedTMRef &operator=(std::unique_ptr<TransactionManager> tm) {
    this->~InterruptedTMRef();
    new (this) InterruptedTMRef(std::move(tm));
    return *this;
  }

  InterruptedTMRef &operator=(InterruptedTMRef &&rhs) {
    this->~InterruptedTMRef();
    new (this) InterruptedTMRef(std::move(rhs));
    return *this;
  }

  void reset() {
    itm = std::nullopt;
    ref.reset();
  }

  auto &operator*() const {
    return *itm;
  }

  auto operator->() const {
    return &*itm;
  }
};



}
