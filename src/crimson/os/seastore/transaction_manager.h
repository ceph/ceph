// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <optional>
#include <vector>
#include <utility>
#include <functional>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/journal.h"

namespace crimson::os::seastore {
class Journal;

/**
 * LogicalCachedExtent
 *
 * CachedExtent with associated lba mapping.
 *
 * Users of TransactionManager should be using extents derived from
 * LogicalCachedExtent.
 */
class LogicalCachedExtent : public CachedExtent {
public:
  template <typename... T>
  LogicalCachedExtent(T&&... t) : CachedExtent(std::forward<T>(t)...) {}

  void set_pin(LBAPinRef &&pin) { this->pin = std::move(pin); }

  LBAPin &get_pin() {
    assert(pin);
    return *pin;
  }

  laddr_t get_laddr() const {
    assert(pin);
    return pin->get_laddr();
  }

private:
  LBAPinRef pin;
};

using LogicalCachedExtentRef = TCachedExtentRef<LogicalCachedExtent>;
struct ref_laddr_cmp {
  using is_transparent = laddr_t;
  bool operator()(const LogicalCachedExtentRef &lhs,
		  const LogicalCachedExtentRef &rhs) const {
    return lhs->get_laddr() < rhs->get_laddr();
  }
  bool operator()(const laddr_t &lhs,
		  const LogicalCachedExtentRef &rhs) const {
    return lhs < rhs->get_laddr();
  }
  bool operator()(const LogicalCachedExtentRef &lhs,
		  const laddr_t &rhs) const {
    return lhs->get_laddr() < rhs;
  }
};

using lextent_set_t = addr_extent_set_base_t<
  laddr_t,
  LogicalCachedExtentRef,
  ref_laddr_cmp
  >;

template <typename T>
using lextent_list_t = addr_extent_list_base_t<
  laddr_t, TCachedExtentRef<T>>;

/**
 * TransactionManager
 *
 * Abstraction hiding reading and writing to persistence.
 * Exposes transaction based interface with read isolation.
 */
class TransactionManager : public JournalSegmentProvider {
public:
  TransactionManager(
    SegmentManager &segment_manager,
    Journal &journal,
    Cache &cache,
    LBAManager &lba_manager);

  segment_id_t next = 0;
  get_segment_ret get_segment() final {
    // TODO -- part of gc
    return get_segment_ret(
      get_segment_ertr::ready_future_marker{},
      next++);
  }

  void put_segment(segment_id_t segment) final {
    // TODO -- part of gc
    return;
  }

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
  TransactionRef create_transaction() {
    return lba_manager.create_transaction();
  }

  /**
   * Read extents corresponding to specified lba range
   */
  using read_extent_ertr = SegmentManager::read_ertr;
  template <typename T>
  using read_extent_ret = read_extent_ertr::future<lextent_list_t<T>>;
  template <typename T>
  read_extent_ret<T> read_extents(
    Transaction &t,
    laddr_t offset,
    extent_len_t length)
  {
    std::unique_ptr<lextent_list_t<T>> ret =
      std::make_unique<lextent_list_t<T>>();
    auto &ret_ref = *ret;
    std::unique_ptr<lba_pin_list_t> pin_list =
      std::make_unique<lba_pin_list_t>();
    auto &pin_list_ref = *pin_list;
    return lba_manager.get_mapping(
      t, offset, length
    ).safe_then([this, &t, &pin_list_ref, &ret_ref](auto pins) {
      crimson::get_logger(ceph_subsys_filestore).debug(
	"read_extents: mappings {}",
	pins);
      pins.swap(pin_list_ref);
      return crimson::do_for_each(
	pin_list_ref.begin(),
	pin_list_ref.end(),
	[this, &t, &ret_ref](auto &pin) {
	  crimson::get_logger(ceph_subsys_filestore).debug(
	    "read_extents: get_extent {}~{}",
	    pin->get_paddr(),
	    pin->get_length());
	  return cache.get_extent<T>(
	    t,
	    pin->get_paddr(),
	    pin->get_length()
	  ).safe_then([&pin, &ret_ref](auto ref) mutable {
	    ref->set_pin(std::move(pin));
	    ret_ref.push_back(std::make_pair(ref->get_laddr(), ref));
	    crimson::get_logger(ceph_subsys_filestore).debug(
	      "read_extents: got extent {}",
	      *ref);
	    return read_extent_ertr::now();
	  });
	});
    }).safe_then([ret=std::move(ret), pin_list=std::move(pin_list)]() mutable {
      return read_extent_ret<T>(
	read_extent_ertr::ready_future_marker{},
	std::move(*ret));
    });
  }

  /// Obtain mutable copy of extent
  LogicalCachedExtentRef get_mutable_extent(Transaction &t, LogicalCachedExtentRef ref) {
    auto ret = cache.duplicate_for_write(
      t,
      ref)->cast<LogicalCachedExtent>();
    ret->set_pin(ref->get_pin().duplicate());
    return ret;
  }

  using ref_ertr = LBAManager::ref_ertr;
  using ref_ret = LBAManager::ref_ret;

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

  /**
   * alloc_extent
   *
   * Allocates a new block of type T with the minimum lba range of size len
   * greater than hint.
   */
  using alloc_extent_ertr = SegmentManager::read_ertr;
  template <typename T>
  using alloc_extent_ret = alloc_extent_ertr::future<TCachedExtentRef<T>>;
  template <typename T>
  alloc_extent_ret<T> alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) {
    auto ext = cache.alloc_new_extent<T>(
      t,
      len);
    return lba_manager.alloc_extent(
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

  ~TransactionManager();

private:
  friend class Transaction;

  SegmentManager &segment_manager;
  Cache &cache;
  LBAManager &lba_manager;
  Journal &journal;
};
using TransactionManagerRef = std::unique_ptr<TransactionManager>;

}
