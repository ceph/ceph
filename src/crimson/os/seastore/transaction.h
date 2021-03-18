// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/root_block.h"

namespace crimson::os::seastore {

/**
 * Transaction
 *
 * Representation of in-progress mutation. Used exclusively through Cache methods.
 */
class Transaction {
public:
  OrderingHandle handle;

  using Ref = std::unique_ptr<Transaction>;
  enum class get_extent_ret {
    PRESENT,
    ABSENT,
    RETIRED
  };
  get_extent_ret get_extent(paddr_t addr, CachedExtentRef *out) {
    if (retired_set.count(addr)) {
      return get_extent_ret::RETIRED;
    } else if (auto iter = write_set.find_offset(addr);
	iter != write_set.end()) {
      if (out)
	*out = CachedExtentRef(&*iter);
      return get_extent_ret::PRESENT;
    } else if (
      auto iter = read_set.find(addr);
      iter != read_set.end()) {
      if (out)
	*out = CachedExtentRef(*iter);
      return get_extent_ret::PRESENT;
    } else {
      return get_extent_ret::ABSENT;
    }
  }

  void add_to_retired_set(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    if (!ref->is_initial_pending()) {
      // && retired_set.count(ref->get_paddr()) == 0
      // If it's already in the set, insert here will be a noop,
      // which is what we want.
      retired_set.insert(ref);
    } else {
      ref->state = CachedExtent::extent_state_t::INVALID;
    }
    if (ref->is_pending()) {
      write_set.erase(*ref);
    }
  }

  void add_to_retired_uncached(paddr_t addr, extent_len_t length) {
    retired_uncached.emplace_back(std::make_pair(addr, length));
  }

  void add_to_read_set(CachedExtentRef ref) {
    if (is_weak()) return;

    ceph_assert(read_set.count(ref) == 0);
    read_set.insert(ref);
  }

  void add_fresh_extent(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    fresh_block_list.push_back(ref);
    ref->set_paddr(make_record_relative_paddr(offset));
    offset += ref->get_length();
    write_set.insert(*ref);
  }

  void add_mutated_extent(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    mutated_block_list.push_back(ref);
    write_set.insert(*ref);
  }

  void mark_segment_to_release(segment_id_t segment) {
    assert(to_release == NULL_SEG_ID);
    to_release = segment;
  }

  segment_id_t get_segment_to_release() const {
    return to_release;
  }

  const auto &get_fresh_block_list() {
    return fresh_block_list;
  }

  const auto &get_mutated_block_list() {
    return mutated_block_list;
  }

  const auto &get_retired_set() {
    return retired_set;
  }

  bool is_weak() const {
    return weak;
  }

private:
  friend class Cache;
  friend Ref make_transaction();
  friend Ref make_weak_transaction();

  /**
   * If set, *this may not be used to perform writes and will not provide
   * consistentency allowing operations using to avoid maintaining a read_set.
   */
  const bool weak;

  RootBlockRef root;        ///< ref to root if read or written by transaction

  segment_off_t offset = 0; ///< relative offset of next block

  pextent_set_t read_set;   ///< set of extents read by paddr
  ExtentIndex write_set;    ///< set of extents written by paddr

  std::list<CachedExtentRef> fresh_block_list;   ///< list of fresh blocks
  std::list<CachedExtentRef> mutated_block_list; ///< list of mutated blocks

  pextent_set_t retired_set; ///< list of extents mutated by this transaction

  ///< if != NULL_SEG_ID, release this segment after completion
  segment_id_t to_release = NULL_SEG_ID;

  std::vector<std::pair<paddr_t, extent_len_t>> retired_uncached;

public:
  Transaction(
    OrderingHandle &&handle,
    bool weak
  ) : handle(std::move(handle)), weak(weak) {}

  ~Transaction() {
    for (auto i = write_set.begin();
	 i != write_set.end();) {
      i->state = CachedExtent::extent_state_t::INVALID;
      write_set.erase(*i++);
    }
  }
};
using TransactionRef = Transaction::Ref;

inline TransactionRef make_transaction() {
  return std::make_unique<Transaction>(
    get_dummy_ordering_handle(),
    false
  );
}

inline TransactionRef make_weak_transaction() {
  return std::make_unique<Transaction>(
    get_dummy_ordering_handle(),
    true);
}

}
