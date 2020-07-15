// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

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
  friend class Cache;

  RootBlockRef root;        ///< ref to root if mutated by transaction

  segment_off_t offset = 0; ///< relative offset of next block

  pextent_set_t read_set;   ///< set of extents read by paddr
  ExtentIndex write_set;    ///< set of extents written by paddr

  std::list<CachedExtentRef> fresh_block_list;   ///< list of fresh blocks
  std::list<CachedExtentRef> mutated_block_list; ///< list of mutated blocks

  pextent_set_t retired_set; ///< list of extents mutated by this transaction

public:
  CachedExtentRef get_extent(paddr_t addr) {
    if (auto iter = write_set.find_offset(addr);
	iter != write_set.end()) {
      return CachedExtentRef(&*iter);
    } else if (
      auto iter = read_set.find(addr);
      iter != read_set.end()) {
      return *iter;
    } else {
      return CachedExtentRef();
    }
  }

  void add_to_retired_set(CachedExtentRef ref) {
    if (!ref->is_initial_pending()) {
      // && retired_set.count(ref->get_paddr()) == 0
      // If it's already in the set, insert here will be a noop,
      // which is what we want.
      retired_set.insert(ref);
    }
    if (ref->is_pending()) {
      write_set.erase(*ref);
      ref->state = CachedExtent::extent_state_t::INVALID;
    }
  }

  void add_to_read_set(CachedExtentRef ref) {
    ceph_assert(read_set.count(ref) == 0);
    read_set.insert(ref);
  }

  void add_fresh_extent(CachedExtentRef ref) {
    fresh_block_list.push_back(ref);
    ref->set_paddr(make_record_relative_paddr(offset));
    offset += ref->get_length();
    write_set.insert(*ref);
  }

  void add_mutated_extent(CachedExtentRef ref) {
    mutated_block_list.push_back(ref);
    write_set.insert(*ref);
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
};
using TransactionRef = std::unique_ptr<Transaction>;

inline TransactionRef make_transaction() {
  return std::make_unique<Transaction>();
}

}
