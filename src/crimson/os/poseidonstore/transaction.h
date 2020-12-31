// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "crimson/os/poseidonstore/poseidonstore_types.h"
#include "crimson/os/poseidonstore/cache.h"

namespace crimson::os::poseidonstore {

/**
 * Transaction processing
 *
 * 1. Find out read and wrte sets the transaction is going to access
 * 2. Fill cache extent 
 * 3. a. Convert cache extent's state in the readset to READING, which mean this cache extent
 * 	shouldn't be modified until the transaction is complete
 *    b. Convert cache extent's state in the writeset to WRITING.
 * 4. Submit transactions to underlying WAL
 * 5. Wait completion
 * 6. Complete transaction
 *    a. update cache extent's state in read and write set (--> CLEAN)
 *
 *
 * TODO: add rest of transaction infos when handling ops in the transaction
 *
 */
class Transaction {
public:
  using Ref = std::unique_ptr<Transaction>;

  enum class get_extent_ret {
    PRESENT,
    ABSENT
  };
  get_extent_ret get_extent(laddr_t addr, CachedExtentRef *out) {
    if (auto iter = write_set.find(addr);
	iter != write_set.end()) {
      if (out)
	*out = CachedExtentRef(*iter);
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

  void add_to_read_set(CachedExtentRef ref) {
    if (is_weak()) return;
    ceph_assert(read_set.count(ref) == 0);
    ref->set_state(CachedExtent::ce_state_t::READING);
    read_set.insert(ref);
  }

  void add_to_write_set(CachedExtentRef ref) {
    /*
     * If overwrite occurs in the same transaction, do not touch prior_instance.
     */
    if (!ref->prior_instance) {
      CachedExtentRef copy_ref(ref);
      ref->prior_instance = copy_ref;
    }
    ref->set_state(CachedExtent::ce_state_t::WRITING);
    write_set.insert(ref);
  }

  bool is_weak() const {
    return weak;
  }

private:
  friend class Cache;
  friend Ref make_transaction();
  friend Ref make_weak_transaction();

  const bool weak;

  pextent_set_t read_set;   ///< set of extents read by laddr
  pextent_set_t write_set;    ///< set of extents written by laddr

  Transaction(bool weak) : weak(weak) {}
};
using TransactionRef = Transaction::Ref;

inline TransactionRef make_transaction() {
  return std::unique_ptr<Transaction>(new Transaction(false));
}
}
