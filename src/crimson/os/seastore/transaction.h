// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive/list.hpp>

#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/root_block.h"

namespace crimson::os::seastore {

struct retired_extent_gate_t;
class SeaStore;
class Transaction;

/**
 * Transaction
 *
 * Representation of in-progress mutation. Used exclusively through Cache methods.
 */
class Transaction {
public:
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
      // placeholder in read-set should be in the retired-set
      // at the same time.
      assert(iter->ref->get_type() != extent_types_t::RETIRED_PLACEHOLDER);
      if (out)
	*out = iter->ref;
      return get_extent_ret::PRESENT;
    } else {
      return get_extent_ret::ABSENT;
    }
  }

  void add_to_retired_set(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    if (ref->is_initial_pending()) {
      // We decide not to remove it from fresh_block_list because touching this
      // will affect relative paddrs, and it should be rare to retire a fresh
      // extent.
      ref->state = CachedExtent::extent_state_t::INVALID;
      write_set.erase(*ref);
    } else if (ref->is_mutation_pending()) {
      ref->state = CachedExtent::extent_state_t::INVALID;
      write_set.erase(*ref);
      assert(ref->prior_instance);
      retired_set.insert(ref->prior_instance);
      assert(read_set.count(ref->prior_instance->get_paddr()));
      ref->prior_instance.reset();
    } else {
      // && retired_set.count(ref->get_paddr()) == 0
      // If it's already in the set, insert here will be a noop,
      // which is what we want.
      retired_set.insert(ref);
    }
  }

  void add_to_read_set(CachedExtentRef ref) {
    if (is_weak()) return;

    auto [iter, inserted] = read_set.emplace(this, ref);
    ceph_assert(inserted);
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

  void replace_placeholder(CachedExtent& placeholder, CachedExtent& extent) {
    ceph_assert(!is_weak());

    assert(placeholder.get_type() == extent_types_t::RETIRED_PLACEHOLDER);
    assert(extent.get_type() != extent_types_t::RETIRED_PLACEHOLDER);
    assert(extent.get_type() != extent_types_t::ROOT);
    assert(extent.get_paddr() == placeholder.get_paddr());
    {
      auto where = read_set.find(placeholder.get_paddr());
      assert(where != read_set.end());
      assert(where->ref.get() == &placeholder);
      where = read_set.erase(where);
      read_set.emplace_hint(where, this, &extent);
    }
    {
      auto where = retired_set.find(&placeholder);
      assert(where != retired_set.end());
      assert(where->get() == &placeholder);
      where = retired_set.erase(where);
      retired_set.emplace_hint(where, &extent);
    }
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

  bool is_conflicted() const {
    return conflicted;
  }

  auto &get_handle() {
    return handle;
  }

  Transaction(
    OrderingHandle &&handle,
    bool weak,
    journal_seq_t initiated_after
  ) : weak(weak),
      retired_gate_token(initiated_after),
      handle(std::move(handle))
  {}


  ~Transaction() {
    for (auto i = write_set.begin();
	 i != write_set.end();) {
      i->state = CachedExtent::extent_state_t::INVALID;
      write_set.erase(*i++);
    }
  }

  friend class crimson::os::seastore::SeaStore;
  friend class TransactionConflictCondition;

  void reset_preserve_handle(journal_seq_t initiated_after) {
    root.reset();
    offset = 0;
    read_set.clear();
    write_set.clear();
    fresh_block_list.clear();
    mutated_block_list.clear();
    retired_set.clear();
    to_release = NULL_SEG_ID;
    retired_gate_token.reset(initiated_after);
    conflicted = false;
  }

private:
  friend class Cache;
  friend Ref make_test_transaction();

  /**
   * If set, *this may not be used to perform writes and will not provide
   * consistentency allowing operations using to avoid maintaining a read_set.
   */
  const bool weak;

  RootBlockRef root;        ///< ref to root if read or written by transaction

  segment_off_t offset = 0; ///< relative offset of next block

  read_set_t<Transaction> read_set; ///< set of extents read by paddr
  ExtentIndex write_set;            ///< set of extents written by paddr

  std::list<CachedExtentRef> fresh_block_list;   ///< list of fresh blocks
  std::list<CachedExtentRef> mutated_block_list; ///< list of mutated blocks

  pextent_set_t retired_set; ///< list of extents mutated by this transaction

  ///< if != NULL_SEG_ID, release this segment after completion
  segment_id_t to_release = NULL_SEG_ID;

  retired_extent_gate_t::token_t retired_gate_token;

  bool conflicted = false;

  OrderingHandle handle;
};
using TransactionRef = Transaction::Ref;

/// Should only be used with dummy staged-fltree node extent manager
inline TransactionRef make_test_transaction() {
  return std::make_unique<Transaction>(
    get_dummy_ordering_handle(),
    false,
    journal_seq_t{}
  );
}

struct TransactionConflictCondition {
  class transaction_conflict final : public std::exception {
  public:
    const char* what() const noexcept final {
      return "transaction conflict detected";
    }
  };

public:
  TransactionConflictCondition(Transaction &t) : t(t) {}

  template <typename Fut>
  std::pair<bool, std::optional<Fut>> may_interrupt() {
    if (t.conflicted) {
      return {
	true,
	seastar::futurize<Fut>::make_exception_future(
	  transaction_conflict())};
    } else {
      return {false, std::optional<Fut>()};
    }
  }

  template <typename T>
  static constexpr bool is_interruption_v =
    std::is_same_v<T, transaction_conflict>;


  static bool is_interruption(std::exception_ptr& eptr) {
    return *eptr.__cxa_exception_type() == typeid(transaction_conflict);
  }

private:
  Transaction &t;
};

using trans_intr = crimson::interruptible::interruptor<
  TransactionConflictCondition
  >;

template <typename E>
using trans_iertr =
  crimson::interruptible::interruptible_errorator<
    TransactionConflictCondition,
    E
  >;

template <typename F, typename... Args>
auto with_trans_intr(Transaction &t, F &&f, Args&&... args) {
  return trans_intr::with_interruption_to_error<crimson::ct_error::eagain>(
    std::move(f),
    TransactionConflictCondition(t),
    t,
    std::forward<Args>(args)...);
}

template <typename T>
using with_trans_ertr = typename T::base_ertr::template extend<crimson::ct_error::eagain>;

}
