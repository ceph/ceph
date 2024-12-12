// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive/list.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/transaction_interruptor.h"

namespace crimson::os::seastore {

class SeaStore;

struct io_stat_t {
  uint64_t num = 0;
  uint64_t bytes = 0;

  bool is_clear() const {
    return (num == 0 && bytes == 0);
  }

  void increment(uint64_t _bytes) {
    ++num;
    bytes += _bytes;
  }

  void increment_stat(const io_stat_t& stat) {
    num += stat.num;
    bytes += stat.bytes;
  }
};
inline std::ostream& operator<<(std::ostream& out, const io_stat_t& stat) {
  return out << stat.num << "(" << stat.bytes << "B)";
}

struct rewrite_stats_t {
  uint64_t num_n_dirty = 0;
  uint64_t num_dirty = 0;
  uint64_t dirty_version = 0;

  bool is_clear() const {
    return (num_n_dirty == 0 && num_dirty == 0);
  }

  uint64_t get_num_rewrites() const {
    return num_n_dirty + num_dirty;
  }

  double get_avg_version() const {
    return static_cast<double>(dirty_version)/num_dirty;
  }

  void account_n_dirty() {
    ++num_n_dirty;
  }

  void account_dirty(extent_version_t v) {
    ++num_dirty;
    dirty_version += v;
  }

  void add(const rewrite_stats_t& o) {
    num_n_dirty += o.num_n_dirty;
    num_dirty += o.num_dirty;
    dirty_version += o.dirty_version;
  }

  void minus(const rewrite_stats_t& o) {
    num_n_dirty -= o.num_n_dirty;
    num_dirty -= o.num_dirty;
    dirty_version -= o.dirty_version;
  }
};

struct rbm_pending_ool_t {
  bool is_conflicted = false;
  std::list<CachedExtentRef> pending_extents;
};

/**
 * Transaction
 *
 * Representation of in-progress mutation. Used exclusively through Cache methods.
 *
 * Transaction log levels:
 * seastore_t
 * - DEBUG: transaction create, conflict, commit events
 * - TRACE: DEBUG details
 * - seastore_cache logs
 */
class Transaction {
public:
  using Ref = std::unique_ptr<Transaction>;
  using on_destruct_func_t = std::function<void(Transaction&)>;
  enum class get_extent_ret {
    PRESENT,
    ABSENT,
    RETIRED
  };
  get_extent_ret get_extent(paddr_t addr, CachedExtentRef *out) {
    LOG_PREFIX(Transaction::get_extent);
    // it's possible that both write_set and retired_set contain
    // this addr at the same time when addr is absolute and the
    // corresponding extent is used to map existing extent on disk.
    // So search write_set first.
    if (auto iter = write_set.find_offset(addr);
	iter != write_set.end()) {
      if (out)
	*out = CachedExtentRef(&*iter);
      SUBTRACET(seastore_cache, "{} is present in write_set -- {}",
                *this, addr, *iter);
      assert(!out || (*out)->is_valid());
      return get_extent_ret::PRESENT;
    } else if (retired_set.count(addr)) {
      return get_extent_ret::RETIRED;
    } else if (
      auto iter = read_set.find(addr);
      iter != read_set.end()) {
      // placeholder in read-set should be in the retired-set
      // at the same time.
      assert(!is_retired_placeholder_type(iter->ref->get_type()));
      if (out)
	*out = iter->ref;
      SUBTRACET(seastore_cache, "{} is present in read_set -- {}",
                *this, addr, *(iter->ref));
      return get_extent_ret::PRESENT;
    } else {
      return get_extent_ret::ABSENT;
    }
  }

  void add_to_retired_set(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    if (ref->is_exist_clean() ||
	ref->is_exist_mutation_pending()) {
      existing_block_stats.dec(ref);
      ref->set_invalid(*this);
      write_set.erase(*ref);
    } else if (ref->is_initial_pending()) {
      ref->set_invalid(*this);
      write_set.erase(*ref);
    } else if (ref->is_mutation_pending()) {
      ref->set_invalid(*this);
      write_set.erase(*ref);
      assert(ref->prior_instance);
      retired_set.emplace(ref->prior_instance, trans_id);
      assert(read_set.count(ref->prior_instance->get_paddr()));
      ref->prior_instance.reset();
    } else {
      // && retired_set.count(ref->get_paddr()) == 0
      // If it's already in the set, insert here will be a noop,
      // which is what we want.
      retired_set.emplace(ref, trans_id);
    }
  }

  // Returns true if added, false if already added or weak
  bool maybe_add_to_read_set(CachedExtentRef ref) {
    if (is_weak()) {
      return false;
    }

    assert(ref->is_valid());

    auto it = ref->transactions.lower_bound(
      this, read_set_item_t<Transaction>::trans_cmp_t());
    if (it != ref->transactions.end() && it->t == this) {
      return false;
    }

    auto [iter, inserted] = read_set.emplace(this, ref);
    ceph_assert(inserted);
    ref->transactions.insert_before(
      it, const_cast<read_set_item_t<Transaction>&>(*iter));
    return true;
  }

  void add_to_read_set(CachedExtentRef ref) {
    if (is_weak()) {
      return;
    }

    assert(ref->is_valid());

    auto it = ref->transactions.lower_bound(
      this, read_set_item_t<Transaction>::trans_cmp_t());
    assert(it == ref->transactions.end() || it->t != this);

    auto [iter, inserted] = read_set.emplace(this, ref);
    ceph_assert(inserted);
    ref->transactions.insert_before(
      it, const_cast<read_set_item_t<Transaction>&>(*iter));
  }

  void add_fresh_extent(
    CachedExtentRef ref) {
    ceph_assert(!is_weak());
    if (ref->is_exist_clean()) {
      existing_block_stats.inc(ref);
      existing_block_list.push_back(ref);
    } else if (ref->get_paddr().is_delayed()) {
      assert(ref->get_paddr() == make_delayed_temp_paddr(0));
      assert(ref->is_logical());
      ref->set_paddr(make_delayed_temp_paddr(delayed_temp_offset));
      delayed_temp_offset += ref->get_length();
      delayed_alloc_list.emplace_back(ref);
      fresh_block_stats.increment(ref->get_length());
    } else if (ref->get_paddr().is_absolute()) {
      pre_alloc_list.emplace_back(ref);
      fresh_block_stats.increment(ref->get_length());
    } else {
      if (likely(ref->get_paddr() == make_record_relative_paddr(0))) {
	ref->set_paddr(make_record_relative_paddr(offset));
      } else {
	ceph_assert(ref->get_paddr().is_fake());
      }
      offset += ref->get_length();
      inline_block_list.push_back(ref);
      fresh_block_stats.increment(ref->get_length());
    }
    write_set.insert(*ref);
    if (is_backref_node(ref->get_type()))
      fresh_backref_extents++;
  }

  uint64_t get_num_fresh_backref() const {
    return fresh_backref_extents;
  }

  void mark_delayed_extent_inline(CachedExtentRef& ref) {
    write_set.erase(*ref);
    assert(ref->get_paddr().is_delayed());
    ref->set_paddr(make_record_relative_paddr(offset),
                   /* need_update_mapping: */ true);
    offset += ref->get_length();
    inline_block_list.push_back(ref);
    write_set.insert(*ref);
  }

  void mark_delayed_extent_ool(CachedExtentRef& ref) {
    ool_block_list.push_back(ref);
  }

  void update_delayed_ool_extent_addr(LogicalCachedExtentRef& ref,
                                      paddr_t final_addr) {
    write_set.erase(*ref);
    assert(ref->get_paddr().is_delayed());
    ref->set_paddr(final_addr, /* need_update_mapping: */ true);
    assert(!ref->get_paddr().is_null());
    assert(!ref->is_inline());
    write_set.insert(*ref);
  }

  void mark_allocated_extent_ool(CachedExtentRef& ref) {
    assert(ref->get_paddr().is_absolute());
    assert(!ref->is_inline());
    ool_block_list.push_back(ref);
  }

  void mark_inplace_rewrite_extent_ool(LogicalCachedExtentRef ref) {
    assert(ref->get_paddr().is_absolute());
    assert(!ref->is_inline());
    inplace_ool_block_list.push_back(ref);
  }

  void add_inplace_rewrite_extent(CachedExtentRef ref) {
   ceph_assert(!is_weak());
   ceph_assert(ref);
   ceph_assert(ref->get_paddr().is_absolute());
   assert(ref->state == CachedExtent::extent_state_t::DIRTY);
   pre_inplace_rewrite_list.emplace_back(ref->cast<LogicalCachedExtent>());
  }

  void add_mutated_extent(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    assert(ref->is_exist_mutation_pending() ||
	   read_set.count(ref->prior_instance->get_paddr()));
    mutated_block_list.push_back(ref);
    if (!ref->is_exist_mutation_pending()) {
      write_set.insert(*ref);
    } else {
      assert(write_set.find_offset(ref->get_paddr()) !=
	     write_set.end());
    }
  }

  void replace_placeholder(CachedExtent& placeholder, CachedExtent& extent) {
    ceph_assert(!is_weak());

    assert(is_retired_placeholder_type(placeholder.get_type()));
    assert(!is_retired_placeholder_type(extent.get_type()));
    assert(!is_root_type(extent.get_type()));
    assert(extent.get_paddr() == placeholder.get_paddr());
    {
      auto where = read_set.find(placeholder.get_paddr());
      assert(where != read_set.end());
      assert(where->ref.get() == &placeholder);
      where = read_set.erase(where);
      auto it = read_set.emplace_hint(where, this, &extent);
      extent.transactions.insert(const_cast<read_set_item_t<Transaction>&>(*it));
    }
    {
      auto where = retired_set.find(&placeholder);
      assert(where != retired_set.end());
      assert(where->extent.get() == &placeholder);
      where = retired_set.erase(where);
      retired_set.emplace_hint(where, &extent, trans_id);
    }
  }

  auto get_delayed_alloc_list() {
    std::list<CachedExtentRef> ret;
    for (auto& extent : delayed_alloc_list) {
      // delayed extents may be invalidated
      if (extent->is_valid()) {
        ret.push_back(std::move(extent));
      } else {
        ++num_delayed_invalid_extents;
      }
    }
    delayed_alloc_list.clear();
    return ret;
  }

  auto get_valid_pre_alloc_list() {
    std::list<CachedExtentRef> ret;
    assert(num_allocated_invalid_extents == 0);
    for (auto& extent : pre_alloc_list) {
      if (extent->is_valid()) {
	ret.push_back(extent);
      } else {
	++num_allocated_invalid_extents;
      }
    }
    for (auto& extent : pre_inplace_rewrite_list) {
      if (extent->is_valid()) {
	ret.push_back(extent);
      } 
    }
    return ret;
  }

  const auto &get_inline_block_list() {
    return inline_block_list;
  }

  bool is_retired(paddr_t paddr, extent_len_t len) {
    auto iter = retired_set.lower_bound(paddr);
    if (iter == retired_set.end()) {
      return false;
    }
    auto &extent = iter->extent;
    if (extent->get_paddr() != paddr) {
      return false;
    } else {
      assert(len == extent->get_length());
      return true;
    }
  }

  template <typename F>
  auto for_each_finalized_fresh_block(F &&f) const {
    std::for_each(ool_block_list.begin(), ool_block_list.end(), f);
    std::for_each(inline_block_list.begin(), inline_block_list.end(), f);
  }

  template <typename F>
  auto for_each_existing_block(F &&f) {
    std::for_each(existing_block_list.begin(), existing_block_list.end(), f);
  }

  const io_stat_t& get_fresh_block_stats() const {
    return fresh_block_stats;
  }

  using src_t = transaction_type_t;
  src_t get_src() const {
    return src;
  }

  bool is_weak() const {
    return weak;
  }

  void test_set_conflict() {
    conflicted = true;
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
    src_t src,
    journal_seq_t initiated_after,
    on_destruct_func_t&& f,
    transaction_id_t trans_id
  ) : weak(weak),
      handle(std::move(handle)),
      on_destruct(std::move(f)),
      src(src),
      trans_id(trans_id)
  {}

  void invalidate_clear_write_set() {
    for (auto &&i: write_set) {
      i.set_invalid(*this);
    }
    write_set.clear();
  }

  ~Transaction() {
    get_handle().exit();
    on_destruct(*this);
    invalidate_clear_write_set();
    views.clear();
  }

  friend class crimson::os::seastore::SeaStore;
  friend class TransactionConflictCondition;

  void reset_preserve_handle(journal_seq_t initiated_after) {
    root.reset();
    offset = 0;
    delayed_temp_offset = 0;
    read_set.clear();
    fresh_backref_extents = 0;
    invalidate_clear_write_set();
    mutated_block_list.clear();
    fresh_block_stats = {};
    num_delayed_invalid_extents = 0;
    num_allocated_invalid_extents = 0;
    delayed_alloc_list.clear();
    inline_block_list.clear();
    ool_block_list.clear();
    inplace_ool_block_list.clear();
    pre_alloc_list.clear();
    pre_inplace_rewrite_list.clear();
    retired_set.clear();
    existing_block_list.clear();
    existing_block_stats = {};
    onode_tree_stats = {};
    omap_tree_stats = {};
    lba_tree_stats = {};
    backref_tree_stats = {};
    ool_write_stats = {};
    rewrite_stats = {};
    conflicted = false;
    if (!has_reset) {
      has_reset = true;
    }
    get_handle().exit();
    views.clear();
  }

  bool did_reset() const {
    return has_reset;
  }

  struct tree_stats_t {
    uint64_t depth = 0;
    uint64_t num_inserts = 0;
    uint64_t num_erases = 0;
    uint64_t num_updates = 0;
    int64_t extents_num_delta = 0;

    bool is_clear() const {
      return (depth == 0 &&
              num_inserts == 0 &&
              num_erases == 0 &&
              num_updates == 0 &&
	      extents_num_delta == 0);
    }
  };
  tree_stats_t& get_onode_tree_stats() {
    return onode_tree_stats;
  }
  tree_stats_t& get_omap_tree_stats() {
    return omap_tree_stats;
  }
  tree_stats_t& get_lba_tree_stats() {
    return lba_tree_stats;
  }
  tree_stats_t& get_backref_tree_stats() {
    return backref_tree_stats;
  }

  struct ool_write_stats_t {
    io_stat_t extents;
    uint64_t md_bytes = 0;
    uint64_t num_records = 0;

    uint64_t get_data_bytes() const {
      return extents.bytes;
    }

    bool is_clear() const {
      return (extents.is_clear() &&
              md_bytes == 0 &&
              num_records == 0);
    }
  };
  ool_write_stats_t& get_ool_write_stats() {
    return ool_write_stats;
  }
  rewrite_stats_t& get_rewrite_stats() {
    return rewrite_stats;
  }

  struct existing_block_stats_t {
    uint64_t valid_num = 0;
    uint64_t clean_num = 0;
    uint64_t mutated_num = 0;
    void inc(const CachedExtentRef &ref) {
      valid_num++;
      if (ref->is_exist_clean()) {
	clean_num++;
      } else {
	mutated_num++;
      }
    }
    void dec(const CachedExtentRef &ref) {
      valid_num--;
      if (ref->is_exist_clean()) {
	clean_num--;
      } else {
	mutated_num--;
      }
    }
  };
  existing_block_stats_t& get_existing_block_stats() {
    return existing_block_stats;
  }

  transaction_id_t get_trans_id() const {
    return trans_id;
  }

  using view_ref = std::unique_ptr<trans_spec_view_t>;
  template <typename T, typename... Args,
	   std::enable_if_t<std::is_base_of_v<trans_spec_view_t, T>, int> = 0>
  T& add_transactional_view(Args&&... args) {
    auto &view = views.emplace_back(
      std::make_unique<T>(std::forward<Args>(args)...));
    return static_cast<T&>(*view);
  }

  void set_pending_ool(seastar::lw_shared_ptr<rbm_pending_ool_t> ptr) {
    pending_ool = ptr;
  }

  seastar::lw_shared_ptr<rbm_pending_ool_t> get_pending_ool() {
    return pending_ool;
  }

  const auto& get_pre_alloc_list() {
    return pre_alloc_list;
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

  device_off_t offset = 0; ///< relative offset of next block
  device_off_t delayed_temp_offset = 0;

  /**
   * read_set
   *
   * Holds a reference (with a refcount) to every extent read via *this.
   * Submitting a transaction mutating any contained extent/addr will
   * invalidate *this.
   */
  read_set_t<Transaction> read_set; ///< set of extents read by paddr

  uint64_t fresh_backref_extents = 0; // counter of new backref extents

  /**
   * write_set
   *
   * Contains a reference (without a refcount) to every extent mutated
   * as part of *this.  No contained extent may be referenced outside
   * of *this.  Every contained extent will be in one of inline_block_list,
   * ool_block_list or/and pre_alloc_list, mutated_block_list,
   * or delayed_alloc_list.
   */
  ExtentIndex write_set;

  /**
   * lists of fresh blocks, holds refcounts, subset of write_set
   */
  io_stat_t fresh_block_stats;
  uint64_t num_delayed_invalid_extents = 0;
  uint64_t num_allocated_invalid_extents = 0;
  /// fresh blocks with delayed allocation,
  /// may become inline_block_list or ool_block_list below
  std::list<CachedExtentRef> delayed_alloc_list;
  /// fresh blocks with pre-allocated addresses with RBM,
  /// should be released upon conflicts,
  /// will be added to ool_block_list below
  std::list<CachedExtentRef> pre_alloc_list;
  /// dirty blocks for inplace rewrite with RBM,
  /// will be added to inplace inplace_ool_block_list below
  std::list<LogicalCachedExtentRef> pre_inplace_rewrite_list;

  /// fresh blocks that will be committed with inline journal record
  std::list<CachedExtentRef> inline_block_list;
  /// fresh blocks that will be committed with out-of-line record
  std::list<CachedExtentRef> ool_block_list;
  /// dirty blocks that will be committed out-of-line with inplace rewrite
  std::list<LogicalCachedExtentRef> inplace_ool_block_list;

  /// list of mutated blocks, holds refcounts, subset of write_set
  std::list<CachedExtentRef> mutated_block_list;

  /// partial blocks of extents on disk, with data and refcounts
  std::list<CachedExtentRef> existing_block_list;
  existing_block_stats_t existing_block_stats;

  std::list<view_ref> views;

  /**
   * retire_set
   *
   * Set of extents retired by *this.
   */
  pextent_set_t retired_set;

  /// stats to collect when commit or invalidate
  tree_stats_t onode_tree_stats;
  tree_stats_t omap_tree_stats; // exclude omap tree depth
  tree_stats_t lba_tree_stats;
  tree_stats_t backref_tree_stats;
  ool_write_stats_t ool_write_stats;
  rewrite_stats_t rewrite_stats;

  bool conflicted = false;

  bool has_reset = false;

  OrderingHandle handle;

  on_destruct_func_t on_destruct;

  const src_t src;

  transaction_id_t trans_id = TRANS_ID_NULL;

  seastar::lw_shared_ptr<rbm_pending_ool_t> pending_ool;
};
using TransactionRef = Transaction::Ref;

/// Should only be used with dummy staged-fltree node extent manager
inline TransactionRef make_test_transaction() {
  static transaction_id_t next_id = 0;
  return std::make_unique<Transaction>(
    get_dummy_ordering_handle(),
    false,
    Transaction::src_t::MUTATE,
    JOURNAL_SEQ_NULL,
    [](Transaction&) {},
    ++next_id
  );
}

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::io_stat_t> : fmt::ostream_formatter {};
#endif
