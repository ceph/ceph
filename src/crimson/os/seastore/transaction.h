// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <chrono>
#include <iostream>
#include <memory_resource>
#include <vector>

#include <boost/intrusive/list.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/backref_entry.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/root_block.h"
#include "crimson/os/seastore/seastore_types.h"

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

struct btree_cursor_stats_t {
  uint64_t num_refresh_parent_total = 0;
  uint64_t num_refresh_invalid_parent = 0;
  uint64_t num_refresh_unviewable_parent = 0;
  uint64_t num_refresh_modified_viewable_parent = 0;

  void apply(btree_cursor_stats_t &stats) {
    num_refresh_parent_total +=
      stats.num_refresh_parent_total;
    num_refresh_invalid_parent +=
      stats.num_refresh_invalid_parent;
    num_refresh_unviewable_parent +=
      stats.num_refresh_unviewable_parent;
    num_refresh_modified_viewable_parent +=
      stats.num_refresh_modified_viewable_parent;
  }
};

struct rbm_pending_ool_t {
  bool is_conflicted = false;
  // Extents of the in-flight write, all on the issuing writer's tier.
  std::vector<CachedExtentRef> pending_extents;
  // On conflict, the pre-allocated extents among pending_extents; freed by
  // the writer once the write completes.
  std::vector<CachedExtentRef> deferred_frees;
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
class Transaction : public boost::intrusive_ref_counter<
  Transaction, boost::thread_unsafe_counter> {
public:
  using Ref = boost::intrusive_ptr<Transaction>;
  using on_destruct_func_t = std::function<void(Transaction&)>;
  enum class get_extent_ret {
    PRESENT,
    ABSENT,
    RETIRED
  };
  get_extent_ret get_extent(paddr_t addr, CachedExtentRef *out) {
    assert(addr.is_real_location() || addr.is_root());
    auto [result, ext] = do_get_extent(addr);
    if (out && result == get_extent_ret::PRESENT) {
      *out = ext;
    }
    return result;
  }

  void add_absent_to_retired_set(CachedExtentRef ref) {
    assert(ref->get_paddr().is_absolute());
    add_present_to_retired_set(ref);
  }

  void add_present_to_retired_set(CachedExtentRef ref) {
    assert(ref->get_paddr().is_real_location());
    assert(!is_weak());
#ifndef NDEBUG
    auto [result, ext] = do_get_extent(ref->get_paddr());
    if (ref->is_stable()) {
      // Writers keep no read set, so the transaction only tracks its
      // pending extents: a stable extent is absent, or already retired.
      assert(result != get_extent_ret::PRESENT);
    } else {
      assert(result == get_extent_ret::PRESENT);
      assert(ext == ref);
    }
#endif
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
      retired_set.emplace(ref->prior_instance, *this);
      ref->reset_prior_instance();
    } else {
      ceph_assert(ref->is_stable());
      ceph_assert(ref->get_paddr().is_absolute());
      // XXX: prevent double retire -- retired_set.count(ref->get_paddr()) == 0
      // If it's already in the set, insert here will be a noop,
      // which is what we want.
      retired_set.emplace(ref, *this);
    }
  }

  // Writers keep no read set (single writer per shard). For a READ
  // transaction these hooks record the extents it accesses, see
  // is_reader_stale().
  struct maybe_add_readset_ret {
    bool added = false;
    bool is_paddr_known = true;
  };
  // Without a read set, a repeated access can't be told apart from the
  // first one: report every access as added, so that the caller counts it
  // as a cache hit and touches the extent in the LRU. A record-relative
  // paddr is only known once the extent's io completes, the caller then
  // touches it in step 2.
  maybe_add_readset_ret maybe_add_to_read_set(CachedExtentRef ref) {
    assert(ref->is_stable());
    assert(ref->get_paddr().is_absolute()
           || ref->get_paddr().is_record_relative());
    if (is_weak()) {
      return {false, true /* meaningless */};
    }
    bool is_paddr_known = ref->get_paddr().is_absolute();
    reader_track(std::move(ref));
    return {true, is_paddr_known};
  }

  bool is_in_read_set(CachedExtentRef extent) const {
    return false;
  }

  void add_to_read_set(CachedExtentRef ref) {
    reader_track(std::move(ref));
  }
  void maybe_add_to_read_set_step_2(CachedExtentRef ref) {
    reader_track(std::move(ref));
  }

  /**
   * Reader validation
   *
   * Writers never conflict, as there is a single writer per shard (see
   * writer_gate_t). READ transactions still run concurrently with the
   * writer, and a commit may retire or replace an extent a reader is using
   * while it waits. A reader remembers the extents it accessed, and is
   * stale once one of them got invalidated. TransactionConflictCondition
   * then interrupts it at its next continuation, and the repeat_eagain
   * loops of the read paths retry it with a new transaction.
   *
   * Nothing is tracked on the extent side, and the writer only bumps
   * Cache's invalidation epoch. A reader rescans its extents only when that
   * epoch moved since its last check.
   *
   * Weak transactions are not validated.
   */
  bool is_validated_reader() const {
    return src == src_t::READ && !weak;
  }

  bool is_reader_stale() {
    if (!is_validated_reader() || !invalidation_epoch) {
      return false;
    }
    if (reader_stale) {
      return true;
    }
    if (*invalidation_epoch == reader_checked_epoch) {
      return false;
    }
    reader_checked_epoch = *invalidation_epoch;
    for (auto &e : reader_extents) {
      if (!e->is_valid()) {
        reader_stale = true;
        return true;
      }
    }
    return false;
  }

  /// Set by Cache when creating the transaction
  void set_invalidation_epoch(const uint64_t *epoch) {
    invalidation_epoch = epoch;
    reader_checked_epoch = *epoch;
  }

  void add_fresh_extent(
    CachedExtentRef ref) {
    assert(ref->get_paddr().is_real_location());
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
    } else if (ref->get_paddr().is_absolute_random_block()) {
      pre_alloc_list.emplace_back(ref);
      fresh_block_stats.increment(ref->get_length());
    } else {
#ifdef UNIT_TESTS_BUILT
      if (likely(ref->get_paddr() == make_record_relative_paddr(0))) {
	ref->set_paddr(make_record_relative_paddr(offset));
      } else {
	ceph_assert(ref->get_paddr().is_fake());
      }
#else
      assert(ref->get_paddr() == make_record_relative_paddr(0));
      ref->set_paddr(make_record_relative_paddr(offset));
#endif
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
    assert(ref->get_paddr().is_delayed());
    ool_block_list.push_back(ref);
  }

  void update_delayed_ool_extent_addr(LogicalCachedExtentRef& ref,
                                      paddr_t final_addr) {
    assert(ref->get_paddr().is_delayed());
    assert(final_addr.is_absolute());
    write_set.erase(*ref);
    ref->set_paddr(final_addr, /* need_update_mapping: */ true);
    write_set.insert(*ref);
  }

  void mark_allocated_extent_ool(CachedExtentRef& ref) {
    assert(ref->get_paddr().is_absolute());
    ool_block_list.push_back(ref);
  }

  void mark_inplace_rewrite_extent_ool(LogicalCachedExtentRef ref) {
    assert(ref->get_paddr().is_absolute_random_block());
    inplace_ool_block_list.push_back(ref);
  }

  void add_inplace_rewrite_extent(CachedExtentRef ref) {
   ceph_assert(!is_weak());
   ceph_assert(ref);
   ceph_assert(ref->get_paddr().is_absolute_random_block());
   assert(ref->state == CachedExtent::extent_state_t::DIRTY);
   pre_inplace_rewrite_list.emplace_back(ref->cast<LogicalCachedExtent>());
  }

  void add_mutated_extent(CachedExtentRef ref) {
    ceph_assert(!is_weak());
    assert(ref->get_paddr().is_absolute() ||
           ref->get_paddr().is_root());
    mutated_block_list.push_back(ref);
    if (ref->is_mutation_pending()) {
      write_set.insert(*ref);
    } else {
      assert(ref->is_exist_mutation_pending());
      // already added as fresh extent in write_set
      assert(write_set.exists(*ref));
    }
  }

  std::vector<CachedExtentRef> get_delayed_alloc_list() {
    std::vector<CachedExtentRef> ret;
    ret.reserve(delayed_alloc_list.size());
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

  std::vector<CachedExtentRef> get_valid_pre_alloc_list() {
    std::vector<CachedExtentRef> ret;
    ret.reserve(pre_alloc_list.size() + pre_inplace_rewrite_list.size());
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

  bool is_stable_extent_retired(paddr_t paddr, extent_len_t len) {
    assert(paddr.is_absolute());
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

  bool remove_from_retired_set(CachedExtent &ext) {
    auto it = retired_set.find(ext.get_paddr());
    if (it == retired_set.end()) {
      return false;
    }
    auto &extent = it->extent;
    if (extent->get_paddr() != ext.get_paddr()) {
      return false;
    } else {
      assert(ext.get_length() == extent->get_length());
      retired_set.erase(it);
      return true;
    }
  }

  void maybe_update_pending_paddr(
    const paddr_t &old_paddr,
    const paddr_t &new_paddr,
    extent_len_t len) {
    assert(new_paddr.is_absolute());

    std::vector<CachedExtent*> exts;
    for (auto [bottom, top] = write_set.get_overlap(old_paddr, len);
         bottom != top;
         bottom++) {
      auto &mextent = *bottom;
      if (mextent.is_initial_pending()) {
        continue;
      }
      exts.emplace_back(&mextent);
    }
    for (auto i : exts) {
      auto &mextent = *i;
      write_set.erase(mextent);
      extent_len_t off = 0;
      if (old_paddr.is_absolute_segmented()) {
        assert(mextent.get_paddr().as_seg_paddr().get_segment_id() ==
          old_paddr.as_seg_paddr().get_segment_id());
        assert(mextent.get_paddr().as_seg_paddr().get_segment_off() >=
          old_paddr.as_seg_paddr().get_segment_off());
        assert(mextent.get_paddr().as_seg_paddr().get_segment_off()
                + mextent.get_length() <=
          old_paddr.as_seg_paddr().get_segment_off() + len);
        off = mextent.get_paddr().as_seg_paddr().get_segment_off() -
          old_paddr.as_seg_paddr().get_segment_off();
      } else {
        assert(mextent.get_paddr().is_absolute_random_block());
        off = mextent.get_paddr().as_blk_paddr().get_device_off() -
          old_paddr.as_blk_paddr().get_device_off();
      }
      mextent.set_paddr(new_paddr + off);
      write_set.insert(mextent);
    }
  }
  void remove_shadow_from_write_set(
    const paddr_t &shadow_paddr, extent_len_t len) {
    std::vector<CachedExtent*> exts;
    for (auto [bottom, top] = write_set.get_overlap(shadow_paddr, len);
         bottom != top;
         bottom++) {
      auto &mextent = *bottom;
      if (mextent.is_initial_pending()) {
        assert(!mextent.is_shadow_extent());
        continue;
      }
      assert(mextent.is_shadow_extent() && mextent.is_exist_clean());
      exts.emplace_back(&mextent);
    }
    for (auto i :exts) {
      write_set.erase(*i);
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

  template <typename F>
  void for_each_mutated_extent(F &&f) {
    std::for_each(
      retired_set.begin(),
      retired_set.end(),
      [&f](auto &link) {
        std::invoke(f, *link.extent);
      });
    std::for_each(
      mutated_block_list.begin(),
      mutated_block_list.end(),
      [&f](auto &e) {
        if (!e->is_valid() ||
            e->is_exist_mutation_pending()) {
          return;
        }
        std::invoke(f, *e->get_prior_instance());
      });
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

  void test_set_conflict() {}

  bool is_conflicted() const {
    return false;
  }

  std::size_t get_num_replays() const {
    return 0;
  }

  // Time spent in each sub-phase of submit_transaction, accumulated across retries
  struct phase_durations_t {
    std::chrono::steady_clock::duration reserve{0};         // enter reserve + epm reserve
    std::chrono::steady_clock::duration ool_write{0};       // delayed + preallocated OOL writes
    std::chrono::steady_clock::duration lba_update{0};      // update_lba_mappings
    std::chrono::steady_clock::duration prepare_enter{0};   // enter(prepare) pipeline stage
    std::chrono::steady_clock::duration prepare_record{0};  // prepare_record
    std::chrono::steady_clock::duration journal{0};         // journal->submit_record (post-lock)
  };
  phase_durations_t &get_phase_durations() {
    return phase_durations;
  }

  auto &get_handle() {
    return handle;
  }

  Transaction(
    OrderingHandle &&handle,
    bool weak,
    src_t src,
    on_destruct_func_t&& f,
    transaction_id_t trans_id,
    cache_hint_t cache_hint,
    bool read_set_enabled = false
  ) : weak(weak),
      read_set_enabled(read_set_enabled),
      handle(std::move(handle)),
      on_destruct(std::move(f)),
      src(src),
      trans_id(trans_id),
      cache_hint(cache_hint)
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

  uint64_t write_hit_hot = 0;
  uint64_t write_hit_cold = 0;
  uint64_t read_hit_hot = 0;
  uint64_t read_hit_cold = 0;

  void reset_preserve_handle() {
    write_hit_hot = 0;
    write_hit_cold = 0;
    read_hit_hot = 0;
    read_hit_cold = 0;
    root.reset();
    offset = 0;
    delayed_temp_offset = 0;
    clear_read_set();
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
    pending_ool = nullptr;
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
    force_rewrite_invalidate = false;
    assert(backref_entries.empty());
    if (!has_reset) {
      has_reset = true;
    }
    get_handle().exit();
    views.clear();
    copied_lba_keys.clear();
    update_copied_lba_key = nullptr;
    touched_prefix.clear();
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
    uint64_t lookup_count = 0;
    uint64_t nodes_visited = 0;
    uint64_t string_cmp_count = 0;

    bool is_clear() const {
      return (depth == 0 &&
              num_inserts == 0 &&
              num_erases == 0 &&
              num_updates == 0 &&
	      extents_num_delta == 0 &&
              lookup_count == 0 &&
              nodes_visited == 0 &&
              string_cmp_count == 0);
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

  // Paired with clear_pending_ool() when the OOL write completes.
  void set_pending_ool(seastar::lw_shared_ptr<rbm_pending_ool_t> ptr) {
    assert(!pending_ool);
    pending_ool = ptr;
  }

  void clear_pending_ool() {
    pending_ool = nullptr;
  }

  seastar::lw_shared_ptr<rbm_pending_ool_t> get_pending_ool() {
    return pending_ool;
  }

  const auto& get_pre_alloc_list() {
    return pre_alloc_list;
  }

  cache_hint_t get_cache_hint() const {
    return cache_hint;
  }

  void touch_laddr_prefix(laddr_t laddr) {
    touched_prefix.insert(laddr.get_object_prefix());
  }

  std::unordered_set<laddr_t> &get_touched_laddr_prefix() {
    return touched_prefix;
  }

  btree_cursor_stats_t cursor_stats;

  /**
   * force_rewrite_invalidate
   *
   * Set by a background rewrite that changes the LBA tree's shape, e.g.
   * a logical range rewritten as multiple extents. Its commit then
   * replaces and invalidates the priors instead of taking the no-conflict
   * publish path, which keeps priors valid and isn't safe for concurrent
   * readers when the tree changes shape. Readers holding an invalidated
   * prior restart, see is_reader_stale().
   */
  bool force_rewrite_invalidate = false;

  struct lmapping_t {
    laddr_t dest = L_ADDR_NULL;
    extent_len_t len = 0;
  };
  using update_copied_lba_key_func_t =
    std::function<void (laddr_t, paddr_t,
                  extent_len_t, std::optional<paddr_t>)>;
  void new_lba_key_copied(
    laddr_t src,
    laddr_t dest,
    extent_len_t len,
    update_copied_lba_key_func_t &&func) {
    copied_lba_keys.emplace(src, lmapping_t{dest, len});
    if (!update_copied_lba_key) {
      update_copied_lba_key = std::move(func);
    }
  }
  void maybe_sync_copied_lba_key(
    laddr_t laddr, paddr_t paddr, std::optional<paddr_t> shadow) {
    if (likely(copied_lba_keys.empty())) {
      return;
    }
    assert(update_copied_lba_key);
    auto it = copied_lba_keys.find(laddr);
    if (it == copied_lba_keys.end()) {
      return;
    }
    laddr_t key = it->second.dest;
    extent_len_t len = it->second.len;
    update_copied_lba_key(key, paddr, len, shadow);
  }
  RootBlockRef peek_root() {
    return root;
  }
private:
  friend class Cache;
  friend Ref make_test_transaction();

  void clear_read_set() {
    reader_extents.clear();
    reader_stale = false;
    if (invalidation_epoch) {
      reader_checked_epoch = *invalidation_epoch;
    }
  }

  void reader_track(CachedExtentRef ref) {
    if (!is_validated_reader()) {
      return;
    }
    // consecutive accesses to the same extent are common
    if (!reader_extents.empty() && reader_extents.back() == ref) {
      return;
    }
    reader_extents.emplace_back(std::move(ref));
  }

  /// See is_reader_stale()
  const uint64_t *invalidation_epoch = nullptr;
  uint64_t reader_checked_epoch = 0;
  bool reader_stale = false;
  std::vector<CachedExtentRef> reader_extents;

  std::pair<get_extent_ret, CachedExtentRef> do_get_extent(paddr_t addr) {
    LOG_PREFIX(Transaction::do_get_extent);
    // it's possible that both write_set and retired_set contain
    // this addr at the same time when addr is absolute and the
    // corresponding extent is used to map existing extent on disk.
    // So search write_set first.
    if (auto iter = write_set.find_offset(addr);
	iter != write_set.end()) {
      auto ret = CachedExtentRef(&*iter);
      SUBTRACET(seastore_cache, "{} is present in write_set -- {}",
                *this, addr, *ret);
      assert(ret->is_valid());
      return {get_extent_ret::PRESENT, ret};
    } else if (retired_set.count(addr)) {
      return {get_extent_ret::RETIRED, nullptr};
    } else {
      return {get_extent_ret::ABSENT, nullptr};
    }
  }

  void set_backref_entries(backref_entry_refs_t&& entries) {
    assert(backref_entries.empty());
    backref_entries = std::move(entries);
  }

  backref_entry_refs_t move_backref_entries() {
    return std::move(backref_entries);
  }

  /**
   * If set, *this may not be used to perform writes.
   */
  const bool weak;
  const bool read_set_enabled = false;

  RootBlockRef root;        ///< ref to root if read or written by transaction

  device_off_t offset = 0; ///< relative offset of next block
  device_off_t delayed_temp_offset = 0;

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
   *
   * Backed by a small inline arena so that typical transactions
   * (fewer than ~200 total entries across all lists) never touch
   * the heap for list storage.  The monotonic_buffer_resource falls
   * back to upstream (new/delete) for very large transactions.
   */
  io_stat_t fresh_block_stats;
  uint64_t num_delayed_invalid_extents = 0;
  uint64_t num_allocated_invalid_extents = 0;

  static constexpr std::size_t BLOCK_LIST_ARENA_BYTES = 4096;
  alignas(std::max_align_t)
    std::byte block_list_arena_[BLOCK_LIST_ARENA_BYTES];
  std::pmr::monotonic_buffer_resource block_list_mr_{
    block_list_arena_, BLOCK_LIST_ARENA_BYTES};

  /// fresh blocks with delayed allocation,
  /// may become inline_block_list or ool_block_list below
  std::pmr::vector<CachedExtentRef> delayed_alloc_list{&block_list_mr_};
  /// fresh blocks with pre-allocated addresses with RBM,
  /// should be released upon conflicts,
  /// will be added to ool_block_list below
  std::pmr::vector<CachedExtentRef> pre_alloc_list{&block_list_mr_};
  /// dirty blocks for inplace rewrite with RBM,
  /// will be added to inplace inplace_ool_block_list below
  std::pmr::vector<LogicalCachedExtentRef>
    pre_inplace_rewrite_list{&block_list_mr_};

  /// fresh blocks that will be committed with inline journal record
  std::pmr::vector<CachedExtentRef> inline_block_list{&block_list_mr_};
  /// fresh blocks that will be committed with out-of-line record
  std::pmr::vector<CachedExtentRef> ool_block_list{&block_list_mr_};
  /// dirty blocks that will be committed out-of-line with inplace rewrite
  std::pmr::vector<LogicalCachedExtentRef>
    inplace_ool_block_list{&block_list_mr_};

  /// list of mutated blocks, holds refcounts, subset of write_set
  std::pmr::vector<CachedExtentRef> mutated_block_list{&block_list_mr_};

  /// partial blocks of extents on disk, with data and refcounts
  std::pmr::vector<CachedExtentRef> existing_block_list{&block_list_mr_};
  existing_block_stats_t existing_block_stats;

  std::list<view_ref> views;

  /**
   * retire_set
   *
   * Set of extents retired by *this.
   */
  retired_extent_set_t retired_set;

  std::unordered_set<laddr_t> touched_prefix;

  /// stats to collect when commit or invalidate
  tree_stats_t onode_tree_stats;
  tree_stats_t omap_tree_stats; // exclude omap tree depth
  tree_stats_t lba_tree_stats;
  tree_stats_t backref_tree_stats;
  ool_write_stats_t ool_write_stats;
  rewrite_stats_t rewrite_stats;

  phase_durations_t phase_durations;

  bool has_reset = false;

  OrderingHandle handle;

  on_destruct_func_t on_destruct;

  const src_t src;

  transaction_id_t trans_id = TRANS_ID_NULL;

  seastar::lw_shared_ptr<rbm_pending_ool_t> pending_ool;

  backref_entry_refs_t backref_entries;

  cache_hint_t cache_hint = CACHE_HINT_TOUCH;

  std::map<laddr_t, lmapping_t> copied_lba_keys;
  update_copied_lba_key_func_t update_copied_lba_key;
};
using TransactionRef = Transaction::Ref;

/// Should only be used with dummy staged-fltree node extent manager
inline TransactionRef make_test_transaction() {
  static transaction_id_t next_id = 0;
  return boost::intrusive_ptr(new Transaction(
    get_dummy_ordering_handle(),
    false,
    Transaction::src_t::MUTATE,
    [](Transaction&) {},
    ++next_id,
    CACHE_HINT_TOUCH)
  );
}
/**
 * should_use_no_conflict_publish()
 *
 * Returns true when this (transaction source, extent type) pair should take
 * the no-conflict publish path (i.e avoid invalidate-and-retry and use the
 * committer + visibility hand-off).
 *
 * Currently true for:
 *  - rewrite (background) transactions, for any non-root extent
 *
 *  To be expanded to:
 *  - user (txn_manager) transactions that mutate LBA nodes
 *  - Onode/Omap nodes
 */
constexpr bool should_use_no_conflict_publish(const Transaction &t,
                                              extent_types_t ext_type) {
  if (is_root_type(ext_type)) {
    return false;
  }
  return !t.force_rewrite_invalidate && is_rewrite_transaction(t.get_src());
}

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::io_stat_t> : fmt::ostream_formatter {};
#endif
