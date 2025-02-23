// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "seastar/core/shared_future.hh"

#include "include/buffer.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_interruptor.h"

struct btree_lba_manager_test;
struct lba_btree_test;
struct btree_test_base;
struct cache_test_t;

namespace crimson::os::seastore {

class CachedExtent;
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;
class SegmentedAllocator;
class TransactionManager;
class ExtentPlacementManager;

// #define DEBUG_CACHED_EXTENT_REF
#ifdef DEBUG_CACHED_EXTENT_REF

void intrusive_ptr_add_ref(CachedExtent *);
void intrusive_ptr_release(CachedExtent *);

#endif

// Note: BufferSpace::to_full_ptr() also creates extent ptr.

inline ceph::bufferptr create_extent_ptr_rand(extent_len_t len) {
  assert(is_aligned(len, CEPH_PAGE_SIZE));
  assert(len > 0);
  return ceph::bufferptr(buffer::create_page_aligned(len));
}

inline ceph::bufferptr create_extent_ptr_zero(extent_len_t len) {
  auto bp = create_extent_ptr_rand(len);
  bp.zero();
  return bp;
}

template <typename T>
using TCachedExtentRef = boost::intrusive_ptr<T>;

/**
 * CachedExtent
 */
namespace onode {
  class DummyNodeExtent;
  class TestReplayExtent;
}

template <typename T>
class read_set_item_t {
  using set_hook_t = boost::intrusive::set_member_hook<
    boost::intrusive::link_mode<
      boost::intrusive::auto_unlink>>;
  set_hook_t trans_hook;
  using set_hook_options = boost::intrusive::member_hook<
    read_set_item_t,
    set_hook_t,
    &read_set_item_t::trans_hook>;

public:
  struct cmp_t {
    using is_transparent = paddr_t;
    bool operator()(const read_set_item_t<T> &lhs, const read_set_item_t &rhs) const;
    bool operator()(const paddr_t &lhs, const read_set_item_t<T> &rhs) const;
    bool operator()(const read_set_item_t<T> &lhs, const paddr_t &rhs) const;
  };

  struct trans_cmp_t {
    bool operator()(
      const read_set_item_t<Transaction> &lhs,
      const read_set_item_t<Transaction> &rhs) const {
      return lhs.t < rhs.t;
    }
    bool operator()(
      const Transaction *lhs,
      const read_set_item_t<Transaction> &rhs) const {
      return lhs < rhs.t;
    }
    bool operator()(
      const read_set_item_t<Transaction> &lhs,
      const Transaction *rhs) const {
      return lhs.t < rhs;
    }
  };

  using trans_set_t =  boost::intrusive::set<
    read_set_item_t,
    set_hook_options,
    boost::intrusive::constant_time_size<false>,
    boost::intrusive::compare<trans_cmp_t>>;

  T *t = nullptr;
  CachedExtentRef ref;

  read_set_item_t(T *t, CachedExtentRef ref);
  read_set_item_t(const read_set_item_t &) = delete;
  read_set_item_t(read_set_item_t &&) = default;
  ~read_set_item_t() = default;
};
template <typename T>
using read_set_t = std::set<
  read_set_item_t<T>,
  typename read_set_item_t<T>::cmp_t>;

struct trans_spec_view_t {
  // if the extent is pending, contains the id of the owning transaction;
  // TRANS_ID_NULL otherwise
  transaction_id_t pending_for_transaction = TRANS_ID_NULL;
  trans_spec_view_t() = default;
  trans_spec_view_t(transaction_id_t id) : pending_for_transaction(id) {}
  virtual ~trans_spec_view_t() = default;

  struct cmp_t {
    bool operator()(
      const trans_spec_view_t &lhs,
      const trans_spec_view_t &rhs) const
    {
      return lhs.pending_for_transaction < rhs.pending_for_transaction;
    }
    bool operator()(
      const transaction_id_t &lhs,
      const trans_spec_view_t &rhs) const
    {
      return lhs < rhs.pending_for_transaction;
    }
    bool operator()(
      const trans_spec_view_t &lhs,
      const transaction_id_t &rhs) const
    {
      return lhs.pending_for_transaction < rhs;
    }
  };

  using trans_view_hook_t =
    boost::intrusive::set_member_hook<
      boost::intrusive::link_mode<
        boost::intrusive::auto_unlink>>;
  trans_view_hook_t trans_view_hook;

  using trans_view_member_options =
    boost::intrusive::member_hook<
      trans_spec_view_t,
      trans_view_hook_t,
      &trans_spec_view_t::trans_view_hook>;
  using trans_view_set_t = boost::intrusive::set<
    trans_spec_view_t,
    trans_view_member_options,
    boost::intrusive::constant_time_size<false>,
    boost::intrusive::compare<cmp_t>>;
};

struct load_range_t {
  extent_len_t offset;
  ceph::bufferptr ptr;

  extent_len_t get_length() const {
    return ptr.length();
  }

  extent_len_t get_end() const {
    extent_len_t end = offset + ptr.length();
    assert(end > offset);
    return end;
  }
};
struct load_ranges_t {
  extent_len_t length = 0;
  std::list<load_range_t> ranges;

  void push_back(extent_len_t offset, ceph::bufferptr ptr) {
    assert(ranges.empty() ||
           (ranges.back().get_end() < offset));
    assert(ptr.length());
    length += ptr.length();
    ranges.push_back({offset, std::move(ptr)});
  }
};

/// manage small chunks of extent
class BufferSpace {
  using map_t = std::map<extent_len_t, ceph::bufferlist>;
public:
  BufferSpace() = default;

  /// Returns true if offset~length is fully loaded
  bool is_range_loaded(extent_len_t offset, extent_len_t length) const;

  /// Returns the bufferlist of offset~length
  ceph::bufferlist get_buffer(extent_len_t offset, extent_len_t length) const;

  /// Returns the ranges to load, merge the buffer_map if possible
  load_ranges_t load_ranges(extent_len_t offset, extent_len_t length);

  /// Converts to ptr when fully loaded
  ceph::bufferptr to_full_ptr(extent_len_t length);

private:
  // create and append the read-hole to
  // load_ranges_t and bl
  static void create_hole_append_bl(
      load_ranges_t& ret,
      ceph::bufferlist& bl,
      extent_len_t hole_offset,
      extent_len_t hole_length) {
    ceph::bufferptr hole_ptr = create_extent_ptr_rand(hole_length);
    bl.append(hole_ptr);
    ret.push_back(hole_offset, std::move(hole_ptr));
  }

  // create and insert the read-hole to buffer_map,
  // and append to load_ranges_t
  // returns the iterator containing the inserted read-hole
  auto create_hole_insert_map(
      load_ranges_t& ret,
      extent_len_t hole_offset,
      extent_len_t hole_length,
      const map_t::const_iterator& next_it) {
    assert(!buffer_map.contains(hole_offset));
    ceph::bufferlist bl;
    create_hole_append_bl(ret, bl, hole_offset, hole_length);
    auto it = buffer_map.insert(
        next_it, std::pair{hole_offset, std::move(bl)});
    assert(next_it == std::next(it));
    return it;
  }

  /// extent offset -> buffer, won't overlap nor contiguous
  map_t buffer_map;
};

class ExtentIndex;
class CachedExtent
  : public boost::intrusive_ref_counter<
      CachedExtent, boost::thread_unsafe_counter>,
    public trans_spec_view_t {
  enum class extent_state_t : uint8_t {
    INITIAL_WRITE_PENDING, // In Transaction::write_set and fresh_block_list
    MUTATION_PENDING,      // In Transaction::write_set and mutated_block_list
    CLEAN_PENDING,         // CLEAN, but not yet read out
    CLEAN,                 // In Cache::extent_index, Transaction::read_set
                           //  during write, contents match disk, version == 0
    DIRTY,                 // Same as CLEAN, but contents do not match disk,
                           //  version > 0
    EXIST_CLEAN,           // Similar to CLEAN, but its metadata not yet
			   //  persisted to disk.
    			   //  In Transaction::write_set and existing_block_list.
			   //  After transaction commits, state becomes CLEAN
			   //  and add extent to Cache. Modifing such extents
			   //  will cause state turn to EXIST_MUTATION_PENDING.
    EXIST_MUTATION_PENDING,// Similar to MUTATION_PENDING, but its prior_instance
			   //  is empty.
			   //  In Transaction::write_set, existing_block_list and
			   //  mutated_block_list. State becomes DIRTY and it is
			   //  added to Cache after transaction commits.
    INVALID                // Part of no ExtentIndex set
  } state = extent_state_t::INVALID;
  friend std::ostream &operator<<(std::ostream &, extent_state_t);
  // allow a dummy extent to pretend it is at a specific state
  friend class onode::DummyNodeExtent;
  friend class onode::TestReplayExtent;

  uint32_t last_committed_crc = 0;

  // Points at current version while in state MUTATION_PENDING
  CachedExtentRef prior_instance;

  // time of the last modification
  sea_time_point modify_time = NULL_TIME;

public:
  void init(extent_state_t _state,
            paddr_t paddr,
            placement_hint_t hint,
            rewrite_gen_t gen,
	    transaction_id_t trans_id) {
    assert(gen == NULL_GENERATION || is_rewrite_generation(gen));
    state = _state;
    set_paddr(paddr);
    user_hint = hint;
    rewrite_generation = gen;
    pending_for_transaction = trans_id;
  }

  void set_modify_time(sea_time_point t) {
    modify_time = t;
  }

  sea_time_point get_modify_time() const {
    return modify_time;
  }

  /**
   *  duplicate_for_write
   *
   * Implementation should return a fresh CachedExtentRef
   * which represents a copy of *this until on_delta_write()
   * is complete, at which point the user may assume *this
   * will be in state INVALID.  As such, the implementation
   * may involve a copy of get_bptr(), or an ancillary
   * structure which defers updating the actual buffer until
   * on_delta_write().
   */
  virtual CachedExtentRef duplicate_for_write(Transaction &t) = 0;

  /**
   * prepare_write
   *
   * Called prior to reading buffer.
   * Implemenation may use this callback to fully write out
   * updates to the buffer.
   */
  virtual void prepare_write() {}

  /**
   * prepare_commit
   *
   * Called prior to committing the transaction in which this extent
   * is living.
   */
  virtual void prepare_commit() {}

  /**
   * on_initial_write
   *
   * Called after commit of extent.  State will be CLEAN.
   * Implentation may use this call to fixup the buffer
   * with the newly available absolute get_paddr().
   */
  virtual void on_initial_write() {}

  /**
   * on_fully_loaded
   *
   * Called when ptr is ready. Normally this should be used to initiate
   * the extent to be identical to CachedExtent(ptr).
   *
   * Note this doesn't mean the content is fully read, use on_clean_read for
   * this purpose.
   */
  virtual void on_fully_loaded() {}

  /**
   * on_clean_read
   *
   * Called after read of initially written extent.
   *  State will be CLEAN. Implentation may use this
   * call to fixup the buffer with the newly available
   * absolute get_paddr().
   */
  virtual void on_clean_read() {}

  /**
   * on_delta_write
   *
   * Called after commit of delta.  State will be DIRTY.
   * Implentation may use this call to fixup any relative
   * references in the the buffer with the passed
   * record_block_offset record location.
   */
  virtual void on_delta_write(paddr_t record_block_offset) {}

  /**
   * on_replace_prior
   *
   * Called after the extent has replaced a previous one. State
   * of the extent must be MUTATION_PENDING. Implementation
   * may use this call to synchronize states that must be synchronized
   * with the states of Cache and can't wait till transaction
   * completes.
   */
  virtual void on_replace_prior() {}

  /**
   * on_invalidated
   *
   * Called after the extent is invalidated, either by Cache::invalidate_extent
   * or Transaction::add_to_retired_set. Implementation may use this
   * call to adjust states that must be changed immediately once
   * invalidated.
   */
  virtual void on_invalidated(Transaction &t) {}
  /**
   * get_type
   *
   * Returns concrete type.
   */
  virtual extent_types_t get_type() const = 0;

  virtual bool is_logical() const {
    return false;
  }

  virtual bool may_conflict() const {
    return true;
  }

  void rewrite(Transaction &t, CachedExtent &e, extent_len_t o) {
    assert(is_initial_pending());
    if (!e.is_pending()) {
      prior_instance = &e;
    } else {
      assert(e.is_mutation_pending());
      prior_instance = e.get_prior_instance();
    }
    e.get_bptr().copy_out(
      o,
      get_length(),
      get_bptr().c_str());
    set_modify_time(e.get_modify_time());
    set_last_committed_crc(e.get_last_committed_crc());
    on_rewrite(t, e, o);
  }

  /**
   * on_rewrite
   *
   * Called when this extent is rewriting another one.
   *
   */
  virtual void on_rewrite(Transaction &, CachedExtent &, extent_len_t) = 0;

  friend std::ostream &operator<<(std::ostream &, extent_state_t);
  virtual std::ostream &print_detail(std::ostream &out) const { return out; }
  std::ostream &print(std::ostream &out) const {
    std::string prior_poffset_str = prior_poffset
      ? fmt::format("{}", *prior_poffset)
      : "nullopt";
    out << "CachedExtent(addr=" << this
	<< ", type=" << get_type()
	<< ", trans=" << pending_for_transaction
	<< ", pending_io=" << is_pending_io()
	<< ", version=" << version
	<< ", dirty_from_or_retired_at=" << dirty_from_or_retired_at
	<< ", modify_time=" << sea_time_point_printer_t{modify_time}
	<< ", paddr=" << get_paddr()
	<< ", prior_paddr=" << prior_poffset_str
	<< std::hex << ", length=0x" << get_length()
	<< ", loaded=0x" << get_loaded_length() << std::dec
	<< ", state=" << state
	<< ", last_committed_crc=" << last_committed_crc
	<< ", refcount=" << use_count()
	<< ", user_hint=" << user_hint
	<< ", rewrite_gen=" << rewrite_gen_printer_t{rewrite_generation};
    if (state != extent_state_t::INVALID &&
        state != extent_state_t::CLEAN_PENDING) {
      print_detail(out);
    }
    return out << ")";
  }

  /**
   * get_delta
   *
   * Must return a valid delta usable in apply_delta() in submit_transaction
   * if state == MUTATION_PENDING.
   */
  virtual ceph::bufferlist get_delta() = 0;

  /**
   * apply_delta
   *
   * bl is a delta obtained previously from get_delta.  The versions will
   * match.  Implementation should mutate buffer based on bl.  base matches
   * the address passed on_delta_write.
   *
   * Implementation *must* use set_last_committed_crc to update the crc to
   * what the crc of the buffer would have been at submission.  For physical
   * extents that use base to adjust internal record-relative deltas, this
   * means that the crc should be of the buffer after applying the delta,
   * but before that adjustment.  We do it this way because the crc in the
   * commit path does not yet know the record base address.
   *
   * LogicalCachedExtent overrides this method and provides a simpler
   * apply_delta override for LogicalCachedExtent implementers.
   */
  virtual void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &bl) = 0;

  /**
   * Called on dirty CachedExtent implementation after replay.
   * Implementation should perform any reads/in-memory-setup
   * necessary. (for instance, the lba implementation will use this
   * to load in lba_manager blocks)
   */
  using complete_load_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual complete_load_ertr::future<> complete_load() {
    return complete_load_ertr::now();
  }

  /**
   * cast
   *
   * Returns a TCachedExtentRef of the specified type.
   * TODO: add dynamic check that the requested type is actually correct.
   */
  template <typename T>
  TCachedExtentRef<T> cast() {
    return TCachedExtentRef<T>(static_cast<T*>(this));
  }
  template <typename T>
  TCachedExtentRef<const T> cast() const {
    return TCachedExtentRef<const T>(static_cast<const T*>(this));
  }

  /// Returns true if extent can be mutated in an open transaction
  bool is_mutable() const {
    return state == extent_state_t::INITIAL_WRITE_PENDING ||
      state == extent_state_t::MUTATION_PENDING ||
      state == extent_state_t::EXIST_MUTATION_PENDING;
  }

  /// Returns true if extent is part of an open transaction
  bool is_pending() const {
    return is_mutable() || state == extent_state_t::EXIST_CLEAN;
  }

  bool is_rewrite() {
    return is_initial_pending() && get_prior_instance();
  }

  /// Returns true if extent is stable, written and shared among transactions
  bool is_stable_written() const {
    return state == extent_state_t::CLEAN_PENDING ||
      state == extent_state_t::CLEAN ||
      state == extent_state_t::DIRTY;
  }

  bool is_stable_writting() const {
    // MUTATION_PENDING and under-io extents are already stable and visible,
    // see prepare_record().
    //
    // XXX: It might be good to mark this case as DIRTY from the definition,
    // which probably can make things simpler.
    return is_mutation_pending() && is_pending_io();
  }

  /// Returns true if extent is stable and shared among transactions
  bool is_stable() const {
    return is_stable_written() || is_stable_writting();
  }

  bool is_data_stable() const {
    return is_stable() || is_exist_clean();
  }

  /// Returns true if extent has a pending delta
  bool is_mutation_pending() const {
    return state == extent_state_t::MUTATION_PENDING
      || state == extent_state_t::EXIST_MUTATION_PENDING;
  }

  /// Returns true if extent is a fresh extent
  bool is_initial_pending() const {
    return state == extent_state_t::INITIAL_WRITE_PENDING;
  }

  /// Returns true if extent is clean (does not have deltas on disk)
  bool is_clean() const {
    ceph_assert(is_valid());
    return state == extent_state_t::INITIAL_WRITE_PENDING ||
           state == extent_state_t::CLEAN ||
           state == extent_state_t::CLEAN_PENDING ||
           state == extent_state_t::EXIST_CLEAN;
  }

  // Returs true if extent is stable and clean
  bool is_stable_clean() const {
    ceph_assert(is_valid());
    return state == extent_state_t::CLEAN ||
           state == extent_state_t::CLEAN_PENDING;
  }

  /// Ruturns true if data is persisted while metadata isn't
  bool is_exist_clean() const {
    return state == extent_state_t::EXIST_CLEAN;
  }

  /// Returns true if the extent with EXTIST_CLEAN is modified
  bool is_exist_mutation_pending() const {
    return state == extent_state_t::EXIST_MUTATION_PENDING;
  }

  /// Returns true if extent is dirty (has deltas on disk)
  bool is_dirty() const {
    ceph_assert(is_valid());
    return !is_clean();
  }

  /// Returns true if extent has not been superceded or retired
  bool is_valid() const {
    return state != extent_state_t::INVALID;
  }

  /// Returns true if extent or prior_instance has been invalidated
  bool has_been_invalidated() const {
    return !is_valid() || (is_mutation_pending() && !prior_instance->is_valid());
  }

  /// Returns true if extent is a plcaeholder
  bool is_placeholder() const {
    return is_retired_placeholder_type(get_type());
  }

  bool is_pending_io() const {
    return !!io_wait_promise;
  }

  /// Return journal location of oldest relevant delta, only valid while DIRTY
  auto get_dirty_from() const {
    ceph_assert(is_dirty());
    return dirty_from_or_retired_at;
  }

  /// Return journal location of oldest relevant delta, only valid while RETIRED
  auto get_retired_at() const {
    ceph_assert(!is_valid());
    return dirty_from_or_retired_at;
  }

  /// Return true if extent is fully loaded or is about to be fully loaded (call 
  /// wait_io() in this case)
  bool is_fully_loaded() const {
    if (ptr.has_value()) {
      // length == 0 iff root
      assert(length == loaded_length);
      assert(!buffer_space.has_value());
      return true;
    } else { // ptr is std::nullopt
      assert(length > loaded_length);
      assert(buffer_space.has_value());
      return false;
    }
  }

  /// Return true if range offset~_length is loaded
  bool is_range_loaded(extent_len_t offset, extent_len_t _length) {
    assert(is_aligned(offset, CEPH_PAGE_SIZE));
    assert(is_aligned(_length, CEPH_PAGE_SIZE));
    assert(_length > 0);
    assert(offset + _length <= length);
    if (is_fully_loaded()) {
      return true;
    }
    return buffer_space->is_range_loaded(offset, _length);
  }

  /// Get buffer by given offset and _length.
  ceph::bufferlist get_range(extent_len_t offset, extent_len_t _length) {
    assert(is_range_loaded(offset, _length));
    ceph::bufferlist res;
    if (is_fully_loaded()) {
      res.append(ceph::bufferptr(get_bptr(), offset, _length));
    } else {
      res = buffer_space->get_buffer(offset, _length);
    }
    return res;
  }

  /**
   * get_paddr
   *
   * Returns current address of extent.  If is_initial_pending(), address will
   * be relative, otherwise address will be absolute.
   */
  paddr_t get_paddr() const { return poffset; }

  /// Returns length of extent data in disk
  extent_len_t get_length() const {
    return length;
  }

  /// Returns length of partially loaded extent data in cache
  extent_len_t get_loaded_length() const {
    return loaded_length;
  }

  /// Returns version, get_version() == 0 iff is_clean()
  extent_version_t get_version() const {
    return version;
  }

  /// Returns crc32c of buffer
  virtual uint32_t calc_crc32c() const {
    return ceph_crc32c(
      1,
      reinterpret_cast<const unsigned char *>(get_bptr().c_str()),
      get_length());
  }

  /// Get ref to raw buffer
  virtual bufferptr &get_bptr() {
    assert(ptr.has_value());
    return *ptr;
  }
  virtual const bufferptr &get_bptr() const {
    assert(ptr.has_value());
    return *ptr;
  }

  /// Compare by paddr
  friend bool operator< (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset < b.poffset;
  }
  friend bool operator> (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset > b.poffset;
  }
  friend bool operator== (const CachedExtent &a, const CachedExtent &b) {
    return a.poffset == b.poffset;
  }

  virtual ~CachedExtent();

  placement_hint_t get_user_hint() const {
    return user_hint;
  }

  rewrite_gen_t get_rewrite_generation() const {
    return rewrite_generation;
  }

  void invalidate_hints() {
    user_hint = PLACEMENT_HINT_NULL;
    rewrite_generation = NULL_GENERATION;
  }

  /// assign the target rewrite generation for the followup rewrite
  void set_target_rewrite_generation(rewrite_gen_t gen) {
    assert(is_target_rewrite_generation(gen));

    user_hint = placement_hint_t::REWRITE;
    rewrite_generation = gen;
  }

  void set_inplace_rewrite_generation() {
    user_hint = placement_hint_t::REWRITE;
    rewrite_generation = OOL_GENERATION;
  }

  bool is_inline() const {
    return poffset.is_relative();
  }

  paddr_t get_prior_paddr_and_reset() {
    if (!prior_poffset) {
      return poffset;
    }
    auto ret = *prior_poffset;
    prior_poffset.reset();
    return ret;
  }

  void set_invalid(Transaction &t);

  // a rewrite extent has an invalid prior_instance,
  // and a mutation_pending extent has a valid prior_instance
  CachedExtentRef get_prior_instance() const {
    return prior_instance;
  }

  uint32_t get_last_committed_crc() const {
    return last_committed_crc;
  }

  /// Returns true if the extent part of the open transaction
  bool is_pending_in_trans(transaction_id_t id) const {
    return is_pending() && pending_for_transaction == id;
  }

private:
  template <typename T>
  friend class read_set_item_t;

  friend struct paddr_cmp;
  friend struct ref_paddr_cmp;
  friend class ExtentIndex;
  friend struct trans_retired_extent_link_t;

  /// Pointer to containing index (or null)
  ExtentIndex *parent_index = nullptr;

  /// hook for intrusive extent_index
  boost::intrusive::set_member_hook<> extent_index_hook;
  using index_member_options = boost::intrusive::member_hook<
    CachedExtent,
    boost::intrusive::set_member_hook<>,
    &CachedExtent::extent_index_hook>;
  using index = boost::intrusive::set<CachedExtent, index_member_options>;
  friend class ExtentIndex;
  friend class Transaction;

  bool is_linked() {
    return extent_index_hook.is_linked();
  }

  /// hook for intrusive ref list (mainly dirty or lru list)
  boost::intrusive::list_member_hook<> primary_ref_list_hook;
  using primary_ref_list_member_options = boost::intrusive::member_hook<
    CachedExtent,
    boost::intrusive::list_member_hook<>,
    &CachedExtent::primary_ref_list_hook>;
  using primary_ref_list = boost::intrusive::list<
    CachedExtent,
    primary_ref_list_member_options>;

  /**
   * dirty_from_or_retired_at
   *
   * Encodes ordering token for primary_ref_list -- dirty_from when
   * dirty or retired_at if retired.
   */
  journal_seq_t dirty_from_or_retired_at;

  /// cache data contents, std::nullopt iff partially loaded
  std::optional<ceph::bufferptr> ptr;

  /// disk data length, 0 iff root
  extent_len_t length;

  /// loaded data length, <length iff partially loaded
  extent_len_t loaded_length;

  /// manager of buffer pieces for ObjectDataBLock
  /// valid iff partially loaded
  std::optional<BufferSpace> buffer_space;

  /// number of deltas since initial write
  extent_version_t version = 0;

  /// address of original block -- record relative iff is_initial_pending()
  paddr_t poffset;

  /// relative address before ool write, used to update mapping
  std::optional<paddr_t> prior_poffset = std::nullopt;

  /// used to wait while in-progress commit completes
  std::optional<seastar::shared_promise<>> io_wait_promise;
  void set_io_wait() {
    ceph_assert(!io_wait_promise);
    io_wait_promise = seastar::shared_promise<>();
  }
  void complete_io() {
    ceph_assert(io_wait_promise);
    io_wait_promise->set_value();
    io_wait_promise = std::nullopt;
  }

  seastar::future<> wait_io() {
    if (!io_wait_promise) {
      return seastar::now();
    } else {
      return io_wait_promise->get_shared_future();
    }
  }

  CachedExtent* get_transactional_view(Transaction &t);
  CachedExtent* get_transactional_view(transaction_id_t tid);

  read_set_item_t<Transaction>::trans_set_t transactions;

  placement_hint_t user_hint = PLACEMENT_HINT_NULL;

  // the target rewrite generation for the followup rewrite
  // or the rewrite generation for the fresh write
  rewrite_gen_t rewrite_generation = NULL_GENERATION;

protected:
  trans_view_set_t mutation_pendings;
  trans_view_set_t retired_transactions;

  CachedExtent(CachedExtent &&other) = delete;

  /// construct a fully loaded CachedExtent
  explicit CachedExtent(ceph::bufferptr &&_ptr)
    : length(_ptr.length()),
      loaded_length(_ptr.length()) {
    ptr = std::move(_ptr);

    assert(ptr->is_page_aligned());
    assert(length > 0);
    assert(is_fully_loaded());
    // must call init() to fully initialize
  }

  /// construct a partially loaded CachedExtent
  /// must be identical with CachedExtent(ptr) after on_fully_loaded()
  explicit CachedExtent(extent_len_t _length)
    : length(_length),
      loaded_length(0),
      buffer_space(std::in_place) {
    assert(is_aligned(length, CEPH_PAGE_SIZE));
    assert(length > 0);
    assert(!is_fully_loaded());
    // must call init() to fully initialize
  }

  /// construct new CachedExtent, will deep copy the buffer
  CachedExtent(const CachedExtent &other)
    : state(other.state),
      dirty_from_or_retired_at(other.dirty_from_or_retired_at),
      length(other.get_length()),
      loaded_length(other.get_loaded_length()),
      version(other.version),
      poffset(other.poffset) {
    // the extent must be fully loaded before CoW
    assert(other.is_fully_loaded());
    assert(is_aligned(length, CEPH_PAGE_SIZE));
    if (length > 0) {
      ptr = create_extent_ptr_rand(length);
      other.ptr->copy_out(0, length, ptr->c_str());
    } else { // length == 0, must be root
      ptr = ceph::bufferptr(0);
    }

    assert(is_fully_loaded());
  }

  struct share_buffer_t {};
  /// construct new CachedExtent, will shallow copy the buffer
  CachedExtent(const CachedExtent &other, share_buffer_t)
    : state(other.state),
      dirty_from_or_retired_at(other.dirty_from_or_retired_at),
      ptr(other.ptr),
      length(other.get_length()),
      loaded_length(other.get_loaded_length()),
      version(other.version),
      poffset(other.poffset) {
    // the extent must be fully loaded before CoW
    assert(other.is_fully_loaded());
    assert(is_aligned(length, CEPH_PAGE_SIZE));
    assert(length > 0);
    assert(is_fully_loaded());
  }

  // 0 length is only possible for the RootBlock
  struct root_construct_t {};
  CachedExtent(root_construct_t)
    : ptr(ceph::bufferptr(0)),
      length(0),
      loaded_length(0) {
    assert(is_fully_loaded());
    // must call init() to fully initialize
  }

  struct retired_placeholder_construct_t {};
  CachedExtent(retired_placeholder_construct_t, extent_len_t _length)
    : state(extent_state_t::CLEAN),
      length(_length),
      loaded_length(0),
      buffer_space(std::in_place) {
    assert(!is_fully_loaded());
    assert(is_aligned(length, CEPH_PAGE_SIZE));
    // must call init() to fully initialize
  }

  friend class Cache;
  template <typename T, typename... Args>
  static TCachedExtentRef<T> make_cached_extent_ref(
    Args&&... args) {
    return new T(std::forward<Args>(args)...);
  }

  template <typename T>
  static TCachedExtentRef<T> make_cached_extent_ref() {
    return new T();
  }

  void reset_prior_instance() {
    prior_instance.reset();
  }

  /**
   * Called when updating extents' last_committed_crc, some extents may
   * have in-extent checksum fields, like LBA/backref nodes, which are
   * supposed to be updated in this method.
   */
  virtual void update_in_extent_chksum_field(uint32_t) {}

  void set_prior_instance(CachedExtentRef p) {
    prior_instance = p;
  }

  /// Sets last_committed_crc
  void set_last_committed_crc(uint32_t crc) {
    last_committed_crc = crc;
  }

  void set_paddr(paddr_t offset, bool need_update_mapping = false) {
    if (need_update_mapping) {
      assert(!prior_poffset);
      prior_poffset = poffset;
    }
    poffset = offset;
  }

  /// set bufferptr
  void set_bptr(ceph::bufferptr &&nptr) {
    ptr = nptr;
  }

  /**
   * maybe_generate_relative
   *
   * There are three kinds of addresses one might want to
   * store within an extent:
   * - addr for a block within the same transaction relative to the
   *   physical location of this extent in the
   *   event that we will read it in the initial read of the extent
   * - addr relative to the physical location of the next record to a
   *   block within that record to contain a delta for this extent in
   *   the event that we'll read it from a delta and overlay it onto a
   *   dirty representation of the extent.
   * - absolute addr to a block already written outside of the current
   *   transaction.
   *
   * This helper checks addr and the current state to create the correct
   * reference.
   */
  paddr_t maybe_generate_relative(paddr_t addr) {
    if (is_initial_pending() && addr.is_record_relative()) {
      return addr.block_relative_to(get_paddr());
    } else {
      ceph_assert(!addr.is_record_relative() || is_mutation_pending());
      return addr;
    }
  }

  /// Returns the ranges to load, convert to fully loaded is possible
  load_ranges_t load_ranges(extent_len_t offset, extent_len_t _length) {
    assert(is_aligned(offset, CEPH_PAGE_SIZE));
    assert(is_aligned(_length, CEPH_PAGE_SIZE));
    assert(_length > 0);
    assert(offset + _length <= length);
    assert(!is_fully_loaded());

    if (loaded_length == 0 && _length == length) {
      assert(offset == 0);
      // skip rebuilding the buffer from buffer_space
      ptr = create_extent_ptr_rand(length);
      loaded_length = _length;
      buffer_space.reset();
      assert(is_fully_loaded());
      on_fully_loaded();
      load_ranges_t ret;
      ret.push_back(offset, *ptr);
      return ret;
    }

    load_ranges_t ret = buffer_space->load_ranges(offset, _length);
    loaded_length += ret.length;
    assert(length >= loaded_length);
    if (length == loaded_length) {
      // convert to fully loaded
      ptr = buffer_space->to_full_ptr(length);
      buffer_space.reset();
      assert(is_fully_loaded());
      on_fully_loaded();
      // adjust ret since the ptr has been rebuild
      for (load_range_t& range : ret.ranges) {
        auto range_length = range.ptr.length();
        range.ptr = ceph::bufferptr(*ptr, range.offset, range_length);
      }
    }
    return ret;
  }

  friend class crimson::os::seastore::SegmentedAllocator;
  friend class crimson::os::seastore::TransactionManager;
  friend class crimson::os::seastore::ExtentPlacementManager;
  template <typename, typename>
  friend class BtreeNodeMapping;
  friend class ::btree_lba_manager_test;
  friend class ::lba_btree_test;
  friend class ::btree_test_base;
  friend class ::cache_test_t;
  template <typename, typename, typename>
  friend class ParentNode;
};

std::ostream &operator<<(std::ostream &, CachedExtent::extent_state_t);
std::ostream &operator<<(std::ostream &, const CachedExtent&);

/// Compare extents by paddr
struct paddr_cmp {
  bool operator()(paddr_t lhs, const CachedExtent &rhs) const {
    return lhs < rhs.poffset;
  }
  bool operator()(const CachedExtent &lhs, paddr_t rhs) const {
    return lhs.poffset < rhs;
  }
};

// trans_retired_extent_link_t is used to link stable extents with
// the transactions that retired them. With this link, we can find
// out whether an extent has been retired by a specific transaction
// in a way that's more efficient than searching through the transaction's
// retired_set (Transaction::is_retired())
struct trans_retired_extent_link_t {
  CachedExtentRef extent;
  // We use trans_spec_view_t instead of transaction_id_t, so that,
  // when a transaction is deleted or reset, we can efficiently remove
  // that transaction from the extents' extent-transaction link set.
  // Otherwise, we have to search through each extent's "retired_transactions"
  // to remove the transaction
  trans_spec_view_t trans_view;
  trans_retired_extent_link_t(CachedExtentRef extent, transaction_id_t id)
    : extent(extent), trans_view{id}
  {
    assert(extent->is_stable());
    extent->retired_transactions.insert(trans_view);
  }
};

/// Compare extent refs by paddr
struct ref_paddr_cmp {
  using is_transparent = paddr_t;
  bool operator()(
    const trans_retired_extent_link_t &lhs,
    const trans_retired_extent_link_t &rhs) const {
    return lhs.extent->poffset < rhs.extent->poffset;
  }
  bool operator()(
    const paddr_t &lhs,
    const trans_retired_extent_link_t &rhs) const {
    return lhs < rhs.extent->poffset;
  }
  bool operator()(
    const trans_retired_extent_link_t &lhs,
    const paddr_t &rhs) const {
    return lhs.extent->poffset < rhs;
  }
  bool operator()(
    const CachedExtentRef &lhs,
    const trans_retired_extent_link_t &rhs) const {
    return lhs->poffset < rhs.extent->poffset;
  }
  bool operator()(
    const trans_retired_extent_link_t &lhs,
    const CachedExtentRef &rhs) const {
    return lhs.extent->poffset < rhs->poffset;
  }
};

template <typename T, typename C>
class addr_extent_list_base_t
  : public std::list<std::pair<T, C>> {};

using pextent_list_t = addr_extent_list_base_t<paddr_t, CachedExtentRef>;

template <typename T, typename C, typename Cmp>
class addr_extent_set_base_t
  : public std::set<C, Cmp> {};

using pextent_set_t = addr_extent_set_base_t<
  paddr_t,
  trans_retired_extent_link_t,
  ref_paddr_cmp
  >;

template <typename T>
using t_pextent_list_t = addr_extent_list_base_t<paddr_t, TCachedExtentRef<T>>;

/**
 * ExtentIndex
 *
 * Index of CachedExtent & by poffset, does not hold a reference,
 * user must ensure each extent is removed prior to deletion
 */
class ExtentIndex {
  friend class Cache;
  CachedExtent::index extent_index;
public:
  auto get_overlap(paddr_t addr, extent_len_t len) {
    auto bottom = extent_index.upper_bound(addr, paddr_cmp());
    if (bottom != extent_index.begin())
      --bottom;
    if (bottom != extent_index.end() &&
	bottom->get_paddr().add_offset(bottom->get_length()) <= addr)
      ++bottom;

    auto top = extent_index.lower_bound(addr.add_offset(len), paddr_cmp());
    return std::make_pair(
      bottom,
      top
    );
  }

  void clear() {
    struct cached_extent_disposer {
      void operator() (CachedExtent* extent) {
	extent->parent_index = nullptr;
      }
    };
    extent_index.clear_and_dispose(cached_extent_disposer());
    bytes = 0;
  }

  void insert(CachedExtent &extent) {
    // sanity check
    ceph_assert(!extent.parent_index);
    auto [a, b] = get_overlap(
      extent.get_paddr(),
      extent.get_length());
    ceph_assert(a == b);

    [[maybe_unused]] auto [iter, inserted] = extent_index.insert(extent);
    assert(inserted);
    extent.parent_index = this;

    bytes += extent.get_length();
  }

  void erase(CachedExtent &extent) {
    assert(extent.parent_index);
    assert(extent.is_linked());
    [[maybe_unused]] auto erased = extent_index.erase(
      extent_index.s_iterator_to(extent));
    extent.parent_index = nullptr;

    assert(erased);
    bytes -= extent.get_length();
  }

  void replace(CachedExtent &to, CachedExtent &from) {
    assert(to.get_length() == from.get_length());
    extent_index.replace_node(extent_index.s_iterator_to(from), to);
    from.parent_index = nullptr;
    to.parent_index = this;
  }

  bool empty() const {
    return extent_index.empty();
  }

  auto find_offset(paddr_t offset) {
    return extent_index.find(offset, paddr_cmp());
  }

  auto begin() {
    return extent_index.begin();
  }

  auto end() {
    return extent_index.end();
  }

  auto size() const {
    return extent_index.size();
  }

  auto get_bytes() const {
    return bytes;
  }

  ~ExtentIndex() {
    assert(extent_index.empty());
    assert(bytes == 0);
  }

private:
  uint64_t bytes = 0;
};

template <typename key_t, typename>
class PhysicalNodeMapping;

template <typename key_t, typename val_t>
using PhysicalNodeMappingRef = std::unique_ptr<PhysicalNodeMapping<key_t, val_t>>;

template <typename key_t, typename val_t>
class PhysicalNodeMapping {
public:
  PhysicalNodeMapping() = default;
  PhysicalNodeMapping(const PhysicalNodeMapping&) = delete;
  virtual extent_len_t get_length() const = 0;
  virtual val_t get_val() const = 0;
  virtual key_t get_key() const = 0;
  virtual bool has_been_invalidated() const = 0;
  virtual CachedExtentRef get_parent() const = 0;
  virtual uint16_t get_pos() const = 0;
  virtual uint32_t get_checksum() const {
    ceph_abort("impossible");
    return 0;
  }
  virtual bool is_parent_viewable() const = 0;
  virtual bool is_parent_valid() const = 0;
  virtual bool parent_modified() const {
    ceph_abort("impossible");
    return false;
  };

  virtual void maybe_fix_pos() {
    ceph_abort("impossible");
  }

  virtual ~PhysicalNodeMapping() {}
};

/**
 * RetiredExtentPlaceholder
 *
 * Cache::retire_extent_addr(Transaction&, paddr_t, extent_len_t) can retire an
 * extent not currently in cache. In that case, in order to detect transaction
 * invalidation, we need to add a placeholder to the cache to create the
 * mapping back to the transaction. And whenever there is a transaction tries
 * to read the placeholder extent out, Cache is responsible to replace the
 * placeholder by the real one. Anyway, No placeholder extents should escape
 * the Cache interface boundary.
 */
class RetiredExtentPlaceholder : public CachedExtent {

public:
  RetiredExtentPlaceholder(extent_len_t length)
    : CachedExtent(CachedExtent::retired_placeholder_construct_t{}, length) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    ceph_assert(0 == "Should never happen for a placeholder");
    return CachedExtentRef();
  }

  ceph::bufferlist get_delta() final {
    ceph_assert(0 == "Should never happen for a placeholder");
    return ceph::bufferlist();
  }

  static constexpr extent_types_t TYPE = extent_types_t::RETIRED_PLACEHOLDER;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &bl) final {
    ceph_assert(0 == "Should never happen for a placeholder");
  }

  bool is_logical() const final {
    return false;
  }

  void on_rewrite(Transaction &, CachedExtent&, extent_len_t) final {}

  std::ostream &print_detail(std::ostream &out) const final {
    return out << ", RetiredExtentPlaceholder";
  }

  void on_delta_write(paddr_t record_block_offset) final {
    ceph_assert(0 == "Should never happen for a placeholder");
  }
};

class LBAMapping;
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

  void on_rewrite(Transaction&, CachedExtent &extent, extent_len_t off) final {
    assert(get_type() == extent.get_type());
    auto &lextent = (LogicalCachedExtent&)extent;
    set_laddr((lextent.get_laddr() + off).checked_to_laddr());
  }

  bool has_laddr() const {
    return laddr != L_ADDR_NULL;
  }

  laddr_t get_laddr() const {
    assert(laddr != L_ADDR_NULL);
    return laddr;
  }

  void set_laddr(laddr_t nladdr) {
    laddr = nladdr;
  }

  void maybe_set_intermediate_laddr(LBAMapping &mapping);

  void apply_delta_and_adjust_crc(
    paddr_t base, const ceph::bufferlist &bl) final {
    apply_delta(bl);
    set_last_committed_crc(calc_crc32c());
  }

  bool is_logical() const final {
    return true;
  }

  std::ostream &print_detail(std::ostream &out) const final;

  struct modified_region_t {
    extent_len_t offset;
    extent_len_t len;
  };
  virtual std::optional<modified_region_t> get_modified_region() {
    return std::nullopt;
  }

  virtual void clear_modified_region() {}

  virtual ~LogicalCachedExtent() {}

protected:

  virtual void apply_delta(const ceph::bufferlist &bl) = 0;

  virtual std::ostream &print_detail_l(std::ostream &out) const {
    return out;
  }

  virtual void logical_on_delta_write() {}

  void on_delta_write(paddr_t record_block_offset) final {
    assert(is_exist_mutation_pending() ||
	   get_prior_instance());
    logical_on_delta_write();
  }

private:
  // the logical address of the extent, and if shared,
  // it is the intermediate_base, see BtreeLBAMapping comments.
  laddr_t laddr = L_ADDR_NULL;
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

template <typename T>
read_set_item_t<T>::read_set_item_t(T *t, CachedExtentRef ref)
  : t(t), ref(ref)
{}

template <typename T>
inline bool read_set_item_t<T>::cmp_t::operator()(
  const read_set_item_t<T> &lhs, const read_set_item_t<T> &rhs) const {
  return lhs.ref->poffset < rhs.ref->poffset;
}
template <typename T>
inline bool read_set_item_t<T>::cmp_t::operator()(
  const paddr_t &lhs, const read_set_item_t<T> &rhs) const {
  return lhs < rhs.ref->poffset;
}
template <typename T>
inline bool read_set_item_t<T>::cmp_t::operator()(
  const read_set_item_t<T> &lhs, const paddr_t &rhs) const {
  return lhs.ref->poffset < rhs;
}

using lextent_set_t = addr_extent_set_base_t<
  laddr_t,
  LogicalCachedExtentRef,
  ref_laddr_cmp
  >;

template <typename T>
using lextent_list_t = addr_extent_list_base_t<
  laddr_t, TCachedExtentRef<T>>;

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::CachedExtent> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::LogicalCachedExtent> : fmt::ostream_formatter {};
#endif
