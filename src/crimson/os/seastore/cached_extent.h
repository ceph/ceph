// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive/list.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "seastar/core/shared_future.hh"

#include "include/buffer.h"
#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

class CachedExtent;
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;

// #define DEBUG_CACHED_EXTENT_REF
#ifdef DEBUG_CACHED_EXTENT_REF

void intrusive_ptr_add_ref(CachedExtent *);
void intrusive_ptr_release(CachedExtent *);

#endif

template <typename T>
using TCachedExtentRef = boost::intrusive_ptr<T>;

/**
 * CachedExtent
 */
class ExtentIndex;
class CachedExtent : public boost::intrusive_ref_counter<
  CachedExtent, boost::thread_unsafe_counter> {
  enum class extent_state_t : uint8_t {
    INITIAL_WRITE_PENDING, // In Transaction::write_set and fresh_block_list
    MUTATION_PENDING,      // In Transaction::write_set and mutated_block_list
    CLEAN,                 // In Cache::extent_index, Transaction::read_set
                           //  during write, contents match disk, version == 0
    DIRTY,                 // Same as CLEAN, but contents do not match disk,
                           //  version > 0
    INVALID                // Part of no ExtentIndex set
  } state = extent_state_t::INVALID;
  friend std::ostream &operator<<(std::ostream &, extent_state_t);

public:
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
  virtual CachedExtentRef duplicate_for_write() = 0;

  /**
   * prepare_write
   *
   * Called prior to reading buffer.
   * Implemenation may use this callback to fully write out
   * updates to the buffer.
   */
  virtual void prepare_write() {}

  /**
   * on_initial_write
   *
   * Called after commit of extent.  State will be CLEAN.
   * Implentation may use this call to fixup the buffer
   * with the newly available absolute get_paddr().
   */
  virtual void on_initial_write() {}

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
   * get_type
   *
   * Returns concrete type.
   */
  virtual extent_types_t get_type() const = 0;

  friend std::ostream &operator<<(std::ostream &, extent_state_t);
  std::ostream &print(std::ostream &out) const {
    return out << "CachedExtent(addr=" << this
	       << ", type=" << get_type()
	       << ", version=" << version
	       << ", paddr=" << get_paddr()
	       << ", state=" << state
	       << ", refcount=" << use_count()
	       << ")";
  }

  /**
   * get_delta
   *
   * Must return a valid delta usable in apply_delta() in submit_transaction
   * if state == MUTATION_PENDING.
   */
  virtual ceph::bufferlist get_delta() = 0;

  /**
   * bl is a delta obtained previously from get_delta.  The versions will
   * match.  Implementation should mutate buffer based on bl.  base matches
   * the address passed on_delta_write.
   */
  virtual void apply_delta(paddr_t base, ceph::bufferlist &bl) = 0;

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

  /// Returns true if extent is part of an open transaction
  bool is_pending() const {
    return state == extent_state_t::INITIAL_WRITE_PENDING ||
      state == extent_state_t::MUTATION_PENDING;
  }

  /// Returns true if extent has a pending delta
  bool is_mutation_pending() const {
    return state == extent_state_t::MUTATION_PENDING;
  }

  /// Returns true if extent is a fresh extent
  bool is_initial_pending() const {
    return state == extent_state_t::INITIAL_WRITE_PENDING;
  }

  /// Returns true if extent is clean (does not have deltas on disk)
  bool is_clean() const {
    ceph_assert(is_valid());
    return state == extent_state_t::INITIAL_WRITE_PENDING ||
      state == extent_state_t::CLEAN;
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

  /**
   * get_paddr
   *
   * Returns current address of extent.  If is_initial_pending(), address will
   * be relative, otherwise address will be absolute.
   */
  paddr_t get_paddr() const { return poffset; }

  /// Returns length of extent
  extent_len_t get_length() const { return ptr.length(); }

  /// Returns version, get_version() == 0 iff is_clean()
  extent_version_t get_version() const {
    return version;
  }

  /// Get ref to raw buffer
  bufferptr &get_bptr() { return ptr; }
  const bufferptr &get_bptr() const { return ptr; }

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

private:
  friend struct paddr_cmp;
  friend struct ref_paddr_cmp;
  friend class ExtentIndex;

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

  /// hook for intrusive ref list (mainly dirty or lru list)
  boost::intrusive::list_member_hook<> primary_ref_list_hook;
  using primary_ref_list_member_options = boost::intrusive::member_hook<
    CachedExtent,
    boost::intrusive::list_member_hook<>,
    &CachedExtent::primary_ref_list_hook>;
  using list = boost::intrusive::list<
    CachedExtent,
    primary_ref_list_member_options>;

  /// Actual data contents
  ceph::bufferptr ptr;

  /// number of deltas since initial write
  extent_version_t version = EXTENT_VERSION_NULL;

  /// address of original block -- relative iff is_pending() and is_clean()
  paddr_t poffset;

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

protected:
  CachedExtent(ceph::bufferptr &&ptr) : ptr(std::move(ptr)) {}
  CachedExtent(const CachedExtent &other)
    : state(other.state),
      ptr(other.ptr.c_str(), other.ptr.length()),
      version(other.version),
      poffset(other.poffset) {}

  friend class Cache;
  template <typename T, typename... Args>
  static TCachedExtentRef<T> make_cached_extent_ref(Args&&... args) {
    return new T(std::forward<Args>(args)...);
  }

  void set_paddr(paddr_t offset) { poffset = offset; }

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
    if (!addr.is_relative()) {
      return addr;
    } else if (is_mutation_pending()) {
      return addr;
    } else {
      ceph_assert(is_initial_pending());
      ceph_assert(get_paddr().is_record_relative());
      return addr - get_paddr();
    }
  }

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

/// Compare extent refs by paddr
struct ref_paddr_cmp {
  using is_transparent = paddr_t;
  bool operator()(const CachedExtentRef &lhs, const CachedExtentRef &rhs) const {
    return lhs->poffset < rhs->poffset;
  }
  bool operator()(const paddr_t &lhs, const CachedExtentRef &rhs) const {
    return lhs < rhs->poffset;
  }
  bool operator()(const CachedExtentRef &lhs, const paddr_t &rhs) const {
    return lhs->poffset < rhs;
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
  CachedExtentRef,
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
  auto get_overlap(paddr_t addr, segment_off_t len) {
    auto bottom = extent_index.upper_bound(addr, paddr_cmp());
    if (bottom != extent_index.begin())
      --bottom;
    if (bottom != extent_index.end() &&
	bottom->get_paddr().add_offset(bottom->get_length()) <= addr)
      ++bottom;

    auto top = extent_index.upper_bound(addr.add_offset(len), paddr_cmp());
    return std::make_pair(
      bottom,
      top
    );
  }

  void clear() {
    extent_index.clear();
  }

  void insert(CachedExtent &extent) {
    // sanity check
    auto [a, b] = get_overlap(
      extent.get_paddr(),
      extent.get_length());
    ceph_assert(a == b);

    extent_index.insert(extent);
    extent.parent_index = this;
  }

  void erase(CachedExtent &extent) {
    extent_index.erase(extent);
    extent.parent_index = nullptr;
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

  void merge(ExtentIndex &&other) {
    for (auto it = other.extent_index.begin();
	 it != other.extent_index.end();
	 ) {
      auto &ext = *it;
      ++it;
      other.extent_index.erase(ext);
      extent_index.insert(ext);
    }
  }

  template <typename T>
  void remove(T &l) {
    for (auto &ext : l) {
      extent_index.erase(l);
    }
  }
};


}
