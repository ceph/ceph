// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include "seastar/core/shared_future.hh"

#include "include/buffer.h"
#include "include/interval_set.h"
#include "crimson/os/poseidonstore/poseidonstore_types.h"
#include "crimson/os/poseidonstore/device_manager.h"
#include "crimson/common/errorator.h"

namespace crimson::os::poseidonstore {

/** 
 *
 * As initail step, we borrow seastore's data structure to read/write the data to the cache.
 * Similar to seastore, Cache will maintain general info for entire cache.
 * CacheExtent is a cache entry and any cache contents such as TestBlock, Onode and Block
 * can be created inherited from CacheExtent.
 * But, in PoseidonStore, extent_map in the onode will manage CacheExtents it owns. 
 * Plus, shared_block_list will manage shared cache extent based on laddr_t.
 *
 *
 * TODO: Cache should be sharded.
 *       Managing cache entries (e.g., shrink, memory pool).
 *
 */
class Transaction;
class CachedExtent;
using CachedExtentRef = boost::intrusive_ptr<CachedExtent>;
template <typename T>
using TCachedExtentRef = boost::intrusive_ptr<T>;
struct ref_laddr_cmp;

class CachedExtent : public boost::intrusive_ref_counter<
  CachedExtent, boost::thread_unsafe_counter> {
public:
  enum class ce_state_t : uint8_t {
    NEW,     ///< initial state
    WRITING, ///< the data is being written
    READING, ///< refered by read_set in the transaction
    CLEAN    ///< clean
  } state = ce_state_t::NEW;

  friend class Cache;
  template <typename T>
  static TCachedExtentRef<T> make_cached_extent_ref(bufferptr &&ptr) {
    return new T(std::move(ptr));
  }

  laddr_t get_laddr() const { return laddr; }

  void set_laddr(laddr_t offset) { laddr = offset; }

  uint64_t get_length() const { return ptr.length(); }

  bufferptr &get_bptr() { return ptr; }

  const bufferptr &get_bptr() const { return ptr; }

  /**
   * set_content
   *
   * copy data from src to ptr
   *
   * @param source bufferptr
   * @param offset of ptr
   * @param length 
   *
   */
  void set_content(bufferptr src, uint16_t offset, uint16_t len) {
    ceph_assert(src.length() >= len);
    ptr.copy_in(offset, len, src.c_str());
    modified_range.insert(offset, len);
  }

  /**
   * copy_to_ptr
   *
   * copy data from ptr to tgt
   *
   * @param target bufferptr
   * @param offset of ptr
   * @param length 
   *
   */
  void copy_to_ptr(bufferptr tgt, uint16_t offset, uint16_t len) {
    ceph_assert(tgt.length() >= len);
    tgt.copy_in(0, len, ptr.c_str() + offset);
  }

  void truncate(uint32_t len) {
    ceph_assert(len < get_length());
    if (get_length()) {
      auto bp = ceph::bufferptr(
	buffer::create_page_aligned(len));
      bp.copy_in(0, len, ptr.c_str());
      ptr = bp;
    }
  }

  friend bool operator< (const CachedExtent &a, const CachedExtent &b) {
    return a.laddr < b.laddr;
  }
  friend bool operator> (const CachedExtent &a, const CachedExtent &b) {
    return a.laddr > b.laddr;
  }
  friend bool operator== (const CachedExtent &a, const CachedExtent &b) {
    return a.laddr == b.laddr;
  }

  bool is_clean() const {
    return state == ce_state_t::CLEAN;
  }
  bool is_new() const {
    return state == ce_state_t::NEW;
  }
  bool is_reading() const {
    return state == ce_state_t::READING;
  }
  bool is_writing() const {
    return state == ce_state_t::WRITING;
  }

  void set_state(ce_state_t to_set) {
    state = to_set;
  }

  virtual ce_types_t get_type() const = 0;

  /**
   * on_read
   *
   * callback after finishing read()
   */
  virtual void on_read() {}

  /**
   * on_read
   *
   * callback after finishing write()
   */
  virtual void on_write() {}

  virtual laddr_t get_laddr() = 0;

  friend std::ostream &operator<<(std::ostream &, CachedExtent::ce_state_t);
  friend ref_laddr_cmp;
  friend Transaction;

protected:
  CachedExtent(ceph::bufferptr &&ptr) : ptr(std::move(ptr)) {}
  CachedExtent(const CachedExtent &other)
    : state(other.state),
      ptr(other.ptr.c_str(), other.ptr.length()),
      laddr(other.laddr) {}

  ceph::bufferptr ptr;
  laddr_t laddr;
  CachedExtentRef prior_instance;
  interval_set<uint64_t> modified_range;
};

std::ostream &operator<<(std::ostream &out, CachedExtent::ce_state_t state);
std::ostream &operator<<(std::ostream &out, const CachedExtent &ext);

template <typename T, typename C, typename Cmp>
class addr_extent_set_base_t
  : public std::set<C, Cmp> {};

struct ref_laddr_cmp {
  using is_transparent = laddr_t;
  bool operator()(const CachedExtentRef &lhs, const CachedExtentRef &rhs) const {
    return lhs->laddr < rhs->laddr;
  }
  bool operator()(const laddr_t &lhs, const CachedExtentRef &rhs) const {
    return lhs < rhs->laddr;
  }
  bool operator()(const CachedExtentRef &lhs, const laddr_t &rhs) const {
    return lhs->laddr < rhs;
  }
};

using pextent_set_t = addr_extent_set_base_t<
  laddr_t,
  CachedExtentRef,
  ref_laddr_cmp
  >;


class Cache {
public:
  Cache(DeviceManager &manager) : device_manager(manager) {}
  ~Cache() {}

  bufferptr alloc_cache_buf(size_t size) {
    auto bp = ceph::bufferptr(
      buffer::create_page_aligned(size));
    bp.zero();
    return bp;
  }

  template <typename T>
  TCachedExtentRef<T> alloc_new_extent(
    uint32_t length ///< [in] length
  ) {
    auto ret = CachedExtent::make_cached_extent_ref<T>(
      alloc_cache_buf(length));
    ret->state = CachedExtent::ce_state_t::NEW;
    return ret;
  }

  /**
   * split_extent
   *
   * split original cache_extent into two cache_extents.
   * should be aligned.
   */
  template <typename T>
  TCachedExtentRef<T> split_extent(
      uint64_t offset,         ///< [in] offset of source 
      uint32_t length,         ///< [in] length 
      TCachedExtentRef<T> ref  ///< [in] source cache_extent to be splitted
  ) 
  {
    if (!ref || (ref->get_length() % ce_aligned_size) || (length % ce_aligned_size) ||
	(offset + length != ref->get_length())) {
      return nullptr;
    }
    TCachedExtentRef<T> new_ref = alloc_new_extent<T>(length);
    ref->copy_to_ptr(new_ref->get_bptr(), offset, length);
    ref->truncate(offset);
    new_ref->set_laddr(ref->get_laddr() + offset);
    return new_ref;
  }

  /**
   * merge_extent
   *
   * merge two cache_extents to a single extent.
   * should be aligned.
   */
  template <typename T>
  TCachedExtentRef<T> merge_extent(
      TCachedExtentRef<T> l,  ///< [in] source cache_extent
      TCachedExtentRef<T> r   ///< [in] source cache_extent
  )
  {
    interval_set<uint64_t> ch;
    if (!l || !r) {
      return nullptr;
    }
    if ((l->get_length() % ce_aligned_size) ||
        (r->get_length() % ce_aligned_size)) {
      return nullptr;
    }
    ch.insert(l->get_laddr(), l->get_length());
    ch.insert(r->get_laddr(), r->get_length());
    auto new_ref = alloc_new_extent<T>(ch.range_end() - ch.range_start());
    ceph_assert(new_ref);
    new_ref->set_content(l->get_bptr(), 0, l->get_length());
    new_ref->set_content(r->get_bptr(), l->get_length(), r->get_length());
    new_ref->set_laddr(l->get_laddr());
    return new_ref;
  }

  /* for test */
  using fill_cache_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using fill_cache_ret = fill_cache_ertr::future<>;
  fill_cache_ret fill_cache(Transaction &t);

  /**
   * complete_commit
   *
   * After writting transaction to wal is complete, this should be called.
   * Do post works after transaction is committed.
   */
  void complete_commit(Transaction &t);

  std::optional<RecordRef> try_construct_record(Transaction &t);

  void close() {}

private:
  DeviceManager &device_manager; 
};


struct TestBlock : crimson::os::poseidonstore::CachedExtent{
  constexpr static uint64_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<TestBlock>;

  TestBlock(ceph::bufferptr &&ptr)
    : CachedExtent(std::move(ptr)) {}

  void set_contents(char c, uint16_t offset, uint16_t len) {
    ::memset(get_bptr().c_str() + offset, c, len);
  }

  void set_contents(char c) {
    set_contents(c, 0, get_length());
  }

  void on_read() {
    set_state(CachedExtent::ce_state_t::CLEAN);
  }

  void on_write() {
    set_state(CachedExtent::ce_state_t::CLEAN);
  }

  ce_types_t get_type() const { return ce_types_t::TEST_BLOCK; }

  laddr_t get_laddr() { return laddr; }

};

using TestBlockRef = TCachedExtentRef<TestBlock>;

}
