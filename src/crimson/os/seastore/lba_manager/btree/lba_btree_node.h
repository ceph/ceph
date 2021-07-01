// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <memory>
#include <string.h>

#include "crimson/common/log.h"
#include "crimson/os/seastore/lba_manager/btree/btree_range_pin.h"
#include "crimson/os/seastore/lba_manager.h"

namespace crimson::os::seastore::lba_manager::btree {

using base_iertr = LBAManager::base_iertr;

struct op_context_t {
  Cache &cache;
  btree_pin_set_t &pins;
  Transaction &trans;
};

/**
 * lba_map_val_t
 *
 * struct representing a single lba mapping
 */
struct lba_map_val_t {
  extent_len_t len = 0;  ///< length of mapping
  paddr_t paddr;         ///< physical addr of mapping
  uint32_t refcount = 0; ///< refcount
  uint32_t checksum = 0; ///< checksum of original block written at paddr (TODO)

  lba_map_val_t(
    extent_len_t len,
    paddr_t paddr,
    uint32_t refcount,
    uint32_t checksum)
    : len(len), paddr(paddr), refcount(refcount), checksum(checksum) {}
};

class BtreeLBAPin;
using BtreeLBAPinRef = std::unique_ptr<BtreeLBAPin>;

/**
 * LBANode
 *
 * Base class enabling recursive lookup between internal and leaf nodes.
 */
struct LBANode : CachedExtent {
  using LBANodeRef = TCachedExtentRef<LBANode>;

  btree_range_pin_t pin;

  LBANode(ceph::bufferptr &&ptr) : CachedExtent(std::move(ptr)), pin(this) {}
  LBANode(const LBANode &rhs)
    : CachedExtent(rhs), pin(rhs.pin, this) {}

  virtual lba_node_meta_t get_node_meta() const = 0;

  /**
   * lookup
   *
   * Returns the node at the specified depth responsible
   * for laddr
   */
  using lookup_iertr = base_iertr;
  using lookup_ret = lookup_iertr::future<LBANodeRef>;
  virtual lookup_ret lookup(
    op_context_t c,
    laddr_t addr,
    depth_t depth) = 0;

  /**
   * lookup_range
   *
   * Returns mappings within range [addr, addr+len)
   */
  using lookup_range_iertr = LBAManager::get_mappings_iertr;
  using lookup_range_ret = LBAManager::get_mappings_ret;
  virtual lookup_range_ret lookup_range(
    op_context_t c,
    laddr_t addr,
    extent_len_t len) = 0;

  /**
   * lookup_pin
   *
   * Returns the mapping at addr
   */
  using lookup_pin_iertr = LBAManager::get_mapping_iertr;
  using lookup_pin_ret = LBAManager::get_mapping_ret;
  virtual lookup_pin_ret lookup_pin(
    op_context_t c,
    laddr_t addr) = 0;

  /**
   * insert
   *
   * Recursively inserts into subtree rooted at *this.  Caller
   * must already have handled splitting if at_max_capacity().
   *
   * Precondition: !at_max_capacity()
   */
  using insert_iertr = base_iertr;
  using insert_ret = insert_iertr::future<LBAPinRef>;
  virtual insert_ret insert(
    op_context_t c,
    laddr_t laddr,
    lba_map_val_t val) = 0;

  /**
   * find_hole
   *
   * Finds minimum hole of size len in [min, max)
   *
   * @return addr of hole, L_ADDR_NULL if unfound
   */
  using find_hole_iertr = base_iertr;
  using find_hole_ret = find_hole_iertr::future<laddr_t>;
  virtual find_hole_ret find_hole(
    op_context_t c,
    laddr_t min,
    laddr_t max,
    extent_len_t len) = 0;

  /**
   * scan_mappings
   *
   * Call f for all mappings in [begin, end)
   */
  using scan_mappings_iertr = LBAManager::scan_mappings_iertr;
  using scan_mappings_ret = LBAManager::scan_mappings_ret;
  using scan_mappings_func_t = LBAManager::scan_mappings_func_t;
  virtual scan_mappings_ret scan_mappings(
    op_context_t c,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &f) = 0;

  using scan_mapped_space_iertr = LBAManager::scan_mapped_space_iertr;
  using scan_mapped_space_ret = LBAManager::scan_mapped_space_ret;
  using scan_mapped_space_func_t = LBAManager::scan_mapped_space_func_t;
  virtual scan_mapped_space_ret scan_mapped_space(
    op_context_t c,
    scan_mapped_space_func_t &f) = 0;

  /**
   * mutate_mapping
   *
   * Lookups up laddr, calls f on value. If f returns a value, inserts it.
   * If it returns nullopt, removes the value.
   * Caller must already have merged if at_min_capacity().
   *
   * Recursive calls use mutate_mapping_internal.
   *
   * Precondition: !at_min_capacity()
   */
  using mutate_mapping_iertr = base_iertr::extend<
    crimson::ct_error::enoent             ///< mapping does not exist
    >;
  using mutate_mapping_ret = mutate_mapping_iertr::future<
    lba_map_val_t>;
  using mutate_func_t = std::function<
    lba_map_val_t(const lba_map_val_t &v)
    >;
  virtual mutate_mapping_ret mutate_mapping(
    op_context_t c,
    laddr_t laddr,
    mutate_func_t &&f) = 0;
  virtual mutate_mapping_ret mutate_mapping_internal(
    op_context_t c,
    laddr_t laddr,
    bool is_root,
    mutate_func_t &&f) = 0;

  /**
   * mutate_internal_address
   *
   * Looks up internal node mapping at laddr, depth and
   * updates the mapping to paddr.  Returns previous paddr
   * (for debugging purposes).
   */
  using mutate_internal_address_iertr = base_iertr::extend<
    crimson::ct_error::enoent             ///< mapping does not exist
    >;
  using mutate_internal_address_ret = mutate_internal_address_iertr::future<
    paddr_t>;
  virtual mutate_internal_address_ret mutate_internal_address(
    op_context_t c,
    depth_t depth,
    laddr_t laddr,
    paddr_t paddr) = 0;

  /**
   * make_split_children
   *
   * Generates appropriately typed left and right nodes formed from the
   * contents of *this.
   *
   * Returns <left, right, pivot> where pivot is the first value of right.
   */
  virtual std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_split_children(
    op_context_t c) = 0;

  /**
   * make_full_merge
   *
   * Returns a single node formed from merging *this and right.
   * Precondition: at_min_capacity() && right.at_min_capacity()
   */
  virtual LBANodeRef make_full_merge(
    op_context_t c,
    LBANodeRef &right) = 0;

  /**
   * make_balanced
   *
   * Returns nodes formed by balancing the contents of *this and right.
   *
   * Returns <left, right, pivot> where pivot is the first value of right.
   */
  virtual std::tuple<
    LBANodeRef,
    LBANodeRef,
    laddr_t>
  make_balanced(
    op_context_t c,
    LBANodeRef &right,
    bool prefer_left) = 0;

  virtual bool at_max_capacity() const = 0;
  virtual bool at_min_capacity() const = 0;

  virtual ~LBANode() = default;

  void on_delta_write(paddr_t record_block_offset) final {
    // All in-memory relative addrs are necessarily record-relative
    assert(get_prior_instance());
    pin.take_pin(get_prior_instance()->cast<LBANode>()->pin);
    resolve_relative_addrs(record_block_offset);
  }

  void on_initial_write() final {
    // All in-memory relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  void on_clean_read() final {
    // From initial write of block, relative addrs are necessarily block-relative
    resolve_relative_addrs(get_paddr());
  }

  virtual void resolve_relative_addrs(paddr_t base) = 0;
};
using LBANodeRef = LBANode::LBANodeRef;

/**
 * get_lba_btree_extent
 *
 * Fetches node at depth of the appropriate type.
 */
using get_lba_node_iertr = base_iertr;
using get_lba_node_ret = get_lba_node_iertr::future<LBANodeRef>;
get_lba_node_ret get_lba_btree_extent(
  op_context_t c, ///< [in] context structure
  CachedExtentRef parent, ///< [in] paddr ref source
  depth_t depth,  ///< [in] depth of node to fetch
  paddr_t offset, ///< [in] physical addr of node
  paddr_t base    ///< [in] depending on user, block addr or record addr
                  ///       in case offset is relative
);

}
