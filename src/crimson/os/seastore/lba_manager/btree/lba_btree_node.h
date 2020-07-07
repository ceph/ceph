// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <sys/mman.h>
#include <memory>
#include <string.h>

#include "crimson/common/log.h"
#include "crimson/os/seastore/lba_manager/btree/btree_range_pin.h"

namespace crimson::os::seastore::lba_manager::btree {

struct op_context_t {
  Cache &cache;
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
  using lookup_range_ertr = LBAManager::get_mapping_ertr;
  using lookup_range_ret = LBAManager::get_mapping_ret;


  LBANode(ceph::bufferptr &&ptr) : CachedExtent(std::move(ptr)) {}
  LBANode(const LBANode &rhs)
    : CachedExtent(rhs) {}

  virtual lba_node_meta_t get_node_meta() const = 0;

  /**
   * lookup_range
   *
   * Returns mappings within range [addr, addr+len)
   */
  virtual lookup_range_ret lookup_range(
    op_context_t c,
    laddr_t addr,
    extent_len_t len) = 0;

  /**
   * insert
   *
   * Recursively inserts into subtree rooted at *this.  Caller
   * must already have handled splitting if at_max_capacity().
   *
   * Precondition: !at_max_capacity()
   */
  using insert_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using insert_ret = insert_ertr::future<LBAPinRef>;
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
  using find_hole_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using find_hole_ret = find_hole_ertr::future<laddr_t>;
  virtual find_hole_ret find_hole(
    op_context_t c,
    laddr_t min,
    laddr_t max,
    extent_len_t len) = 0;

  /**
   * mutate_mapping
   *
   * Lookups up laddr, calls f on value. If f returns a value, inserts it.
   * If it returns nullopt, removes the value.
   * Caller must already have merged if at_min_capacity().
   *
   * Precondition: !at_min_capacity()
   */
  using mutate_mapping_ertr = crimson::errorator<
    crimson::ct_error::enoent,            ///< mapping does not exist
    crimson::ct_error::input_output_error
    >;
  using mutate_mapping_ret = mutate_mapping_ertr::future<
    std::optional<lba_map_val_t>>;
  using mutate_func_t = std::function<
    std::optional<lba_map_val_t>(const lba_map_val_t &v)
    >;
  virtual mutate_mapping_ret mutate_mapping(
    op_context_t c,
    laddr_t laddr,
    mutate_func_t &&f) = 0;

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
};
using LBANodeRef = LBANode::LBANodeRef;

/**
 * get_lba_btree_extent
 *
 * Fetches node at depth of the appropriate type.
 */
Cache::get_extent_ertr::future<LBANodeRef> get_lba_btree_extent(
  op_context_t c, ///< [in] context structure
  depth_t depth,  ///< [in] depth of node to fetch
  paddr_t offset, ///< [in] physical addr of node
  paddr_t base    ///< [in] depending on user, block addr or record addr
                  ///       in case offset is relative
);

}
