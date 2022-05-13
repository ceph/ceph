// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"
#include "crimson/os/seastore/btree/fixed_kv_btree.h"

namespace crimson::os::seastore::backref {

constexpr size_t BACKREF_BLOCK_SIZE = 4096;

class BtreeBackrefPin : public BtreeNodePin<paddr_t, laddr_t> {
  extent_types_t type;
public:
  BtreeBackrefPin() = default;
  BtreeBackrefPin(
    CachedExtentRef parent,
    backref_map_val_t &val,
    backref_node_meta_t &&meta)
    : BtreeNodePin(
	parent,
	val.laddr,
	val.len,
	std::forward<backref_node_meta_t>(meta)),
      type(val.type)
  {}
  extent_types_t get_type() const final {
    return type;
  }
};

using BackrefBtree = FixedKVBtree<
  paddr_t, backref_map_val_t, BackrefInternalNode,
  BackrefLeafNode, BtreeBackrefPin, BACKREF_BLOCK_SIZE>;

class BtreeBackrefManager : public BackrefManager {
public:

  BtreeBackrefManager(
    SegmentManagerGroup &sm_group,
    Cache &cache)
    : sm_group(sm_group),
      cache(cache)
  {}

  mkfs_ret mkfs(
    Transaction &t) final;

  get_mapping_ret  get_mapping(
    Transaction &t,
    paddr_t offset) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    paddr_t offset,
    paddr_t end) final;

  new_mapping_ret new_mapping(
    Transaction &t,
    paddr_t key,
    extent_len_t len,
    laddr_t val,
    extent_types_t type) final;

  batch_insert_ret batch_insert(
    Transaction &t,
    backref_buffer_ref &bbr,
    const journal_seq_t &limit,
    const uint64_t max) final;

  batch_insert_ret batch_insert_from_cache(
    Transaction &t,
    const journal_seq_t &limit,
    const uint64_t max) final;

  remove_mapping_ret remove_mapping(
    Transaction &t,
    paddr_t offset) final;

  scan_mapped_space_ret scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) final;

  init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) final;

  void complete_transaction(
    Transaction &t,
    std::vector<CachedExtentRef> &,
    std::vector<CachedExtentRef> &) final;

  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final;

  void add_pin(BackrefPin &pin) final {
    auto *bpin = reinterpret_cast<BtreeBackrefPin*>(&pin);
    pin_set.add_pin(bpin->get_range_pin());
    bpin->set_parent(nullptr);
  }
  void remove_pin(BackrefPin &pin) final {
    auto *bpin = reinterpret_cast<BtreeBackrefPin*>(&pin);
    pin_set.retire(bpin->get_range_pin());
  }

  Cache::backref_buf_entry_query_set_t
  get_cached_backrefs_in_range(
    paddr_t start,
    paddr_t end) final;

  Cache::backref_buf_entry_query_set_t
  get_cached_backref_removals_in_range(
    paddr_t start,
    paddr_t end) final;

  const backref_buf_entry_t::set_t& get_cached_backref_removals() final;
  const backref_buf_entry_t::set_t& get_cached_backrefs() final;
  backref_buf_entry_t get_cached_backref_removal(paddr_t addr) final;

  Cache::backref_extent_buf_entry_query_set_t
  get_cached_backref_extents_in_range(
    paddr_t start,
    paddr_t end) final;

  retrieve_backref_extents_ret retrieve_backref_extents(
    Transaction &t,
    Cache::backref_extent_buf_entry_query_set_t &&backref_extents,
    std::vector<CachedExtentRef> &extents) final;

  void cache_new_backref_extent(paddr_t paddr, extent_types_t type) final;

private:
  SegmentManagerGroup &sm_group;
  Cache &cache;

  btree_pin_set_t<paddr_t> pin_set;

  op_context_t<paddr_t> get_context(Transaction &t) {
    return op_context_t<paddr_t>{cache, t, &pin_set};
  }
};

} // namespace crimson::os::seastore::backref
