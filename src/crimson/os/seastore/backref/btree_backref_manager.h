// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/backref/backref_tree_node.h"
#include "crimson/os/seastore/btree/fixed_kv_btree.h"

namespace crimson::os::seastore::backref {

constexpr size_t BACKREF_BLOCK_SIZE = 4096;

using BackrefBtree = FixedKVBtree<
  paddr_t, backref_map_val_t, BackrefInternalNode,
  BackrefLeafNode, BackrefCursor, BACKREF_BLOCK_SIZE>;

class BtreeBackrefManager : public BackrefManager {
public:

  BtreeBackrefManager(Cache &cache)
    : cache(cache)
  {}

  mkfs_ret mkfs(
    Transaction &t) final override;

  get_mapping_ret  get_mapping(
    Transaction &t,
    paddr_t offset) final override;

  get_mappings_ret get_mappings(
    Transaction &t,
    paddr_t offset,
    paddr_t end) final override;

  new_mapping_ret new_mapping(
    Transaction &t,
    paddr_t key,
    extent_len_t len,
    laddr_t val,
    extent_types_t type) final override;

  merge_cached_backrefs_ret merge_cached_backrefs(
    Transaction &t,
    const journal_seq_t &limit,
    const uint64_t max) final override;

  remove_mapping_ret remove_mapping(
    Transaction &t,
    paddr_t offset) final override;

  scan_mapped_space_ret scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) final override;

  init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) final override;

  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final override;

  Cache::backref_entry_query_mset_t
  get_cached_backref_entries_in_range(
    paddr_t start,
    paddr_t end) final override;

  retrieve_backref_extents_in_range_ret
  retrieve_backref_extents_in_range(
    Transaction &t,
    paddr_t start,
    paddr_t end) final override;

  void cache_new_backref_extent(
    paddr_t paddr,
    paddr_t key,
    extent_types_t type) final override;

private:
  Cache &cache;

  op_context_t get_context(Transaction &t) {
    return op_context_t{cache, t};
  }
};

} // namespace crimson::os::seastore::backref
