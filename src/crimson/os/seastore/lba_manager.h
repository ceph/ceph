// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "include/interval_set.h"
#include "common/interval_map.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_mapping.h"
#include "crimson/os/seastore/logical_child_node.h"

namespace crimson::os::seastore {

/**
 * Abstract interface for managing the logical to physical mapping
 */
class LBAManager {
public:
  using base_iertr = Cache::base_iertr;

  using mkfs_iertr = base_iertr;
  using mkfs_ret = mkfs_iertr::future<>;
  virtual mkfs_ret mkfs(
    Transaction &t
  ) = 0;

  /**
   * Fetches mappings for laddr_t in range [offset, offset + len)
   *
   * Future will not resolve until all pins have resolved (set_paddr called)
   * For indirect lba mappings, get_mappings will always retrieve the original
   * lba value.
   */
  using get_mappings_iertr = base_iertr;
  using get_mappings_ret = get_mappings_iertr::future<lba_mapping_list_t>;
  virtual get_mappings_ret get_mappings(
    Transaction &t,
    laddr_t offset, extent_len_t length) = 0;

  /**
   * Fetches the mapping for laddr_t
   *
   * Future will not resolve until the pin has resolved (set_paddr called)
   * For indirect lba mappings, get_mapping will always retrieve the original
   * lba value.
   */
  using get_mapping_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using get_mapping_ret = get_mapping_iertr::future<LBAMapping>;
  virtual get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset) = 0;

  /**
   * Allocates a new mapping referenced by LBARef
   *
   * Offset will be relative to the block offset of the record
   * This mapping will block from transaction submission until set_paddr
   * is called on the LBAMapping.
   */
  using alloc_extent_iertr = base_iertr;
  using alloc_extent_ret = alloc_extent_iertr::future<LBAMapping>;
  virtual alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    LogicalChildNode &nextent,
    extent_ref_count_t refcount) = 0;

  using alloc_extents_ret = alloc_extent_iertr::future<
    std::vector<LBAMapping>>;
  virtual alloc_extents_ret alloc_extents(
    Transaction &t,
    laddr_t hint,
    std::vector<LogicalChildNodeRef> extents,
    extent_ref_count_t refcount) = 0;

  virtual alloc_extent_ret clone_mapping(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    laddr_t intermediate_key,
    laddr_t intermediate_base) = 0;

  virtual alloc_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) = 0;

  struct ref_update_result_t {
    laddr_t direct_key;
    extent_ref_count_t refcount = 0;
    pladdr_t addr;
    extent_len_t length = 0;
  };
  using ref_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using ref_ret = ref_iertr::future<ref_update_result_t>;

  /**
   * Removes a mapping and deal with indirection
   *
   * @return returns resulting refcount
   */
  virtual ref_ret remove_mapping(
    Transaction &t,
    laddr_t addr) = 0;

  struct remap_entry_t {
    extent_len_t offset;
    extent_len_t len;
    remap_entry_t(extent_len_t _offset, extent_len_t _len) {
      offset = _offset;
      len = _len;
    }
  };
  using remap_iertr = ref_iertr;
  using remap_ret = remap_iertr::future<std::vector<LBAMapping>>;

  /**
   * remap_mappings
   *
   * Remap an original mapping into new ones
   * Return the old mapping's info and new mappings
   */
  virtual remap_ret remap_mappings(
    Transaction &t,
    LBAMapping orig_mapping,
    std::vector<remap_entry_t> remaps,
    std::vector<LogicalChildNodeRef> extents  // Required if and only
						 // if pin isn't indirect
    ) = 0;

  /**
   * Should be called after replay on each cached extent.
   * Implementation must initialize the LBAMapping on any
   * LogicalChildNode's and may also read in any dependent
   * structures, etc.
   *
   * @return returns whether the extent is alive
   */
  using init_cached_extent_iertr = base_iertr;
  using init_cached_extent_ret = init_cached_extent_iertr::future<bool>;
  virtual init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) = 0;

#ifdef UNIT_TESTS_BUILT
  using check_child_trackers_ret = base_iertr::future<>;
  virtual check_child_trackers_ret check_child_trackers(Transaction &t) = 0;
#endif

  /**
   * Calls f for each mapping in [begin, end)
   */
  using scan_mappings_iertr = base_iertr;
  using scan_mappings_ret = scan_mappings_iertr::future<>;
  using scan_mappings_func_t = std::function<
    void(laddr_t, paddr_t, extent_len_t)>;
  virtual scan_mappings_ret scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) = 0;

  /**
   * rewrite_extent
   *
   * rewrite extent into passed transaction
   */
  using rewrite_extent_iertr = base_iertr;
  using rewrite_extent_ret = rewrite_extent_iertr::future<>;
  virtual rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) = 0;

  /**
   * update_mapping
   *
   * update lba mapping for rewrite
   */
  using update_mapping_iertr = base_iertr;
  using update_mapping_ret = base_iertr::future<extent_ref_count_t>;
  virtual update_mapping_ret update_mapping(
    Transaction& t,
    laddr_t laddr,
    extent_len_t prev_len,
    paddr_t prev_addr,
    LogicalChildNode& nextent) = 0;

  /**
   * update_mappings
   *
   * update lba mappings for delayed allocated extents
   */
  using update_mappings_iertr = update_mapping_iertr;
  using update_mappings_ret = update_mappings_iertr::future<>;
  virtual update_mappings_ret update_mappings(
    Transaction& t,
    const std::list<LogicalChildNodeRef>& extents) = 0;

  /**
   * get_physical_extent_if_live
   *
   * Returns extent at addr/laddr if still live (if laddr
   * still points at addr).  Extent must be an internal, physical
   * extent.
   *
   * Returns a null CachedExtentRef if extent is not live.
   */
  using get_physical_extent_if_live_iertr = base_iertr;
  using get_physical_extent_if_live_ret =
    get_physical_extent_if_live_iertr::future<CachedExtentRef>;
  virtual get_physical_extent_if_live_ret get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    extent_len_t len) = 0;

  using refresh_lba_mapping_iertr = base_iertr;
  using refresh_lba_mapping_ret = refresh_lba_mapping_iertr::future<LBAMapping>;
  virtual refresh_lba_mapping_ret refresh_lba_mapping(
    Transaction &t,
    LBAMapping mapping) = 0;

  virtual ~LBAManager() {}
};
using LBAManagerRef = std::unique_ptr<LBAManager>;

class Cache;
namespace lba {
LBAManagerRef create_lba_manager(Cache &cache);
}

}
