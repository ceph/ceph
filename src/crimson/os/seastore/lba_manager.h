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
#include "crimson/os/seastore/segment_manager.h"

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
   */
  using get_mappings_iertr = base_iertr;
  using get_mappings_ret = get_mappings_iertr::future<lba_pin_list_t>;
  virtual get_mappings_ret get_mappings(
    Transaction &t,
    laddr_t offset, extent_len_t length) = 0;

  /**
   * Fetches mappings for a list of laddr_t in range [offset, offset + len)
   *
   * Future will not resolve until all pins have resolved (set_paddr called)
   */
  virtual get_mappings_ret get_mappings(
    Transaction &t,
    laddr_list_t &&extent_lisk) = 0;

  /**
   * Fetches the mapping for laddr_t
   *
   * Future will not resolve until the pin has resolved (set_paddr called)
   */
  using get_mapping_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using get_mapping_ret = get_mapping_iertr::future<LBAPinRef>;
  virtual get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset) = 0;

  /**
   * Allocates a new mapping referenced by LBARef
   *
   * Offset will be relative to the block offset of the record
   * This mapping will block from transaction submission until set_paddr
   * is called on the LBAPin.
   */
  using alloc_extent_iertr = base_iertr;
  using alloc_extent_ret = alloc_extent_iertr::future<LBAPinRef>;
  virtual alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    paddr_t addr) = 0;

  struct ref_update_result_t {
    unsigned refcount = 0;
    paddr_t addr;
    extent_len_t length = 0;
  };
  using ref_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using ref_ret = ref_iertr::future<ref_update_result_t>;

  /**
   * Decrements ref count on extent
   *
   * @return returns resulting refcount
   */
  virtual ref_ret decref_extent(
    Transaction &t,
    laddr_t addr) = 0;

  /**
   * Increments ref count on extent
   *
   * @return returns resulting refcount
   */
  virtual ref_ret incref_extent(
    Transaction &t,
    laddr_t addr) = 0;

  virtual void complete_transaction(
    Transaction &t) = 0;

  /**
   * Should be called after replay on each cached extent.
   * Implementation must initialize the LBAPin on any
   * LogicalCachedExtent's and may also read in any dependent
   * structures, etc.
   *
   * @return returns whether the extent is alive
   */
  using init_cached_extent_iertr = base_iertr;
  using init_cached_extent_ret = init_cached_extent_iertr::future<bool>;
  virtual init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) = 0;

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
   * Calls f for each mapped space usage
   */
  using scan_mapped_space_iertr = base_iertr::extend_ertr<
    SegmentManager::read_ertr>;
  using scan_mapped_space_ret = scan_mapped_space_iertr::future<>;
  using scan_mapped_space_func_t = std::function<
    void(paddr_t, extent_len_t)>;
  virtual scan_mapped_space_ret scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) = 0;

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
   * update lba mapping for a delayed allocated extent
   */
  using update_mapping_iertr = base_iertr;
  using update_mapping_ret = base_iertr::future<>;
  virtual update_mapping_ret update_mapping(
    Transaction& t,
    laddr_t laddr,
    paddr_t prev_addr,
    paddr_t paddr) = 0;

  /**
   * update_mappings
   *
   * update lba mappings for delayed allocated extents
   */
  using update_mappings_iertr = update_mapping_iertr;
  using update_mappings_ret = update_mapping_ret;
  update_mappings_ret update_mappings(
    Transaction& t,
    const std::list<LogicalCachedExtentRef>& extents,
    const std::vector<paddr_t>& original_paddrs);

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
    seastore_off_t len) = 0;

  virtual void add_pin(LBAPin &pin) = 0;

  virtual ~LBAManager() {}
};
using LBAManagerRef = std::unique_ptr<LBAManager>;

class Cache;
namespace lba_manager {
LBAManagerRef create_lba_manager(
  SegmentManager &segment_manager,
  Cache &cache);
}

}
