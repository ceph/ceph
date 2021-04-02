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
  using base_ertr = Cache::get_extent_ertr;

  using mkfs_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using mkfs_ret = mkfs_ertr::future<>;
  virtual mkfs_ret mkfs(
    Transaction &t
  ) = 0;

  /**
   * Fetches mappings for laddr_t in range [offset, offset + len)
   *
   * Future will not resolve until all pins have resolved (set_paddr called)
   */
  using get_mapping_ertr = base_ertr;
  using get_mapping_ret = get_mapping_ertr::future<lba_pin_list_t>;
  virtual get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset, extent_len_t length) = 0;

  /**
   * Fetches mappings for laddr_t in range [offset, offset + len)
   *
   * Future will not result until all pins have resolved (set_paddr called)
   */
  using get_mappings_ertr = base_ertr;
  using get_mappings_ret = get_mapping_ertr::future<lba_pin_list_t>;
  virtual get_mappings_ret get_mappings(
    Transaction &t,
    laddr_list_t &&extent_lisk) = 0;

  /**
   * Allocates a new mapping referenced by LBARef
   *
   * Offset will be relative to the block offset of the record
   * This mapping will block from transaction submission until set_paddr
   * is called on the LBAPin.
   */
  using alloc_extent_ertr = base_ertr;
  using alloc_extent_ret = alloc_extent_ertr::future<LBAPinRef>;
  virtual alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    paddr_t addr) = 0;

  /**
   * Creates a new absolute mapping.
   *
   * off~len must be unreferenced
   */
  using set_extent_ertr = base_ertr::extend<
    crimson::ct_error::invarg>;
  using set_extent_ret = set_extent_ertr::future<LBAPinRef>;
  virtual set_extent_ret set_extent(
    Transaction &t,
    laddr_t off, extent_len_t len, paddr_t addr) = 0;


  struct ref_update_result_t {
    unsigned refcount = 0;
    paddr_t addr;
    extent_len_t length = 0;
  };
  using ref_ertr = base_ertr::extend<
    crimson::ct_error::enoent>;
  using ref_ret = ref_ertr::future<ref_update_result_t>;

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

  using complete_transaction_ertr = base_ertr;
  using complete_transaction_ret = complete_transaction_ertr::future<>;
  virtual complete_transaction_ret complete_transaction(
    Transaction &t) = 0;

  /**
   * Should be called after replay on each cached extent.
   * Implementation must initialize the LBAPin on any
   * LogicalCachedExtent's and may also read in any dependent
   * structures, etc.
   */
  using init_cached_extent_ertr = base_ertr;
  using init_cached_extent_ret = init_cached_extent_ertr::future<>;
  virtual init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) = 0;

  /**
   * Calls f for each mapping in [begin, end)
   */
  using scan_mappings_ertr = base_ertr;
  using scan_mappings_ret = scan_mappings_ertr::future<>;
  using scan_mappings_func_t = std::function<
    void(laddr_t, paddr_t, extent_len_t)>;
  virtual scan_mappings_ret scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) = 0;

  /**
   * Calls f for each mapped space usage in [begin, end)
   */
  using scan_mapped_space_ertr = base_ertr::extend_ertr<
    SegmentManager::read_ertr>;
  using scan_mapped_space_ret = scan_mapped_space_ertr::future<>;
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
  using rewrite_extent_ertr = base_ertr;
  using rewrite_extent_ret = rewrite_extent_ertr::future<>;
  virtual rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) = 0;

  /**
   * get_physical_extent_if_live
   *
   * Returns extent at addr/laddr if still live (if laddr
   * still points at addr).  Extent must be an internal, physical
   * extent.
   *
   * Returns a null CachedExtentRef if extent is not live.
   */
  using get_physical_extent_if_live_ertr = base_ertr;
  using get_physical_extent_if_live_ret =
    get_physical_extent_if_live_ertr::future<CachedExtentRef>;
  virtual get_physical_extent_if_live_ret get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    segment_off_t len) = 0;

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
