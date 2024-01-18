// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "include/interval_set.h"

namespace crimson::os::seastore {

class ExtentAllocator {
public:
  /**
   * alloc_extent
   *
   * Allocate continous region as much as given size.
   * Note that the inital state of extent is RESERVED after alloc_extent().
   * see rbm_extent_state_t in random_block_manager.h
   *
   * @param size
   * @return nullopt or the address range (rbm_abs_addr, len)
   */
  virtual std::optional<interval_set<rbm_abs_addr>> alloc_extent(
    size_t size) = 0;

  /**
   * alloc_extents
   *
   * Allocate regions for the given size. A continuous region is returned
   * if possible.
   *
   */
  virtual std::optional<interval_set<rbm_abs_addr>> alloc_extents(
    size_t size) = 0;

  /**
   * free_extent
   *
   * free given region
   *
   * @param rbm_abs_addr
   * @param size
   */
  virtual void free_extent(rbm_abs_addr addr, size_t size) = 0;
  /**
   * mark_extent_used
   *
   * This marks given region as used without alloc_extent.
   *
   * @param rbm_abs_addr
   * @param size
   */
  virtual void mark_extent_used(rbm_abs_addr addr, size_t size) = 0;
  /**
   * init 
   *
   * Initialize the address space the ExtentAllocator will manage
   *
   * @param start address (rbm_abs_addr)
   * @param total size
   * @param block size
   */
  virtual void init(rbm_abs_addr addr, size_t size, size_t b_size) = 0;
  virtual uint64_t get_available_size() const = 0;
  virtual uint64_t get_max_alloc_size() const = 0;
  virtual void close() = 0;
  /**
   * complete_allocation
   *
   * This changes this extent state from RESERVED to ALLOCATED
   *
   * @param start address
   * @param size
   */
  virtual void complete_allocation(rbm_abs_addr start, size_t size) = 0;
  virtual rbm_extent_state_t get_extent_state(rbm_abs_addr addr, size_t size) = 0;
  virtual ~ExtentAllocator() {}
};
using ExtentAllocatorRef = std::unique_ptr<ExtentAllocator>;


}
