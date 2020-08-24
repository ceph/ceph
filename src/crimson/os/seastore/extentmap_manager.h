// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>
#include <list>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "crimson/osd/exceptions.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

#define PAGE_SIZE 4096
#define EXTMAP_BLOCK_SIZE 4096

namespace crimson::os::seastore {

struct lext_map_val_t {
  laddr_t laddr;
  extent_len_t length = 0;

  lext_map_val_t(
    laddr_t laddr,
    extent_len_t length)
    : laddr(laddr), length(length) {}

};

class extent_mapping_t
{
public:
  objaddr_t logical_offset = 0;  //offset in object
  laddr_t laddr;     // lextent start address aligned with block size.
  extent_len_t length = 0;
  explicit extent_mapping_t(objaddr_t lo) : logical_offset(lo) { }

  extent_mapping_t(
    objaddr_t lo,
    laddr_t laddr,
    extent_len_t length)
    : logical_offset(lo), laddr(laddr), length(length) {}

  ~extent_mapping_t() {}
};

enum class extmap_root_state_t : uint8_t {
  INITIAL = 0,
  MUTATED = 1,
  NONE = 0xFF
};

using extent_map_list_t = std::list<extent_mapping_t>;
std::ostream &operator<<(std::ostream &out, const extent_mapping_t &rhs);
std::ostream &operator<<(std::ostream &out, const extent_map_list_t &rhs);

struct extmap_root_t {
  depth_t depth = 0;
  extmap_root_state_t state;
  laddr_t extmap_root_laddr;
  extmap_root_t(depth_t dep, laddr_t laddr)
  : depth(dep),
    extmap_root_laddr(laddr) { state = extmap_root_state_t::INITIAL; }
};

/**
 * Abstract interface for managing the object inner offset to logical addr mapping
 * each onode has an extentmap tree for a particular onode.
 */
class ExtentMapManager {
public:
  using initialize_extmap_ertr = TransactionManager::alloc_extent_ertr;
  using initialize_extmap_ret = initialize_extmap_ertr::future<extmap_root_t>;
  virtual initialize_extmap_ret initialize_extmap(Transaction &t) = 0;

  /* find_lextents
   *
   * Return a list of all extent_mapping_t overlapping any portion of lo~len.
   * or if not find any overlap extent_mapping_t will return the next extent after the range.
   */
  using find_lextent_ertr = TransactionManager::read_extent_ertr;
  using find_lextent_ret = find_lextent_ertr::future<extent_map_list_t>;
  virtual find_lextent_ret
    find_lextent(const extmap_root_t &extmap_root, Transaction &t, objaddr_t lo, extent_len_t len) = 0;

  /* add_lextent
   *
   * add a new mapping (object offset -> laddr, length) to extent map
   * return the added extent_mapping_t
   */
  using add_lextent_ertr = TransactionManager::read_extent_ertr;
  using add_lextent_ret = add_lextent_ertr::future<extent_mapping_t>;
  virtual add_lextent_ret
    add_lextent(extmap_root_t &extmap_root, Transaction &t, objaddr_t lo, lext_map_val_t val) = 0;

  /* rm_lextent
   *
   * remove an existing extent mapping from extent map
   * return true if the extent mapping is removed, otherwise return false
   */
  using rm_lextent_ertr = TransactionManager::read_extent_ertr;
  using rm_lextent_ret = rm_lextent_ertr::future<bool>;
  virtual rm_lextent_ret rm_lextent(extmap_root_t &extmap_root, Transaction &t, objaddr_t lo, lext_map_val_t val) = 0;

  virtual ~ExtentMapManager() {}
};
using ExtentMapManagerRef = std::unique_ptr<ExtentMapManager>;

namespace extentmap_manager {
/* creat ExtentMapManager for an extentmap
 * if it is a new extmap after create_extentmap_manager need call initialize_extmap
 * to initialize the extent map before use it
 * if it is an exsiting extmap, needn't initialize_extmap
 */
ExtentMapManagerRef create_extentmap_manager(
  TransactionManager &trans_manager);

}

}
