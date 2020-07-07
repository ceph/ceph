// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::lba_manager::btree {

struct lba_node_meta_t {
  laddr_t begin = 0;
  laddr_t end = 0;
  depth_t depth = 0;

  std::pair<lba_node_meta_t, lba_node_meta_t> split_into(laddr_t pivot) const {
    return std::make_pair(
      lba_node_meta_t{begin, pivot, depth},
      lba_node_meta_t{pivot, end, depth});
  }

  static lba_node_meta_t merge_from(const lba_node_meta_t &lhs, const lba_node_meta_t &rhs) {
    assert(lhs.depth == rhs.depth);
    return lba_node_meta_t{lhs.begin, rhs.end, lhs.depth};
  }

  static std::pair<lba_node_meta_t, lba_node_meta_t>
  rebalance(const lba_node_meta_t &lhs, const lba_node_meta_t &rhs, laddr_t pivot) {
    assert(lhs.depth == rhs.depth);
    return std::make_pair(
      lba_node_meta_t{lhs.begin, pivot, lhs.depth},
      lba_node_meta_t{pivot, rhs.end, lhs.depth});
  }
};

inline std::ostream &operator<<(
  std::ostream &lhs,
  const lba_node_meta_t &rhs)
{
  return lhs << "btree_node_meta_t("
	     << "begin=" << rhs.begin
	     << ", end=" << rhs.end
	     << ", depth=" << rhs.depth
	     << ")";
}

/* BtreeLBAPin
 *
 * References leaf node
 *
 * TODO: does not at this time actually keep the relevant
 * leaf resident in memory.  This is actually a bit tricky
 * as we can mutate and therefore replace a leaf referenced
 * by other, uninvolved but cached extents.  Will need to
 * come up with some kind of pinning mechanism that handles
 * that well.
 */
struct BtreeLBAPin : LBAPin {
  paddr_t paddr;
  laddr_t laddr = L_ADDR_NULL;
  extent_len_t length = 0;
  unsigned refcount = 0;

public:
  BtreeLBAPin(
    paddr_t paddr,
    laddr_t laddr,
    extent_len_t length)
    : paddr(paddr), laddr(laddr), length(length) {}

  extent_len_t get_length() const final {
    return length;
  }
  paddr_t get_paddr() const final {
    return paddr;
  }
  laddr_t get_laddr() const final {
    return laddr;
  }
  LBAPinRef duplicate() const final {
    return LBAPinRef(new BtreeLBAPin(*this));
  }
};

}
