// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore::lba_manager::btree {

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
