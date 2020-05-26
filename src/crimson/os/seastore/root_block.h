// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

using depth_t = uint32_t;

/* Belongs in lba_manager/btree, TODO: generalize this to
 * permit more than one lba_manager implementation
 */
struct btree_lba_root_t {
  depth_t lba_depth;
  depth_t segment_depth;
  paddr_t lba_root_addr;
  paddr_t segment_root;

  DENC(btree_lba_root_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.lba_depth, p);
    denc(v.segment_depth, p);
    denc(v.lba_root_addr, p);
    denc(v.segment_root, p);
    DENC_FINISH(p);
  }
};

struct root_block_t {
  btree_lba_root_t lba_root;

  DENC(root_block_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.lba_root, p);
    DENC_FINISH(p);
  }
};

struct RootBlock : CachedExtent {
  constexpr static segment_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<RootBlock>;

  root_block_t root;

  template <typename... T>
  RootBlock(T&&... t) : CachedExtent(std::forward<T>(t)...) {}

  RootBlock(const RootBlock &rhs) = default;

  CachedExtentRef duplicate_for_write() final {
    return CachedExtentRef(new RootBlock(*this));
  };

  void prepare_write() final;

  static constexpr extent_types_t TYPE = extent_types_t::ROOT;
  extent_types_t get_type() const final {
    return extent_types_t::ROOT;
  }

  ceph::bufferlist get_delta() final {
    ceph_assert(0 == "TODO");
    return ceph::bufferlist();
  }

  void apply_delta(paddr_t base, ceph::bufferlist &bl) final {
    ceph_assert(0 == "TODO");
  }

  complete_load_ertr::future<> complete_load() final;

  void set_lba_root(btree_lba_root_t lba_root);
  btree_lba_root_t &get_lba_root() {
    return root.lba_root;
  }

};
using RootBlockRef = RootBlock::Ref;

}

WRITE_CLASS_DENC(crimson::os::seastore::btree_lba_root_t)
WRITE_CLASS_DENC(crimson::os::seastore::root_block_t)
