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
  depth_t lba_depth = 0;
  depth_t segment_depth = 0;
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

/**
 * RootBlock
 *
 * Holds the physical addresses of all metadata roots.
 * In-memory values may be
 * - absolute: reference to block which predates the current transaction
 * - record_relative: reference to block updated in this transaction
 *   if !is_initial_pending()
 * - block_relative: reference to block updated in this transaction
 *   if is_initial_pending()
 *
 * Upon initial commit, on_initial_write checks physical references and updates
 * based on newly discovered address (relative ones must be block_relative).
 *
 * complete_load also updates addresses in memory post load based on block addr.
 *
 * Upon delta commit, on_delta_write uses record addr to update in-memory values.
 * apply_delta will do the same once implemented (TODO).
 */
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

  /**
   * prepare_write
   *
   * For RootBlock, serializes RootBlock::root into the bptr.
   */
  void prepare_write() final;

  static constexpr extent_types_t TYPE = extent_types_t::ROOT;
  extent_types_t get_type() const final {
    return extent_types_t::ROOT;
  }

  ceph::bufferlist get_delta() final {
    return ceph::bufferlist();
  }

  void apply_delta_and_adjust_crc(paddr_t base, const ceph::bufferlist &_bl) final {
    ceph_assert(0 == "TODO");
  }

  /// Patches relative addrs in memory based on actual address
  complete_load_ertr::future<> complete_load() final;

  /// Patches relative addrs in memory based on record commit addr
  void on_delta_write(paddr_t record_block_offset) final;

  /// Patches relative addrs in memory based on record addr
  void on_initial_write() final;

  btree_lba_root_t &get_lba_root();

};
using RootBlockRef = RootBlock::Ref;

}

WRITE_CLASS_DENC(crimson::os::seastore::btree_lba_root_t)
WRITE_CLASS_DENC(crimson::os::seastore::root_block_t)
