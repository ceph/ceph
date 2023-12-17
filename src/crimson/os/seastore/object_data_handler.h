// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <limits>

#include "include/buffer.h"

#include "test/crimson/seastore/test_block.h" // TODO

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/transaction.h"

namespace crimson::os::seastore {

struct block_delta_t {
  uint64_t offset = 0;
  extent_len_t len = 0;
  bufferlist bl;

  DENC(block_delta_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.offset, p);
    denc(v.len, p);
    denc(v.bl, p);
    DENC_FINISH(p);
  }
};

struct ObjectDataBlock : crimson::os::seastore::LogicalCachedExtent {
  using Ref = TCachedExtentRef<ObjectDataBlock>;

  std::vector<block_delta_t> delta = {};

  interval_set<extent_len_t> modified_region;

  explicit ObjectDataBlock(ceph::bufferptr &&ptr)
    : LogicalCachedExtent(std::move(ptr)) {}
  explicit ObjectDataBlock(const ObjectDataBlock &other)
    : LogicalCachedExtent(other), modified_region(other.modified_region) {}
  explicit ObjectDataBlock(extent_len_t length)
    : LogicalCachedExtent(length) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    return CachedExtentRef(new ObjectDataBlock(*this));
  };

  static constexpr extent_types_t TYPE = extent_types_t::OBJECT_DATA_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void overwrite(extent_len_t offset, bufferlist bl) {
    auto iter = bl.cbegin();
    iter.copy(bl.length(), get_bptr().c_str() + offset);
    delta.push_back({offset, bl.length(), bl});
    modified_region.union_insert(offset, bl.length());
  }

  ceph::bufferlist get_delta() final;

  void apply_delta(const ceph::bufferlist &bl) final;

  std::optional<modified_region_t> get_modified_region() final {
    if (modified_region.empty()) {
      return std::nullopt;
    }
    return modified_region_t{modified_region.range_start(),
      modified_region.range_end() - modified_region.range_start()};
  }

  void clear_modified_region() final {
    modified_region.clear();
  }

  void logical_on_delta_write() final {
    delta.clear();
  }
};
using ObjectDataBlockRef = TCachedExtentRef<ObjectDataBlock>;

class ObjectDataHandler {
public:
  using base_iertr = TransactionManager::base_iertr;

  ObjectDataHandler(uint32_t mos) : max_object_size(mos),
    delta_based_overwrite_max_extent_size(
      crimson::common::get_conf<Option::size_t>("seastore_data_delta_based_overwrite")) {}

  struct context_t {
    TransactionManager &tm;
    Transaction &t;
    Onode &onode;
    Onode *d_onode = nullptr; // The desination node in case of clone
  };

  /// Writes bl to [offset, offset + bl.length())
  using write_iertr = base_iertr;
  using write_ret = write_iertr::future<>;
  write_ret write(
    context_t ctx,
    objaddr_t offset,
    const bufferlist &bl);

  using zero_iertr = base_iertr;
  using zero_ret = zero_iertr::future<>;
  zero_ret zero(
    context_t ctx,
    objaddr_t offset,
    extent_len_t len);

  /// Reads data in [offset, offset + len)
  using read_iertr = base_iertr;
  using read_ret = read_iertr::future<bufferlist>;
  read_ret read(
    context_t ctx,
    objaddr_t offset,
    extent_len_t len);

  /// sparse read data, get range interval in [offset, offset + len)
  using fiemap_iertr = base_iertr;
  using fiemap_ret = fiemap_iertr::future<std::map<uint64_t, uint64_t>>;
  fiemap_ret fiemap(
    context_t ctx,
    objaddr_t offset,
    extent_len_t len);

  /// Clears data past offset
  using truncate_iertr = base_iertr;
  using truncate_ret = truncate_iertr::future<>;
  truncate_ret truncate(
    context_t ctx,
    objaddr_t offset);

  /// Clears data and reservation
  using clear_iertr = base_iertr;
  using clear_ret = clear_iertr::future<>;
  clear_ret clear(context_t ctx);

  /// Clone data of an Onode
  using clone_iertr = base_iertr;
  using clone_ret = clone_iertr::future<>;
  clone_ret clone(context_t ctx);

private:
  /// Updates region [_offset, _offset + bl.length) to bl
  write_ret overwrite(
    context_t ctx,        ///< [in] ctx
    laddr_t offset,       ///< [in] write offset
    extent_len_t len,     ///< [in] len to write, len == bl->length() if bl
    std::optional<bufferlist> &&bl, ///< [in] buffer to write, empty for zeros
    lba_pin_list_t &&pins ///< [in] set of pins overlapping above region
  );

  /// Ensures object_data reserved region is prepared
  write_ret prepare_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);

  /// Trims data past size
  clear_ret trim_data_reservation(
    context_t ctx,
    object_data_t &object_data,
    extent_len_t size);

  clone_ret clone_extents(
    context_t ctx,
    object_data_t &object_data,
    lba_pin_list_t &pins,
    laddr_t data_base);

private:
  /**
   * max_object_size
   *
   * For now, we allocate a fixed region of laddr space of size max_object_size
   * for any object.  In the future, once we have the ability to remap logical
   * mappings (necessary for clone), we'll add the ability to grow and shrink
   * these regions and remove this assumption.
   */
  const uint32_t max_object_size = 0;
  extent_len_t delta_based_overwrite_max_extent_size = 0; // enable only if rbm is used
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::block_delta_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::ObjectDataBlock> : fmt::ostream_formatter {};
#endif
