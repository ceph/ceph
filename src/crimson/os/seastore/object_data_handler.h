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

class overwrite_buf_t {
public:
  overwrite_buf_t() = default;
  bool is_empty() const {
    return changes.empty() && !has_cached_bptr();
  }
  bool has_cached_bptr() const {
    return ptr.has_value();
  }
  void add(const block_delta_t &b) {
    changes.push_back(b);
  }
  void apply_changes_to(bufferptr &b) const {
    assert(!changes.empty());
    for (auto p : changes) {
      auto iter = p.bl.cbegin();
      iter.copy(p.bl.length(), b.c_str() + p.offset);
    }
    changes.clear();
  }
  const bufferptr &get_cached_bptr(const bufferptr &_ptr) const {
    apply_changes_to_cache(_ptr);
    return *ptr;
  }
  bufferptr &get_cached_bptr(const bufferptr &_ptr) {
    apply_changes_to_cache(_ptr);
    return *ptr;
  }
  bufferptr &&move_cached_bptr() {
    assert(has_cached_bptr());
    apply_changes_to(*ptr);
    return std::move(*ptr);
  }
private:
  void apply_changes_to_cache(const bufferptr &_ptr) const {
    assert(!is_empty());
    if (!has_cached_bptr()) {
      ptr = ceph::buffer::copy(_ptr.c_str(), _ptr.length());
    }
    if (!changes.empty()) {
      apply_changes_to(*ptr);
    }
  }
  mutable std::vector<block_delta_t> changes = {};
  mutable std::optional<ceph::bufferptr> ptr = std::nullopt;
};

struct ObjectDataBlock : crimson::os::seastore::LogicalCachedExtent {
  using Ref = TCachedExtentRef<ObjectDataBlock>;

  std::vector<block_delta_t> delta = {};

  interval_set<extent_len_t> modified_region;

  // to provide the local modified view during transaction
  overwrite_buf_t cached_overwrites;

  explicit ObjectDataBlock(ceph::bufferptr &&ptr)
    : LogicalCachedExtent(std::move(ptr)) {}
  explicit ObjectDataBlock(const ObjectDataBlock &other, share_buffer_t s)
    : LogicalCachedExtent(other, s), modified_region(other.modified_region) {}
  explicit ObjectDataBlock(extent_len_t length)
    : LogicalCachedExtent(length) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    return CachedExtentRef(new ObjectDataBlock(*this, share_buffer_t{}));
  };

  static constexpr extent_types_t TYPE = extent_types_t::OBJECT_DATA_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void overwrite(extent_len_t offset, bufferlist bl) {
    block_delta_t b {offset, bl.length(), bl};
    cached_overwrites.add(b);
    delta.push_back(b);
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

  void prepare_commit() final {
    if (is_mutation_pending() || is_exist_mutation_pending()) {
      ceph_assert(!cached_overwrites.is_empty());
      if (cached_overwrites.has_cached_bptr()) {
        set_bptr(cached_overwrites.move_cached_bptr());
      } else {
        // The optimized path to minimize data copy
        cached_overwrites.apply_changes_to(CachedExtent::get_bptr());
      }
    } else {
      assert(cached_overwrites.is_empty());
    }
  }

  void logical_on_delta_write() final {
    delta.clear();
  }

  bufferptr &get_bptr() override {
    if (cached_overwrites.is_empty()) {
      return CachedExtent::get_bptr();
    } else {
      return cached_overwrites.get_cached_bptr(CachedExtent::get_bptr());
    }
  }

  const bufferptr &get_bptr() const override {
    if (cached_overwrites.is_empty()) {
      return CachedExtent::get_bptr();
    } else {
      return cached_overwrites.get_cached_bptr(CachedExtent::get_bptr());
    }
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
