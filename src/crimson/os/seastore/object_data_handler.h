// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>
#include <limits>

#include "include/buffer.h"

#include "test/crimson/seastore/test_block.h" // TODO

#include "crimson/os/seastore/laddr_interval_set.h"
#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/logical_child_node.h"

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

struct overwrite_range_t {
  objaddr_t unaligned_len = 0;
  laddr_offset_t unaligned_begin;
  laddr_offset_t unaligned_end;
  laddr_t aligned_begin = L_ADDR_NULL;
  laddr_t aligned_end = L_ADDR_NULL;
  objaddr_t aligned_len = 0;
  overwrite_range_t(
    objaddr_t unaligned_len,
    laddr_offset_t unaligned_begin,
    laddr_offset_t unaligned_end,
    extent_len_t block_size)
    : unaligned_len(unaligned_len),
      unaligned_begin(unaligned_begin),
      unaligned_end(unaligned_end),
      aligned_begin(unaligned_begin.get_aligned_laddr(block_size)),
      aligned_end(unaligned_end.get_roundup_laddr(block_size)),
      aligned_len(
	aligned_end.template get_byte_distance<
	  extent_len_t>(aligned_begin))
  {}

  bool is_empty() const {
    return unaligned_begin == unaligned_end;
  }
  bool is_range_in_mapping(
    const LBAMapping &mapping) const
  {
    return unaligned_begin >= mapping.get_key() &&
       unaligned_end <= mapping.get_key() + mapping.get_length();
  }
  bool is_begin_aligned(size_t alignment) const {
    return unaligned_begin.is_aligned(alignment);
  }
  bool is_end_aligned(size_t alignment) const {
    return unaligned_end.is_aligned(alignment);
  }
#ifndef NDEBUG
  bool is_begin_in_mapping(const LBAMapping &mapping) const {
    return unaligned_begin > mapping.get_key() &&
      unaligned_begin < mapping.get_key() + mapping.get_length();
  }
  bool is_end_in_mapping(const LBAMapping &mapping) const {
    return unaligned_end > mapping.get_key() &&
      unaligned_end < mapping.get_key() + mapping.get_length();
  }
#endif
  void expand_begin(laddr_t new_begin) {
    assert(new_begin <= aligned_begin);
    unaligned_len += new_begin.template get_byte_distance<
      extent_len_t>(unaligned_begin);
    aligned_len += new_begin.template get_byte_distance<
      extent_len_t>(aligned_begin);
    aligned_begin = new_begin;
    unaligned_begin = laddr_offset_t{new_begin};
  }
  void expand_end(laddr_t new_end) {
    assert(new_end >= aligned_end);
    unaligned_len += new_end.template get_byte_distance<
      extent_len_t>(unaligned_end);
    aligned_len += new_end.template get_byte_distance<
      extent_len_t>(aligned_end);
    aligned_end = new_end;
    unaligned_end = laddr_offset_t{new_end};
  }
  void shrink_begin(laddr_t new_begin) {
    assert(new_begin >= aligned_begin);
    unaligned_len -= new_begin.template get_byte_distance<
      extent_len_t>(unaligned_begin);
    aligned_len -= new_begin.template get_byte_distance<
      extent_len_t>(aligned_begin);
    aligned_begin = new_begin;
    unaligned_begin = laddr_offset_t{new_begin};
  }
  void shrink_end(laddr_t new_end) {
    assert(new_end <= aligned_end);
    unaligned_len -= new_end.template get_byte_distance<
      extent_len_t>(unaligned_end);
    aligned_len -= new_end.template get_byte_distance<
      extent_len_t>(aligned_end);
    aligned_end = new_end;
    unaligned_end = laddr_offset_t{new_end};
  }
};
std::ostream& operator<<(std::ostream &, const overwrite_range_t &);

struct data_t {
  std::optional<bufferlist> headbl;
  std::optional<bufferlist> bl;
  std::optional<bufferlist> tailbl;
};
std::ostream& operator<<(std::ostream &out, const data_t &data);

enum edge_t : uint8_t {
  NONE = 0x0,
  LEFT = 0x1,
  RIGHT = 0x2,
  BOTH = 0x3
};
std::ostream& operator<<(std::ostream &out, const edge_t &edge);

struct ObjectDataBlock : crimson::os::seastore::LogicalChildNode {
  using Ref = TCachedExtentRef<ObjectDataBlock>;

  std::vector<block_delta_t> delta = {};

  interval_set<extent_len_t> modified_region;

  // to provide the local modified view during transaction
  overwrite_buf_t cached_overwrites;

  explicit ObjectDataBlock(ceph::bufferptr &&ptr)
    : LogicalChildNode(std::move(ptr)) {}
  explicit ObjectDataBlock(const ObjectDataBlock &other, share_buffer_t s)
    : LogicalChildNode(other, s), modified_region(other.modified_region) {}
  explicit ObjectDataBlock(extent_len_t length)
    : LogicalChildNode(length) {}

  CachedExtentRef duplicate_for_write(Transaction&) final {
    return CachedExtentRef(new ObjectDataBlock(*this, share_buffer_t{}));
  };

  static constexpr extent_types_t TYPE = extent_types_t::OBJECT_DATA_BLOCK;
  extent_types_t get_type() const final {
    return TYPE;
  }

  void overwrite(extent_len_t offset, bufferlist bl) {
    assert(is_mutation_pending() || is_exist_mutation_pending());
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
    if (has_mutation()) {
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
  /// Note that the clone always assume that ctx.onode
  /// is a snap onode, so, for OP_CLONE, the caller of
  /// this method should swap the layout of the onode
  /// and the dest_onode first.
  using clone_iertr = base_iertr;
  using clone_ret = clone_iertr::future<>;
  clone_ret clone(context_t ctx);

private:
  /// Updates region [_offset, _offset + bl.length) to bl
  write_ret overwrite(
    context_t ctx,
    laddr_t data_base,
    objaddr_t offset,
    extent_len_t len,
    std::optional<bufferlist> &&bl,
    LBAMapping first_mapping);

  /**
   * do_clone
   *
   * Clone lba mappings from object_data to d_object_data.
   * object_data must belong to ctx.onode, and d_object_data must belong to ctx.d_onode
   * This implementation is asymmetric and optimizes for (but does not require) the case
   * that source is not further mutated.
   */
  clone_ret do_clone(
    context_t ctx,
    object_data_t &object_data,
    object_data_t &d_object_data,
    LBAMapping first_mapping,
    bool updateref);

  /// Ensures object_data reserved region is prepared
  write_iertr::future<std::optional<LBAMapping>>
  prepare_data_reservation(
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
    lba_mapping_list_t &pins,
    laddr_t data_base);

  enum op_type_t : uint8_t {
    OVERWRITE,
    ZERO,
    TRIM
  };
  enum edge_handle_policy_t : uint8_t {
    DELTA_BASED_PUNCH,
    MERGE_INPLACE,
    REMAP
  };

  edge_handle_policy_t get_edge_handle_policy(
    const LBAMapping &edge_mapping,
    laddr_t start,
    extent_len_t len,
    op_type_t op_type) const
  {
#ifndef NDEBUG
    laddr_interval_set_t range;
    range.insert(edge_mapping.get_key(), edge_mapping.get_length());
    assert(range.contains(start, len));
#endif

    //XXX: may need to adjust once object data partial write is available.
    if (edge_mapping.is_pending()) {
      // TODO: all LBAMapping::is_XXX_pending() methods search the parent
      //       lba nodes, which consumes cpu. Fortunately, this branch happens
      //       mostly in the recovery case, which is relatively rare compared
      //       to normal IO processing.
      if (edge_mapping.is_initial_pending()) {
	return edge_handle_policy_t::MERGE_INPLACE;
      } else {
	return edge_handle_policy_t::DELTA_BASED_PUNCH;
      }
    }

    // TODO: allow TRIM to do delta based overwrites. We forbid it
    // 	     now because it violate unit tests.
    if (op_type == op_type_t::TRIM ||
	op_type == op_type_t::ZERO ||
	len > delta_based_overwrite_max_extent_size ||
	edge_mapping.is_zero_reserved() ||
	edge_mapping.is_indirect()) {
      return edge_handle_policy_t::REMAP;
    }

    return edge_handle_policy_t::DELTA_BASED_PUNCH;
  }

  write_ret delta_based_overwrite(
    context_t ctx,
    extent_len_t offset,
    extent_len_t len,
    LBAMapping mapping,
    std::optional<bufferlist> data);

  // read the padding edge data into data.headbl/data.tailbl
  read_iertr::future<> read_unaligned_edge_data(
    context_t ctx,
    const overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping &mapping,
    edge_t edge);

  // read the pending edge mapping's data into data.headbl/data.tailbl,
  // remove the mapping and expand the overwrite_range; basically, this
  // is equivalent to merge the current overwrite range with the pending
  // edge mapping
  read_iertr::future<> merge_pending_edge(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping &mapping,
    edge_t edge);

  // cut the overlapped part of data.bl, apply it to the
  // edge_maping as a mutation and shrink the overwrite_range.
  base_iertr::future<LBAMapping> delta_based_edge_overwrite(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t& data,
    LBAMapping edge_mapping,
    edge_t edge);

  // drop the overlapped part of the edge mapping
  base_iertr::future<LBAMapping> do_remap_based_edge_punch(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping edge_mapping,
    edge_t edge);

  // merge the overwrite data with that of the edge_mapping,
  // remove the edge_mapping and expand the overwrite_range.
  base_iertr::future<LBAMapping> do_merge_based_edge_punch(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping edge_mapping,
    edge_t edge);

  // punch the edge mapping following the edge_handle_policy_t.
  // Specifically:
  // 1. edge_handle_policy_t::DELTA_BASED_PUNCH: cut the overlapped part
  //    of data.bl, apply it to the edge_maping as a mutation and shrink
  //    the overwrite_range.
  // 2. edge_handle_policy_t::MERGE_PENDING: merge the overwrite data with
  //    that of the edge_mapping, remove the edge_mapping and expand the
  //    overwrite_range.
  // 3. edge_handle_policy_t::REMAP: drop the overlapped part of the edge mapping
  base_iertr::future<LBAMapping>
  punch_mapping_on_edge(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping edge_mapping,
    edge_t edge,
    op_type_t op_type);

  // The first step in a multi-mapping-hole-punching scenario: remap the
  // left mapping if it crosses the left edge of the hole's range
  base_iertr::future<LBAMapping> punch_left_mapping(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &overwrite_data,
    LBAMapping left_mapping,
    op_type_t op_type);

  // The second step in a multi-mapping-hole-punching scenario: remove
  // all the mappings that are strictly inside the hole's range
  base_iertr::future<LBAMapping> punch_inner_mappings(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    LBAMapping mapping /*the first inner mapping*/);

  // The last step in the multi-mapping-hole-punching scenario: remap
  // the right mapping if it crosses the right edge of the hole's range
  base_iertr::future<LBAMapping> punch_right_mapping(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &overwrite_data,
    LBAMapping right_mapping,
    op_type_t op_type);

  // punch the hole whose range is within a single pending mapping
  base_iertr::future<LBAMapping> punch_hole_in_pending_mapping(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping mapping);

  // handle the overwrite the range of which is within a single lba mapping.
  write_ret handle_single_mapping_overwrite(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping mapping,
    op_type_t op_type);

  // handle overwrites whose ranges cross multiple lba mappings.
  write_ret handle_multi_mapping_overwrite(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping mapping,
    op_type_t op_type);

  // punch a lba hole that crosses multiple lba mappings.
  base_iertr::future<LBAMapping> punch_multi_mapping_hole(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping left_mapping,
    op_type_t op_type);

  // merge the data of the range on which the current overwrite and
  // the pending edge mapping overlaps into the corresponding pending
  // extent
  base_iertr::future<LBAMapping> merge_into_pending_edge(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping edge_mapping,
    edge_t edge);

  // merge the data of the current overwrite into
  // the pending mapping's extent
  write_ret merge_into_mapping(
    context_t ctx,
    overwrite_range_t &overwrite_range,
    data_t &data,
    LBAMapping edge_mapping);

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
