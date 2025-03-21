// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <utility>
#include <functional>

#include "crimson/common/log.h"

#include "crimson/os/seastore/object_data_handler.h"
#include "crimson/os/seastore/laddr_interval_set.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_odata);
  }
}

SET_SUBSYS(seastore_odata);

namespace crimson::os::seastore {
#define assert_aligned(x) ceph_assert(((x)%ctx.tm.get_block_size()) == 0)

using context_t = ObjectDataHandler::context_t;
using get_iertr = ObjectDataHandler::write_iertr;

/**
 * extent_to_write_t
 *
 * Encapsulates smallest write operations in overwrite.
 * Indicates a zero/existing extent or a data extent based on whether
 * to_write is populate.
 * Should be handled by prepare_ops_list.
 */
struct extent_to_write_t {
  enum class type_t {
    DATA,
    ZERO,
    EXISTING,
  };
  type_t type;

  /// pin of original extent, not std::nullopt if type == EXISTING
  std::optional<LBAMapping> pin;

  laddr_t addr;
  extent_len_t len;

  /// non-nullopt if and only if type == DATA
  std::optional<bufferlist> to_write;

  extent_to_write_t(const extent_to_write_t &) = delete;
  extent_to_write_t(extent_to_write_t &&) = default;
  extent_to_write_t& operator=(const extent_to_write_t&) = delete;
  extent_to_write_t& operator=(extent_to_write_t&&) = default;

  bool is_data() const {
    return type == type_t::DATA;
  }

  bool is_zero() const {
    return type == type_t::ZERO;
  }

  bool is_existing() const {
    return type == type_t::EXISTING;
  }

  laddr_t get_end_addr() const {
    return (addr + len).checked_to_laddr();
  }

  static extent_to_write_t create_data(
      laddr_t addr, bufferlist to_write) {
    return extent_to_write_t(addr, to_write);
  }

  static extent_to_write_t create_zero(
    laddr_t addr, extent_len_t len) {
    return extent_to_write_t(addr, len);
  }

  static extent_to_write_t create_existing(
    LBAMapping &&pin, laddr_t addr, extent_len_t len) {
    return extent_to_write_t(std::move(pin), addr, len);
  }

private:
  extent_to_write_t(laddr_t addr, bufferlist to_write)
    : type(type_t::DATA), addr(addr), len(to_write.length()),
      to_write(to_write) {}

  extent_to_write_t(laddr_t addr, extent_len_t len)
    : type(type_t::ZERO), addr(addr), len(len) {}

  extent_to_write_t(LBAMapping &&pin, laddr_t addr, extent_len_t len)
    : type(type_t::EXISTING), pin(std::move(pin)), addr(addr), len(len) {}
};
using extent_to_write_list_t = std::list<extent_to_write_t>;

// Encapsulates extents to be written out using do_remappings.
struct extent_to_remap_t {
  enum class type_t {
    REMAP1,
    REMAP2,
    OVERWRITE
  };
  type_t type;
  /// pin of original extent
  std::optional<LBAMapping> pin;
  /// offset of remapped extent or overwrite part of overwrite extent.
  /// overwrite part of overwrite extent might correspond to mutiple 
  /// fresh write extent.
  extent_len_t new_offset;
  /// length of remapped extent or overwrite part of overwrite extent
  extent_len_t new_len;

  extent_to_remap_t(const extent_to_remap_t &) = delete;
  extent_to_remap_t(extent_to_remap_t &&) = default;

  bool is_remap1() const {
    return type == type_t::REMAP1;
  }

  bool is_remap2() const {
    assert(pin);
    assert((new_offset != 0) && (pin->get_length() != new_offset + new_len));
    return type == type_t::REMAP2;
  }

  bool is_overwrite() const {
    return type == type_t::OVERWRITE;
  }

  using remap_entry_t = TransactionManager::remap_entry_t;
  remap_entry_t create_remap_entry() {
    assert(is_remap1());
    return remap_entry_t(
      new_offset,
      new_len);
  }

  remap_entry_t create_left_remap_entry() {
    assert(is_remap2());
    return remap_entry_t(
      0,
      new_offset);
  }

  remap_entry_t create_right_remap_entry() {
    assert(pin);
    assert(is_remap2());
    return remap_entry_t(
      new_offset + new_len,
      pin->get_length() - new_offset - new_len);
  }

  static extent_to_remap_t create_remap1(
    LBAMapping &&pin, extent_len_t new_offset, extent_len_t new_len) {
    return extent_to_remap_t(type_t::REMAP1,
      std::move(pin), new_offset, new_len);
  }

  static extent_to_remap_t create_remap2(
    LBAMapping &&pin, extent_len_t new_offset, extent_len_t new_len) {
    return extent_to_remap_t(type_t::REMAP2,
      std::move(pin), new_offset, new_len);
  }

  static extent_to_remap_t create_overwrite(
    extent_len_t new_offset, extent_len_t new_len, LBAMapping p,
    bufferlist b) {
    return extent_to_remap_t(type_t::OVERWRITE,
      new_offset, new_len, p.get_key(), p.get_length(), b);
  }

  laddr_t laddr_start;
  extent_len_t length;
  std::optional<bufferlist> bl;

private:
  extent_to_remap_t(type_t type,
    LBAMapping &&pin, extent_len_t new_offset, extent_len_t new_len)
    : type(type),
      pin(std::move(pin)), new_offset(new_offset), new_len(new_len) {}
  extent_to_remap_t(type_t type,
    extent_len_t new_offset, extent_len_t new_len,
    laddr_t ori_laddr, extent_len_t ori_len, std::optional<bufferlist> b)
    : type(type),
      new_offset(new_offset), new_len(new_len),
      laddr_start(ori_laddr), length(ori_len), bl(b) {}
};
using extent_to_remap_list_t = std::list<extent_to_remap_t>;

// Encapsulates extents to be written out using do_insertions.
struct extent_to_insert_t {
  enum class type_t {
    DATA,
    ZERO
  };
  type_t type;
  /// laddr of new extent
  laddr_t addr;
  /// length of new extent
  extent_len_t len;
  /// non-nullopt if type == DATA
  std::optional<bufferlist> bl;

  extent_to_insert_t(const extent_to_insert_t &) = default;
  extent_to_insert_t(extent_to_insert_t &&) = default;

  bool is_data() const {
    return type == type_t::DATA;
  }

  bool is_zero() const {
    return type == type_t::ZERO;
  }

  static extent_to_insert_t create_data(
    laddr_t addr, extent_len_t len, std::optional<bufferlist> bl) {
    return extent_to_insert_t(addr, len, bl);
  }

  static extent_to_insert_t create_zero(
    laddr_t addr, extent_len_t len) {
    return extent_to_insert_t(addr, len);
  }

private:
  extent_to_insert_t(laddr_t addr, extent_len_t len,
    std::optional<bufferlist> bl)
    :type(type_t::DATA), addr(addr), len(len), bl(bl) {}

  extent_to_insert_t(laddr_t addr, extent_len_t len)
    :type(type_t::ZERO), addr(addr), len(len) {}
};
using extent_to_insert_list_t = std::list<extent_to_insert_t>;

// Encapsulates extents to be retired in do_removals.
using extent_to_remove_list_t = std::list<LBAMapping>;

struct overwrite_ops_t {
  extent_to_remap_list_t to_remap;
  extent_to_insert_list_t to_insert;
  extent_to_remove_list_t to_remove;
};

// prepare to_remap, to_retire, to_insert list
overwrite_ops_t prepare_ops_list(
  lba_mapping_list_t &pins_to_remove,
  extent_to_write_list_t &to_write,
  size_t delta_based_overwrite_max_extent_size) {
  assert(pins_to_remove.size() != 0);
  overwrite_ops_t ops;
  ops.to_remove.swap(pins_to_remove);
  if (to_write.empty()) {
    logger().debug("empty to_write");
    return ops;
  }
  long unsigned int visitted = 0;
  auto& front = to_write.front();
  auto& back = to_write.back();

  // prepare overwrite, happens in one original extent.
  if (ops.to_remove.size() == 1 &&
      front.is_existing() && back.is_existing()) {
      visitted += 2;
      assert(to_write.size() > 2);
      assert(front.pin);
      assert(front.addr == front.pin->get_key());
      assert(back.addr > back.pin->get_key());
      ops.to_remap.push_back(extent_to_remap_t::create_remap2(
	std::move(*front.pin),
	front.len,
	back.addr.get_byte_distance<extent_len_t>(front.addr) - front.len));
      ops.to_remove.pop_front();
  } else {
    // prepare to_remap, happens in one or multiple extents
    if (front.is_existing()) {
      visitted++;
      assert(to_write.size() > 1);
      assert(front.pin);
      assert(front.addr == front.pin->get_key());
      ops.to_remap.push_back(extent_to_remap_t::create_remap1(
	std::move(*front.pin),
	0,
	front.len));
      ops.to_remove.pop_front();
    }
    if (back.is_existing()) {
      visitted++;
      assert(to_write.size() > 1);
      assert(back.pin);
      assert(back.addr + back.len ==
	back.pin->get_key() + back.pin->get_length());
      auto key = back.pin->get_key();
      ops.to_remap.push_back(extent_to_remap_t::create_remap1(
	std::move(*back.pin),
	back.addr.get_byte_distance<extent_len_t>(key),
	back.len));
      ops.to_remove.pop_back();
    }
  }

  laddr_interval_set_t pre_alloc_addr_removed, pre_alloc_addr_remapped;
  if (delta_based_overwrite_max_extent_size) {
    for (auto &r : ops.to_remove) {
      if (r.is_data_stable() && !r.is_zero_reserved()) {
	pre_alloc_addr_removed.insert(r.get_key(), r.get_length());

      }
    }
    for (auto &r : ops.to_remap) {
      if (r.pin && r.pin->is_data_stable() && !r.pin->is_zero_reserved()) {
	pre_alloc_addr_remapped.insert(r.pin->get_key(), r.pin->get_length());
      }
    }
  }

  // prepare to insert
  extent_to_remap_list_t to_remap;
  for (auto &region : to_write) {
    if (region.is_data()) {
      visitted++;
      assert(region.to_write.has_value());
      int erased_num = 0;
      if (pre_alloc_addr_removed.contains(region.addr, region.len) &&
	  region.len <= delta_based_overwrite_max_extent_size) {
	erased_num = std::erase_if(
	  ops.to_remove,
	  [&region, &to_remap](auto &r) {
	    laddr_interval_set_t range;
	    range.insert(r.get_key(), r.get_length());
	    if (range.contains(region.addr, region.len) && !r.is_clone()) {
	      to_remap.push_back(extent_to_remap_t::create_overwrite(
		0, region.len, std::move(r), *region.to_write));
	      return true;
	    }
	    return false;
	  });
	// if the size of the region is wider than the ragne from the enry in to_remove,
	// we create a separated extent in the original way.
      } else if (pre_alloc_addr_remapped.contains(region.addr, region.len) &&
		 region.len <= delta_based_overwrite_max_extent_size) {
	erased_num = std::erase_if(
	  ops.to_remap,
	  [&region, &to_remap](auto &r) {
	    laddr_interval_set_t range;
	    assert(r.pin);
	    range.insert(r.pin->get_key(), r.pin->get_length());
	    if (range.contains(region.addr, region.len) && !r.pin->is_clone()) {
	      to_remap.push_back(extent_to_remap_t::create_overwrite(
		region.addr.get_byte_distance<
		  extent_len_t> (range.begin().get_start()),
		region.len, std::move(*r.pin), *region.to_write));
	      return true;
	    }
	    return false;
	  });
	assert(erased_num > 0);
      }
      if (erased_num == 0)  {
	ops.to_insert.push_back(extent_to_insert_t::create_data(
	  region.addr, region.len, region.to_write));
      }
    } else if (region.is_zero()) {
      visitted++;
      assert(!(region.to_write.has_value()));
      ops.to_insert.push_back(extent_to_insert_t::create_zero(
	region.addr, region.len));
    }
  }
  ops.to_remap.splice(ops.to_remap.end(), to_remap);

  logger().debug(
    "to_remap list size: {}"
    " to_insert list size: {}"
    " to_remove list size: {}",
    ops.to_remap.size(), ops.to_insert.size(), ops.to_remove.size());
  assert(visitted == to_write.size());
  return ops;
}

/**
 * append_extent_to_write
 *
 * Appends passed extent_to_write_t maintaining invariant that the
 * list may not contain consecutive zero elements by checking and
 * combining them.
 */
void append_extent_to_write(
  extent_to_write_list_t &to_write, extent_to_write_t &&to_append)
{
  assert(to_write.empty() ||
         to_write.back().get_end_addr() == to_append.addr);
  if (to_write.empty() ||
      to_write.back().is_data() ||
      to_append.is_data() ||
      to_write.back().type != to_append.type) {
    to_write.push_back(std::move(to_append));
  } else {
    to_write.back().len += to_append.len;
  }
}

/**
 * splice_extent_to_write
 *
 * splices passed extent_to_write_list_t maintaining invariant that the
 * list may not contain consecutive zero elements by checking and
 * combining them.
 */
void splice_extent_to_write(
  extent_to_write_list_t &to_write, extent_to_write_list_t &&to_splice)
{
  if (!to_splice.empty()) {
    append_extent_to_write(to_write, std::move(to_splice.front()));
    to_splice.pop_front();
    to_write.splice(to_write.end(), std::move(to_splice));
  }
}

ceph::bufferlist ObjectDataBlock::get_delta() {
  ceph::bufferlist bl;
  encode(delta, bl);
  return bl;
}

void ObjectDataBlock::apply_delta(const ceph::bufferlist &bl) {
  auto biter = bl.begin();
  decltype(delta) deltas;
  decode(deltas, biter);
  for (auto &&d : deltas) {
    auto iter = d.bl.cbegin();
    iter.copy(d.len, get_bptr().c_str() + d.offset);
    modified_region.union_insert(d.offset, d.len);
  }
}

/// Creates remap extents in to_remap
ObjectDataHandler::write_ret do_remappings(
  context_t ctx,
  extent_to_remap_list_t &to_remap)
{
  return trans_intr::do_for_each(
    to_remap,
    [ctx](auto &region) {
      if (region.is_remap1()) {
	assert(region.pin);
        return ctx.tm.remap_pin<ObjectDataBlock, 1>(
          ctx.t,
          std::move(*region.pin),
          std::array{
            region.create_remap_entry()
          }
        ).si_then([&region](auto pins) {
          ceph_assert(pins.size() == 1);
          ceph_assert(region.new_len == pins[0].get_length());
          return ObjectDataHandler::write_iertr::now();
        });
      } else if (region.is_overwrite()) {
	return ctx.tm.get_mutable_extent_by_laddr<ObjectDataBlock>(
	  ctx.t,
	  region.laddr_start,
	  region.length
	).handle_error_interruptible(
	  TransactionManager::base_iertr::pass_further{},
	  crimson::ct_error::assert_all{
	    "ObjectDataHandler::do_remapping hit invalid error"
	  }
	).si_then([&region](auto extent) {
	  extent_len_t off = region.new_offset;
	  assert(region.bl->length() == region.new_len);
	  extent->overwrite(off, *region.bl);
	  return ObjectDataHandler::write_iertr::now();
	});
      } else if (region.is_remap2()) {
	assert(region.pin);
	auto pin_key = region.pin->get_key();
        return ctx.tm.remap_pin<ObjectDataBlock, 2>(
          ctx.t,
          std::move(*region.pin),
          std::array{
            region.create_left_remap_entry(),
            region.create_right_remap_entry()
          }
        ).si_then([&region, pin_key](auto pins) {
          ceph_assert(pins.size() == 2);
          ceph_assert(pin_key == pins[0].get_key());
          ceph_assert(pin_key + pins[0].get_length() +
            region.new_len == pins[1].get_key());
          return ObjectDataHandler::write_iertr::now();
        });
      } else {
        ceph_abort("impossible");
        return ObjectDataHandler::write_iertr::now();
      }
  });
}

ObjectDataHandler::write_ret do_removals(
  context_t ctx,
  lba_mapping_list_t &to_remove)
{
  return trans_intr::do_for_each(
    to_remove,
    [ctx](auto &pin) {
      LOG_PREFIX(object_data_handler.cc::do_removals);
      DEBUGT("decreasing ref: {}",
	     ctx.t,
	     pin.get_key());
      return ctx.tm.remove(
	ctx.t,
	pin.get_key()
      ).discard_result().handle_error_interruptible(
	ObjectDataHandler::write_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "object_data_handler::do_removals invalid error"
	}
      );
    });
}

/// Creates zero/data extents in to_insert
ObjectDataHandler::write_ret do_insertions(
  context_t ctx,
  extent_to_insert_list_t &to_insert)
{
  return trans_intr::do_for_each(
    to_insert,
    [ctx](auto &region) {
      LOG_PREFIX(object_data_handler.cc::do_insertions);
      if (region.is_data()) {
	assert_aligned(region.len);
	ceph_assert(region.len == region.bl->length());
	DEBUGT("allocating extent: {}~0x{:x}",
	       ctx.t,
	       region.addr,
	       region.len);
	return ctx.tm.alloc_data_extents<ObjectDataBlock>(
	  ctx.t,
	  region.addr,
	  region.len
        ).si_then([&region](auto extents) {
          auto off = region.addr;
          auto left = region.len;
	  auto iter = region.bl->cbegin();
          for (auto &extent : extents) {
            ceph_assert(left >= extent->get_length());
            if (extent->get_laddr() != off) {
              logger().debug(
                "object_data_handler::do_insertions alloc got addr {},"
                " should have been {}",
                extent->get_laddr(),
                off);
            }
            iter.copy(extent->get_length(), extent->get_bptr().c_str());
            off = (off + extent->get_length()).checked_to_laddr();
            left -= extent->get_length();
          }
	  return ObjectDataHandler::write_iertr::now();
	}).handle_error_interruptible(
	  crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	  ObjectDataHandler::write_iertr::pass_further{}
	);
      } else if (region.is_zero()) {
	DEBUGT("reserving: {}~0x{:x}",
	       ctx.t,
	       region.addr,
	       region.len);
	return ctx.tm.reserve_region(
	  ctx.t,
	  region.addr,
	  region.len
	).si_then([FNAME, ctx, &region](auto pin) {
	  ceph_assert(pin.get_length() == region.len);
	  if (pin.get_key() != region.addr) {
	    ERRORT(
	      "inconsistent laddr: pin: {} region {}",
	      ctx.t,
	      pin.get_key(),
	      region.addr);
	  }
	  ceph_assert(pin.get_key() == region.addr);
	  return ObjectDataHandler::write_iertr::now();
	}).handle_error_interruptible(
	  crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	  ObjectDataHandler::write_iertr::pass_further{}
	);
      } else {
	ceph_abort("impossible");
	return ObjectDataHandler::write_iertr::now();
      }
    });
}

enum class overwrite_operation_t {
  UNKNOWN,
  OVERWRITE_ZERO,           // fill unaligned data with zero
  MERGE_EXISTING,           // if present, merge data with the clean/pending extent
  SPLIT_EXISTING,           // split the existing extent, and fill unaligned data
};

std::ostream& operator<<(
  std::ostream &out,
  const overwrite_operation_t &operation)
{
  switch (operation) {
  case overwrite_operation_t::UNKNOWN:
    return out << "UNKNOWN";
  case overwrite_operation_t::OVERWRITE_ZERO:
    return out << "OVERWRITE_ZERO";
  case overwrite_operation_t::MERGE_EXISTING:
    return out << "MERGE_EXISTING";
  case overwrite_operation_t::SPLIT_EXISTING:
    return out << "SPLIT_EXISTING";
  default:
    return out << "!IMPOSSIBLE_OPERATION";
  }
}

/**
 * overwrite_plan_t
 *
 * |<--------------------------pins_size---------------------------------------------->|
 * pin_begin(aligned)                                                   pin_end(aligned)
 *                 |<------aligned_data_size-------------------------->| (aligned-bl)
 *                 aligned_data_begin                   aligned_data_end
 *                                    |<-data_size->| (bl)
 *                                    data_begin  end
 *             left(l)                                            right(r)
 * |<l_extent_size>|<l_alignment_size>|             |<r_alignment_size>|<r_extent_size>|
 * |<-----------left_size------------>|             |<-----------right_size----------->|
 *
 * |<-----(existing left extent/pin)----->|    |<-----(existing right extent/pin)----->|
 * left_paddr                                  right_paddr
 */
struct overwrite_plan_t {
  // reserved data base of object data
  laddr_t data_base;

  // addresses about extents
  laddr_t pin_begin;
  laddr_t pin_end;
  paddr_t left_paddr;
  paddr_t right_paddr;
  laddr_offset_t data_begin;
  laddr_offset_t data_end;
  laddr_t aligned_data_begin;
  laddr_t aligned_data_end;

  // operations
  overwrite_operation_t left_operation;
  overwrite_operation_t right_operation;

  // helper member
  extent_len_t block_size;
  bool is_left_fresh;
  bool is_right_fresh;

public:
  extent_len_t get_left_size() const {
    return data_begin.get_byte_distance<extent_len_t>(pin_begin);
  }

  extent_len_t get_left_extent_size() const {
    return aligned_data_begin.get_byte_distance<extent_len_t>(pin_begin);
  }

  extent_len_t get_left_alignment_size() const {
    return data_begin.get_byte_distance<extent_len_t>(aligned_data_begin);
  }

  extent_len_t get_right_size() const {
    return pin_end.get_byte_distance<extent_len_t>(data_end);
  }

  extent_len_t get_right_extent_size() const {
    return pin_end.get_byte_distance<extent_len_t>(aligned_data_end);
  }

  extent_len_t get_right_alignment_size() const {
    return aligned_data_end.get_byte_distance<extent_len_t>(data_end);
  }

  extent_len_t get_aligned_data_size() const {
    return aligned_data_end.get_byte_distance<extent_len_t>(aligned_data_begin);
  }

  extent_len_t get_pins_size() const {
    return pin_end.get_byte_distance<extent_len_t>(pin_begin);
  }

  friend std::ostream& operator<<(
    std::ostream& out,
    const overwrite_plan_t& overwrite_plan) {
    return out << "overwrite_plan_t("
	       << "data_base=" << overwrite_plan.data_base
	       << ", pin_begin=" << overwrite_plan.pin_begin
	       << ", pin_end=" << overwrite_plan.pin_end
	       << ", left_paddr=" << overwrite_plan.left_paddr
	       << ", right_paddr=" << overwrite_plan.right_paddr
	       << ", data_begin=" << overwrite_plan.data_begin
	       << ", data_end=" << overwrite_plan.data_end
	       << ", aligned_data_begin=" << overwrite_plan.aligned_data_begin
	       << ", aligned_data_end=" << overwrite_plan.aligned_data_end
	       << ", left_operation=" << overwrite_plan.left_operation
	       << ", right_operation=" << overwrite_plan.right_operation
	       << ", block_size=0x" << std::hex << overwrite_plan.block_size << std::dec
	       << ", is_left_fresh=" << overwrite_plan.is_left_fresh
	       << ", is_right_fresh=" << overwrite_plan.is_right_fresh
	       << ")";
  }

  overwrite_plan_t(laddr_t data_base,
		   objaddr_t offset,
		   extent_len_t len,
		   const lba_mapping_list_t& pins,
		   extent_len_t block_size) :
      data_base(data_base),
      pin_begin(pins.front().get_key()),
      pin_end((pins.back().get_key() + pins.back().get_length()).checked_to_laddr()),
      left_paddr(pins.front().get_val()),
      right_paddr(pins.back().get_val()),
      data_begin(data_base + offset),
      data_end(data_base + offset + len),
      aligned_data_begin(data_begin.get_aligned_laddr()),
      aligned_data_end(data_end.get_roundup_laddr()),
      left_operation(overwrite_operation_t::UNKNOWN),
      right_operation(overwrite_operation_t::UNKNOWN),
      block_size(block_size),
      // TODO: introduce LBAMapping::is_fresh()
      // Note: fresh write can be merged with overwrite if they overlap.
      is_left_fresh(!pins.front().is_stable()),
      is_right_fresh(!pins.back().is_stable()) {
    validate();
    evaluate_operations();
    assert(left_operation != overwrite_operation_t::UNKNOWN);
    assert(right_operation != overwrite_operation_t::UNKNOWN);
  }

private:
  // refer to overwrite_plan_t description
  void validate() const {
    ceph_assert(pin_begin <= aligned_data_begin);
    ceph_assert(aligned_data_begin <= data_begin);
    ceph_assert(data_begin <= data_end);
    ceph_assert(data_end <= aligned_data_end);
    ceph_assert(aligned_data_end <= pin_end);
  }

  /*
   * When trying to modify a portion of an object data block, follow
   * the read-full-extent-then-merge-new-data strategy, if the write
   * amplification caused by it is not greater than
   * seastore_obj_data_write_amplification; otherwise, split the
   * original extent into at most three parts: origin-left, part-to-be-modified
   * and origin-right.
   *
   * TODO: seastore_obj_data_write_amplification needs to be reconsidered because
   * delta-based overwrite is introduced
   */
  void evaluate_operations() {
    auto actual_write_size = get_pins_size();
    auto aligned_data_size = get_aligned_data_size();
    auto left_ext_size = get_left_extent_size();
    auto right_ext_size = get_right_extent_size();

    if (left_paddr.is_zero()) {
      actual_write_size -= left_ext_size;
      left_ext_size = 0;
      left_operation = overwrite_operation_t::OVERWRITE_ZERO;
    } else if (is_left_fresh) {
      aligned_data_size += left_ext_size;
      left_ext_size = 0;
      left_operation = overwrite_operation_t::MERGE_EXISTING;
    }

    if (right_paddr.is_zero()) {
      actual_write_size -= right_ext_size;
      right_ext_size = 0;
      right_operation = overwrite_operation_t::OVERWRITE_ZERO;
    } else if (is_right_fresh) {
      aligned_data_size += right_ext_size;
      right_ext_size = 0;
      right_operation = overwrite_operation_t::MERGE_EXISTING;
    }

    while (left_operation == overwrite_operation_t::UNKNOWN ||
           right_operation == overwrite_operation_t::UNKNOWN) {
      if (((double)actual_write_size / (double)aligned_data_size) <=
          crimson::common::get_conf<double>("seastore_obj_data_write_amplification")) {
        break;
      }
      if (left_ext_size == 0 && right_ext_size == 0) {
        break;
      }
      if (left_ext_size >= right_ext_size) {
        // split left
        assert(left_operation == overwrite_operation_t::UNKNOWN);
        actual_write_size -= left_ext_size;
        left_ext_size = 0;
        left_operation = overwrite_operation_t::SPLIT_EXISTING;
      } else { // left_ext_size < right_ext_size
        // split right
        assert(right_operation == overwrite_operation_t::UNKNOWN);
        actual_write_size -= right_ext_size;
        right_ext_size = 0;
        right_operation = overwrite_operation_t::SPLIT_EXISTING;
      }
    }

    if (left_operation == overwrite_operation_t::UNKNOWN) {
      // no split left, so merge with left
      left_operation = overwrite_operation_t::MERGE_EXISTING;
    }

    if (right_operation == overwrite_operation_t::UNKNOWN) {
      // no split right, so merge with right
      right_operation = overwrite_operation_t::MERGE_EXISTING;
    }
  }
};

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template<> struct fmt::formatter<crimson::os::seastore::overwrite_plan_t> : fmt::ostream_formatter {};
#endif

namespace crimson::os::seastore {

/**
 * operate_left
 *
 * Proceed overwrite_plan.left_operation.
 */
using operate_ret_bare = std::pair<
  std::optional<extent_to_write_t>,
  std::optional<ceph::bufferlist>>;
using operate_ret = get_iertr::future<operate_ret_bare>;
operate_ret operate_left(context_t ctx, LBAMapping &pin, const overwrite_plan_t &overwrite_plan)
{
  if (overwrite_plan.get_left_size() == 0) {
    return get_iertr::make_ready_future<operate_ret_bare>(
      std::nullopt,
      std::nullopt);
  }

  if (overwrite_plan.left_operation == overwrite_operation_t::OVERWRITE_ZERO) {
    assert(pin.get_val().is_zero());

    auto zero_extent_len = overwrite_plan.get_left_extent_size();
    assert_aligned(zero_extent_len);
    std::optional<extent_to_write_t> extent_to_write;
    if (zero_extent_len != 0) {
      extent_to_write = extent_to_write_t::create_zero(
         overwrite_plan.pin_begin, zero_extent_len);
    }

    auto zero_prepend_len = overwrite_plan.get_left_alignment_size();
    std::optional<ceph::bufferlist> prepend_bl;
    if (zero_prepend_len != 0) {
      ceph::bufferlist zero_bl;
      zero_bl.append_zero(zero_prepend_len);
      prepend_bl = std::move(zero_bl);
    }

    return get_iertr::make_ready_future<operate_ret_bare>(
      std::move(extent_to_write),
      std::move(prepend_bl));
  } else if (overwrite_plan.left_operation == overwrite_operation_t::MERGE_EXISTING) {
    auto prepend_len = overwrite_plan.get_left_size();
    if (prepend_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::nullopt,
        std::nullopt);
    } else {
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin.duplicate()
      ).si_then([prepend_len](auto maybe_indirect_left_extent) {
        auto read_bl = maybe_indirect_left_extent.get_bl();
        ceph::bufferlist prepend_bl;
        prepend_bl.substr_of(read_bl, 0, prepend_len);
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::nullopt,
          std::move(prepend_bl));
      });
    }
  } else {
    assert(overwrite_plan.left_operation == overwrite_operation_t::SPLIT_EXISTING);

    auto extent_len = overwrite_plan.get_left_extent_size();
    assert(extent_len);
    std::optional<extent_to_write_t> left_to_write_extent =
      std::make_optional(extent_to_write_t::create_existing(
        pin.duplicate(),
        pin.get_key(),
        extent_len));

    auto prepend_len = overwrite_plan.get_left_alignment_size();
    if (prepend_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::move(left_to_write_extent),
        std::nullopt);
    } else {
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin.duplicate()
      ).si_then([prepend_offset=extent_len, prepend_len,
                 left_to_write_extent=std::move(left_to_write_extent)]
                (auto left_maybe_indirect_extent) mutable {
        auto read_bl = left_maybe_indirect_extent.get_bl();
        ceph::bufferlist prepend_bl;
        prepend_bl.substr_of(read_bl, prepend_offset, prepend_len);
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::move(left_to_write_extent),
          std::move(prepend_bl));
      });
    }
  }
};

/**
 * operate_right
 *
 * Proceed overwrite_plan.right_operation.
 */
operate_ret operate_right(context_t ctx, LBAMapping &pin, const overwrite_plan_t &overwrite_plan)
{
  if (overwrite_plan.get_right_size() == 0) {
    return get_iertr::make_ready_future<operate_ret_bare>(
      std::nullopt,
      std::nullopt);
  }

  auto right_pin_begin = pin.get_key();
  assert(overwrite_plan.data_end >= right_pin_begin);
  if (overwrite_plan.right_operation == overwrite_operation_t::OVERWRITE_ZERO) {
    assert(pin.get_val().is_zero());

    auto zero_suffix_len = overwrite_plan.get_right_alignment_size();
    std::optional<ceph::bufferlist> suffix_bl;
    if (zero_suffix_len != 0) {
      ceph::bufferlist zero_bl;
      zero_bl.append_zero(zero_suffix_len);
      suffix_bl = std::move(zero_bl);
    }

    auto zero_extent_len = overwrite_plan.get_right_extent_size();
    assert_aligned(zero_extent_len);
    std::optional<extent_to_write_t> extent_to_write;
    if (zero_extent_len != 0) {
      extent_to_write = extent_to_write_t::create_zero(
        overwrite_plan.aligned_data_end, zero_extent_len);
    }

    return get_iertr::make_ready_future<operate_ret_bare>(
      std::move(extent_to_write),
      std::move(suffix_bl));
  } else if (overwrite_plan.right_operation == overwrite_operation_t::MERGE_EXISTING) {
    auto append_len = overwrite_plan.get_right_size();
    if (append_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::nullopt,
        std::nullopt);
    } else {
      auto append_offset =
	overwrite_plan.data_end.get_byte_distance<
	  extent_len_t>(right_pin_begin);
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin.duplicate()
      ).si_then([append_offset, append_len]
                (auto right_maybe_indirect_extent) {
        auto read_bl = right_maybe_indirect_extent.get_bl();
        ceph::bufferlist suffix_bl;
        suffix_bl.substr_of(read_bl, append_offset, append_len);
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::nullopt,
          std::move(suffix_bl));
      });
    }
  } else {
    assert(overwrite_plan.right_operation == overwrite_operation_t::SPLIT_EXISTING);

    auto extent_len = overwrite_plan.get_right_extent_size();
    assert(extent_len);
    std::optional<extent_to_write_t> right_to_write_extent =
      std::make_optional(extent_to_write_t::create_existing(
        pin.duplicate(),
        overwrite_plan.aligned_data_end,
        extent_len));

    auto append_len = overwrite_plan.get_right_alignment_size();
    if (append_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::move(right_to_write_extent),
        std::nullopt);
    } else {
      auto append_offset =
	overwrite_plan.data_end.get_byte_distance<
	  extent_len_t>(right_pin_begin);
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin.duplicate()
      ).si_then([append_offset, append_len,
                 right_to_write_extent=std::move(right_to_write_extent)]
                (auto maybe_indirect_right_extent) mutable {
        auto read_bl = maybe_indirect_right_extent.get_bl();
        ceph::bufferlist suffix_bl;
        suffix_bl.substr_of(read_bl, append_offset, append_len);
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::move(right_to_write_extent),
          std::move(suffix_bl));
      });
    }
  }
};

template <typename F>
auto with_object_data(
  ObjectDataHandler::context_t ctx,
  F &&f)
{
  return seastar::do_with(
    ctx.onode.get_layout().object_data.get(),
    std::forward<F>(f),
    [ctx](auto &object_data, auto &f) {
      return std::invoke(f, object_data
      ).si_then([ctx, &object_data] {
	if (object_data.must_update()) {
	  ctx.onode.update_object_data(ctx.t, object_data);
	}
	return seastar::now();
      });
    });
}

template <typename F>
auto with_objects_data(
  ObjectDataHandler::context_t ctx,
  F &&f)
{
  ceph_assert(ctx.d_onode);
  return seastar::do_with(
    ctx.onode.get_layout().object_data.get(),
    ctx.d_onode->get_layout().object_data.get(),
    std::forward<F>(f),
    [ctx](auto &object_data, auto &d_object_data, auto &f) {
      return std::invoke(f, object_data, d_object_data
      ).si_then([ctx, &object_data, &d_object_data] {
	if (object_data.must_update()) {
	  ctx.onode.update_object_data(ctx.t, object_data);
	}
	if (d_object_data.must_update()) {
	  ctx.d_onode->update_object_data(ctx.t, d_object_data);
	}
	return seastar::now();
      });
    });
}

ObjectDataHandler::write_iertr::future<LBAMapping>
ObjectDataHandler::prepare_data_reservation(
  context_t ctx,
  object_data_t &object_data,
  extent_len_t size)
{
  LOG_PREFIX(ObjectDataHandler::prepare_data_reservation);
  ceph_assert(size <= max_object_size);
  if (!object_data.is_null()) {
    ceph_assert(object_data.get_reserved_data_len() == max_object_size);
    DEBUGT("reservation present: {}~0x{:x}",
           ctx.t,
           object_data.get_reserved_data_base(),
           object_data.get_reserved_data_len());
    return write_iertr::make_ready_future<LBAMapping>();
  } else {
    DEBUGT("reserving: {}~0x{:x}",
           ctx.t,
           ctx.onode.get_data_hint(),
           max_object_size);
    return ctx.tm.reserve_region(
      ctx.t,
      ctx.onode.get_data_hint(),
      max_object_size
    ).si_then([max_object_size=max_object_size, &object_data](auto pin) {
      ceph_assert(pin.get_length() == max_object_size);
      object_data.update_reserved(
	pin.get_key(),
	pin.get_length());
      return pin;
    }).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      write_iertr::pass_further{}
    );
  }
}

using remap_ret = ObjectDataHandler::write_iertr::future<std::vector<LBAMapping>>;
template <std::size_t N>
remap_ret remap_mappings(
  context_t ctx,
  LBAMapping mapping,
  std::array<TransactionManager::remap_entry_t, N> remaps)
{
  if (!mapping.is_indirect() && mapping.get_val().is_zero()) {
    return seastar::do_with(
      std::vector<TransactionManager::remap_entry_t>(
	remaps.begin(), remaps.end()),
      std::vector<LBAMapping>(),
      [ctx, mapping=std::move(mapping)](auto &remaps, auto &mappings) mutable {
      auto orig_laddr = mapping.get_key();
      return ctx.tm.remove(ctx.t, std::move(mapping)
      ).si_then([&remaps, ctx, &mappings, orig_laddr](auto next_mapping) {
	return seastar::do_with(
	  std::move(next_mapping),
	  [ctx, &remaps, orig_laddr, &mappings](auto &next_mapping) {
	  return trans_intr::do_for_each(
	    remaps.begin(),
	    remaps.end(),
	    [ctx, &next_mapping, orig_laddr, &mappings]
	    (const auto &remap) mutable {
	    auto laddr = (orig_laddr + remap.offset).checked_to_laddr();
	    return ctx.tm.reserve_region(
	      ctx.t,
	      std::move(next_mapping),
	      laddr,
	      remap.len
	    ).si_then([&mappings, ctx](auto new_mapping) {
	      mappings.emplace_back(new_mapping.duplicate());
	      return ctx.tm.next_mapping(ctx.t, std::move(new_mapping));
	    }).si_then([&next_mapping](auto new_mapping) {
	      next_mapping = std::move(new_mapping);
	      return seastar::now();
	    });
	  });
	});
      }).si_then([&mappings] { return std::move(mappings); });
    }).handle_error_interruptible(
      ObjectDataHandler::write_iertr::pass_further{},
      crimson::ct_error::assert_all{
	"ObjectDataHandler::read hit invalid error"
      }
    );
  } else {
    return ctx.tm.remap_pin<ObjectDataBlock, N>(
      ctx.t, std::move(mapping), std::move(remaps));
  }
}

ObjectDataHandler::read_ret load_padding(
  ObjectDataHandler::context_t ctx,
  const LBAMapping mapping,
  extent_len_t offset,
  extent_len_t len)
{
  if (mapping.get_val().is_zero()) {
    bufferlist bl;
    bl.append_zero(len);
    return ObjectDataHandler::read_iertr::make_ready_future<
      bufferlist>(std::move(bl));
  } else {
    return ctx.tm.read_pin<ObjectDataBlock>(ctx.t, mapping.duplicate()
    ).si_then([offset, len](auto maybe_indirect_left_extent) {
      auto read_bl = maybe_indirect_left_extent.get_bl();
      ceph::bufferlist prepend_bl;
      prepend_bl.substr_of(read_bl, offset, len);
      return ObjectDataHandler::read_iertr::make_ready_future<
	bufferlist>(std::move(prepend_bl));
    });
  }
}

/**
 * get_to_writes
 *
 * Returns extent_to_write_t's from bl.
 *
 * TODO: probably add some kind of upper limit on extent size.
 */
extent_to_write_list_t get_to_writes(laddr_t offset, bufferlist &bl)
{
  auto ret = extent_to_write_list_t();
  ret.push_back(extent_to_write_t::create_data(offset, bl));
  return ret;
};

struct overwrite_params_t {
  objaddr_t offset = 0;
  extent_len_t len = 0;
  laddr_t first_key = L_ADDR_NULL;
  extent_len_t first_len = 0;
  laddr_offset_t raw_begin;
  laddr_t data_begin = L_ADDR_NULL;
  laddr_offset_t raw_end;
  laddr_t data_end = L_ADDR_NULL;
};

struct data_t {
  std::optional<bufferlist> headbl;
  std::optional<bufferlist> bl;
  std::optional<bufferlist> tailbl;
};

using do_mappings_ret = ObjectDataHandler::write_iertr::future<LBAMapping>;

do_mappings_ret maybe_delta_based_overwrite(
  context_t ctx,
  const overwrite_params_t &params,
  LBAMapping mapping,
  data_t &data,
  extent_len_t delta_based_overwrite_max_extent_size)
{
  if (mapping.is_indirect() ||
      params.len > delta_based_overwrite_max_extent_size ||
      mapping.get_val().is_zero()) {
    return ObjectDataHandler::write_iertr::make_ready_future<
      LBAMapping>(std::move(mapping));
  }

  laddr_interval_set_t range;
  range.insert(mapping.get_key(), mapping.get_length());
  bool in_range = range.contains(
    params.data_begin,
    params.data_end.template get_byte_distance<
      extent_len_t>(params.data_begin));
  if (!in_range) {
    return ObjectDataHandler::write_iertr::make_ready_future<
      LBAMapping>(std::move(mapping));
  }

  // delta based overwrite
  return ctx.tm.read_pin<ObjectDataBlock>(
    ctx.t,
    std::move(mapping)
  ).handle_error_interruptible(
    TransactionManager::base_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "ObjectDataHandler::do_remapping hit invalid error"
    }
  ).si_then([ctx](auto maybe_indirect_extent) {
    assert(!maybe_indirect_extent.is_indirect());
    return ctx.tm.get_mutable_extent(ctx.t, maybe_indirect_extent.extent);
  }).si_then([&params, &data](auto extent) {
    bufferlist bl;
    if (data.bl) {
      bl.append(*data.bl);
    } else {
      bl.append_zero(params.len);
    }
    auto odblock = extent->template cast<ObjectDataBlock>();
    odblock->overwrite(
      params.first_key.template get_byte_distance<
	extent_len_t>(params.raw_begin),
      std::move(bl));
    return ObjectDataHandler::write_iertr::make_ready_future<
      LBAMapping>();
  });
}

do_mappings_ret do_first_mapping(
  context_t ctx,
  overwrite_params_t &params,
  LBAMapping first_mapping,
  data_t &data)
{
  if (!first_mapping.is_indirect() && !first_mapping.is_data_stable()) {
    // merge with existing pending extents
    auto length = params.first_key.template get_byte_distance<
      extent_len_t>(params.raw_begin);
    assert(params.offset >= length);
    params.offset -= length;
    params.len += length;
    params.data_begin = params.first_key;
    params.raw_begin = laddr_offset_t{params.first_key};
    return load_padding(ctx, first_mapping.duplicate(), 0, length
    ).si_then([&data, first_mapping=first_mapping.duplicate(),
	      &params, ctx](auto headbl) mutable {
      data.headbl = std::move(headbl);
      auto first_end = (params.first_key + params.first_len).checked_to_laddr();
      if (params.raw_end < first_end) {
	auto off = params.raw_end.template get_byte_distance<
	  extent_len_t>(params.first_key);
	auto len = params.raw_end.template get_byte_distance<
	  extent_len_t>(first_end);
	params.data_end = first_end;
	params.raw_end = laddr_offset_t{first_end};
	params.len += len;
	return load_padding(ctx, first_mapping.duplicate(), off, len);
      }
      return ObjectDataHandler::read_iertr::make_ready_future<bufferlist>();
    }).si_then([first_mapping=std::move(first_mapping),
		&data, ctx](auto tailbl) mutable {
      data.tailbl = std::move(tailbl);
      return ctx.tm.remove(ctx.t, std::move(first_mapping));
    }).handle_error_interruptible(
      ObjectDataHandler::base_iertr::pass_further{},
      crimson::ct_error::assert_all{
	"ObjectDataHandler::read hit invalid error"
      }
    );
  }
  auto pad_fut = ObjectDataHandler::read_iertr::make_ready_future<bufferlist>();
  if (params.raw_begin != params.data_begin) {
    // load the left padding
    assert(params.raw_begin > params.data_begin);
    pad_fut = load_padding(
      ctx,
      first_mapping.duplicate(),
      params.first_key.template get_byte_distance<
	extent_len_t>(params.data_begin),
      params.raw_begin.get_offset());
  }
  return pad_fut.si_then([&data, first_mapping=std::move(first_mapping),
			  &params, ctx](auto headbl) mutable {
    using remap_entry_t = TransactionManager::remap_entry_t;
    if (headbl.length() > 0) {
      data.headbl = std::move(headbl);
    }
    if (params.first_key == params.data_begin) {
      return TransactionManager::remap_pin_iertr::make_ready_future<
	LBAMapping>(std::move(first_mapping));
    }

    return remap_mappings<2>(
      ctx,
      std::move(first_mapping),
      std::array{
	// from the start of the first_mapping to the offset of overwrite
	remap_entry_t{
	  0,
	  params.data_begin.template get_byte_distance<
	    extent_len_t>(params.first_key)},
	// from the end of overwrite to the end of the first mapping
	remap_entry_t{
	  params.data_begin.template get_byte_distance<
	    extent_len_t>(params.first_key),
	  params.data_begin.template get_byte_distance<
	      extent_len_t>(params.first_key + params.first_len)}}
    ).si_then([](auto mappings) {
      assert(mappings.size() == 2);
      return std::move(mappings.back());
    });
  });
}

do_mappings_ret do_middle_mappings(
  context_t ctx,
  const overwrite_params_t &params,
  LBAMapping mapping)
{
  // remove all middle mappings
  return seastar::do_with(
    std::move(mapping),
    [ctx, &params](auto &mapping) {
    return trans_intr::repeat([ctx, &params, &mapping] {
      if (mapping.is_end()) {
	return ObjectDataHandler::base_iertr::make_ready_future<
	  seastar::stop_iteration>(seastar::stop_iteration::yes);
      }
      assert(mapping.get_key() >= params.data_begin);
      auto mapping_end =
	(mapping.get_key() + mapping.get_length()).checked_to_laddr();
      if (mapping_end > params.raw_end) {
	return ObjectDataHandler::base_iertr::make_ready_future<
	  seastar::stop_iteration>(seastar::stop_iteration::yes);
      }
      return ctx.tm.remove(ctx.t, std::move(mapping)
      ).si_then([&mapping](auto next_mapping) {
	mapping = std::move(next_mapping);
	return seastar::stop_iteration::no;
      }).handle_error_interruptible(
	ObjectDataHandler::base_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "ObjectDataHandler::read hit invalid error"
	}
      );
    }).si_then([&mapping] {
      return std::move(mapping);
    });
  });
}

do_mappings_ret do_last_mapping(
  context_t ctx,
  overwrite_params_t &params,
  LBAMapping mapping,
  data_t &data)
{
  if (!mapping.is_indirect() && !mapping.is_data_stable()) {
    // merge with existing pending extents
    auto offset = params.raw_end.template get_byte_distance<
      extent_len_t>(mapping.get_key());
    auto end = (mapping.get_key() + mapping.get_length()).checked_to_laddr();
    auto len = mapping.get_length() - offset;
    params.data_end = end;
    params.raw_end = laddr_offset_t{end};
    params.len += len;
    return load_padding(ctx, mapping.duplicate(), offset, len
    ).si_then([&data, mapping=std::move(mapping), ctx](auto tailbl) mutable {
      data.tailbl = std::move(tailbl);
      return ctx.tm.remove(ctx.t, std::move(mapping));
    }).handle_error_interruptible(
      ObjectDataHandler::base_iertr::pass_further{},
      crimson::ct_error::assert_all{
	"ObjectDataHandler::read hit invalid error"
      }
    );
  }
  auto mapping_end =
    (mapping.get_key() + mapping.get_length()).checked_to_laddr();
  auto pad_fut = ObjectDataHandler::read_iertr::make_ready_future<bufferlist>();
  if (mapping_end >= params.data_end &&
      params.raw_end != params.data_end) {
    // load the right padding
    pad_fut = load_padding(
      ctx,
      mapping.duplicate(),
      params.raw_end.template get_byte_distance<
	extent_len_t>(mapping.get_key()),
      params.data_end.template get_byte_distance<
	extent_len_t>(params.raw_end));
  }
  return pad_fut.si_then(
    [mapping_end, mapping=std::move(mapping),
    &data, ctx, &params](auto tailbl) mutable {
    if (tailbl.length() > 0) {
      data.tailbl = std::move(tailbl);
    }
    if (mapping_end > params.data_end) {
      auto laddr = mapping.get_key();
      using remap_entry_t = TransactionManager::remap_entry_t;
      return remap_mappings<1>(
	ctx,
	std::move(mapping),
	std::array{
	  remap_entry_t{
	    params.data_end.template get_byte_distance<
	      extent_len_t>(laddr),
	    mapping_end.template get_byte_distance<
	      extent_len_t>(params.data_end)}}
      ).si_then([](auto mappings) {
	assert(mappings.size() == 1);
	return std::move(mappings.front());
      });
    }
    return ctx.tm.remove(ctx.t, std::move(mapping)
    ).si_then([](auto next_mapping) {
      return std::move(next_mapping);
    }).handle_error_interruptible(
      ObjectDataHandler::base_iertr::pass_further{},
      crimson::ct_error::assert_all{
	"ObjectDataHandler::read hit invalid error"
      }
    );
  });
}

using punch_hole_iertr = ObjectDataHandler::write_iertr;
using punch_hole_ret = do_mappings_ret;
punch_hole_ret punch_hole(
  context_t ctx,
  overwrite_params_t &params,
  LBAMapping mapping,
  data_t &data)
{
  return do_first_mapping(
    ctx, params, std::move(mapping), data
  ).si_then([&params, ctx](auto mapping) {
    return do_middle_mappings(ctx, params, std::move(mapping));
  }).si_then([&params, ctx, &data](auto mapping) {
    if (mapping.is_end() || mapping.get_key() >= params.data_end) {
      return punch_hole_iertr::make_ready_future<
	LBAMapping>(std::move(mapping));
    }
    return do_last_mapping(ctx, params, std::move(mapping), data);
  });
}

ObjectDataHandler::write_ret do_zero(
  context_t ctx,
  LBAMapping mapping,
  const overwrite_params_t &params,
  data_t &data)
{
  assert(!data.bl);
  auto fut = TransactionManager::get_pin_iertr::make_ready_future<LBAMapping>();
  if (data.tailbl) {
    assert(data.tailbl->length() < ctx.tm.get_block_size());
    data.tailbl->prepend_zero(
      ctx.tm.get_block_size() - data.tailbl->length());
    fut = ctx.tm.alloc_data_extents<ObjectDataBlock>(
      ctx.t,
      (params.data_end - ctx.tm.get_block_size()).checked_to_laddr(),
      ctx.tm.get_block_size(),
      std::move(mapping)
    ).si_then([ctx, &data](auto extents) {
      assert(extents.size() == 1);
      auto &extent = extents.back();
      auto iter = data.tailbl->cbegin();
      iter.copy(extent->get_length(), extent->get_bptr().c_str());
      return ctx.tm.get_pin(ctx.t, *extent);
    }).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      TransactionManager::get_pin_iertr::pass_further{}
    );
  }
  fut = fut.si_then([ctx, &params, mapping=std::move(mapping),
		    &data](auto pin) mutable {
    assert(mapping.is_null() == !pin.is_null());
    auto laddr =
      (params.data_begin +
       (data.headbl ? ctx.tm.get_block_size() : 0)
      ).checked_to_laddr();
    auto end =
      (params.data_end -
       (data.tailbl ? ctx.tm.get_block_size() : 0)
      ).checked_to_laddr();
    auto len = end.get_byte_distance<extent_len_t>(laddr);
    return ctx.tm.reserve_region(
      ctx.t,
      mapping.is_null() ? std::move(pin) : std::move(mapping),
      laddr,
      len);
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    TransactionManager::get_pin_iertr::pass_further{}
  );
  if (data.headbl) {
    assert(data.headbl->length() < ctx.tm.get_block_size());
    data.headbl->append_zero(
      ctx.tm.get_block_size() - data.headbl->length());
    fut = fut.si_then([ctx, &params](auto pin) {
      return ctx.tm.alloc_data_extents<ObjectDataBlock>(
	ctx.t,
	params.data_begin,
	ctx.tm.get_block_size(),
	std::move(pin));
    }).si_then([&data](auto extents) {
      assert(extents.size() == 1);
      auto &extent = extents.back();
      auto iter = data.headbl->cbegin();
      iter.copy(extent->get_length(), extent->get_bptr().c_str());
      return LBAMapping{};
    }).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      TransactionManager::get_pin_iertr::pass_further{}
    );
  }
  return fut.discard_result().handle_error_interruptible(
    ObjectDataHandler::write_iertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected error"}
  );
}

ObjectDataHandler::write_ret do_write(
  context_t ctx,
  LBAMapping mapping,
  const overwrite_params_t &params,
  data_t &data)
{
  assert(data.bl);
  return ctx.tm.alloc_data_extents<ObjectDataBlock>(
    ctx.t,
    params.data_begin,
    params.data_end.template get_byte_distance<
      extent_len_t>(params.data_begin),
    std::move(mapping)
  ).si_then([&params, &data](auto extents) {
    auto off = params.data_begin;
    auto left = params.data_begin.template get_byte_distance<
      extent_len_t>(params.data_end);
    bufferlist _bl;
    if (data.headbl) {
      _bl.append(*data.headbl);
    }
    _bl.append(*data.bl);
    if (data.tailbl) {
      _bl.append(*data.tailbl);
    }
    auto iter = _bl.cbegin();
    assert(_bl.length() == left);
    for (auto &extent : extents) {
      ceph_assert(left >= extent->get_length());
      if (extent->get_laddr() != off) {
	logger().debug(
	  "object_data_handler::do_insertions alloc got addr {},"
	  " should have been {}",
	  extent->get_laddr(),
	  off);
      }
      iter.copy(extent->get_length(), extent->get_bptr().c_str());
      off = (off + extent->get_length()).checked_to_laddr();
      left -= extent->get_length();
    }
    return ObjectDataHandler::write_iertr::now();
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    ObjectDataHandler::write_iertr::pass_further{}
  );
}

ObjectDataHandler::write_ret ObjectDataHandler::overwrite(
  context_t ctx,
  laddr_t data_base,
  objaddr_t offset,
  extent_len_t len,
  std::optional<bufferlist> &&bl,
  LBAMapping first_mapping)
{
  LOG_PREFIX(ObjectDataHandler::overwrite);
  assert(!bl.has_value() || bl->length() == len);
  auto raw_begin = data_base + offset;
  auto raw_end = data_base + offset + len;
  assert(first_mapping.get_key() <= raw_begin.get_aligned_laddr());
  DEBUGT(
    "data_base={}, offset=0x{:x}, len=0x{:x}, "
    "{}, data_begin={}, data_end={}",
    ctx.t, data_base, offset, len, first_mapping,
    raw_begin.get_aligned_laddr(), raw_end.get_roundup_laddr());
  return seastar::do_with(
    data_t{std::nullopt, std::move(bl), std::nullopt},
    overwrite_params_t{
      offset,
      len,
      first_mapping.get_key(),
      first_mapping.get_length(),
      raw_begin,
      raw_begin.get_aligned_laddr(),
      raw_end,
      raw_end.get_roundup_laddr()},
    [this, ctx, first_mapping=std::move(first_mapping)]
    (auto &data, auto &params) mutable {
    return maybe_delta_based_overwrite(
      ctx, params, std::move(first_mapping), data,
      delta_based_overwrite_max_extent_size
    ).si_then([ctx, &params, &data](auto mapping) {
      if (mapping.is_null()) {
	// the modified range is within the first mapping
	// and can be applied through delta based overwrite
	return write_iertr::now();
      }
      return punch_hole(ctx, params, std::move(mapping), data
      ).si_then([ctx, &params, &data](auto mapping) {
	if (params.data_begin.template get_byte_distance<
	      extent_len_t>(params.data_end) == ctx.tm.get_block_size()
	    && (data.headbl || data.tailbl)) {
	  // the range to zero is within a block
	  bufferlist bl;
	  if (data.headbl) {
	    bl.append(*data.headbl);
	  }
	  if (!data.bl) {
	    bl.append_zero(params.len);
	  } else {
	    bl.append(*data.bl);
	  }
	  if (data.tailbl) {
	    bl.append(*data.tailbl);
	  }
	  data.headbl.reset();
	  data.tailbl.reset();
	  data.bl = std::move(bl);
	}
	if (data.bl) {
	  return do_write(ctx, std::move(mapping), params, data);
	} else {
	  return do_zero(ctx, std::move(mapping), params, data);
	}
      });
    });
  });
}

ObjectDataHandler::zero_ret ObjectDataHandler::zero(
  context_t ctx,
  objaddr_t offset,
  extent_len_t len)
{
  return with_object_data(
    ctx,
    [this, ctx, offset, len](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::zero);
      DEBUGT("zero to 0x{:x}~0x{:x}, object_data: {}~0x{:x}, is_null {}",
             ctx.t,
             offset,
             len,
             object_data.get_reserved_data_base(),
             object_data.get_reserved_data_len(),
             object_data.is_null());
      return prepare_data_reservation(
	ctx,
	object_data,
	p2roundup(offset + len, ctx.tm.get_block_size())
      ).si_then([this, ctx, offset, len, &object_data](auto mapping) {
	auto data_base = object_data.get_reserved_data_base();
	if (!mapping.is_null()) {
	  return overwrite(
	    ctx, data_base, offset, len,
	    std::nullopt, std::move(mapping));
	}
	laddr_offset_t l_start = data_base + offset;
	laddr_t aligned_start = l_start.get_aligned_laddr();
	return ctx.tm.get_pin(ctx.t, aligned_start, true
	).si_then([this, ctx, data_base, offset, len](auto pin) {
	  return overwrite(
	    ctx, data_base, offset, len,
	    std::nullopt, std::move(pin));
	}).handle_error_interruptible(
	  write_iertr::pass_further{},
	  crimson::ct_error::assert_all("unexpected enoent")
	);
      });
    });
}

ObjectDataHandler::write_ret ObjectDataHandler::write(
  context_t ctx,
  objaddr_t offset,
  const bufferlist &bl)
{
  return with_object_data(
    ctx,
    [this, ctx, offset, &bl](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::write);
      DEBUGT("writing to 0x{:x}~0x{:x}, object_data: {}~0x{:x}, is_null {}",
             ctx.t,
             offset,
	     bl.length(),
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len(),
             object_data.is_null());
      return prepare_data_reservation(
	ctx,
	object_data,
	p2roundup(offset + bl.length(), ctx.tm.get_block_size())
      ).si_then([this, ctx, offset, &object_data, &bl]
		(auto mapping) -> write_ret {
	auto data_base = object_data.get_reserved_data_base();
	if (!mapping.is_null()) {
	  return overwrite(
	    ctx, data_base, offset, bl.length(),
	    bufferlist(bl), std::move(mapping));
	}
	laddr_offset_t l_start = data_base + offset;
	laddr_t aligned_start = l_start.get_aligned_laddr();
	return ctx.tm.get_pin(ctx.t, aligned_start, true
	).si_then([this, ctx, offset, data_base, &bl](auto pin) {
	  return overwrite(
	    ctx, data_base, offset, bl.length(),
	    bufferlist(bl), std::move(pin));
	}).handle_error_interruptible(
	  write_iertr::pass_further{},
	  crimson::ct_error::assert_all{"unexpected enoent"}
	);
      });
    });
}

ObjectDataHandler::clear_ret ObjectDataHandler::trim_data_reservation(
  context_t ctx, object_data_t &object_data, extent_len_t size)
{
  LOG_PREFIX(ObjectDataHandler::trim_data_reservation);
  DEBUGT("{}~{}, {}",
    ctx.t, object_data.get_reserved_data_base(),
    object_data.get_reserved_data_len(), size);
  ceph_assert(!object_data.is_null());
  ceph_assert(size <= object_data.get_reserved_data_len());
  auto data_base = object_data.get_reserved_data_base();
  auto key = (data_base + size).get_aligned_laddr();
  return ctx.tm.get_pin(ctx.t, key, true
  ).si_then([ctx, data_base, size, key, &object_data](auto mapping) {
    assert(mapping.get_key() <= key &&
      mapping.get_key() + mapping.get_length() > key);
    auto data_len = object_data.get_reserved_data_len();
    auto raw_begin = data_base + size;
    auto raw_end = data_base + data_len;
    return seastar::do_with(
      data_t{},
      overwrite_params_t{
	size,
	object_data.get_reserved_data_len(),
	mapping.get_key(),
	mapping.get_length(),
	raw_begin,
	key,
	raw_end,
	raw_end.get_roundup_laddr()},
      [ctx, mapping=std::move(mapping)]
      (auto &data, auto &params) mutable {
      return punch_hole(ctx, params, std::move(mapping), data
      ).si_then([&params, ctx, &data](auto mapping) {
	assert(mapping.is_end() || mapping.get_key() >= params.data_end);
	return do_zero(ctx, std::move(mapping), params, data);
      });
    });
  }).handle_error_interruptible(
    clear_iertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected enoent"}
  );
}

ObjectDataHandler::read_ret ObjectDataHandler::read(
  context_t ctx,
  objaddr_t obj_offset,
  extent_len_t len)
{
  return seastar::do_with(
    bufferlist(),
    [ctx, obj_offset, len](auto &ret) {
    return with_object_data(
      ctx,
      [ctx, obj_offset, len, &ret](const auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::read);
      DEBUGT("reading {}~0x{:x}",
             ctx.t,
             object_data.get_reserved_data_base(),
             object_data.get_reserved_data_len());
      /* Assumption: callers ensure that onode size is <= reserved
       * size and that len is adjusted here prior to call */
      ceph_assert(!object_data.is_null());
      ceph_assert((obj_offset + len) <= object_data.get_reserved_data_len());
      ceph_assert(len > 0);
      laddr_offset_t l_start =
        object_data.get_reserved_data_base() + obj_offset;
      laddr_offset_t l_end = l_start + len;
      laddr_t aligned_start = l_start.get_aligned_laddr();
      loffset_t aligned_length =
	  l_end.get_roundup_laddr().get_byte_distance<
	    loffset_t>(aligned_start);
      return ctx.tm.get_pins(
        ctx.t,
	aligned_start,
	aligned_length
      ).si_then([FNAME, ctx, l_start, l_end, &ret](auto _pins) {
        // offset~len falls within reserved region and len > 0
        ceph_assert(_pins.size() >= 1);
        ceph_assert(_pins.front().get_key() <= l_start);
        return seastar::do_with(
          std::move(_pins),
          l_start,
          [FNAME, ctx, l_start, l_end, &ret](auto &pins, auto &l_current) {
          return trans_intr::do_for_each(
            pins,
            [FNAME, ctx, l_start, l_end,
             &l_current, &ret](auto &pin) -> read_iertr::future<> {
            auto pin_start = pin.get_key();
            extent_len_t read_start;
            extent_len_t read_start_aligned;
            if (l_current == l_start) { // first pin may skip head
              ceph_assert(l_current.get_aligned_laddr() >= pin_start);
              read_start = l_current.template
                get_byte_distance<extent_len_t>(pin_start);
              read_start_aligned = p2align(read_start, ctx.tm.get_block_size());
            } else { // non-first pin must match start
              assert(l_current > l_start);
              ceph_assert(l_current == pin_start);
              read_start = 0;
              read_start_aligned = 0;
            }

            ceph_assert(l_current < l_end);
            auto pin_len = pin.get_length();
            assert(pin_len > 0);
            laddr_offset_t pin_end = pin_start + pin_len;
            assert(l_current < pin_end);
            laddr_offset_t l_current_end = std::min(pin_end, l_end);
            extent_len_t read_len =
              l_current_end.get_byte_distance<extent_len_t>(l_current);

            if (pin.get_val().is_zero()) {
              DEBUGT("got {}~0x{:x} from zero-pin {}~0x{:x}",
                ctx.t,
                l_current,
                read_len,
                pin_start,
                pin_len);
              ret.append_zero(read_len);
              l_current = l_current_end;
              return seastar::now();
            }

            // non-zero pin
            laddr_t l_current_end_aligned = l_current_end.get_roundup_laddr();
            extent_len_t read_len_aligned =
              l_current_end_aligned.get_byte_distance<extent_len_t>(pin_start);
            read_len_aligned -= read_start_aligned;
            extent_len_t unalign_start_offset = read_start - read_start_aligned;
            DEBUGT("reading {}~0x{:x} from pin {}~0x{:x}",
              ctx.t,
              l_current,
              read_len,
              pin_start,
              pin_len);
            return ctx.tm.read_pin<ObjectDataBlock>(
              ctx.t,
              std::move(pin),
              read_start_aligned,
              read_len_aligned
            ).si_then([&ret, &l_current, l_current_end,
                       read_start_aligned, read_len_aligned,
                       unalign_start_offset, read_len](auto maybe_indirect_extent) {
              auto aligned_bl = maybe_indirect_extent.get_range(
                  read_start_aligned, read_len_aligned);
              if (read_len < read_len_aligned) {
                ceph::bufferlist unaligned_bl;
                unaligned_bl.substr_of(
                    aligned_bl, unalign_start_offset, read_len);
                ret.append(std::move(unaligned_bl));
              } else {
                assert(read_len == read_len_aligned);
                assert(unalign_start_offset == 0);
                ret.append(std::move(aligned_bl));
              }
              l_current = l_current_end;
              return seastar::now();
            }).handle_error_interruptible(
              read_iertr::pass_further{},
              crimson::ct_error::assert_all{
                "ObjectDataHandler::read hit invalid error"
              }
            );
          }); // trans_intr::do_for_each()
        }); // do_with()
      });
    }).si_then([&ret] { // with_object_data()
      return std::move(ret);
    });
  }); // do_with()
}

ObjectDataHandler::fiemap_ret ObjectDataHandler::fiemap(
  context_t ctx,
  objaddr_t obj_offset,
  extent_len_t len)
{
  return seastar::do_with(
    std::map<uint64_t, uint64_t>(),
    [ctx, obj_offset, len](auto &ret) {
    return with_object_data(
      ctx,
      [ctx, obj_offset, len, &ret](const auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::fiemap);
      DEBUGT(
	"0x{:x}~0x{:x}, reservation {}~0x{:x}",
        ctx.t,
        obj_offset,
        len,
        object_data.get_reserved_data_base(),
        object_data.get_reserved_data_len());
      /* Assumption: callers ensure that onode size is <= reserved
       * size and that len is adjusted here prior to call */
      ceph_assert(!object_data.is_null());
      ceph_assert((obj_offset + len) <= object_data.get_reserved_data_len());
      ceph_assert(len > 0);
      laddr_offset_t l_start =
        object_data.get_reserved_data_base() + obj_offset;
      laddr_offset_t l_end = l_start + len;
      laddr_t aligned_start = l_start.get_aligned_laddr();
      loffset_t aligned_length =
	  l_end.get_roundup_laddr().get_byte_distance<
	    loffset_t>(aligned_start);
      return ctx.tm.get_pins(
        ctx.t,
	aligned_start,
	aligned_length
      ).si_then([l_start, len, &object_data, &ret](auto &&pins) {
	ceph_assert(pins.size() >= 1);
        ceph_assert(pins.front().get_key() <= l_start);
	for (auto &&i: pins) {
	  if (!(i.get_val().is_zero())) {
	    laddr_offset_t ret_left = std::max(laddr_offset_t(i.get_key(), 0), l_start);
	    laddr_offset_t ret_right = std::min(
	      i.get_key() + i.get_length(),
	      l_start + len);
	    assert(ret_right > ret_left);
	    ret.emplace(
	      std::make_pair(
		ret_left.get_byte_distance<uint64_t>(
		  object_data.get_reserved_data_base()),
		ret_right.get_byte_distance<uint64_t>(ret_left)
	      ));
	  }
	}
      });
    }).si_then([&ret] {
      return std::move(ret);
    });
  });
}

ObjectDataHandler::truncate_ret ObjectDataHandler::truncate(
  context_t ctx,
  objaddr_t offset)
{
  return with_object_data(
    ctx,
    [this, ctx, offset](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::truncate);
      DEBUGT("truncating {}~0x{:x} offset: 0x{:x}",
	     ctx.t,
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len(),
	     offset);
      if (offset < object_data.get_reserved_data_len()) {
	return trim_data_reservation(ctx, object_data, offset);
      } else if (offset > object_data.get_reserved_data_len()) {
	return prepare_data_reservation(
	  ctx,
	  object_data,
	  p2roundup(offset, ctx.tm.get_block_size())).discard_result();
      } else {
	return truncate_iertr::now();
      }
    });
}

ObjectDataHandler::clear_ret ObjectDataHandler::clear(
  context_t ctx)
{
  return with_object_data(
    ctx,
    [this, ctx](auto &object_data) {
      LOG_PREFIX(ObjectDataHandler::clear);
      DEBUGT("clearing: {}~{}",
	     ctx.t,
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len());
      if (object_data.is_null()) {
	return clear_iertr::now();
      }
      return trim_data_reservation(ctx, object_data, 0);
    });
}

ObjectDataHandler::clone_ret ObjectDataHandler::clone_extents(
  context_t ctx,
  object_data_t &object_data,
  lba_mapping_list_t &pins,
  laddr_t data_base)
{
  LOG_PREFIX(ObjectDataHandler::clone_extents);
  TRACET("object_data: {}~0x{:x}, data_base: 0x{:x}",
    ctx.t,
    object_data.get_reserved_data_base(),
    object_data.get_reserved_data_len(),
    data_base);
  return ctx.tm.remove(
    ctx.t,
    object_data.get_reserved_data_base()
  ).si_then(
    [&pins, &object_data, ctx, data_base](auto) mutable {
      return seastar::do_with(
	(extent_len_t)0,
	[&object_data, ctx, data_base, &pins](auto &last_pos) {
	return trans_intr::do_for_each(
	  pins,
	  [&last_pos, &object_data, ctx, data_base](auto &pin) {
	  auto offset = pin.get_key().template get_byte_distance<
	    extent_len_t>(data_base);
	  ceph_assert(offset == last_pos);
	  laddr_t addr = (object_data.get_reserved_data_base() + offset)
	      .checked_to_laddr();
	  return seastar::futurize_invoke([ctx, addr, &pin] {
	    if (pin.get_val().is_zero()) {
	      return ctx.tm.reserve_region(ctx.t, addr, pin.get_length());
	    } else {
	      return ctx.tm.clone_pin(ctx.t, addr, pin);
	    }
	  }).si_then(
	    [&pin, &last_pos, offset](auto) {
	    last_pos = offset + pin.get_length();
	    return seastar::now();
	  }).handle_error_interruptible(
	    crimson::ct_error::input_output_error::pass_further(),
	    crimson::ct_error::assert_all("not possible")
	  );
	}).si_then([&last_pos, &object_data, ctx] {
	  if (last_pos != object_data.get_reserved_data_len()) {
	    return ctx.tm.reserve_region(
	      ctx.t,
	      (object_data.get_reserved_data_base() + last_pos).checked_to_laddr(),
	      object_data.get_reserved_data_len() - last_pos
	    ).si_then([](auto) {
	      return seastar::now();
	    });
	  }
	  return TransactionManager::reserve_extent_iertr::now();
	});
      });
    }
  ).handle_error_interruptible(
    ObjectDataHandler::write_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "object_data_handler::clone invalid error"
  });
}

ObjectDataHandler::clone_ret ObjectDataHandler::clone(
  context_t ctx)
{
  // the whole clone procedure can be seperated into the following steps:
  // 	1. let clone onode(d_object_data) take the head onode's
  // 	   object data base;
  // 	2. reserve a new region in lba tree for the head onode;
  // 	3. clone all extents of the clone onode, see transaction_manager.h
  // 	   for the details of clone_pin;
  // 	4. reserve the space between the head onode's size and its reservation
  // 	   length.
  return with_objects_data(
    ctx,
    [ctx, this](auto &object_data, auto &d_object_data) {
    ceph_assert(d_object_data.is_null());
    if (object_data.is_null()) {
      return clone_iertr::now();
    }
    return prepare_data_reservation(
      ctx,
      d_object_data,
      object_data.get_reserved_data_len()
    ).si_then([&object_data, &d_object_data, ctx, this](auto) {
      assert(!object_data.is_null());
      auto base = object_data.get_reserved_data_base();
      auto len = object_data.get_reserved_data_len();
      object_data.clear();
      LOG_PREFIX(ObjectDataHandler::clone);
      DEBUGT("cloned obj reserve_data_base: {}, len 0x{:x}",
	ctx.t,
	d_object_data.get_reserved_data_base(),
	d_object_data.get_reserved_data_len());
      return prepare_data_reservation(
	ctx,
	object_data,
	d_object_data.get_reserved_data_len()
      ).si_then([&d_object_data, ctx, &object_data, base, len, this](auto) {
	LOG_PREFIX("ObjectDataHandler::clone");
	DEBUGT("head obj reserve_data_base: {}, len 0x{:x}",
	  ctx.t,
	  object_data.get_reserved_data_base(),
	  object_data.get_reserved_data_len());
	return ctx.tm.get_pins(ctx.t, base, len
	).si_then([ctx, &object_data, &d_object_data, base, this](auto pins) {
	  return seastar::do_with(
	    std::move(pins),
	    [ctx, &object_data, &d_object_data, base, this](auto &pins) {
	    return clone_extents(ctx, object_data, pins, base
	    ).si_then([ctx, &d_object_data, base, &pins, this] {
	      return clone_extents(ctx, d_object_data, pins, base);
	    }).si_then([&pins, ctx] {
	      return do_removals(ctx, pins);
	    });
	  });
	});
      });
    });
  });
}

} // namespace crimson::os::seastore
