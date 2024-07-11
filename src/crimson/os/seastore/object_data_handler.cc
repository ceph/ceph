// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <utility>
#include <functional>

#include "crimson/common/log.h"

#include "crimson/os/seastore/object_data_handler.h"

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

  /// pin of original extent, not nullptr if type == EXISTING
  LBAMappingRef pin;

  laddr_t addr;
  extent_len_t len;

  /// non-nullopt if and only if type == DATA
  std::optional<bufferlist> to_write;

  extent_to_write_t(const extent_to_write_t &) = delete;
  extent_to_write_t(extent_to_write_t &&) = default;

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
    return addr + len;
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
    LBAMappingRef &&pin, laddr_t addr, extent_len_t len) {
    assert(pin);
    return extent_to_write_t(std::move(pin), addr, len);
  }

private:
  extent_to_write_t(laddr_t addr, bufferlist to_write)
    : type(type_t::DATA), addr(addr), len(to_write.length()),
      to_write(to_write) {}

  extent_to_write_t(laddr_t addr, extent_len_t len)
    : type(type_t::ZERO), addr(addr), len(len) {}

  extent_to_write_t(LBAMappingRef &&pin, laddr_t addr, extent_len_t len)
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
  LBAMappingRef pin;
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
    assert((new_offset != 0) && (pin->get_length() != new_offset + new_len));
    return type == type_t::REMAP2;
  }

  bool is_overwrite() const {
    return type == type_t::OVERWRITE;
  }

  using remap_entry = TransactionManager::remap_entry;
  remap_entry create_remap_entry() {
    assert(is_remap1());
    return remap_entry(
      new_offset,
      new_len);
  }

  remap_entry create_left_remap_entry() {
    assert(is_remap2());
    return remap_entry(
      0,
      new_offset);
  }

  remap_entry create_right_remap_entry() {
    assert(is_remap2());
    return remap_entry(
      new_offset + new_len,
      pin->get_length() - new_offset - new_len);
  }

  static extent_to_remap_t create_remap1(
    LBAMappingRef &&pin, extent_len_t new_offset, extent_len_t new_len) {
    return extent_to_remap_t(type_t::REMAP1,
      std::move(pin), new_offset, new_len);
  }

  static extent_to_remap_t create_remap2(
    LBAMappingRef &&pin, extent_len_t new_offset, extent_len_t new_len) {
    return extent_to_remap_t(type_t::REMAP2,
      std::move(pin), new_offset, new_len);
  }

  static extent_to_remap_t create_overwrite(
    extent_len_t new_offset, extent_len_t new_len, LBAMappingRef p,
    bufferlist b) {
    return extent_to_remap_t(type_t::OVERWRITE,
      nullptr, new_offset, new_len, p->get_key(), p->get_length(), b);
  }

  uint64_t laddr_start;
  extent_len_t length;
  std::optional<bufferlist> bl;

private:
  extent_to_remap_t(type_t type,
    LBAMappingRef &&pin, extent_len_t new_offset, extent_len_t new_len)
    : type(type),
      pin(std::move(pin)), new_offset(new_offset), new_len(new_len) {}
  extent_to_remap_t(type_t type,
    LBAMappingRef &&pin, extent_len_t new_offset, extent_len_t new_len,
    uint64_t ori_laddr, extent_len_t ori_len, std::optional<bufferlist> b)
    : type(type),
      pin(std::move(pin)), new_offset(new_offset), new_len(new_len),
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
using extent_to_remove_list_t = std::list<LBAMappingRef>;

struct overwrite_ops_t {
  extent_to_remap_list_t to_remap;
  extent_to_insert_list_t to_insert;
  extent_to_remove_list_t to_remove;
};

// prepare to_remap, to_retire, to_insert list
overwrite_ops_t prepare_ops_list(
  lba_pin_list_t &pins_to_remove,
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
      assert(front.addr == front.pin->get_key());
      assert(back.addr > back.pin->get_key());
      ops.to_remap.push_back(extent_to_remap_t::create_remap2(
	std::move(front.pin),
	front.len,
	back.addr - front.addr - front.len));
      ops.to_remove.pop_front();
  } else {
    // prepare to_remap, happens in one or multiple extents
    if (front.is_existing()) {
      visitted++;
      assert(to_write.size() > 1);
      assert(front.addr == front.pin->get_key());
      ops.to_remap.push_back(extent_to_remap_t::create_remap1(
	std::move(front.pin),
	0,
	front.len));
      ops.to_remove.pop_front();
    }
    if (back.is_existing()) {
      visitted++;
      assert(to_write.size() > 1);
      assert(back.addr + back.len ==
	back.pin->get_key() + back.pin->get_length());
      ops.to_remap.push_back(extent_to_remap_t::create_remap1(
	std::move(back.pin),
	back.addr - back.pin->get_key(),
	back.len));
      ops.to_remove.pop_back();
    }
  }

  interval_set<uint64_t> pre_alloc_addr_removed, pre_alloc_addr_remapped;
  if (delta_based_overwrite_max_extent_size) {
    for (auto &r : ops.to_remove) {
      if (r->is_data_stable() && !r->is_zero_reserved()) {
	pre_alloc_addr_removed.insert(r->get_key(), r->get_length());

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
	    interval_set<uint64_t> range;
	    range.insert(r->get_key(), r->get_length());
	    if (range.contains(region.addr, region.len) && !r->is_clone()) {
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
	    interval_set<uint64_t> range;
	    range.insert(r.pin->get_key(), r.pin->get_length());
	    if (range.contains(region.addr, region.len) && !r.pin->is_clone()) {
	      to_remap.push_back(extent_to_remap_t::create_overwrite(
		region.addr - range.begin().get_start(), region.len,
		std::move(r.pin), *region.to_write));
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
        return ctx.tm.remap_pin<ObjectDataBlock, 1>(
          ctx.t,
          std::move(region.pin),
          std::array{
            region.create_remap_entry()
          }
        ).si_then([&region](auto pins) {
          ceph_assert(pins.size() == 1);
          ceph_assert(region.new_len == pins[0]->get_length());
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
	auto pin_key = region.pin->get_key();
        return ctx.tm.remap_pin<ObjectDataBlock, 2>(
          ctx.t,
          std::move(region.pin),
          std::array{
            region.create_left_remap_entry(),
            region.create_right_remap_entry()
          }
        ).si_then([&region, pin_key](auto pins) {
          ceph_assert(pins.size() == 2);
          ceph_assert(pin_key == pins[0]->get_key());
          ceph_assert(pin_key + pins[0]->get_length() +
            region.new_len == pins[1]->get_key());
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
  lba_pin_list_t &to_remove)
{
  return trans_intr::do_for_each(
    to_remove,
    [ctx](auto &pin) {
      LOG_PREFIX(object_data_handler.cc::do_removals);
      DEBUGT("decreasing ref: {}",
	     ctx.t,
	     pin->get_key());
      return ctx.tm.remove(
	ctx.t,
	pin->get_key()
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
	assert_aligned(region.addr);
	assert_aligned(region.len);
	ceph_assert(region.len == region.bl->length());
	DEBUGT("allocating extent: {}~{}",
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
            off += extent->get_length();
            left -= extent->get_length();
          }
	  return ObjectDataHandler::write_iertr::now();
	}).handle_error_interruptible(
	  crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	  ObjectDataHandler::write_iertr::pass_further{}
	);
      } else if (region.is_zero()) {
	DEBUGT("reserving: {}~{}",
	       ctx.t,
	       region.addr,
	       region.len);
	return ctx.tm.reserve_region(
	  ctx.t,
	  region.addr,
	  region.len
	).si_then([FNAME, ctx, &region](auto pin) {
	  ceph_assert(pin->get_length() == region.len);
	  if (pin->get_key() != region.addr) {
	    ERRORT(
	      "inconsistent laddr: pin: {} region {}",
	      ctx.t,
	      pin->get_key(),
	      region.addr);
	  }
	  ceph_assert(pin->get_key() == region.addr);
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
  // addresses
  laddr_t pin_begin;
  laddr_t pin_end;
  paddr_t left_paddr;
  paddr_t right_paddr;
  laddr_t data_begin;
  laddr_t data_end;
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
    return data_begin - pin_begin;
  }

  extent_len_t get_left_extent_size() const {
    return aligned_data_begin - pin_begin;
  }

  extent_len_t get_left_alignment_size() const {
    return data_begin - aligned_data_begin;
  }

  extent_len_t get_right_size() const {
    return pin_end - data_end;
  }

  extent_len_t get_right_extent_size() const {
    return pin_end - aligned_data_end;
  }

  extent_len_t get_right_alignment_size() const {
    return aligned_data_end - data_end;
  }

  extent_len_t get_aligned_data_size() const {
    return aligned_data_end - aligned_data_begin;
  }

  extent_len_t get_pins_size() const {
    return pin_end - pin_begin;
  }

  friend std::ostream& operator<<(
    std::ostream& out,
    const overwrite_plan_t& overwrite_plan) {
    return out << "overwrite_plan_t("
	       << "pin_begin=" << overwrite_plan.pin_begin
	       << ", pin_end=" << overwrite_plan.pin_end
	       << ", left_paddr=" << overwrite_plan.left_paddr
	       << ", right_paddr=" << overwrite_plan.right_paddr
	       << ", data_begin=" << overwrite_plan.data_begin
	       << ", data_end=" << overwrite_plan.data_end
	       << ", aligned_data_begin=" << overwrite_plan.aligned_data_begin
	       << ", aligned_data_end=" << overwrite_plan.aligned_data_end
	       << ", left_operation=" << overwrite_plan.left_operation
	       << ", right_operation=" << overwrite_plan.right_operation
	       << ", block_size=" << overwrite_plan.block_size
	       << ", is_left_fresh=" << overwrite_plan.is_left_fresh
	       << ", is_right_fresh=" << overwrite_plan.is_right_fresh
	       << ")";
  }

  overwrite_plan_t(laddr_t offset,
		   extent_len_t len,
		   const lba_pin_list_t& pins,
		   extent_len_t block_size) :
      pin_begin(pins.front()->get_key()),
      pin_end(pins.back()->get_key() + pins.back()->get_length()),
      left_paddr(pins.front()->get_val()),
      right_paddr(pins.back()->get_val()),
      data_begin(offset),
      data_end(offset + len),
      aligned_data_begin(p2align((uint64_t)data_begin, (uint64_t)block_size)),
      aligned_data_end(p2roundup((uint64_t)data_end, (uint64_t)block_size)),
      left_operation(overwrite_operation_t::UNKNOWN),
      right_operation(overwrite_operation_t::UNKNOWN),
      block_size(block_size),
      // TODO: introduce PhysicalNodeMapping::is_fresh()
      // Note: fresh write can be merged with overwrite if they overlap.
      is_left_fresh(!pins.front()->is_stable()),
      is_right_fresh(!pins.back()->is_stable()) {
    validate();
    evaluate_operations();
    assert(left_operation != overwrite_operation_t::UNKNOWN);
    assert(right_operation != overwrite_operation_t::UNKNOWN);
  }

private:
  // refer to overwrite_plan_t description
  void validate() const {
    ceph_assert(pin_begin % block_size == 0);
    ceph_assert(pin_end % block_size == 0);
    ceph_assert(aligned_data_begin % block_size == 0);
    ceph_assert(aligned_data_end % block_size == 0);

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
  std::optional<bufferptr>>;
using operate_ret = get_iertr::future<operate_ret_bare>;
operate_ret operate_left(context_t ctx, LBAMappingRef &pin, const overwrite_plan_t &overwrite_plan)
{
  if (overwrite_plan.get_left_size() == 0) {
    return get_iertr::make_ready_future<operate_ret_bare>(
      std::nullopt,
      std::nullopt);
  }

  if (overwrite_plan.left_operation == overwrite_operation_t::OVERWRITE_ZERO) {
    assert(pin->get_val().is_zero());
    auto zero_extent_len = overwrite_plan.get_left_extent_size();
    assert_aligned(zero_extent_len);
    auto zero_prepend_len = overwrite_plan.get_left_alignment_size();
    return get_iertr::make_ready_future<operate_ret_bare>(
      (zero_extent_len == 0
       ? std::nullopt
       : std::make_optional(extent_to_write_t::create_zero(
           overwrite_plan.pin_begin, zero_extent_len))),
      (zero_prepend_len == 0
       ? std::nullopt
       : std::make_optional(bufferptr(
           ceph::buffer::create(zero_prepend_len, 0))))
    );
  } else if (overwrite_plan.left_operation == overwrite_operation_t::MERGE_EXISTING) {
    auto prepend_len = overwrite_plan.get_left_size();
    if (prepend_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::nullopt,
        std::nullopt);
    } else {
      extent_len_t off = pin->get_intermediate_offset();
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin->duplicate()
      ).si_then([prepend_len, off](auto left_extent) {
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::nullopt,
          std::make_optional(bufferptr(
            left_extent->get_bptr(),
            off,
            prepend_len)));
      });
    }
  } else {
    assert(overwrite_plan.left_operation == overwrite_operation_t::SPLIT_EXISTING);

    auto extent_len = overwrite_plan.get_left_extent_size();
    assert(extent_len);
    std::optional<extent_to_write_t> left_to_write_extent =
      std::make_optional(extent_to_write_t::create_existing(
        pin->duplicate(),
        pin->get_key(),
        extent_len));

    auto prepend_len = overwrite_plan.get_left_alignment_size();
    if (prepend_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::move(left_to_write_extent),
        std::nullopt);
    } else {
      extent_len_t off = pin->get_intermediate_offset();
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin->duplicate()
      ).si_then([prepend_offset=extent_len + off, prepend_len,
                 left_to_write_extent=std::move(left_to_write_extent)]
                (auto left_extent) mutable {
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::move(left_to_write_extent),
          std::make_optional(bufferptr(
            left_extent->get_bptr(),
            prepend_offset,
            prepend_len)));
      });
    }
  }
};

/**
 * operate_right
 *
 * Proceed overwrite_plan.right_operation.
 */
operate_ret operate_right(context_t ctx, LBAMappingRef &pin, const overwrite_plan_t &overwrite_plan)
{
  if (overwrite_plan.get_right_size() == 0) {
    return get_iertr::make_ready_future<operate_ret_bare>(
      std::nullopt,
      std::nullopt);
  }

  auto right_pin_begin = pin->get_key();
  assert(overwrite_plan.data_end >= right_pin_begin);
  if (overwrite_plan.right_operation == overwrite_operation_t::OVERWRITE_ZERO) {
    assert(pin->get_val().is_zero());
    auto zero_suffix_len = overwrite_plan.get_right_alignment_size();
    auto zero_extent_len = overwrite_plan.get_right_extent_size();
    assert_aligned(zero_extent_len);
    return get_iertr::make_ready_future<operate_ret_bare>(
      (zero_extent_len == 0
       ? std::nullopt
       : std::make_optional(extent_to_write_t::create_zero(
           overwrite_plan.aligned_data_end, zero_extent_len))),
      (zero_suffix_len == 0
       ? std::nullopt
       : std::make_optional(bufferptr(
           ceph::buffer::create(zero_suffix_len, 0))))
    );
  } else if (overwrite_plan.right_operation == overwrite_operation_t::MERGE_EXISTING) {
    auto append_len = overwrite_plan.get_right_size();
    if (append_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::nullopt,
        std::nullopt);
    } else {
      auto append_offset =
	overwrite_plan.data_end
	- right_pin_begin
	+ pin->get_intermediate_offset();
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin->duplicate()
      ).si_then([append_offset, append_len](auto right_extent) {
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::nullopt,
          std::make_optional(bufferptr(
            right_extent->get_bptr(),
            append_offset,
            append_len)));
      });
    }
  } else {
    assert(overwrite_plan.right_operation == overwrite_operation_t::SPLIT_EXISTING);

    auto extent_len = overwrite_plan.get_right_extent_size();
    assert(extent_len);
    std::optional<extent_to_write_t> right_to_write_extent =
      std::make_optional(extent_to_write_t::create_existing(
        pin->duplicate(),
        overwrite_plan.aligned_data_end,
        extent_len));

    auto append_len = overwrite_plan.get_right_alignment_size();
    if (append_len == 0) {
      return get_iertr::make_ready_future<operate_ret_bare>(
        std::move(right_to_write_extent),
        std::nullopt);
    } else {
      auto append_offset =
	overwrite_plan.data_end
	- right_pin_begin
	+ pin->get_intermediate_offset();
      return ctx.tm.read_pin<ObjectDataBlock>(
	ctx.t, pin->duplicate()
      ).si_then([append_offset, append_len,
                 right_to_write_extent=std::move(right_to_write_extent)]
                (auto right_extent) mutable {
        return get_iertr::make_ready_future<operate_ret_bare>(
          std::move(right_to_write_extent),
          std::make_optional(bufferptr(
            right_extent->get_bptr(),
            append_offset,
            append_len)));
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

ObjectDataHandler::write_ret ObjectDataHandler::prepare_data_reservation(
  context_t ctx,
  object_data_t &object_data,
  extent_len_t size)
{
  LOG_PREFIX(ObjectDataHandler::prepare_data_reservation);
  ceph_assert(size <= max_object_size);
  if (!object_data.is_null()) {
    ceph_assert(object_data.get_reserved_data_len() == max_object_size);
    DEBUGT("reservation present: {}~{}",
           ctx.t,
           object_data.get_reserved_data_base(),
           object_data.get_reserved_data_len());
    return write_iertr::now();
  } else {
    DEBUGT("reserving: {}~{}",
           ctx.t,
           ctx.onode.get_data_hint(),
           max_object_size);
    return ctx.tm.reserve_region(
      ctx.t,
      ctx.onode.get_data_hint(),
      max_object_size
    ).si_then([max_object_size=max_object_size, &object_data](auto pin) {
      ceph_assert(pin->get_length() == max_object_size);
      object_data.update_reserved(
	pin->get_key(),
	pin->get_length());
      return write_iertr::now();
    }).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      write_iertr::pass_further{}
    );
  }
}

ObjectDataHandler::clear_ret ObjectDataHandler::trim_data_reservation(
  context_t ctx, object_data_t &object_data, extent_len_t size)
{
  ceph_assert(!object_data.is_null());
  ceph_assert(size <= object_data.get_reserved_data_len());
  return seastar::do_with(
    lba_pin_list_t(),
    extent_to_write_list_t(),
    [ctx, size, &object_data, this](auto &pins, auto &to_write) {
      LOG_PREFIX(ObjectDataHandler::trim_data_reservation);
      DEBUGT("object_data: {}~{}",
	     ctx.t,
	     object_data.get_reserved_data_base(),
	     object_data.get_reserved_data_len());
      return ctx.tm.get_pins(
	ctx.t,
	object_data.get_reserved_data_base() + size,
	object_data.get_reserved_data_len() - size
      ).si_then([ctx, size, &pins, &object_data, &to_write](auto _pins) {
	_pins.swap(pins);
	ceph_assert(pins.size());
	if (!size) {
	  // no need to reserve region if we are truncating the object's
	  // size to 0
	  return clear_iertr::now();
	}
	auto &pin = *pins.front();
	ceph_assert(pin.get_key() >= object_data.get_reserved_data_base());
	ceph_assert(
	  pin.get_key() <= object_data.get_reserved_data_base() + size);
	auto pin_offset = pin.get_key() -
	  object_data.get_reserved_data_base();
	if ((pin.get_key() == (object_data.get_reserved_data_base() + size)) ||
	  (pin.get_val().is_zero())) {
	  /* First pin is exactly at the boundary or is a zero pin.  Either way,
	   * remove all pins and add a single zero pin to the end. */
	  to_write.push_back(extent_to_write_t::create_zero(
	    pin.get_key(),
	    object_data.get_reserved_data_len() - pin_offset));
	  return clear_iertr::now();
	} else {
	  /* First pin overlaps the boundary and has data, remap it
	   * if aligned or rewrite it if not aligned to size */
          auto roundup_size = p2roundup(size, ctx.tm.get_block_size());
          auto append_len = roundup_size - size;
          if (append_len == 0) {
            LOG_PREFIX(ObjectDataHandler::trim_data_reservation);
            TRACET("First pin overlaps the boundary and has aligned data"
              "create existing at addr:{}, len:{}",
              ctx.t, pin.get_key(), size - pin_offset);
            to_write.push_back(extent_to_write_t::create_existing(
              pin.duplicate(),
              pin.get_key(),
              size - pin_offset));
	    to_write.push_back(extent_to_write_t::create_zero(
	      object_data.get_reserved_data_base() + roundup_size,
	      object_data.get_reserved_data_len() - roundup_size));
            return clear_iertr::now();
          } else {
            return ctx.tm.read_pin<ObjectDataBlock>(
              ctx.t,
              pin.duplicate()
            ).si_then([ctx, size, pin_offset, append_len, roundup_size,
                      &pin, &object_data, &to_write](auto extent) {
              bufferlist bl;
	      bl.append(
	        bufferptr(
	          extent->get_bptr(),
		  pin.get_intermediate_offset(),
	          size - pin_offset
	      ));
              bl.append_zero(append_len);
              LOG_PREFIX(ObjectDataHandler::trim_data_reservation);
              TRACET("First pin overlaps the boundary and has unaligned data"
                "create data at addr:{}, len:{}",
                ctx.t, pin.get_key(), bl.length());
	      to_write.push_back(extent_to_write_t::create_data(
	        pin.get_key(),
	        bl));
	      to_write.push_back(extent_to_write_t::create_zero(
	        object_data.get_reserved_data_base() + roundup_size,
	        object_data.get_reserved_data_len() - roundup_size));
              return clear_iertr::now();
            });
          }
	}
      }).si_then([ctx, size, &to_write, &object_data, &pins, this] {
        return seastar::do_with(
          prepare_ops_list(pins, to_write,
	    delta_based_overwrite_max_extent_size),
          [ctx, size, &object_data](auto &ops) {
            return do_remappings(ctx, ops.to_remap
            ).si_then([ctx, &ops] {
              return do_removals(ctx, ops.to_remove);
            }).si_then([ctx, &ops] {
              return do_insertions(ctx, ops.to_insert);
            }).si_then([size, &object_data] {
	      if (size == 0) {
	        object_data.clear();
	      }
	      return ObjectDataHandler::clear_iertr::now();
            });
        });
      });
    });
}

/**
 * get_to_writes_with_zero_buffer
 *
 * Returns extent_to_write_t's reflecting a zero region extending
 * from offset~len with headptr optionally on the left and tailptr
 * optionally on the right.
 */
extent_to_write_list_t get_to_writes_with_zero_buffer(
  const extent_len_t block_size,
  laddr_t offset, extent_len_t len,
  std::optional<bufferptr> &&headptr, std::optional<bufferptr> &&tailptr)
{
  auto zero_left = p2roundup(offset, (laddr_t)block_size);
  auto zero_right = p2align(offset + len, (laddr_t)block_size);
  auto left = headptr ? (offset - headptr->length()) : offset;
  auto right = tailptr ?
    (offset + len + tailptr->length()) :
    (offset + len);

  assert(
    (headptr && ((zero_left - left) ==
		 p2roundup(headptr->length(), block_size))) ^
    (!headptr && (zero_left == left)));
  assert(
    (tailptr && ((right - zero_right) ==
		 p2roundup(tailptr->length(), block_size))) ^
    (!tailptr && (right == zero_right)));

  assert(right > left);
  assert((left % block_size) == 0);
  assert((right % block_size) == 0);

  // zero region too small for a reserved section,
  // headptr and tailptr in same extent
  if (zero_right <= zero_left) {
    bufferlist bl;
    if (headptr) {
      bl.append(*headptr);
    }
    bl.append_zero(
      right - left - bl.length() - (tailptr ? tailptr->length() : 0));
    if (tailptr) {
      bl.append(*tailptr);
    }
    assert(bl.length() % block_size == 0);
    assert(bl.length() == (right - left));
    extent_to_write_list_t ret;
    ret.push_back(extent_to_write_t::create_data(left, bl));
    return ret;
  } else {
    // reserved section between ends, headptr and tailptr in different extents
    extent_to_write_list_t ret;
    if (headptr) {
      bufferlist headbl;
      headbl.append(*headptr);
      headbl.append_zero(zero_left - left - headbl.length());
      assert(headbl.length() % block_size == 0);
      assert(headbl.length() > 0);
      ret.push_back(extent_to_write_t::create_data(left, headbl));
    }
    // reserved zero region
    ret.push_back(extent_to_write_t::create_zero(zero_left, zero_right - zero_left));
    assert(ret.back().len % block_size == 0);
    assert(ret.back().len > 0);
    if (tailptr) {
      bufferlist tailbl;
      tailbl.append(*tailptr);
      tailbl.append_zero(right - zero_right - tailbl.length());
      assert(tailbl.length() % block_size == 0);
      assert(tailbl.length() > 0);
      ret.push_back(extent_to_write_t::create_data(zero_right, tailbl));
    }
    return ret;
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

ObjectDataHandler::write_ret ObjectDataHandler::overwrite(
  context_t ctx,
  laddr_t offset,
  extent_len_t len,
  std::optional<bufferlist> &&bl,
  lba_pin_list_t &&_pins)
{
  if (bl.has_value()) {
    assert(bl->length() == len);
  }
  overwrite_plan_t overwrite_plan(offset, len, _pins, ctx.tm.get_block_size());
  return seastar::do_with(
    std::move(_pins),
    extent_to_write_list_t(),
    [ctx, len, offset, overwrite_plan, bl=std::move(bl), this]
    (auto &pins, auto &to_write) mutable
  {
    LOG_PREFIX(ObjectDataHandler::overwrite);
    DEBUGT("overwrite: {}~{}",
           ctx.t,
           offset,
           len);
    ceph_assert(pins.size() >= 1);
    DEBUGT("overwrite: split overwrite_plan {}", ctx.t, overwrite_plan);

    return operate_left(
      ctx,
      pins.front(),
      overwrite_plan
    ).si_then([ctx, len, offset, overwrite_plan, bl=std::move(bl),
               &to_write, &pins, this](auto p) mutable {
      auto &[left_extent, headptr] = p;
      if (left_extent) {
        ceph_assert(left_extent->addr == overwrite_plan.pin_begin);
        append_extent_to_write(to_write, std::move(*left_extent));
      }
      if (headptr) {
        assert(headptr->length() > 0);
      }
      return operate_right(
        ctx,
        pins.back(),
        overwrite_plan
      ).si_then([ctx, len, offset,
                 pin_begin=overwrite_plan.pin_begin,
                 pin_end=overwrite_plan.pin_end,
                 bl=std::move(bl), headptr=std::move(headptr),
                 &to_write, &pins, this](auto p) mutable {
        auto &[right_extent, tailptr] = p;
        if (bl.has_value()) {
          auto write_offset = offset;
          bufferlist write_bl;
          if (headptr) {
            write_bl.append(*headptr);
            write_offset -= headptr->length();
            assert_aligned(write_offset);
          }
          write_bl.claim_append(*bl);
          if (tailptr) {
            write_bl.append(*tailptr);
            assert_aligned(write_bl.length());
          }
          splice_extent_to_write(
            to_write,
            get_to_writes(write_offset, write_bl));
        } else {
          splice_extent_to_write(
            to_write,
            get_to_writes_with_zero_buffer(
              ctx.tm.get_block_size(),
              offset,
              len,
              std::move(headptr),
              std::move(tailptr)));
        }
        if (right_extent) {
          ceph_assert(right_extent->get_end_addr() == pin_end);
          append_extent_to_write(to_write, std::move(*right_extent));
        }
        assert(to_write.size());
        assert(pin_begin == to_write.front().addr);
        assert(pin_end == to_write.back().get_end_addr());

        return seastar::do_with(
          prepare_ops_list(pins, to_write,
	    delta_based_overwrite_max_extent_size),
          [ctx](auto &ops) {
            return do_remappings(ctx, ops.to_remap
            ).si_then([ctx, &ops] {
              return do_removals(ctx, ops.to_remove);
            }).si_then([ctx, &ops] {
              return do_insertions(ctx, ops.to_insert);
            });
        });
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
      DEBUGT("zero to {}~{}, object_data: {}~{}, is_null {}",
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
      ).si_then([this, ctx, offset, len, &object_data] {
	auto logical_offset = object_data.get_reserved_data_base() + offset;
	return ctx.tm.get_pins(
	  ctx.t,
	  logical_offset,
	  len
	).si_then([this, ctx, logical_offset, len](auto pins) {
	  return overwrite(
	    ctx, logical_offset, len,
	    std::nullopt, std::move(pins));
	});
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
      DEBUGT("writing to {}~{}, object_data: {}~{}, is_null {}",
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
      ).si_then([this, ctx, offset, &object_data, &bl] {
	auto logical_offset = object_data.get_reserved_data_base() + offset;
	return ctx.tm.get_pins(
	  ctx.t,
	  logical_offset,
	  bl.length()
	).si_then([this, ctx,logical_offset, &bl](
		   auto pins) {
	  return overwrite(
	    ctx, logical_offset, bl.length(),
	    bufferlist(bl), std::move(pins));
	});
      });
    });
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
      DEBUGT("reading {}~{}",
             ctx.t,
             object_data.get_reserved_data_base(),
             object_data.get_reserved_data_len());
      /* Assumption: callers ensure that onode size is <= reserved
       * size and that len is adjusted here prior to call */
      ceph_assert(!object_data.is_null());
      ceph_assert((obj_offset + len) <= object_data.get_reserved_data_len());
      ceph_assert(len > 0);
      laddr_t l_start =
        object_data.get_reserved_data_base() + obj_offset;
      return ctx.tm.get_pins(
        ctx.t,
        l_start,
        len
      ).si_then([FNAME, ctx, l_start, len, &ret](auto _pins) {
        // offset~len falls within reserved region and len > 0
        ceph_assert(_pins.size() >= 1);
        ceph_assert((*_pins.begin())->get_key() <= l_start);
        auto l_end = l_start + len;
        return seastar::do_with(
          std::move(_pins),
          l_start,
          [FNAME, ctx, l_start, l_end, &ret](auto &pins, auto &l_current) {
          return trans_intr::do_for_each(
            pins,
            [FNAME, ctx, l_start, l_end,
             &l_current, &ret](auto &pin) -> read_iertr::future<> {
            auto pin_key = pin->get_key();
            if (l_current == l_start) {
              ceph_assert(l_current >= pin_key);
            } else {
              assert(l_current > l_start);
              ceph_assert(l_current == pin_key);
            }
            ceph_assert(l_current < l_end);
            auto pin_len = pin->get_length();
            assert(pin_len > 0);
            laddr_t l_pin_end = pin_key + pin_len;
            ceph_assert(l_current < l_pin_end);
            laddr_t l_current_end = std::min(l_pin_end, l_end);
            if (pin->get_val().is_zero()) {
              DEBUGT("got {}~{} from zero-pin {}~{}",
                ctx.t,
                l_current,
                l_current_end - l_current,
                pin_key,
                pin_len);
              ret.append_zero(l_current_end - l_current);
              l_current = l_current_end;
              return seastar::now();
            }

            // non-zero pin
            bool is_indirect = pin->is_indirect();
            laddr_t e_key;
            extent_len_t e_len;
            extent_len_t e_off;
            if (is_indirect) {
              e_key = pin->get_intermediate_base();
              e_len = pin->get_intermediate_length();
              e_off = pin->get_intermediate_offset();
              DEBUGT("reading {}~{} from indirect-pin {}~{}, direct-pin {}~{}(off={})",
                ctx.t,
                l_current,
                l_current_end - l_current,
                pin_key,
                pin_len,
                e_key,
                e_len,
                e_off);
              assert(e_key <= pin->get_intermediate_key());
              assert(e_off + pin_len <= e_len);
            } else {
              DEBUGT("reading {}~{} from pin {}~{}",
                ctx.t,
                l_current,
                l_current_end - l_current,
                pin_key,
                pin_len);
              e_key = pin_key;
              e_len = pin_len;
              e_off = 0;
            }
            extent_len_t e_current_off = e_off + l_current - pin_key;
            return ctx.tm.read_pin<ObjectDataBlock>(
              ctx.t,
              std::move(pin)
            ).si_then([&ret, &l_current, l_current_end,
#ifndef NDEBUG
                       e_key, e_len, e_current_off](auto extent) {
#else
                       e_current_off](auto extent) {
#endif
              assert(e_key == extent->get_laddr());
              assert(e_len == extent->get_length());
              ret.append(
                bufferptr(
                  extent->get_bptr(),
                  e_current_off,
                  l_current_end - l_current));
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
	"{}~{}, reservation {}~{}",
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
      laddr_t l_start =
        object_data.get_reserved_data_base() + obj_offset;
      return ctx.tm.get_pins(
        ctx.t,
        l_start,
        len
      ).si_then([l_start, len, &object_data, &ret](auto &&pins) {
	ceph_assert(pins.size() >= 1);
        ceph_assert((*pins.begin())->get_key() <= l_start);
	for (auto &&i: pins) {
	  if (!(i->get_val().is_zero())) {
	    auto ret_left = std::max(i->get_key(), l_start);
	    auto ret_right = std::min(
	      i->get_key() + i->get_length(),
	      l_start + len);
	    assert(ret_right > ret_left);
	    ret.emplace(
	      std::make_pair(
		ret_left - object_data.get_reserved_data_base(),
		ret_right - ret_left
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
      DEBUGT("truncating {}~{} offset: {}",
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
	  p2roundup(offset, ctx.tm.get_block_size()));
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
  lba_pin_list_t &pins,
  laddr_t data_base)
{
  LOG_PREFIX(ObjectDataHandler::clone_extents);
  TRACET(" object_data: {}~{}, data_base: {}",
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
	  auto offset = pin->get_key() - data_base;
	  ceph_assert(offset == last_pos);
	  auto fut = TransactionManager::alloc_extent_iertr
	    ::make_ready_future<LBAMappingRef>();
	  auto addr = object_data.get_reserved_data_base() + offset;
	  if (pin->get_val().is_zero()) {
	    fut = ctx.tm.reserve_region(ctx.t, addr, pin->get_length());
	  } else {
	    fut = ctx.tm.clone_pin(ctx.t, addr, *pin);
	  }
	  return fut.si_then(
	    [&pin, &last_pos, offset](auto) {
	    last_pos = offset + pin->get_length();
	    return seastar::now();
	  }).handle_error_interruptible(
	    crimson::ct_error::input_output_error::pass_further(),
	    crimson::ct_error::assert_all("not possible")
	  );
	}).si_then([&last_pos, &object_data, ctx] {
	  if (last_pos != object_data.get_reserved_data_len()) {
	    return ctx.tm.reserve_region(
	      ctx.t,
	      object_data.get_reserved_data_base() + last_pos,
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
    ).si_then([&object_data, &d_object_data, ctx, this] {
      assert(!object_data.is_null());
      auto base = object_data.get_reserved_data_base();
      auto len = object_data.get_reserved_data_len();
      object_data.clear();
      LOG_PREFIX(ObjectDataHandler::clone);
      DEBUGT("cloned obj reserve_data_base: {}, len {}",
	ctx.t,
	d_object_data.get_reserved_data_base(),
	d_object_data.get_reserved_data_len());
      return prepare_data_reservation(
	ctx,
	object_data,
	d_object_data.get_reserved_data_len()
      ).si_then([&d_object_data, ctx, &object_data, base, len, this] {
	LOG_PREFIX("ObjectDataHandler::clone");
	DEBUGT("head obj reserve_data_base: {}, len {}",
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
