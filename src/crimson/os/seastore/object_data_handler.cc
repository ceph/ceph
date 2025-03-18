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

  /// pin of original extent, not nullptr if type == EXISTING
  LBAMapping pin;

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
    assert(!pin.is_null());
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
  LBAMapping pin;
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
    assert((new_offset != 0) && (pin.get_length() != new_offset + new_len));
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
      pin.get_length() - new_offset - new_len);
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
      LBAMapping(), new_offset, new_len, p.get_key(), p.get_length(), b);
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
    LBAMapping &&pin, extent_len_t new_offset, extent_len_t new_len,
    laddr_t ori_laddr, extent_len_t ori_len, std::optional<bufferlist> b)
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
      assert(front.addr == front.pin.get_key());
      assert(back.addr > back.pin.get_key());
      ops.to_remap.push_back(extent_to_remap_t::create_remap2(
	std::move(front.pin),
	front.len,
	back.addr.get_byte_distance<extent_len_t>(front.addr) - front.len));
      ops.to_remove.pop_front();
  } else {
    // prepare to_remap, happens in one or multiple extents
    if (front.is_existing()) {
      visitted++;
      assert(to_write.size() > 1);
      assert(front.addr == front.pin.get_key());
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
	back.pin.get_key() + back.pin.get_length());
      ops.to_remap.push_back(extent_to_remap_t::create_remap1(
	std::move(back.pin),
	back.addr.get_byte_distance<extent_len_t>(back.pin.get_key()),
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
      if (!r.pin.is_null() && r.pin.is_data_stable() && !r.pin.is_zero_reserved()) {
	pre_alloc_addr_remapped.insert(r.pin.get_key(), r.pin.get_length());
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
	    range.insert(r.pin.get_key(), r.pin.get_length());
	    if (range.contains(region.addr, region.len) && !r.pin.is_clone()) {
	      to_remap.push_back(extent_to_remap_t::create_overwrite(
		region.addr.get_byte_distance<
		  extent_len_t> (range.begin().get_start()),
		region.len, std::move(r.pin), *region.to_write));
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
	auto pin_key = region.pin.get_key();
        return ctx.tm.remap_pin<ObjectDataBlock, 2>(
          ctx.t,
          std::move(region.pin),
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

} // namespace crimson::os::seastore

namespace crimson::os::seastore {

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

ObjectDataHandler::clear_ret ObjectDataHandler::trim_data_reservation(
  context_t ctx, object_data_t &object_data, extent_len_t size)
{
  ceph_assert(!object_data.is_null());
  ceph_assert(size <= object_data.get_reserved_data_len());
  return seastar::do_with(
    lba_mapping_list_t(),
    extent_to_write_list_t(),
    [ctx, size, &object_data, this](auto &pins, auto &to_write) {
      LOG_PREFIX(ObjectDataHandler::trim_data_reservation);
      auto data_base = object_data.get_reserved_data_base();
      auto data_len = object_data.get_reserved_data_len();
      DEBUGT("object_data: {}~0x{:x}", ctx.t, data_base, data_len);
      laddr_t aligned_start = (data_base + size).get_aligned_laddr();
      loffset_t aligned_length =
	  data_len - aligned_start.get_byte_distance<loffset_t>(data_base);
      return ctx.tm.get_pins(
	ctx.t, aligned_start, aligned_length
      ).si_then([ctx, size, &pins, &object_data, &to_write](auto _pins) {
	_pins.swap(pins);
	ceph_assert(pins.size());
	if (!size) {
	  // no need to reserve region if we are truncating the object's
	  // size to 0
	  return clear_iertr::now();
	}
	auto &pin = pins.front();
	ceph_assert(pin.get_key() >= object_data.get_reserved_data_base());
	ceph_assert(
	  pin.get_key() <= object_data.get_reserved_data_base() + size);
	auto pin_offset = pin.get_key().template get_byte_distance<extent_len_t>(
	  object_data.get_reserved_data_base());
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
              "create existing at addr:{}, len:0x{:x}",
              ctx.t, pin.get_key(), size - pin_offset);
            to_write.push_back(extent_to_write_t::create_existing(
              pin.duplicate(),
              pin.get_key(),
              size - pin_offset));
	    to_write.push_back(extent_to_write_t::create_zero(
	      (object_data.get_reserved_data_base() + roundup_size).checked_to_laddr(),
	      object_data.get_reserved_data_len() - roundup_size));
            return clear_iertr::now();
          } else {
            return ctx.tm.read_pin<ObjectDataBlock>(
              ctx.t,
              pin.duplicate()
            ).si_then([ctx, size, pin_offset, append_len, roundup_size,
                      &pin, &object_data, &to_write](auto maybe_indirect_extent) {
              auto read_bl = maybe_indirect_extent.get_bl();
              ceph::bufferlist write_bl;
              write_bl.substr_of(read_bl, 0, size - pin_offset);
              write_bl.append_zero(append_len);
              LOG_PREFIX(ObjectDataHandler::trim_data_reservation);
              TRACET("First pin overlaps the boundary and has unaligned data"
                "create data at addr:{}, len:0x{:x}",
                ctx.t, pin.get_key(), write_bl.length());
	      to_write.push_back(extent_to_write_t::create_data(
	        pin.get_key(),
	        write_bl));
	      to_write.push_back(extent_to_write_t::create_zero(
	        (object_data.get_reserved_data_base() + roundup_size).checked_to_laddr(),
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

using remap_ret = ObjectDataHandler::write_iertr::future<std::vector<LBAMapping>>;
template <std::size_t N>
remap_ret remap_mappings(
  context_t ctx,
  LBAMapping mapping,
  std::array<TransactionManager::remap_entry, N> remaps)
{
  if (mapping.get_val().is_zero()) {
    return seastar::do_with(
      std::vector<TransactionManager::remap_entry>(
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

struct overwrite_params_t {
  laddr_t data_base = L_ADDR_NULL;
  objaddr_t offset = 0;
  extent_len_t len = 0;
  laddr_t first_key = L_ADDR_NULL;
  extent_len_t first_len = 0;
  laddr_offset_t raw_begin;
  laddr_t data_begin = L_ADDR_NULL;
  laddr_offset_t raw_end;
  laddr_t data_end = L_ADDR_NULL;
};

using do_mappings_ret = ObjectDataHandler::write_iertr::future<LBAMapping>;

do_mappings_ret maybe_delta_based_overwrite(
  context_t ctx,
  const overwrite_params_t &params,
  LBAMapping mapping,
  bufferlist &bl,
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
  }).si_then([&params, &bl](auto extent) {
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
  bufferlist &client_bl,
  bufferlist &write_bl)
{
  if (!first_mapping.is_stable()) {
    // merge with existing pending extents
    auto length = params.first_key.template get_byte_distance<
      extent_len_t>(params.raw_begin);
    assert(params.offset >= length);
    params.offset -= length;
    params.len += length;
    params.data_begin = params.first_key;
    params.raw_begin = laddr_offset_t{params.first_key};
    return load_padding(ctx, first_mapping.duplicate(), 0, length
    ).si_then([&client_bl, &write_bl, first_mapping=first_mapping.duplicate(),
	      &params, ctx](auto headbl) mutable {
      write_bl.append(headbl);
      write_bl.append(client_bl);
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
		&write_bl, ctx](auto tailbl) mutable {
      write_bl.append(tailbl);
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
  return pad_fut.si_then([&write_bl, first_mapping=std::move(first_mapping),
			  &params, &client_bl, ctx](auto headbl) mutable {
    using remap_entry = TransactionManager::remap_entry;
    if (headbl.length() > 0) {
      write_bl.append(headbl);
    }
    write_bl.append(client_bl);
    if (params.first_key == params.data_begin) {
      return TransactionManager::remap_pin_iertr::make_ready_future<
	LBAMapping>(std::move(first_mapping));
    }

    if (params.first_key + params.first_len > params.data_end) {
      auto pad_fut = ObjectDataHandler::read_iertr::make_ready_future<
	bufferlist>();
      if (params.raw_end != params.data_end) {
	// load the right padding
	pad_fut = load_padding(
	  ctx,
	  first_mapping.duplicate(),
	  params.raw_end.template get_byte_distance<
	    extent_len_t>(first_mapping.get_key()),
	  params.data_end.template get_byte_distance<
	    extent_len_t>(params.raw_end));
      }
      return pad_fut.si_then(
	[&params, first_mapping=std::move(first_mapping),
	&write_bl, ctx](auto tailbl) mutable {
	if (tailbl.length() > 0) {
	  write_bl.append(tailbl);
	}
	return remap_mappings<2>(
	  ctx,
	  std::move(first_mapping),
	  std::array{
	    // from the start of the first_mapping to the offset of overwrite
	    remap_entry{
	      0,
	      params.data_begin.template get_byte_distance<
		extent_len_t>(params.first_key)},
	    // from the end of overwrite to the end of the first mapping
	    remap_entry{
	      params.data_end.template get_byte_distance<
		extent_len_t>(params.first_key),
	      (params.first_key + params.first_len
		).checked_to_laddr().template get_byte_distance<
		  extent_len_t>(params.data_end)}}
	).si_then([](auto mappings) {
	  assert(mappings.size() == 2);
	  return std::move(mappings.back());
	});
      });
    } else {
      return remap_mappings<1>(
	ctx,
	std::move(first_mapping),
	std::array{
	  remap_entry{
	    0,
	    params.data_begin.template get_byte_distance<
	      extent_len_t>(params.first_key)}}
      ).si_then([ctx](auto mappings) {
	assert(mappings.size() == 1);
	return ctx.tm.next_mapping(ctx.t, std::move(mappings.front()));
      });
    }
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
      assert(mapping.get_key() >= params.data_begin);
      auto mapping_end =
	(mapping.get_key() + mapping.get_length()).checked_to_laddr();
      if (mapping_end > params.raw_end) {
	return ObjectDataHandler::base_iertr::make_ready_future<
	  seastar::stop_iteration>(
	    seastar::stop_iteration::yes);
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
  bufferlist &write_bl)
{
  if (!mapping.is_stable()) {
    // merge with existing pending extents
    auto offset = params.raw_end.template get_byte_distance<
      extent_len_t>(mapping.get_key());
    auto first_end = (params.first_key + params.first_len).checked_to_laddr();
    auto len = mapping.get_length() - offset;
    params.data_end = first_end;
    params.raw_end = laddr_offset_t{first_end};
    params.len += len;
    return load_padding(ctx, mapping.duplicate(), offset, len
    ).si_then([&write_bl, mapping=std::move(mapping), ctx](auto tailbl) mutable {
      write_bl.append(tailbl);
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
    &write_bl, ctx, &params](auto tailbl) mutable {
    if (tailbl.length() > 0) {
      write_bl.append(tailbl);
    }
    if (mapping_end > params.data_end) {
      auto laddr = mapping.get_key();
      using remap_entry = TransactionManager::remap_entry;
      return remap_mappings<1>(
	ctx,
	std::move(mapping),
	std::array{
	  remap_entry{
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
    bufferlist{},
    overwrite_params_t{
      data_base,
      offset,
      len,
      first_mapping.get_key(),
      first_mapping.get_length(),
      raw_begin,
      raw_begin.get_aligned_laddr(),
      raw_end,
      raw_end.get_roundup_laddr()},
    std::move(bl),
    [this, ctx, first_mapping=std::move(first_mapping)]
    (auto &_bl, auto &params, auto &bl) mutable {
    return maybe_delta_based_overwrite(
      ctx, params, std::move(first_mapping), *bl,
      delta_based_overwrite_max_extent_size
    ).si_then([ctx, &params, &_bl, &bl](auto mapping) {
      if (mapping.is_null()) {
	// the modified range is within the first mapping
	return write_iertr::now();
      }

      return do_first_mapping(
	ctx, params, std::move(mapping), *bl, _bl
      ).si_then([&params, ctx](auto mapping) {
	return do_middle_mappings(ctx, params, std::move(mapping));
      }).si_then([&params, ctx, &_bl](auto mapping) {
	if (mapping.get_key() >= params.data_end) {
	  return write_iertr::make_ready_future<LBAMapping>(std::move(mapping));
	}
	return do_last_mapping(ctx, params, std::move(mapping), _bl);
      }).si_then([ctx, &params, &_bl](auto mapping) {
	return ctx.tm.alloc_data_extents<ObjectDataBlock>(
	  ctx.t,
	  params.data_begin,
	  params.data_end.template get_byte_distance<
	    extent_len_t>(params.data_begin),
	  std::move(mapping)
	).si_then([&params, &_bl](auto extents) {
	  auto off = params.data_begin;
	  auto left = params.data_begin.template get_byte_distance<
	    extent_len_t>(params.data_end);
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
      ).si_then([this, ctx, offset, len, &object_data](auto) {
	auto data_base = object_data.get_reserved_data_base();
	laddr_offset_t l_start = data_base + offset;
	laddr_offset_t l_end = l_start + len;
	laddr_t aligned_start = l_start.get_aligned_laddr();
	loffset_t aligned_length =
	    l_end.get_roundup_laddr().get_byte_distance<
	      loffset_t>(aligned_start);
	return ctx.tm.get_pins(
	  ctx.t,
	  aligned_start,
	  aligned_length
	).si_then([this, ctx, data_base, offset, len](auto pins) {
	  return overwrite(
	    ctx, data_base, offset, len,
	    std::nullopt, std::move(pins.front()));
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
	  auto fut = TransactionManager::alloc_extent_iertr
	    ::make_ready_future<LBAMapping>();
	  laddr_t addr = (object_data.get_reserved_data_base() + offset)
	      .checked_to_laddr();
	  if (pin.get_val().is_zero()) {
	    fut = ctx.tm.reserve_region(ctx.t, addr, pin.get_length());
	  } else {
	    fut = ctx.tm.clone_pin(ctx.t, addr, pin);
	  }
	  return fut.si_then(
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
