// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <utility>
#include <functional>

#include "crimson/common/log.h"

#include "crimson/os/seastore/object_data_handler.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore {

/**
 * MAX_OBJECT_SIZE
 *
 * For now, we allocate a fixed region of laddr space of size MAX_OBJECT_SIZE
 * for any object.  In the future, once we have the ability to remap logical
 * mappings (necessary for clone), we'll add the ability to grow and shrink
 * these regions and remove this assumption.
 */
static constexpr extent_len_t MAX_OBJECT_SIZE = 16<<20;
#define assert_aligned(x) ceph_assert(((x)%ctx.tm.get_block_size()) == 0)

using context_t = ObjectDataHandler::context_t;
using get_ertr = ObjectDataHandler::write_ertr;

auto read_pin(
  context_t ctx,
  LBAPinRef pin) {
  return ctx.tm.pin_to_extent<ObjectDataBlock>(
    ctx.t,
    std::move(pin)
  ).handle_error(
    get_ertr::pass_further{},
    crimson::ct_error::assert_all{ "read_pin: invalid error" }
  );
}

/**
 * extent_to_write_t
 *
 * Encapsulates extents to be written out using do_insertions.
 * Indicates a zero extent or a data extent based on whether
 * to_write is populate.
 */
struct extent_to_write_t {
  laddr_t addr = L_ADDR_NULL;
  extent_len_t len;
  std::optional<bufferlist> to_write;

  extent_to_write_t() = default;
  extent_to_write_t(const extent_to_write_t &) = default;
  extent_to_write_t(extent_to_write_t &&) = default;

  extent_to_write_t(laddr_t addr, bufferlist to_write)
    : addr(addr), len(to_write.length()), to_write(to_write) {}

  extent_to_write_t(laddr_t addr, extent_len_t len)
    : addr(addr), len(len) {}
};
using extent_to_write_list_t = std::list<extent_to_write_t>;

/// Removes extents/mappings in pins
ObjectDataHandler::write_ret do_removals(
  context_t ctx,
  lba_pin_list_t &pins)
{
  return crimson::do_for_each(
    pins.begin(),
    pins.end(),
    [ctx](auto &pin) {
      return ctx.tm.dec_ref(
	ctx.t,
	pin->get_laddr()
      ).safe_then(
	[](auto){},
	ObjectDataHandler::write_ertr::pass_further{},
	crimson::ct_error::assert_all{
	  "object_data_handler::do_removals invalid error"
	}
      );
    });
}

/// Creates zero/data extents in to_write
ObjectDataHandler::write_ret do_insertions(
  context_t ctx,
  extent_to_write_list_t &to_write)
{
  return crimson::do_for_each(
    to_write.begin(),
    to_write.end(),
    [ctx](auto &region) {
      if (region.to_write) {
	assert_aligned(region.addr);
	assert_aligned(region.len);
	ceph_assert(region.len == region.to_write->length());
	return ctx.tm.alloc_extent<ObjectDataBlock>(
	  ctx.t,
	  region.addr,
	  region.len
	).safe_then([&region](auto extent) {
	  if (extent->get_laddr() != region.addr) {
	    logger().debug(
	      "object_data_handler::do_insertions alloc got addr {},"
	      " should have been {}",
	      extent->get_laddr(),
	      region.addr);
	  }
	  ceph_assert(extent->get_laddr() == region.addr);
	  ceph_assert(extent->get_length() == region.len);
	  auto iter = region.to_write->cbegin();
	  iter.copy(region.len, extent->get_bptr().c_str());
	  return ObjectDataHandler::write_ertr::now();
	});
      } else {
	return ctx.tm.reserve_region(
	  ctx.t,
	  region.addr,
	  region.len
	).safe_then([&region](auto pin) {
	  ceph_assert(pin->get_length() == region.len);
	  ceph_assert(pin->get_laddr() == region.addr);
	  return ObjectDataHandler::write_ertr::now();
	});
      }
    });
}

/**
 * split_pin_left
 *
 * Splits the passed pin returning aligned extent to be rewritten
 * to the left (if a zero extent), tail to be prepended to write
 * beginning at offset.  See below for details.
 */
using split_ret_bare = std::pair<
  std::optional<extent_to_write_t>,
  std::optional<bufferptr>>;
using split_ret = get_ertr::future<split_ret_bare>;
split_ret split_pin_left(context_t ctx, LBAPinRef &pin, laddr_t offset)
{
  const auto pin_offset = pin->get_laddr();
  assert_aligned(pin_offset);
  ceph_assert(offset >= pin_offset);
  if (offset == pin_offset) {
    // Aligned, no tail and no extra extent
    return get_ertr::make_ready_future<split_ret_bare>(
      std::nullopt,
      std::nullopt);
  } else if (pin->get_paddr().is_zero()) {
    /* Zero extent unaligned, return largest aligned zero extent to
     * the left and the gap between aligned_offset and offset to prepend. */
    auto aligned_offset = p2align(offset, (uint64_t)ctx.tm.get_block_size());
    assert_aligned(aligned_offset);
    ceph_assert(aligned_offset <= offset);
    auto zero_extent_len = aligned_offset - pin_offset;
    assert_aligned(zero_extent_len);
    auto zero_prepend_len = offset - aligned_offset;
    return get_ertr::make_ready_future<split_ret_bare>(
      (zero_extent_len == 0
       ? std::nullopt
       : std::make_optional(extent_to_write_t(pin_offset, zero_extent_len))),
      bufferptr(ceph::buffer::create(zero_prepend_len, 0))
    );
  } else {
    // Data, return up to offset to prepend
    auto to_prepend = offset - pin->get_laddr();
    return read_pin(ctx, pin->duplicate()
    ).safe_then([to_prepend](auto extent) {
      return get_ertr::make_ready_future<split_ret_bare>(
	std::nullopt,
	bufferptr(extent->get_bptr(), 0, to_prepend));
    });
  }
};

/// Reverse of split_pin_left
split_ret split_pin_right(context_t ctx, LBAPinRef &pin, laddr_t end)
{
  const auto pin_begin = pin->get_laddr();
  const auto pin_end = pin->get_laddr() + pin->get_length();
  assert_aligned(pin_end);
  ceph_assert(pin_end >= end);
  if (end == pin_end) {
    return get_ertr::make_ready_future<split_ret_bare>(
      std::nullopt,
      std::nullopt);
  } else if (pin->get_paddr().is_zero()) {
    auto aligned_end = p2roundup(end, (uint64_t)ctx.tm.get_block_size());
    assert_aligned(aligned_end);
    ceph_assert(aligned_end >= end);
    auto zero_suffix_len = aligned_end - end;
    auto zero_extent_len = pin_end - aligned_end;
    assert_aligned(zero_extent_len);
    return get_ertr::make_ready_future<split_ret_bare>(
      (zero_extent_len == 0
       ? std::nullopt
       : std::make_optional(extent_to_write_t(aligned_end, zero_extent_len))),
      bufferptr(ceph::buffer::create(zero_suffix_len, 0))
    );
  } else {
    return read_pin(ctx, pin->duplicate()
    ).safe_then([end, pin_begin, pin_end](auto extent) {
      return get_ertr::make_ready_future<split_ret_bare>(
	std::nullopt,
	bufferptr(
	  extent->get_bptr(),
	  end - pin_begin,
	  pin_end - end));
    });
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
      ).safe_then([ctx, &object_data] {
	if (object_data.must_update()) {
	  ctx.onode.get_mutable_layout(ctx.t).object_data.update(object_data);
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
  ceph_assert(size <= MAX_OBJECT_SIZE);
  if (!object_data.is_null()) {
    ceph_assert(object_data.get_reserved_data_len() == MAX_OBJECT_SIZE);
    return write_ertr::now();
  } else {
    return ctx.tm.reserve_region(
      ctx.t,
      0 /* TODO -- pass hint based on object hash */,
      MAX_OBJECT_SIZE
    ).safe_then([&object_data](auto pin) {
      ceph_assert(pin->get_length() == MAX_OBJECT_SIZE);
      object_data.update_reserved(
	pin->get_laddr(),
	pin->get_length());
      return write_ertr::now();
    });
  }
}

ObjectDataHandler::clear_ret ObjectDataHandler::trim_data_reservation(
  context_t ctx, object_data_t &object_data, extent_len_t size)
{
  ceph_assert(!object_data.is_null());
  assert_aligned(size);
  ceph_assert(size <= object_data.get_reserved_data_len());
  return seastar::do_with(
    lba_pin_list_t(),
    extent_to_write_list_t(),
    [ctx, size, &object_data](auto &pins, auto &to_write) {
      return ctx.tm.get_pins(
	ctx.t,
	object_data.get_reserved_data_base() + size,
	object_data.get_reserved_data_len() - size
      ).safe_then([ctx, size, &pins, &object_data, &to_write](auto _pins) {
	_pins.swap(pins);
	ceph_assert(pins.size());
	auto &pin = *pins.front();
	ceph_assert(pin.get_laddr() >= object_data.get_reserved_data_base());
	ceph_assert(
	  pin.get_laddr() <= object_data.get_reserved_data_base() + size);
	auto pin_offset = pin.get_laddr() -
	  object_data.get_reserved_data_base();
	if (pin.get_paddr().is_zero()) {
	  to_write.emplace_back(
	    pin.get_laddr(),
	    object_data.get_reserved_data_len() - pin_offset);
	  return clear_ertr::now();
	} else {
	  return read_pin(
	    ctx,
	    pin.duplicate()
	  ).safe_then([size, pin_offset, &pin, &object_data, &to_write](
			auto extent) {
	    bufferlist bl;
	    bl.append(
	      bufferptr(
		extent->get_bptr(),
		0,
		size - pin_offset
	      ));
	    to_write.emplace_back(
	      pin.get_laddr(),
	      bl);
	    to_write.emplace_back(
	      object_data.get_reserved_data_base() + size,
	      object_data.get_reserved_data_len() - size);
	    return clear_ertr::now();
	  });
	}
      }).safe_then([ctx, &pins] {
	return do_removals(ctx, pins);
      }).safe_then([ctx, &to_write] {
	return do_insertions(ctx, to_write);
      }).safe_then([size, &object_data] {
	if (size == 0) {
	  object_data.clear();
	}
	return ObjectDataHandler::clear_ertr::now();
      });
    });
}

/**
 * get_buffers
 *
 * Returns extent_to_write_t's from bl.
 *
 * TODO: probably add some kind of upper limit on extent size.
 */
extent_to_write_list_t get_buffers(laddr_t offset, bufferlist &bl)
{
  auto ret = extent_to_write_list_t();
  ret.emplace_back(offset, bl);
  return ret;
};

ObjectDataHandler::write_ret ObjectDataHandler::overwrite(
  context_t ctx,
  laddr_t _offset,
  bufferlist &&bl,
  lba_pin_list_t &&_pins)
{
  return seastar::do_with(
    _offset,
    std::move(bl),
    std::move(_pins),
    extent_to_write_list_t(),
    [ctx](laddr_t &offset, auto &bl, auto &pins, auto &to_write) {
      ceph_assert(pins.size() >= 1);
      auto pin_begin = pins.front()->get_laddr();
      ceph_assert(pin_begin <= offset);
      auto pin_end = pins.back()->get_laddr() + pins.back()->get_length();
      ceph_assert(pin_end >= (offset + bl.length()));

      return split_pin_left(
	ctx,
	pins.front(),
	offset
      ).safe_then([ctx, pin_begin, &offset, &bl, &pins, &to_write](
		    auto p) {
	auto &[left_extent, headptr] = p;
	if (left_extent) {
	  ceph_assert(left_extent->addr == pin_begin);
	  to_write.push_front(std::move(*left_extent));
	}
	if (headptr) {
	  bufferlist newbl;
	  newbl.append(*headptr);
	  newbl.append(bl);
	  bl.swap(newbl);
	  offset -= headptr->length();
	  assert_aligned(offset);
	}
	return split_pin_right(
	  ctx,
	  pins.back(),
	  offset + bl.length());
      }).safe_then([ctx, pin_end, &offset, &bl, &to_write](
		     auto p) {
	auto &[right_extent, tailptr] = p;
	if (tailptr) {
	  bl.append(*tailptr);
	  assert_aligned(bl.length());
	}
	to_write.splice(to_write.end(), get_buffers(offset, bl));
	if (right_extent) {
	  ceph_assert((right_extent->addr  + right_extent->len) == pin_end);
	  to_write.push_back(std::move(*right_extent));
	}
	return write_ertr::now();
      }).safe_then([ctx, &pins] {
	return do_removals(ctx, pins);
      }).safe_then([ctx, &to_write] {
	return do_insertions(ctx, to_write);
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
      return prepare_data_reservation(
	ctx,
	object_data,
	p2roundup(offset + bl.length(), ctx.tm.get_block_size())
      ).safe_then([this, ctx, offset, &object_data, &bl] {
	auto logical_offset = object_data.get_reserved_data_base() + offset;
	return ctx.tm.get_pins(
	  ctx.t,
	  logical_offset,
	  bl.length()
	).safe_then([this, ctx,logical_offset, &bl](
		      auto pins) {
	  return overwrite(ctx, logical_offset, bufferlist(bl), std::move(pins));
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
	  /* Assumption: callers ensure that onode size is <= reserved
	   * size and that len is adjusted here prior to call */
	  ceph_assert(!object_data.is_null());
	  ceph_assert((obj_offset + len) <= object_data.get_reserved_data_len());
	  ceph_assert(len > 0);
	  laddr_t loffset =
	    object_data.get_reserved_data_base() + obj_offset;
	  return ctx.tm.get_pins(
	    ctx.t,
	    loffset,
	    len
	  ).safe_then([ctx, loffset, len, &ret](auto _pins) {
	    // offset~len falls within reserved region and len > 0
	    ceph_assert(_pins.size() >= 1);
	    ceph_assert((*_pins.begin())->get_laddr() <= loffset);
	    return seastar::do_with(
	      std::move(_pins),
	      loffset,
	      [ctx, loffset, len, &ret](auto &pins, auto &current) {
		return crimson::do_for_each(
		  std::begin(pins),
		  std::end(pins),
		  [ctx, loffset, len, &current, &ret](auto &pin)
		  -> read_ertr::future<> {
		    ceph_assert(current <= (loffset + len));
		    ceph_assert(
		      (loffset + len) > pin->get_laddr());
		    laddr_t end = std::min(
		      pin->get_laddr() + pin->get_length(),
		      loffset + len);
		    if (pin->get_paddr().is_zero()) {
		      ceph_assert(end > current); // See LBAManager::get_mappings
		      ret.append_zero(end - current);
		      current = end;
		      return seastar::now();
		    } else {
		      return ctx.tm.pin_to_extent<ObjectDataBlock>(
			ctx.t,
			std::move(pin)
		      ).safe_then([&ret, &current, end](auto extent) {
			ceph_assert(
			  (extent->get_laddr() + extent->get_length()) >= end);
			ceph_assert(end > current);
			ret.append(
			  bufferptr(
			    extent->get_bptr(),
			    current - extent->get_laddr(),
			    end - current));
			current = end;
			return seastar::now();
		      }).handle_error(
			read_ertr::pass_further{},
			crimson::ct_error::assert_all{
			  "ObjectDataHandler::read hit invalid error"
			}
		      );
		    }
		  });
	      });
	  });
	}).safe_then([&ret] {
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
      if (offset < object_data.get_reserved_data_len()) {
	return trim_data_reservation(ctx, object_data, offset);
      } else if (offset > object_data.get_reserved_data_len()) {
	return prepare_data_reservation(
	  ctx,
	  object_data,
	  offset);
      } else {
	return truncate_ertr::now();
      }
    });
}

ObjectDataHandler::clear_ret ObjectDataHandler::clear(
  context_t ctx)
{
  return with_object_data(
    ctx,
    [this, ctx](auto &object_data) {
      return trim_data_reservation(ctx, object_data, 0);
    });
}

}
