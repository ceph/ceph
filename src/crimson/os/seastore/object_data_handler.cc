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

ObjectDataHandler::write_iertr::future<LBAMapping>
ObjectDataHandler::prepare_shared_region(
  context_t ctx,
  Onode &onode,
  laddr_hint_t hint,
  extent_len_t len)
{
  LOG_PREFIX(ObjectDataHandler::prepare_shared_region);
  DEBUGT("{}~{}", ctx.t, hint, len);
  assert(!onode.get_shared_clone_id());
  return ctx.tm.reserve_region(ctx.t, hint, len
  ).si_then([ctx, &onode](auto shared_region) {
    onode.update_shared_clone_id(
      ctx.t, shared_region.get_key().get_local_clone_id());
    return std::move(shared_region);
  }).handle_error_interruptible(
    write_iertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected error"}
  );
}

ObjectDataHandler::read_ret load_padding(
  ObjectDataHandler::context_t ctx,
  LBAMapping mapping,
  extent_len_t offset,
  extent_len_t len)
{
  if (len == 0) {
    return seastar::make_ready_future<bufferlist>();
  }
  if (mapping.get_val().is_zero()) {
    bufferlist bl;
    bl.append_zero(len);
    return ObjectDataHandler::read_iertr::make_ready_future<
      bufferlist>(std::move(bl));
  } else {
    return ctx.tm.read_pin<ObjectDataBlock>(ctx.t, mapping
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
  objaddr_t offset = 0;
  extent_len_t len = 0;
  laddr_t first_key = L_ADDR_NULL;
  extent_len_t first_len = 0;
  laddr_offset_t raw_begin;
  laddr_t data_begin = L_ADDR_NULL;
  laddr_offset_t raw_end;
  laddr_t data_end = L_ADDR_NULL;

  TransactionManager::punch_hole_params_t
  get_punch_hole_params() const {
    return {raw_begin, raw_end};
  }
};
std::ostream& operator<<(std::ostream &out, const overwrite_params_t &params) {
  return out << "overwrite_params_t{"
    << "offset=" << params.offset
    << ", len=" << params.len
    << ", first_key=" << params.first_key
    << ", first_len=" << params.first_len
    << ", raw_begin=" << params.raw_begin
    << ", data_begin=" << params.data_begin
    << ", raw_end=" << params.raw_end
    << ", data_end=" << params.data_end
    << "}";
}

struct data_t {
  std::optional<bufferlist> headbl;
  std::optional<bufferlist> bl;
  std::optional<bufferlist> tailbl;
};
std::ostream& operator<<(std::ostream &out, const data_t &data) {
  return out << "data_t{"
    << "headbl=" << (data.headbl ? data.headbl->length() : 0)
    << ", bl=" << (data.bl ? data.bl->length() : 0)
    << ", tailbl=" << (data.tailbl ? data.tailbl->length() : 0)
    << "}";
}

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
      laddr_hint_t::create_as_fixed(
	(params.data_end - ctx.tm.get_block_size()).checked_to_laddr()),
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
	laddr_hint_t::create_as_fixed(params.data_begin),
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
    laddr_hint_t::create_as_fixed(params.data_begin),
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

ObjectDataHandler::read_iertr::future<>
on_unaligned_edge(
  context_t ctx,
  const overwrite_params_t &params,
  data_t &data,
  LBAMapping &mapping,
  bool is_beginning)
{
  extent_len_t off = 0;
  extent_len_t length = 0;
  if (is_beginning) {
    off = mapping.get_key().template get_byte_distance<
      extent_len_t>(params.data_begin),
    length = params.raw_begin.get_offset();
  } else {
    off = params.raw_end.template get_byte_distance<
      extent_len_t>(mapping.get_key()),
    length = params.data_end.template get_byte_distance<
      extent_len_t>(params.raw_end);
  }
  return load_padding(ctx, mapping, off, length
  ).si_then([is_beginning, &data](auto bl) {
    if (is_beginning) {
      data.headbl = std::move(bl);
    } else {
      data.tailbl = std::move(bl);
    }
  });
}

ObjectDataHandler::read_iertr::future<>
on_merge(
  context_t ctx,
  overwrite_params_t &params,
  data_t &data,
  LBAMapping &mapping,
  bool merge_left)
{
  if (merge_left) {
    auto length = params.first_key.template get_byte_distance<
      extent_len_t>(params.raw_begin);
    assert(params.offset >= length);
    params.offset -= length;
    params.len += length;
    params.data_begin = params.first_key;
    params.raw_begin = laddr_offset_t{params.first_key};
    return load_padding(ctx, mapping, 0, length
    ).si_then([&data](auto bl) {
      data.headbl = std::move(bl);
    });
  } else {
    auto offset = params.raw_end.template get_byte_distance<
      extent_len_t>(mapping.get_key());
    auto end = (mapping.get_key() + mapping.get_length()).checked_to_laddr();
    auto len = mapping.get_length() - offset;
    params.data_end = end;
    params.raw_end = laddr_offset_t{end};
    params.len += len;
    return load_padding(ctx, mapping, offset, len
    ).si_then([&data](auto bl) {
      data.tailbl = std::move(bl);
    });
  }
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
  auto first_key = first_mapping.get_key();
  auto first_len = first_mapping.get_length();
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
      first_key,
      first_len,
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
      return ctx.tm.punch_hole<ObjectDataBlock>(
	ctx.t, params.get_punch_hole_params(), std::move(mapping),
	[&params, ctx, &data](LBAMapping &mapping, bool is_beginning) {
	  return on_unaligned_edge(ctx, params, data, mapping, is_beginning);
	},
	[&params, ctx, &data](LBAMapping &mapping, bool merge_left) {
	  return on_merge(ctx, params, data, mapping, merge_left);
	}
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

ObjectDataHandler::touch_ret
ObjectDataHandler::touch(context_t ctx)
{
  return with_object_data(ctx, [this, ctx](auto &obj_data) {
    return prepare_data_reservation(
      ctx, ctx.onode, obj_data, max_object_size
    ).discard_result();
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
    auto mapping_key = mapping.get_key();
    auto mapping_len = mapping.get_length();
    return seastar::do_with(
      data_t{},
      overwrite_params_t{
	size,
	object_data.get_reserved_data_len(),
	mapping_key,
	mapping_len,
	raw_begin,
	key,
	raw_end,
	raw_end.get_roundup_laddr()},
      [ctx, mapping=std::move(mapping)]
      (auto &data, auto &params) mutable {
      return ctx.tm.punch_hole<ObjectDataBlock>(
	ctx.t, params.get_punch_hole_params(), std::move(mapping),
	[&params, ctx, &data](LBAMapping &mapping, bool is_beginning) {
	  return on_unaligned_edge(ctx, params, data, mapping, is_beginning);
	},
	[&params, ctx, &data](LBAMapping &mapping, bool merge_left) {
	  return on_merge(ctx, params, data, mapping, merge_left);
	}
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

ObjectDataHandler::clone_ret do_clone_range(
  context_t ctx,
  const laddr_t &src_base,
  const laddr_t &dest_base,
  extent_len_t offset,
  extent_len_t len,
  LBAMapping &src_first_mapping,
  LBAMapping &dest_first_mapping,
  bufferlist &head_padding,
  bufferlist &tail_padding)
{
  LOG_PREFIX(ObjectDataHandler::do_clone_range);
  DEBUGT(
    "src_base: {}, dest_base: {}, {}~{}, "
    "src mapping: {}, dest mapping: {}",
    ctx.t, src_base, dest_base, offset, len,
    src_first_mapping, dest_first_mapping);
  auto mapping_key = dest_first_mapping.get_key();
  auto mapping_len = dest_first_mapping.get_length();
  auto raw_begin = dest_base + offset;
  auto raw_end = raw_begin + len;
  return seastar::do_with(
    data_t{},
    overwrite_params_t{
      offset,
      len,
      mapping_key,
      mapping_len,
      raw_begin,
      raw_begin.get_aligned_laddr(),
      raw_end,
      raw_end.get_roundup_laddr()},
    [&head_padding, &tail_padding, ctx, &src_first_mapping, dest_base,
    &dest_first_mapping, src_base](auto &data, auto &params) {
    return ctx.tm.punch_hole<ObjectDataBlock>(
      ctx.t, params.get_punch_hole_params(), std::move(dest_first_mapping),
      [&params, ctx, &data](LBAMapping &mapping, bool is_beginning) {
	return on_unaligned_edge(ctx, params, data, mapping, is_beginning);
      },
      [&params, ctx, &data](LBAMapping &mapping, bool merge_left) {
	return on_merge(ctx, params, data, mapping, merge_left);
      }
    ).si_then([&params, ctx, &data, &head_padding, src_base, dest_base,
	      &tail_padding, &src_first_mapping](auto mapping) {
      assert(mapping.is_end() || mapping.get_key() >= params.data_end);
      assert((bool)data.headbl == (head_padding.length() != 0));
      assert((bool)data.tailbl == (tail_padding.length() != 0));
      auto fut = TransactionManager::get_pin_iertr::make_ready_future<
	LBAMapping>(mapping);
      if (data.headbl) {
	data.headbl->append(head_padding);
	fut = ctx.tm.alloc_data_extents<ObjectDataBlock>(
	  ctx.t,
	  laddr_hint_t::create_as_fixed(params.data_begin),
	  ctx.tm.get_block_size(),
	  std::move(mapping)
	).si_then([ctx, &data, &params](auto extents) {
	  assert(extents.size() == 1);
	  auto &extent = extents.back();
	  assert(params.data_begin == extent->get_laddr());
	  auto iter = data.headbl->cbegin();
	  iter.copy(extent->get_length(), extent->get_bptr().c_str());
	  return ctx.tm.get_pin(ctx.t, *extent
	  ).si_then([ctx](auto mapping) {
	    return ctx.tm.next_mapping(ctx.t, std::move(mapping));
	  });
	}).handle_error_interruptible(
	  crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	  TransactionManager::get_pin_iertr::pass_further{}
	);
      }
      fut = fut.si_then([ctx, src_base, dest_base, &params,
			&src_first_mapping](auto pos) {
	return ctx.tm.clone_mappings(
	  ctx.t, src_base, dest_base, params.offset,
	  params.len, std::move(pos), std::move(src_first_mapping));
      });
      if (data.tailbl) {
	tail_padding.append(*data.tailbl);
	data.tailbl = std::move(tail_padding);
	fut = fut.si_then([ctx, &params](auto pos) {
	  return ctx.tm.alloc_data_extents<ObjectDataBlock>(
	    ctx.t,
	    laddr_hint_t::create_as_fixed(
	      (params.data_end - ctx.tm.get_block_size()).checked_to_laddr()),
	    ctx.tm.get_block_size(),
	    std::move(pos));
	}).si_then([&data, &params, ctx](auto extents) {
	  assert(extents.size() == 1);
	  auto &extent = extents.back();
	  assert((params.data_end - ctx.tm.get_block_size()
	    ).checked_to_laddr() == extent->get_laddr());
	  auto iter = data.tailbl->cbegin();
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
    });
  });
}

// _clone_range first move the src mappings to the direct range,
// and then clone the moved mappings into the dest range
ObjectDataHandler::clone_ret _clone_range(
  context_t ctx,
  laddr_t &src_base,
  laddr_t &dest_base,
  laddr_t &direct_base,
  extent_len_t offset,
  extent_len_t len,
  LBAMapping src_first_mapping,
  LBAMapping dest_first_mapping,
  LBAMapping first_direct_mapping)
{
  LOG_PREFIX(ObjectDataHandler::_clone_range);
  DEBUGT(
    "src_base={}, dest_base={}, direct_base={}, "
    "{}~{}, sfmapping={}, dfmapping={}, fdmapping={}",
    ctx.t, src_base, dest_base, direct_base,
    offset, len, src_first_mapping, dest_first_mapping,
    first_direct_mapping);
  struct cr_state_t {
    LBAMapping sfmapping;
    LBAMapping dfmapping;
    LBAMapping fdmapping;
    bufferlist head_padding;
    bufferlist tail_padding;
  };
  return seastar::do_with(
    cr_state_t{
      std::move(src_first_mapping),
      std::move(dest_first_mapping),
      std::move(first_direct_mapping),
      bufferlist{},
      bufferlist{}},
    [ctx, src_base, dest_base, direct_base, offset, len](auto &state) {
    // load the unaligned head padding if offset is not 4K aligned
    return load_padding(
      ctx,
      state.sfmapping,
      (src_base + offset).get_byte_distance<
	extent_len_t>(state.sfmapping.get_key()),
      p2roundup(offset, ctx.tm.get_block_size()) - offset
    ).si_then([ctx, src_base, offset, len, &state, direct_base](auto bl) {
      state.head_padding = std::move(bl);
      auto src_key = state.sfmapping.get_key();
      auto src_physical = !state.sfmapping.is_indirect()
	&& !state.sfmapping.is_zero_reserved();
      return ctx.tm.move_and_clone_direct_mappings<ObjectDataBlock>(
	ctx.t,
	{(src_base + offset).get_roundup_laddr(),
	 (src_base + offset + len).get_aligned_laddr()},
	{(direct_base + offset).get_roundup_laddr(),
	 (direct_base + offset + len).get_aligned_laddr()},
	state.sfmapping,
	state.fdmapping
      ).si_then([offset, len, ctx, &state](auto last_mapping) {
	if (auto padding_len = offset + len - p2align(
	      offset + len, ctx.tm.get_block_size());
	    padding_len > 0) {
	  // load the unaligned tail padding if offset + len is not 4K aligned
	  return load_padding(ctx, std::move(last_mapping), 0, padding_len
	  ).si_then([&state](auto bl) {
	    state.tail_padding = std::move(bl);
	  });
	}
	return ObjectDataHandler::read_iertr::now();
      }).si_then([src_base, offset, src_key, &state, ctx, src_physical] {
	if (src_base + offset > src_key && src_physical) {
	  // sfmapping must have been remapped and the current sfmapping must
	  // be out of the moved range, push sfmapping one step further. The
	  // next mapping must be the first mapping of the moved range.
	  return ctx.tm.next_mapping(ctx.t, std::move(state.sfmapping)
	  ).si_then([&state](auto mapping) {
	    state.sfmapping = std::move(mapping);
	  });
	}
	return ObjectDataHandler::base_iertr::now();
      });
    }).si_then([ctx, src_base, dest_base, offset, len, &state] {
      return do_clone_range(
	ctx, src_base, dest_base, offset, len, state.sfmapping,
	state.dfmapping, state.head_padding, state.tail_padding);
    });
  });
}

ObjectDataHandler::clone_ret ObjectDataHandler::do_rollback(
  context_t ctx,
  object_data_t &object_data,
  object_data_t &d_object_data)
{
  assert(ctx.onode.is_clone());
  return prepare_data_reservation(
    ctx, d_object_data,
    object_data.get_reserved_data_len()
  ).si_then([ctx, &object_data, &d_object_data](auto d_mapping) {
    return ctx.tm.remove(ctx.t, std::move(d_mapping)
    ).si_then([ctx, &object_data, &d_object_data](auto pos) {
      return ctx.tm.get_pin(ctx.t, object_data.get_reserved_data_base()
      ).si_then([ctx, pos=std::move(pos), &object_data,
		&d_object_data](auto mapping) mutable {
	auto len = d_object_data.get_reserved_data_len();
	auto src_base = object_data.get_reserved_data_base();
	auto dst_base = d_object_data.get_reserved_data_base();
	return ctx.tm.clone_mappings(
	  ctx.t, src_base, dst_base, 0, len, std::move(pos),
	  std::move(mapping));
      }).discard_result();
    }).handle_error_interruptible(
      clone_iertr::pass_further{},
      crimson::ct_error::assert_all{"unexpected enoent"}
    );
  });
}

ObjectDataHandler::clone_ret ObjectDataHandler::do_clone(
  context_t ctx,
  object_data_t &object_data,
  object_data_t &d_object_data)
{
  struct state_t {
    LBAMapping src_first_mapping;
    LBAMapping dest_first_mapping;
    LBAMapping first_direct_mapping;
    laddr_t src_base = L_ADDR_NULL;
    laddr_t dest_base = L_ADDR_NULL;
    laddr_t direct_base = L_ADDR_NULL;
  };
  return prepare_data_reservation(
    ctx, d_object_data, object_data.get_reserved_data_len()
  ).si_then([ctx, &object_data](auto) {
    return ctx.tm.reserve_region(
      ctx.t,
      ctx.d_onode->get_data_hint(),
      object_data.get_reserved_data_len()
    ).handle_error_interruptible(
      write_iertr::pass_further{},
      crimson::ct_error::assert_all{"unexpected error"}
    );
  }).si_then([ctx, &object_data, &d_object_data](auto shared_region) {
    return seastar::do_with(
      state_t{
	LBAMapping{},
	LBAMapping{},
	LBAMapping{},
	object_data.get_reserved_data_base(),
	d_object_data.get_reserved_data_base(),
	shared_region.get_key()},
      [ctx, &object_data](auto &state) {
      return ctx.tm.get_pin(ctx.t, state.src_base, false
      ).si_then([&state, ctx](auto mapping) {
	state.src_first_mapping = std::move(mapping);
	return ctx.tm.get_pin(ctx.t, state.dest_base, false);
      }).si_then([&state, ctx](auto mapping) {
	state.dest_first_mapping = std::move(mapping);
	return ctx.tm.get_pin(ctx.t, state.direct_base, false);
      }).si_then([&state, ctx, &object_data](auto mapping) {
	state.first_direct_mapping = std::move(mapping);
	return _clone_range(
	  ctx, state.src_base, state.dest_base, state.direct_base,
	  0, object_data.get_reserved_data_len(),
	  std::move(state.src_first_mapping),
	  std::move(state.dest_first_mapping),
	  std::move(state.first_direct_mapping));
      });
    }).handle_error_interruptible(
      clone_iertr::pass_further{},
      crimson::ct_error::assert_all{"unexpected enoent"}
    );
  });
}
ObjectDataHandler::clone_ret ObjectDataHandler::clone_range(
  context_t ctx,
  extent_len_t srcoff,
  extent_len_t len,
  extent_len_t destoff)
{
  assert(ctx.d_onode);
  assert(srcoff == destoff);
  LOG_PREFIX(ObjectDataHandler::clone_range);
  DEBUGT("{}=>{}, {}~{}",
    ctx.t, ctx.onode.get_hobj(),
    ctx.d_onode->get_hobj(), srcoff, len);
  return with_objects_data(
    ctx,
    [ctx, this, srcoff, len](auto &object_data, auto &d_object_data) {
    struct state_t {
      LBAMapping src_first_mapping;
      LBAMapping dest_first_mapping;
      LBAMapping first_direct_mapping;
      laddr_t src_base = L_ADDR_NULL;
      laddr_t dest_base = L_ADDR_NULL;
      laddr_t direct_base = L_ADDR_NULL;
    };
    ceph_assert(!object_data.is_null());
    return prepare_data_reservation(
      ctx, d_object_data, object_data.get_reserved_data_len()
    ).si_then([this, ctx, &object_data](auto) {
      if (ctx.onode.get_shared_clone_id()) {
	return write_iertr::make_ready_future<LBAMapping>();
      }
      return prepare_shared_region(
	ctx, ctx.onode,
	ctx.onode.get_data_clone_hint(),
	object_data.get_reserved_data_len());
    }).si_then([ctx, &object_data, &d_object_data, srcoff, len](auto) {
      auto base = object_data.get_reserved_data_base();
      return seastar::do_with(
	state_t {
	  LBAMapping{},
	  LBAMapping{},
	  LBAMapping{},
	  base,
	  d_object_data.get_reserved_data_base(),
	  base.with_local_clone_id(*ctx.onode.get_shared_clone_id())},
	[ctx, srcoff, len](auto &state) {
	auto laddr = (state.src_base + srcoff).get_aligned_laddr();
	return ctx.tm.get_pin(ctx.t, laddr, true
	).si_then([&state, ctx, srcoff](auto mapping) {
	  state.src_first_mapping = std::move(mapping);
	  auto laddr = (state.dest_base + srcoff).get_aligned_laddr();
	  return ctx.tm.get_pin(ctx.t, laddr, true);
	}).si_then([&state, ctx, srcoff, len](auto mapping) {
	  state.dest_first_mapping = std::move(mapping);
	  auto laddr = (state.direct_base + srcoff).get_aligned_laddr();
	  return ctx.tm.get_pin(ctx.t, laddr, true
	  ).si_then([&state, ctx, srcoff, len](auto mapping) {
	    state.first_direct_mapping = std::move(mapping);
	    return _clone_range(
	      ctx, state.src_base, state.dest_base, state.direct_base,
	      srcoff, len,
	      std::move(state.src_first_mapping),
	      std::move(state.dest_first_mapping),
	      std::move(state.first_direct_mapping));
	  }).handle_error_interruptible(
	    clone_iertr::pass_further{},
	    crimson::ct_error::assert_all{"unexpected enoent"}
	  );
	});
      }).handle_error_interruptible(
	clone_iertr::pass_further{},
	crimson::ct_error::assert_all{"unexpected enoent"}
      );
    });
  });
}

ObjectDataHandler::clone_ret ObjectDataHandler::clone(context_t ctx)
{
  assert(ctx.d_onode);
  LOG_PREFIX(ObjectDataHandler::clone);
  DEBUGT("{}=>{}", ctx.t, ctx.onode.get_hobj(), ctx.d_onode->get_hobj());
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
    if (object_data.is_null()) {
      return clone_iertr::now();
    }
    ceph_assert(d_object_data.is_null());
    if (ctx.d_onode->is_head()) {
      // this is a rollback
      return do_rollback(ctx, object_data, d_object_data);
    } else {
      return do_clone(ctx, object_data, d_object_data);
    }
  });
}

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::overwrite_params_t>
  : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::data_t>
  : fmt::ostream_formatter {};
#endif
