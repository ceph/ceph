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
	      auto fut = ctx.tm.next_mapping(ctx.t, new_mapping.duplicate());
	      mappings.emplace_back(std::move(new_mapping));
	      return fut;
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
    using remap_entry = TransactionManager::remap_entry;
    if (headbl.length() > 0) {
      data.headbl = std::move(headbl);
    }
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
	&data, ctx](auto tailbl) mutable {
	if (tailbl.length() > 0) {
	  data.tailbl = std::move(tailbl);
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

ObjectDataHandler::write_ret do_write(
  context_t ctx,
  LBAMapping mapping,
  const overwrite_params_t &params,
  data_t &data)
{
  if (data.bl) {
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
  } else {
    auto fut = TransactionManager::get_pin_iertr::make_ready_future<LBAMapping>();
    if (data.headbl) {
      assert(data.headbl->length() < ctx.tm.get_block_size());
      data.headbl->append_zero(
	ctx.tm.get_block_size() - data.headbl->length());
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
    if (data.tailbl) {
      assert(data.tailbl->length() < ctx.tm.get_block_size());
      data.tailbl->prepend_zero(
	ctx.tm.get_block_size() - data.tailbl->length());
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
      data_base,
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
      return do_first_mapping(
	ctx, params, std::move(mapping), data
      ).si_then([&params, ctx](auto mapping) {
	return do_middle_mappings(ctx, params, std::move(mapping));
      }).si_then([&params, ctx, &data](auto mapping) {
	if (mapping.get_key() >= params.data_end) {
	  return write_iertr::make_ready_future<LBAMapping>(std::move(mapping));
	}
	return do_last_mapping(ctx, params, std::move(mapping), data);
      }).si_then([ctx, &params, &data](auto mapping) {
	return do_write(ctx, std::move(mapping), params, data);
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
	data_base,
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
      return do_first_mapping(
	ctx, params, std::move(mapping), data
      ).si_then([&params, ctx](auto mapping) {
	return do_middle_mappings(ctx, params, std::move(mapping));
      }).si_then([&params, ctx, &data](auto mapping) {
	assert(mapping.is_end() || mapping.get_key() > params.data_end);
	return do_write(ctx, std::move(mapping), params, data);
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

ObjectDataHandler::clone_ret clone_mappings(
  context_t ctx,
  object_data_t &object_data,
  LBAMapping pos,
  LBAMapping mapping,
  bool updateref)
{
  auto base = object_data.get_reserved_data_base();
  auto len = object_data.get_reserved_data_len();
  LOG_PREFIX(clone_mappings);
  DEBUGT("object_data={}~{} mapping={} updateref={}",
    ctx.t, base, len, mapping, updateref);
  return seastar::do_with(
    std::move(pos),
    std::move(mapping),
    0,
    [ctx, updateref, base, len](auto &pos, auto &mapping, auto &offset) {
    return trans_intr::repeat(
      [ctx, &pos, &mapping, &offset, updateref, base, len]()
      -> ObjectDataHandler::clone_iertr::future<seastar::stop_iteration> {
      if (offset >= len) {
	return ObjectDataHandler::clone_iertr::make_ready_future<
	  seastar::stop_iteration>(seastar::stop_iteration::yes);
      }
      if (!mapping.is_indirect() && mapping.get_val().is_zero()) {
	return ctx.tm.reserve_region(
	  ctx.t,
	  std::move(pos),
	  (base + offset).checked_to_laddr(),
	  mapping.get_length()
	).si_then([base, ctx, &offset](auto r) {
	  assert((base + offset).checked_to_laddr() == r.get_key());
	  offset += r.get_length();
	  return ctx.tm.next_mapping(ctx.t, std::move(r));
	}).si_then([&pos, ctx, &mapping](auto r) {
	  pos = std::move(r);
	  return ctx.tm.refresh_lba_mapping(ctx.t, std::move(mapping));
	}).si_then([ctx](auto p) {
	  return ctx.tm.next_mapping(ctx.t, std::move(p));
	}).si_then([&mapping](auto p) {
	  mapping = std::move(p);
	  return seastar::stop_iteration::no;
	}).handle_error_interruptible(
	  ObjectDataHandler::clone_iertr::pass_further{},
	  crimson::ct_error::assert_all{"unexpected error"}
	);
      }
      return ctx.tm.clone_pin(
	ctx.t, std::move(pos), std::move(mapping),
	(base + offset).checked_to_laddr(), updateref
      ).si_then([ctx, &offset, &pos, &mapping](auto ret) {
	offset += ret.cloned_mapping.get_length();
	return ctx.tm.next_mapping(ctx.t, std::move(ret.cloned_mapping)
	).si_then([ctx, &pos, ret=std::move(ret)](auto p) mutable {
	  pos = std::move(p);
	  return ctx.tm.next_mapping(ctx.t, std::move(ret.orig_mapping));
	}).si_then([&mapping](auto p) {
	  mapping = std::move(p);
	  return seastar::stop_iteration::no;
	});
      });
    });
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
    return ctx.tm.get_pin(ctx.t, object_data.get_reserved_data_base()
    ).si_then([this, &object_data, &d_object_data, ctx](auto mapping) {
      return prepare_data_reservation(
	ctx,
	d_object_data,
	object_data.get_reserved_data_len()
      ).si_then([&object_data, &d_object_data, ctx](auto mapping) {
	assert(!object_data.is_null());
	assert(!mapping.is_null());
	LOG_PREFIX(ObjectDataHandler::clone);
	DEBUGT("cloned obj reserve_data_base: {}, len 0x{:x}",
	  ctx.t,
	  d_object_data.get_reserved_data_base(),
	  d_object_data.get_reserved_data_len());
	return ctx.tm.remove(ctx.t, std::move(mapping));
      }).si_then([mapping=mapping.deep_duplicate(),
		  &d_object_data, ctx](auto pos) mutable {
	return ctx.tm.refresh_lba_mapping(ctx.t, std::move(mapping)
	).si_then([&d_object_data, pos=std::move(pos),
		  ctx](auto mapping) mutable {
	  return clone_mappings(
	    ctx, d_object_data, std::move(pos), std::move(mapping), true);
	});
      }).si_then([ctx, &object_data, &d_object_data, this] {
	object_data.clear();
	return prepare_data_reservation(
	  ctx,
	  object_data,
	  d_object_data.get_reserved_data_len()
	).si_then([ctx, &object_data](auto mapping) {
	  LOG_PREFIX("ObjectDataHandler::clone");
	  DEBUGT("head obj reserve_data_base: {}, len 0x{:x}",
	    ctx.t,
	    object_data.get_reserved_data_base(),
	    object_data.get_reserved_data_len());
	  return ctx.tm.remove(ctx.t, std::move(mapping));
	});
      }).si_then([ctx, &object_data,
		  mapping=std::move(mapping)](auto pos) mutable {
	return ctx.tm.refresh_lba_mapping(ctx.t, std::move(mapping)
	).si_then([&object_data, pos=std::move(pos),
		  ctx](auto mapping) mutable {
	  return clone_mappings(
	    ctx, object_data, std::move(pos), std::move(mapping), false);
	});
      });
    }).handle_error_interruptible(
      clone_iertr::pass_further{},
      crimson::ct_error::assert_all{"unexpected enoent"}
    );
  });
}

} // namespace crimson::os::seastore
