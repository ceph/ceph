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

ObjectDataHandler::write_iertr::future<std::optional<LBAMapping>>
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
    return write_iertr::make_ready_future<std::optional<LBAMapping>>();
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
      return std::make_optional<LBAMapping>(std::move(pin));
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
  laddr_t hint,
  extent_len_t len)
{
  LOG_PREFIX(ObjectDataHandler::prepare_shared_region);
  DEBUGT("{}~{}", ctx.t, hint, len);
  assert(onode.get_shared_region_base() == L_ADDR_NULL);
  return ctx.tm.reserve_region(ctx.t, hint, len
  ).si_then([ctx, &onode](auto shared_region) {
    onode.update_shared_region_base(ctx.t, shared_region.get_key());
    return std::move(shared_region);
  }).handle_error_interruptible(
    write_iertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected error"}
  );
}

ObjectDataHandler::read_iertr::future<std::optional<bufferlist>> read_mapping(
  ObjectDataHandler::context_t ctx,
  LBAMapping read_pos,
  extent_len_t unaligned_offset,
  extent_len_t unaligned_len,
  bool for_zero /* whether this is for zero overwrite*/)
{
  assert(unaligned_len != 0);
  if (read_pos.is_zero_reserved()) {
    if (for_zero) {
      // if we are doing zero overwrite and the current read_pos
      // is already a zero-reserved one, don't add any data to it
      return ObjectDataHandler::read_iertr::make_ready_future<
	std::optional<bufferlist>>();
    } else {
      bufferlist bl;
      bl.append_zero(unaligned_len);
      return ObjectDataHandler::read_iertr::make_ready_future<
	std::optional<bufferlist>>(std::move(bl));
    }
  } else {
    auto aligned_offset = p2align(unaligned_offset, ctx.tm.get_block_size());
    auto aligned_len =
      p2roundup(unaligned_offset + unaligned_len,
		ctx.tm.get_block_size()) - aligned_offset;
    return ctx.tm.read_pin<ObjectDataBlock>(
      ctx.t, read_pos, aligned_offset, aligned_len
    ).si_then([unaligned_offset, unaligned_len, aligned_offset, aligned_len]
	      (auto maybe_indirect_left_extent) {
      auto read_bl = maybe_indirect_left_extent.get_range(
	aligned_offset, aligned_len);
      ceph::bufferlist prepend_bl;
      prepend_bl.substr_of(
	read_bl, unaligned_offset - aligned_offset, unaligned_len);
      return ObjectDataHandler::read_iertr::make_ready_future<
	std::optional<bufferlist>>(std::move(prepend_bl));
    });
  }
}

std::ostream& operator<<(
  std::ostream &out, const overwrite_range_t &overwrite_range) {
  return out << "overwrite_range_t{" << std::hex
    << "unaligned_len=0x" << overwrite_range.unaligned_len
    << ", unaligned_begin=0x" << overwrite_range.unaligned_begin
    << ", aligned_begin=0x" << overwrite_range.aligned_begin
    << ", unaligned_end=0x" << overwrite_range.unaligned_end
    << ", aligned_end=0x" << overwrite_range.aligned_end
    << ", aligned_len=0x" << overwrite_range.aligned_len << std::dec
    << "}";
}

std::ostream& operator<<(std::ostream &out, const data_t &data) {
  return out << "data_t{" << std::hex
    << "headbl=0x" << (data.headbl ? data.headbl->length() : 0)
    << ", bl=0x" << (data.bl ? data.bl->length() : 0)
    << ", tailbl=0x" << (data.tailbl ? data.tailbl->length() : 0) << std::dec
    << "}";
}

ObjectDataHandler::write_ret
ObjectDataHandler::delta_based_overwrite(
  context_t ctx,
  extent_len_t unaligned_offset,
  extent_len_t unaligned_len,
  LBAMapping overwrite_mapping,
  std::optional<bufferlist> data)
{
  LOG_PREFIX(ObjectDataHandler::delta_based_overwrite);
  DEBUGT("0x{:x}~0x{:x} {} zero={}",
    ctx.t, unaligned_offset, unaligned_len, overwrite_mapping, !data.has_value());
  // delta based overwrite
  return ctx.tm.read_pin<ObjectDataBlock>(
    ctx.t,
    overwrite_mapping
  ).handle_error_interruptible(
    base_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "ObjectDataHandler::do_remapping hit invalid error"
    }
  ).si_then([ctx](auto maybe_indirect_extent) {
    assert(!maybe_indirect_extent.is_indirect());
    return ctx.tm.get_mutable_extent(ctx.t, maybe_indirect_extent.extent);
  }).si_then([overwrite_mapping, unaligned_offset,
	      unaligned_len, data=std::move(data)](auto extent) {
    bufferlist bl;
    if (data) {
      bl.append(*data);
    } else {
      bl.append_zero(unaligned_len);
    }
    auto odblock = extent->template cast<ObjectDataBlock>();
    odblock->overwrite(unaligned_offset, std::move(bl));
  });
}

ObjectDataHandler::write_ret do_zero(
  context_t ctx,
  LBAMapping zero_pos,
  const overwrite_range_t &overwrite_range,
  data_t &data)
{
  assert(!data.bl);
  auto fut = TransactionManager::get_pin_iertr::make_ready_future<
    std::optional<LBAMapping>>();
  if (data.tailbl) {
    assert(data.tailbl->length() < ctx.tm.get_block_size());
    data.tailbl->prepend_zero(
      ctx.tm.get_block_size() - data.tailbl->length());
    fut = ctx.tm.alloc_data_extents<ObjectDataBlock>(
      ctx.t,
      (overwrite_range.aligned_end - ctx.tm.get_block_size()).checked_to_laddr(),
      ctx.tm.get_block_size(),
      std::move(zero_pos)
    ).si_then([ctx, &data](auto extents) {
      assert(extents.size() == 1);
      auto &extent = extents.back();
      auto iter = data.tailbl->cbegin();
      iter.copy(extent->get_length(), extent->get_bptr().c_str());
      return ctx.tm.get_pin(ctx.t, *extent);
    }).si_then([](auto zero_pos) {
      return std::make_optional<LBAMapping>(std::move(zero_pos));
    }).handle_error_interruptible(
      crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
      TransactionManager::get_pin_iertr::pass_further{}
    );
  }
  fut = fut.si_then([ctx, &overwrite_range, zero_pos=std::move(zero_pos),
		    &data](auto pin) mutable {
    if (pin) {
      zero_pos = std::move(*pin);
    }
    auto laddr =
      (overwrite_range.aligned_begin +
       (data.headbl ? ctx.tm.get_block_size() : 0)
      ).checked_to_laddr();
    auto end =
      (overwrite_range.aligned_end -
       (data.tailbl ? ctx.tm.get_block_size() : 0)
      ).checked_to_laddr();
    auto len = end.get_byte_distance<extent_len_t>(laddr);
    return ctx.tm.reserve_region(ctx.t, std::move(zero_pos), laddr, len);
  }).si_then([](auto zero_pos) {
    return std::make_optional<LBAMapping>(std::move(zero_pos));
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    TransactionManager::get_pin_iertr::pass_further{}
  );
  if (data.headbl) {
    assert(data.headbl->length() < ctx.tm.get_block_size());
    data.headbl->append_zero(
      ctx.tm.get_block_size() - data.headbl->length());
    fut = fut.si_then([ctx, &overwrite_range](auto zero_pos) {
      return ctx.tm.alloc_data_extents<ObjectDataBlock>(
	ctx.t,
	overwrite_range.aligned_begin,
	ctx.tm.get_block_size(),
	std::move(*zero_pos));
    }).si_then([&data](auto extents) {
      assert(extents.size() == 1);
      auto &extent = extents.back();
      auto iter = data.headbl->cbegin();
      iter.copy(extent->get_length(), extent->get_bptr().c_str());
      return TransactionManager::get_pin_iertr::make_ready_future<
	std::optional<LBAMapping>>();
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
  LBAMapping write_pos,
  const overwrite_range_t &overwrite_range,
  data_t &data)
{
  assert(data.bl);
  return ctx.tm.alloc_data_extents<ObjectDataBlock>(
    ctx.t,
    overwrite_range.aligned_begin,
    overwrite_range.aligned_end.template get_byte_distance<
      extent_len_t>(overwrite_range.aligned_begin),
    std::move(write_pos)
  ).si_then([&overwrite_range, &data](auto extents) {
    auto off = overwrite_range.aligned_begin;
    auto left = overwrite_range.aligned_end.template get_byte_distance<
      extent_len_t>(overwrite_range.aligned_begin);
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

std::ostream& operator<<(std::ostream &out, const edge_t &edge) {
  out << "edge_t{";
  switch (edge) {
  case edge_t::NONE:
    out << "NONE";
    break;
  case edge_t::LEFT:
    out << "LEFT";
    break;
  case edge_t::RIGHT:
    out << "RIGHT";
    break;
  case edge_t::BOTH:
    out << "BOTH";
    break;
  default:
    ceph_abort();
  }
  return out << "}";
}

// read the padding edge data into data.headbl/data.tailbl, note that
// the method doesn't expand the overwrite range, as the aligned boundaries
// are not affected, expands only happens in the merge_pending_edge method.
ObjectDataHandler::read_iertr::future<>
ObjectDataHandler::read_unaligned_edge_data(
  context_t ctx,
  const overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping &read_pos,
  edge_t edge)
{
  assert(edge != edge_t::NONE);
  LOG_PREFIX(ObjectDataHandler::read_unaligned_edge_data);
  DEBUGT("{} {} {} edge={}", ctx.t, overwrite_range, data, read_pos, edge);
  std::vector<ObjectDataHandler::read_iertr::future<>> futs;
  if (edge & edge_t::LEFT) {
    auto unaligned_off = read_pos.get_key().template get_byte_distance<
      extent_len_t>(overwrite_range.aligned_begin);
    auto unaligned_length =
      overwrite_range.unaligned_begin.template get_byte_distance<
	extent_len_t>(overwrite_range.aligned_begin);
    futs.emplace_back(read_mapping(
      ctx, read_pos, unaligned_off, unaligned_length, !data.bl
    ).si_then([&data](auto bl) {
      data.headbl = std::move(bl);
    }));
  }

  if (edge & edge_t::RIGHT) {
    auto unaligned_off =
      overwrite_range.unaligned_end.template get_byte_distance<
	extent_len_t>(read_pos.get_key());
    auto unaligned_length =
      overwrite_range.aligned_end.template get_byte_distance<
	extent_len_t>(overwrite_range.unaligned_end);
    futs.emplace_back(read_mapping(
	ctx, read_pos, unaligned_off, unaligned_length, !data.bl
    ).si_then([&data](auto bl) {
      data.tailbl = std::move(bl);
    }));
  }

  // TODO: when_all_succeed should be utilized here, however, it doesn't
  // 	   actually work with interruptible errorated futures for now.
  return trans_intr::parallel_for_each(
    futs, [](auto &fut) { return std::move(fut); });
}

// read the pending edge mapping's data into data.headbl/data.tailbl,
// remove the mapping and expand the overwrite_range; basically, this
// is equivalent to merge the current overwrite range with the pending
// edge mapping
//
// Note that this method should only be called when the overwrite handle
// policy is MERGE_PENDING.
ObjectDataHandler::read_iertr::future<>
ObjectDataHandler::merge_pending_edge(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping &edge_mapping,
  edge_t edge)
{
  assert(edge != edge_t::NONE);
  assert(edge_mapping.is_pending());
  std::vector<ObjectDataHandler::read_iertr::future<>> futs;
  if (edge & edge_t::LEFT) {
    auto unaligned_length = edge_mapping.get_key().template get_byte_distance<
      extent_len_t>(overwrite_range.unaligned_begin);
    if (unaligned_length != 0) {
      overwrite_range.expand_begin(edge_mapping.get_key());
      futs.emplace_back(read_mapping(
	ctx, edge_mapping, 0, unaligned_length, !data.bl
      ).si_then([&data](auto bl) {
	data.headbl = std::move(bl);
      }));
    }
  }

  if (edge & edge_t::RIGHT) {
    auto unaligned_offset = overwrite_range.unaligned_end.template get_byte_distance<
      extent_len_t>(edge_mapping.get_key());
    auto len = edge_mapping.get_length() - unaligned_offset;
    if (len != 0) {
      auto end = (edge_mapping.get_key() + edge_mapping.get_length()
	).checked_to_laddr();
      overwrite_range.expand_end(end);
      futs.emplace_back(read_mapping(
	ctx, edge_mapping, unaligned_offset, len, !data.bl
      ).si_then([&data](auto bl) {
	data.tailbl = std::move(bl);
      }));
    }
  }

  // TODO: when_all_succeed should be utilized here, however, it doesn't
  // 	   actually work with interruptible errorated futures for now.
  return trans_intr::parallel_for_each(
    futs, [](auto &fut) { return std::move(fut); });
}

base_iertr::future<LBAMapping>
ObjectDataHandler::delta_based_edge_overwrite(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t& data,
  LBAMapping edge_mapping,
  edge_t edge)
{
  LOG_PREFIX(ObjectDataHandler::do_delta_based_edge_push);
  DEBUGT("{} {} {} {}", ctx.t, overwrite_range, data, edge_mapping, edge);
  std::optional<bufferlist> bl = std::nullopt;
  assert(edge != edge_t::BOTH);
  assert(edge != edge_t::NONE);
  if (edge == edge_t::LEFT) {
    assert(overwrite_range.is_begin_in_mapping(edge_mapping));
  } else {
    assert(overwrite_range.is_end_in_mapping(edge_mapping));
  }
  if (data.bl) {
    extent_len_t unaligned_len =
      (edge == edge_t::LEFT)
	? overwrite_range.unaligned_begin.template get_byte_distance<
	    extent_len_t>(edge_mapping.get_key() + edge_mapping.get_length())
	: overwrite_range.unaligned_end.template get_byte_distance<
	    extent_len_t>(edge_mapping.get_key());
    extent_len_t unaligned_offset =
      (edge == edge_t::LEFT) ? 0 : data.bl->length() - unaligned_len;
    assert(unaligned_offset + unaligned_len <= data.bl->length());
    bl = std::make_optional<bufferlist>();
    bl->substr_of(*data.bl, unaligned_offset, unaligned_len);
    bufferlist t_bl;
    if (edge == edge_t::LEFT) {
      t_bl.substr_of(*data.bl, unaligned_len, data.bl->length() - unaligned_len);
    } else {
      t_bl.substr_of(*data.bl, 0, unaligned_offset);
    }
    data.bl = std::move(t_bl);
  }
  extent_len_t unaligned_overlapped_offset =
    (edge == edge_t::LEFT)
      ? overwrite_range.unaligned_begin.template get_byte_distance<
	  extent_len_t>(edge_mapping.get_key())
      : 0;
  extent_len_t unaligned_overlapped_len =
    (edge == edge_t::LEFT)
      ? overwrite_range.unaligned_begin.template get_byte_distance<
	  extent_len_t>(edge_mapping.get_key() + edge_mapping.get_length())
      : overwrite_range.unaligned_end.template get_byte_distance<
	  extent_len_t>(edge_mapping.get_key());
  return delta_based_overwrite(
    ctx,
    unaligned_overlapped_offset,
    unaligned_overlapped_len,
    edge_mapping, std::move(bl)
  ).si_then([edge_mapping, &overwrite_range, edge]() mutable {
    if (edge == edge_t::LEFT) {
      auto new_begin = edge_mapping.get_key() + edge_mapping.get_length();
      overwrite_range.shrink_begin(new_begin.checked_to_laddr());
      return edge_mapping.next();
    } else {
      auto new_end = edge_mapping.get_key();
      overwrite_range.shrink_end(new_end);
      return base_iertr::make_ready_future<
	LBAMapping>(std::move(edge_mapping));
    }
  });
}

ObjectDataHandler::write_ret
ObjectDataHandler::merge_into_mapping(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping edge_mapping)
{
  LOG_PREFIX(ObjectDataHandler::merge_into_mapping);
  DEBUGT("{} {} {}", ctx.t, overwrite_range, data, edge_mapping);
  assert(overwrite_range.is_range_in_mapping(edge_mapping));
  return ctx.tm.read_pin<ObjectDataBlock>(ctx.t, edge_mapping
  ).si_then([&overwrite_range, &data, edge_mapping](auto maybe_indirect_extent) {
    assert(!maybe_indirect_extent.is_indirect());
    assert(maybe_indirect_extent.extent);
    assert(maybe_indirect_extent.extent->is_initial_pending());
    auto offset = overwrite_range.unaligned_begin.template get_byte_distance<
      extent_len_t>(edge_mapping.get_key());
    bufferlist bl;
    if (data.bl) {
      bl.append(*data.bl);
    } else {
      bl.append_zero(overwrite_range.unaligned_len);
    }
    auto iter = bl.cbegin();
    auto &ptr = maybe_indirect_extent.extent->get_bptr();
    iter.copy(bl.length(), ptr.c_str() + offset);
  });
}

base_iertr::future<LBAMapping>
ObjectDataHandler::merge_into_pending_edge(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping edge_mapping,
  edge_t edge)
{
  LOG_PREFIX(ObjectDataHandler::merge_into_pending_edge);
  DEBUGT("{} {} {} {}", ctx.t, overwrite_range, data, edge_mapping, edge);
  bufferlist bl;
  assert(edge != edge_t::BOTH);
  assert(edge != edge_t::NONE);
  assert(edge_mapping.is_initial_pending());
  if (edge == edge_t::LEFT) {
    assert(overwrite_range.is_begin_in_mapping(edge_mapping));
  } else {
    assert(overwrite_range.is_end_in_mapping(edge_mapping));
  }
  extent_len_t unaligned_len =
    (edge == edge_t::LEFT)
      ? overwrite_range.unaligned_begin.template get_byte_distance<
	  extent_len_t>(edge_mapping.get_key() + edge_mapping.get_length())
      : overwrite_range.unaligned_end.template get_byte_distance<
	  extent_len_t>(edge_mapping.get_key());
  if (data.bl) {
    extent_len_t unaligned_offset =
      (edge == edge_t::LEFT) ? 0 : data.bl->length() - unaligned_len;
    assert(unaligned_offset + unaligned_len <= data.bl->length());
    bl.substr_of(*data.bl, unaligned_offset, unaligned_len);
    bufferlist t_bl;
    if (edge == edge_t::LEFT) {
      t_bl.substr_of(*data.bl, unaligned_len, data.bl->length() - unaligned_len);
    } else {
      t_bl.substr_of(*data.bl, 0, unaligned_offset);
    }
    data.bl = std::move(t_bl);
  } else {
    bl.append_zero(unaligned_len);
  }
  return ctx.tm.read_pin<ObjectDataBlock>(ctx.t, edge_mapping
  ).si_then([bl=std::move(bl), &overwrite_range, edge_mapping, edge]
	    (auto maybe_indirect_extent) mutable {
    assert(!maybe_indirect_extent.is_indirect());
    assert(maybe_indirect_extent.extent);
    assert(maybe_indirect_extent.extent->is_initial_pending());
    extent_len_t offset =
      (edge == edge_t::LEFT)
	? overwrite_range.unaligned_begin.template get_byte_distance<
	    extent_len_t>(edge_mapping.get_key())
	: 0;
    auto iter = bl.cbegin();
    auto &ptr = maybe_indirect_extent.extent->get_bptr();
    iter.copy(bl.length(), ptr.c_str() + offset);
    if (edge == edge_t::LEFT) {
      auto new_begin = edge_mapping.get_key() + edge_mapping.get_length();
      overwrite_range.shrink_begin(new_begin.checked_to_laddr());
      return edge_mapping.next();
    } else {
      auto new_end = edge_mapping.get_key();
      overwrite_range.shrink_end(new_end);
      return base_iertr::make_ready_future<
	LBAMapping>(std::move(edge_mapping));
    }
  });
}

base_iertr::future<LBAMapping>
ObjectDataHandler::do_merge_based_edge_punch(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping edge_mapping,
  edge_t edge)
{
  LOG_PREFIX(ObjectDataHandler::do_merge_based_edge_push);
  DEBUGT("{} {} {} {}", ctx.t, overwrite_range, data, edge_mapping, edge);
  assert(edge_mapping.is_pending());
  return merge_pending_edge(ctx, overwrite_range, data, edge_mapping, edge
  ).si_then([edge_mapping, ctx] {
    return ctx.tm.remove(ctx.t, std::move(edge_mapping));
  }).handle_error_interruptible(
    base_iertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected error"}
  );
}

base_iertr::future<LBAMapping>
ObjectDataHandler::do_remap_based_edge_punch(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping edge_mapping,
  edge_t edge)
{
  LOG_PREFIX(ObjectDataHandler::do_remap_based_edge_push);
  DEBUGT("{} {} {} {}", ctx.t, overwrite_range, data, edge_mapping, edge);
  if (edge & edge_t::LEFT) {
    assert(overwrite_range.is_begin_in_mapping(edge_mapping));
  } else {
    assert(edge & edge_t::RIGHT);
    assert(overwrite_range.is_end_in_mapping(edge_mapping));
  }

  auto fut = base_iertr::now();
  if (((edge & edge_t::LEFT) &&
	!overwrite_range.is_begin_aligned(ctx.tm.get_block_size())) ||
      ((edge & edge_t::RIGHT) &&
	!overwrite_range.is_end_aligned(ctx.tm.get_block_size()))) {
    // if the overwrite range is not aligned,
    // we need to read the padding data first.
    fut = read_unaligned_edge_data(
      ctx, overwrite_range, data, edge_mapping, edge);
  }
  return fut.si_then([ctx, edge_mapping, &overwrite_range, edge] {
    if (edge == edge_t::LEFT) {
      if (overwrite_range.aligned_begin > edge_mapping.get_key()) {
	return ctx.tm.cut_mapping<ObjectDataBlock>(
	  ctx.t, overwrite_range.aligned_begin, std::move(edge_mapping), true
	).si_then([](auto mapping) {
	  return mapping.next();
	});
      } else {
	// this branch happens when:
	// "overwrite.aligned_begin == edge_mapping.get_key() &&
	//  overwrite.unaligned_begin > edge_mapping.get_key()"
	return base_iertr::make_ready_future<
	  LBAMapping>(std::move(edge_mapping));
      }
    } else {
      assert(edge == edge_t::RIGHT);
      if (overwrite_range.aligned_end <
		edge_mapping.get_key() + edge_mapping.get_length()) {
	return ctx.tm.cut_mapping<ObjectDataBlock>(
	  ctx.t, overwrite_range.aligned_end, std::move(edge_mapping), false);
      } else {
	// this branch happens when overwrite.aligned_end is equal to
	// the end of the edge_mapping while overwrite.unaligned_end is
	// less than that of the edge_mapping.
	return ctx.tm.remove(ctx.t, std::move(edge_mapping)
	).handle_error_interruptible(
	  base_iertr::pass_further{},
	  crimson::ct_error::assert_all{"unexpected error"}
	);
      }
    }
  });
}

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
ObjectDataHandler::punch_mapping_on_edge(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping edge_mapping,
  edge_t edge,
  op_type_t op_type)
{
  assert(edge != edge_t::NONE);
  LOG_PREFIX(ObjectDataHandler::punch_mapping_on_edge);
  DEBUGT("{}, {}, {}, {}", ctx.t, overwrite_range, data, edge_mapping, edge);
  ceph_assert(edge != edge_t::BOTH);
  assert(edge_mapping.is_viewable());

  auto edge_key = edge_mapping.get_key();
  auto edge_length = edge_mapping.get_length();
  laddr_t aligned_overlapped_start =
    (edge == edge_t::LEFT)
      ? overwrite_range.aligned_begin
      : edge_key;
  extent_len_t aligned_overlapped_len =
    (edge == edge_t::LEFT)
      ? overwrite_range.aligned_begin.template get_byte_distance<
	  extent_len_t>(edge_key + edge_length)
      : overwrite_range.aligned_end.template get_byte_distance<
	  extent_len_t>(edge_key);
  auto ehpolicy = get_edge_handle_policy(
    edge_mapping,
    aligned_overlapped_start,
    aligned_overlapped_len,
    op_type);
  switch (ehpolicy) {
  case edge_handle_policy_t::DELTA_BASED_PUNCH:
    return delta_based_edge_overwrite(
      ctx, overwrite_range, data, std::move(edge_mapping), edge);
  case edge_handle_policy_t::MERGE_INPLACE:
    return merge_into_pending_edge(
      ctx, overwrite_range, data, std::move(edge_mapping), edge);
  case edge_handle_policy_t::REMAP:
    return do_remap_based_edge_punch(
      ctx, overwrite_range, data, std::move(edge_mapping), edge);
  default:
    ceph_abort_msg("unexpected edge handling policy");
  }
}

// The first step in a multi-mapping-hole-punching scenario: remap the
// left mapping if it crosses the left edge of the hole's range
base_iertr::future<LBAMapping>
ObjectDataHandler::punch_left_mapping(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &overwrite_data,
  LBAMapping left_mapping,
  op_type_t op_type)
{
  if (overwrite_range.unaligned_begin > left_mapping.get_key()) {
    // left_mapping crosses the left edge
    assert(overwrite_range.unaligned_begin <
      left_mapping.get_key() + left_mapping.get_length());
    return punch_mapping_on_edge(
      ctx, overwrite_range, overwrite_data,
      std::move(left_mapping), edge_t::LEFT, op_type);
  }
  return base_iertr::make_ready_future<
    LBAMapping>(std::move(left_mapping));
}

// The second step in a multi-mapping-hole-punching scenario: remove
// all the mappings that are strictly inside the hole's range
base_iertr::future<LBAMapping>
ObjectDataHandler::punch_inner_mappings(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  LBAMapping first_mapping)
{
  auto unaligned_len = overwrite_range.unaligned_end.template get_byte_distance<
    extent_len_t>(overwrite_range.aligned_begin);
  return ctx.tm.remove_mappings_in_range(
    ctx.t, overwrite_range.aligned_begin,
    unaligned_len, std::move(first_mapping));
}

// The last step in the multi-mapping-hole-punching scenario: remap
// the right mapping if it crosses the right edge of the hole's range
base_iertr::future<LBAMapping>
ObjectDataHandler::punch_right_mapping(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &overwrite_data,
  LBAMapping right_mapping,
  op_type_t op_type)
{
  if (right_mapping.is_end() ||
      overwrite_range.aligned_end <= right_mapping.get_key()) {
    return base_iertr::make_ready_future<
      LBAMapping>(std::move(right_mapping));
  }
  return punch_mapping_on_edge(
    ctx, overwrite_range, overwrite_data,
    std::move(right_mapping), edge_t::RIGHT, op_type);
}

// punch the hole whose range is within a single pending mapping
base_iertr::future<LBAMapping>
ObjectDataHandler::punch_hole_in_pending_mapping(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping mapping)
{
  return merge_pending_edge(ctx, overwrite_range, data, mapping, edge_t::BOTH
  ).si_then([ctx, mapping=std::move(mapping)]() mutable {
    return ctx.tm.remove(ctx.t, std::move(mapping));
  }).handle_error_interruptible(
    base_iertr::pass_further{},
    crimson::ct_error::assert_all{"impossible"}
  );
}

base_iertr::future<LBAMapping>
ObjectDataHandler::punch_multi_mapping_hole(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping left_mapping,
  op_type_t op_type)
{
  return punch_left_mapping(
    ctx, overwrite_range, data, std::move(left_mapping), op_type
  ).si_then([this, ctx, &overwrite_range](auto mapping) {
    return punch_inner_mappings(ctx, overwrite_range, std::move(mapping));
  }).si_then([this, ctx, &overwrite_range, &data, op_type](auto mapping) {
    return punch_right_mapping(
      ctx, overwrite_range, data, std::move(mapping), op_type);
  });
}

base_iertr::future<LBAMapping>
ObjectDataHandler::punch_single_mapping_hole(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping left_mapping,
  op_type_t op_type)
{
  // since we are punching in-mapping hole, the edge policy must be
  // REMAP; otherwise, we shouldn't be calling this method.
  assert(get_edge_handle_policy(
    left_mapping,
    overwrite_range.aligned_begin,
    overwrite_range.aligned_len,
    op_type) == edge_handle_policy_t::REMAP);
  auto fut = base_iertr::now();
  edge_t edge =  edge_t::NONE;
  if (!overwrite_range.is_begin_aligned(ctx.tm.get_block_size())) {
    edge = static_cast<edge_t>(edge | edge_t::LEFT);
  }
  if (!overwrite_range.is_end_aligned(ctx.tm.get_block_size())) {
    edge = static_cast<edge_t>(edge | edge_t::RIGHT);
  }
  if (edge != edge_t::NONE) {
    fut = read_unaligned_edge_data(
      ctx, overwrite_range, data, left_mapping, edge);
  }
  return fut.si_then([ctx, &overwrite_range, left_mapping] {
    return ctx.tm.punch_hole_in_mapping<ObjectDataBlock>(
      ctx.t, overwrite_range.aligned_begin,
      overwrite_range.aligned_len, std::move(left_mapping));
  });
}

ObjectDataHandler::write_ret
ObjectDataHandler::handle_single_mapping_overwrite(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping mapping,
  op_type_t op_type)
{
  auto ehpolicy = get_edge_handle_policy(
    mapping,
    overwrite_range.aligned_begin,
    overwrite_range.aligned_len,
    op_type);
  auto do_overwrite = [ctx, &overwrite_range, &data](auto pos) {
    if (overwrite_range.is_empty()) {
      // the overwrite is completed in the previous steps,
      // this can happen if delta based overwrites are involved.
      return write_iertr::now();
    }
    if (overwrite_range.aligned_end.template get_byte_distance<
	  extent_len_t>(overwrite_range.aligned_begin) == ctx.tm.get_block_size()
	&& (data.headbl || data.tailbl)) {
      // the range to zero is within a block
      bufferlist bl;
      if (data.headbl) {
	bl.append(*data.headbl);
      }
      if (!data.bl) {
	bl.append_zero(overwrite_range.unaligned_len);
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
      return do_write(ctx, std::move(pos), overwrite_range, data);
    } else {
      return do_zero(ctx, std::move(pos), overwrite_range, data);
    }
  };

  switch (ehpolicy) {
  case edge_handle_policy_t::DELTA_BASED_PUNCH:
    {
      auto unaligned_offset = mapping.get_key().template get_byte_distance<
	extent_len_t>(overwrite_range.unaligned_begin);
      auto unaligned_len = overwrite_range.unaligned_len;
      return delta_based_overwrite(
	ctx, unaligned_offset, unaligned_len, std::move(mapping), data.bl);
    }
  case edge_handle_policy_t::MERGE_INPLACE:
    {
      return merge_into_mapping(
	ctx, overwrite_range, data, std::move(mapping));
    }
  case edge_handle_policy_t::REMAP:
    {
      auto fut = base_iertr::now();
      edge_t edge =  edge_t::NONE;
      if (!overwrite_range.is_begin_aligned(ctx.tm.get_block_size())) {
	edge = static_cast<edge_t>(edge | edge_t::LEFT);
      }
      if (!overwrite_range.is_end_aligned(ctx.tm.get_block_size())) {
	edge = static_cast<edge_t>(edge | edge_t::RIGHT);
      }
      if (edge != edge_t::NONE) {
	fut = read_unaligned_edge_data(
	  ctx, overwrite_range, data, mapping, edge);
      }
      return fut.si_then([ctx, &overwrite_range, mapping] {
	return ctx.tm.punch_hole_in_mapping<ObjectDataBlock>(
	  ctx.t, overwrite_range.aligned_begin,
	  overwrite_range.aligned_len, std::move(mapping));
      }).si_then([do_overwrite=std::move(do_overwrite)](auto pos) {
	return do_overwrite(std::move(pos));
      });
    }
  default:
    ceph_abort_msg("unexpected edge handling policy");
  }
}

ObjectDataHandler::write_ret
ObjectDataHandler::handle_multi_mapping_overwrite(
  context_t ctx,
  overwrite_range_t &overwrite_range,
  data_t &data,
  LBAMapping first_mapping,
  op_type_t op_type)
{
  return punch_multi_mapping_hole(
    ctx, overwrite_range, data, std::move(first_mapping), op_type
  ).si_then([ctx, &overwrite_range, &data](auto pos) {
    if (overwrite_range.is_empty()) {
      // the overwrite is completed in the previous steps,
      // this can happen if delta based overwrites are involved.
      return write_iertr::now();
    }
    if (overwrite_range.aligned_end.template get_byte_distance<
	  extent_len_t>(overwrite_range.aligned_begin) == ctx.tm.get_block_size()
	&& (data.headbl || data.tailbl)) {
      // the range to zero is within a block
      bufferlist bl;
      if (data.headbl) {
	bl.append(*data.headbl);
      }
      if (!data.bl) {
	bl.append_zero(overwrite_range.unaligned_len);
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
      return do_write(ctx, std::move(pos), overwrite_range, data);
    } else {
      return do_zero(ctx, std::move(pos), overwrite_range, data);
    }
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
  auto unaligned_begin = data_base + offset;
  auto unaligned_end = data_base + offset + len;
  assert(first_mapping.get_key() <= unaligned_begin.get_aligned_laddr(
    ctx.tm.get_block_size()));
  DEBUGT(
    "data_base={}, offset=0x{:x}, len=0x{:x}, "
    "aligned_begin={}, aligned_end={}",
    ctx.t, data_base, offset, len,
    unaligned_begin.get_aligned_laddr(ctx.tm.get_block_size()),
    unaligned_end.get_roundup_laddr(ctx.tm.get_block_size()));
  return seastar::do_with(
    data_t{std::nullopt, std::move(bl), std::nullopt},
    overwrite_range_t{
      len,
      unaligned_begin,
      unaligned_end,
      ctx.tm.get_block_size()},
    [first_mapping=std::move(first_mapping),
    this, ctx](auto &data, auto &overwrite_range) {
    if (overwrite_range.is_range_in_mapping(first_mapping)) {
      return handle_single_mapping_overwrite(
	ctx, overwrite_range, data, std::move(first_mapping),
	data.bl.has_value() ? op_type_t::OVERWRITE : op_type_t::ZERO);
    } else {
      return handle_multi_mapping_overwrite(
	ctx, overwrite_range, data, std::move(first_mapping),
	data.bl.has_value() ? op_type_t::OVERWRITE : op_type_t::ZERO);
    }
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
	if (mapping) {
	  return overwrite(
	    ctx, data_base, offset, len,
	    std::nullopt, std::move(*mapping));
	}
	laddr_offset_t l_start = data_base + offset;
	return ctx.tm.get_containing_pin(
	  ctx.t, l_start.get_aligned_laddr(ctx.tm.get_block_size())
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
	if (mapping) {
	  return overwrite(
	    ctx, data_base, offset, bl.length(),
	    bufferlist(bl), std::move(*mapping));
	}
	laddr_offset_t l_start = data_base + offset;
	return ctx.tm.get_containing_pin(
	  ctx.t, l_start.get_aligned_laddr(ctx.tm.get_block_size())
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
  DEBUGT("0x{:x}~0x{:x}, 0x{:x}",
    ctx.t, object_data.get_reserved_data_base(),
    object_data.get_reserved_data_len(), size);
  ceph_assert(!object_data.is_null());
  ceph_assert(size <= object_data.get_reserved_data_len());
  auto data_base = object_data.get_reserved_data_base();
  auto unaligned_begin = data_base + size;
  return ctx.tm.get_containing_pin(
    ctx.t, unaligned_begin.get_aligned_laddr(ctx.tm.get_block_size())
  ).si_then([ctx, data_base, size, this,
	    unaligned_begin, &object_data](auto mapping) {
    assert(mapping.get_key() <= unaligned_begin &&
      mapping.get_key() + mapping.get_length() > unaligned_begin);
    auto data_len = object_data.get_reserved_data_len();
    return overwrite(
      ctx, data_base, size, data_len - size,
      std::nullopt, std::move(mapping));
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
      laddr_t aligned_start = l_start.get_aligned_laddr(
	ctx.tm.get_block_size());
      loffset_t aligned_length =
	  l_end.get_roundup_laddr(ctx.tm.get_block_size()).get_byte_distance<
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
              ceph_assert(l_current.get_aligned_laddr(
		ctx.tm.get_block_size()) >= pin_start);
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
            laddr_t l_current_end_aligned =
	      l_current_end.get_roundup_laddr(ctx.tm.get_block_size());
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
      laddr_t aligned_start = l_start.get_aligned_laddr(
	ctx.tm.get_block_size());
      loffset_t aligned_length =
	  l_end.get_roundup_laddr(ctx.tm.get_block_size()).get_byte_distance<
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

ObjectDataHandler::clone_ret
ObjectDataHandler::do_clone_range(
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
  auto raw_begin = dest_base + offset;
  auto raw_end = raw_begin + len;
  return seastar::do_with(
    data_t{},
    overwrite_range_t{
      len,
      raw_begin,
      raw_end,
      ctx.tm.get_block_size()},
    [this, &head_padding, &tail_padding, ctx,
    &src_first_mapping, dest_base, offset,
    len, &dest_first_mapping, src_base](auto &data, auto &overwrite_range) {
    return punch_hole(
      ctx, overwrite_range, data,
      std::move(dest_first_mapping),
      op_type_t::CLONE_RANGE
    ).si_then([&overwrite_range, ctx, &data, &head_padding, src_base, dest_base,
	      &tail_padding, &src_first_mapping, offset, len](auto mapping) {
      assert(mapping.is_end() ||
	mapping.get_key() >= overwrite_range.aligned_end);
      auto fut = TransactionManager::get_pin_iertr::make_ready_future<
	std::optional<LBAMapping>>(mapping);
      if (head_padding.length() != 0) {
	// write the unaligned data at the left boundary of the src range
	// into the dest range
	assert(head_padding.length() < ctx.tm.get_block_size());
	if (data.headbl) {
	  data.headbl->append(head_padding);
	} else {
	  data.headbl = bufferlist{};
	  data.headbl->append_zero(
	    ctx.tm.get_block_size() - head_padding.length());
	  data.headbl->append(head_padding);
	}
	fut = ctx.tm.alloc_data_extents<ObjectDataBlock>(
	  ctx.t,
	  overwrite_range.aligned_begin,
	  ctx.tm.get_block_size(),
	  std::move(mapping)
	).si_then([ctx, &data, &overwrite_range](auto extents) {
	  assert(extents.size() == 1);
	  auto &extent = extents.back();
	  assert(overwrite_range.aligned_begin == extent->get_laddr());
	  auto iter = data.headbl->cbegin();
	  iter.copy(extent->get_length(), extent->get_bptr().c_str());
	  return ctx.tm.get_pin(ctx.t, *extent
	  ).si_then([](auto mapping) {
	    return mapping.next();
	  }).si_then([](auto mapping) {
	    return std::make_optional<LBAMapping>(std::move(mapping));
	  });
	}).handle_error_interruptible(
	  crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	  TransactionManager::get_pin_iertr::pass_further{}
	);
      }
      fut = fut.si_then([ctx, src_base, dest_base,
			&src_first_mapping, offset, len](auto pos) {
	// clone the src mappings
	auto aligned_off = p2roundup(offset, ctx.tm.get_block_size());
	auto aligned_len =
	  p2align(offset + len, ctx.tm.get_block_size()) - aligned_off;
	return ctx.tm.clone_range(
	  ctx.t, src_base, dest_base, aligned_off, aligned_len,
	  std::move(*pos), std::move(src_first_mapping)
	).si_then([](auto mapping) {
	  return std::make_optional<LBAMapping>(std::move(mapping));
	});
      });
      if (tail_padding.length() != 0) {
	// write the unaligned data at the right boundary of the src range
	// into the dest range
	assert(tail_padding.length() < ctx.tm.get_block_size());
	if (data.tailbl) {
	  tail_padding.append(*data.tailbl);
	} else {
	  tail_padding.append_zero(
	    ctx.tm.get_block_size() - tail_padding.length());
	}
	data.tailbl = std::move(tail_padding);
	fut = fut.si_then([ctx, &overwrite_range](auto pos) {
	  return ctx.tm.alloc_data_extents<ObjectDataBlock>(
	    ctx.t,
	    (overwrite_range.aligned_end - ctx.tm.get_block_size()
	     ).checked_to_laddr(),
	    ctx.tm.get_block_size(),
	    std::move(pos));
	}).si_then([&data, &overwrite_range, ctx](auto extents) {
	  assert(extents.size() == 1);
	  auto &extent = extents.back();
	  assert((overwrite_range.aligned_end - ctx.tm.get_block_size()
	    ).checked_to_laddr() == extent->get_laddr());
	  auto iter = data.tailbl->cbegin();
	  iter.copy(extent->get_length(), extent->get_bptr().c_str());
	  return std::optional<LBAMapping>();
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

ObjectDataHandler::move_mappings_ret
ObjectDataHandler::move_and_clone_direct_mappings(
  context_t ctx,
  move_mapping_params_t src_params,
  move_mapping_params_t dest_params,
  LBAMapping src,
  LBAMapping dest)
{
  // Note that the destination of the moving of direct mappings must be
  // a zero mapping
  LOG_PREFIX(ObjectDataHandler::move_and_clone_direct_mappings);
  DEBUGT("src range:{}-{}, dest range: {}-{}, src: {}, dest: {}",
    ctx.t, src_params.begin, src_params.end,
    dest_params.begin, dest_params.end,
    src, dest);
  assert(src_params.begin >= src.get_key());
  assert(src_params.begin <
    (src.get_key() + src.get_length()).checked_to_laddr());
  assert(dest_params.begin >= dest.get_key());
  assert(dest_params.begin <
    (dest.get_key() + dest.get_length()).checked_to_laddr());
  // split the mapping at the left boundary of the src range if
  // it represents an extent and crosses the left boundary.
  //
  // Returns the right part of the mapping if splitting happened,
  // otherwise the mapping itself
  auto maybe_split_left_src = [ctx](LBAMapping &src,
				    const move_mapping_params_t &src_params) {
    if (src.is_real() && src_params.begin > src.get_key()) {
      return ctx.tm.split_mapping<ObjectDataBlock>(
	ctx.t, src_params.begin, std::move(src)
      ).si_then([](auto ret) {
	assert(ret.size() == 2);
	return std::move(ret.back());
      });
    } else {
      return move_mappings_iertr::make_ready_future<LBAMapping>(
	std::move(src));
    }
  };
  // split the mapping if it fits the following conditions:
  // 1. it's at the right boundary of the src range
  // 2. it represents an extent
  // 3. it crosses the right boundary of the src range
  //
  // return the left part of the mapping if splitting happened,
  // otherwise the mapping itself
  auto maybe_split_right_src = [ctx](LBAMapping &src,
				     const move_mapping_params_t &src_params) {
    if (src.is_real() &&
	src_params.end > src.get_key() &&
	src_params.end < src.get_key() + src.get_length()) {
      return ctx.tm.split_mapping<ObjectDataBlock>(
	ctx.t, src_params.end, std::move(src)
      ).si_then([](auto ret) {
	assert(ret.size() == 2);
	return std::move(ret.front());
      });
    } else {
      return move_mappings_iertr::make_ready_future<LBAMapping>(std::move(src));
    }
  };
  // push the lagged behind dest to the corresponding position to the src
  auto push_dest_to_src = [](LBAMapping &src,
			     LBAMapping &dest,
			     const move_mapping_params_t &src_params,
			     const move_mapping_params_t &dest_params) {
    return trans_intr::repeat([&dest, &src, &src_params, &dest_params] {
      auto src_off = src.get_key().template get_byte_distance<
	extent_len_t>(src_params.begin);
      auto dest_end = (dest.get_key() + dest.get_length()
	).template get_byte_distance<extent_len_t>(dest_params.begin);
      if (src_off < dest_end) {
	// dest has reached src, go on.
	return move_mappings_iertr::make_ready_future<
	  seastar::stop_iteration>(seastar::stop_iteration::yes);
      }
      return dest.next().si_then([&dest](auto d) {
	dest = std::move(d);
	return move_mappings_iertr::make_ready_future<
	  seastar::stop_iteration>(seastar::stop_iteration::no);
      });
    });
  };
  // move the src mapping into the dest, and add a clone
  // mapping at the src's position.
  auto move_src_into_dest = [this, ctx](LBAMapping &src,
					LBAMapping &dest,
					const move_mapping_params_t &src_params,
					const move_mapping_params_t &dest_params) {
    auto src_off = src.get_key().template get_byte_distance<
      extent_len_t>(src_params.begin);
    auto aligned_begin = (dest_params.begin + src_off).checked_to_laddr();
    assert(aligned_begin.is_aligned(ctx.tm.get_block_size()));
    auto aligned_end = std::min(
      (aligned_begin + src.get_length()).checked_to_laddr(),
      dest_params.end);
    assert(aligned_end.is_aligned(ctx.tm.get_block_size()));
    assert(dest.is_zero_reserved());
    return seastar::do_with(
      data_t{},
      overwrite_range_t{
	src.get_length(),
	laddr_offset_t{aligned_begin},
	laddr_offset_t{aligned_end},
	ctx.tm.get_block_size()},
      [ctx, &dest, &src, this, &src_params, &dest_params]
      (auto &data, auto &overwrite_range) {
      return punch_hole(
	ctx, overwrite_range, data,
	std::move(dest), op_type_t::CLONE_RANGE
      ).si_then([&src, &src_params, &dest_params, ctx](auto next_d) {
	auto offset = src.get_key().template get_byte_distance<
	  extent_len_t>(src_params.begin);
	auto dest_laddr = (dest_params.begin + offset).checked_to_laddr();
	return ctx.tm.move_and_clone_direct_mapping<ObjectDataBlock>(
	  ctx.t, std::move(src), dest_laddr, std::move(next_d));
      }).si_then([&src, &dest](auto ret) {
	src = std::move(ret.src);
	dest = std::move(ret.dest);
	return src.next();
      }).si_then([&src, &dest](auto s) {
	src = std::move(s);
	return dest.next();
      }).si_then([&dest](auto d) {
	dest = std::move(d);
	return seastar::stop_iteration::no;
      });
    });
  };

  return seastar::do_with(
    std::move(src),
    std::move(dest),
    src_params,
    dest_params,
    std::move(maybe_split_left_src),
    std::move(maybe_split_right_src),
    std::move(push_dest_to_src),
    std::move(move_src_into_dest),
    [this, ctx](auto &src, auto &dest,
		const auto &src_params,
		const auto &dest_params,
		auto &maybe_split_left_src,
		auto &maybe_split_right_src,
		auto &push_dest_to_src,
		auto &move_src_into_dest) {
    return src.refresh(
    ).si_then([&src_params, this, &maybe_split_left_src](auto left_mapping) {
      assert(!left_mapping.is_pending());
      // remap src if it represents an extent and crosses src_params' begin
      return maybe_split_left_src(left_mapping, src_params);
    }).si_then([&dest, &src](auto s) {
      src = std::move(s);
      return dest.refresh();
    }).si_then([&dest, ctx, this, &src, &src_params, &move_src_into_dest,
		&dest_params, &push_dest_to_src, &maybe_split_right_src](auto d) {
      dest = std::move(d);
      // move direct mappings in range [src_params.begin, src_params.end)
      // to the same position in range [dest_params.begin, dest_params.end)
      return trans_intr::repeat([&src, &dest, &src_params, &push_dest_to_src,
				&dest_params, this, ctx, &maybe_split_right_src,
				&move_src_into_dest] {
	if (src_params.end <= src.get_key()) {
	  // src has stepped out of the range, leave here
	  return move_mappings_iertr::make_ready_future<
	    seastar::stop_iteration>(seastar::stop_iteration::yes);
	}
	if (!src.is_real()) {
	  if (auto src_end = src.get_key() + src.get_length();
	      src_end > src_params.end) {
	    return move_mappings_iertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  } else {
	    return src.next().si_then([&src](auto next_src) {
	      src = std::move(next_src);
	      return move_mappings_iertr::make_ready_future<
		seastar::stop_iteration>(seastar::stop_iteration::no);
	    });
	  }
	}
	// the dest may be still at an earlier position, push it onto src
	return push_dest_to_src(src, dest, src_params, dest_params
	).si_then([&src, &src_params, &maybe_split_right_src] {
	  assert(!src.is_pending());
	  // remap src if it represents an extent and crosses src_params' end
	  return maybe_split_right_src(src, src_params);
	}).si_then([&dest, &src](auto s) {
	  src = std::move(s);
	  return dest.refresh();
	}).si_then([&dest, &src, ctx, this, &src_params,
		    &dest_params, &move_src_into_dest](auto d) {
	  dest = std::move(d);
	  // move src into dest
	  return move_src_into_dest(src, dest, src_params, dest_params);
	});
      });
    }).si_then([&src] {
      return std::move(src);
    });
  });
}

ObjectDataHandler::clone_ret
ObjectDataHandler::_clone_range(
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
    [this, ctx, src_base, dest_base, direct_base, offset, len](auto &state) {
    auto fut = read_iertr::now();
    auto unaligned_len = p2roundup(offset, ctx.tm.get_block_size()) - offset;
    if (unaligned_len) {
      // load the unaligned head padding if offset is not 4K aligned, this is
      // different from "read_unaligned_edge_data" in that: read_unaligned_edge_data
      // reads the unaligned content outside the overwrite range, while here we read
      // the unaligned content inside the range. This is because the content is
      // supposed to write into the dest region.
      fut = read_mapping(
	ctx,
	state.sfmapping,
	(src_base + offset).get_byte_distance<
	  extent_len_t>(state.sfmapping.get_key()),
	unaligned_len,
	false
      ).si_then([&state](auto bl) {
	state.head_padding = std::move(*bl);
      });
    }
    return fut.si_then([this, ctx, src_base, offset, len, &state, direct_base] {
      auto src_key = state.sfmapping.get_key();
      auto src_physical = !state.sfmapping.is_indirect()
	&& !state.sfmapping.is_zero_reserved();
      return move_and_clone_direct_mappings(
	ctx,
	{(src_base + offset).get_roundup_laddr(ctx.tm.get_block_size()),
	 (src_base + offset + len).get_aligned_laddr(ctx.tm.get_block_size())},
	{(direct_base + offset).get_roundup_laddr(ctx.tm.get_block_size()),
	 (direct_base + offset + len).get_aligned_laddr(ctx.tm.get_block_size())},
	state.sfmapping,
	state.fdmapping
      ).si_then([offset, len, ctx, &state](auto last_mapping) {
	if (auto padding_len = offset + len - p2align(
	      offset + len, ctx.tm.get_block_size());
	    padding_len > 0) {
	  // load the unaligned tail padding if offset + len is not 4K aligned,
	  // like the unaligned head padding loadding, this also reads the
	  // unaligned content inside the range.
	  return read_mapping(ctx, std::move(last_mapping), 0, padding_len, false
	  ).si_then([&state](auto bl) {
	    if (bl) {
	      state.tail_padding = std::move(*bl);
	    }
	  });
	}
	return ObjectDataHandler::read_iertr::now();
      }).si_then([src_base, offset, src_key, &state, src_physical] {
	if (src_base + offset > src_key && src_physical) {
	  // sfmapping must have been remapped and the current sfmapping must
	  // be out of the moved range, push sfmapping one step further. The
	  // next mapping must be the first mapping of the moved range.
	  return state.sfmapping.next().si_then([&state](auto mapping) {
	    state.sfmapping = std::move(mapping);
	  });
	}
	return base_iertr::now();
      });
    }).si_then([&state] {
      return state.dfmapping.refresh();
    }).si_then([ctx, src_base, dest_base, offset, len, &state, this](auto dfmapping) {
      state.dfmapping = std::move(dfmapping);
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
    return ctx.tm.remove(ctx.t, std::move(*d_mapping)
    ).si_then([ctx, &object_data, &d_object_data](auto pos) {
      return ctx.tm.get_pin(ctx.t, object_data.get_reserved_data_base()
      ).si_then([ctx, pos=std::move(pos), &object_data,
		&d_object_data](auto mapping) mutable {
	auto len = d_object_data.get_reserved_data_len();
	auto src_base = object_data.get_reserved_data_base();
	auto dst_base = d_object_data.get_reserved_data_base();
	return ctx.tm.clone_range(
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
    std::optional<LBAMapping> src_first_mapping;
    std::optional<LBAMapping> dest_first_mapping;
    std::optional<LBAMapping> first_direct_mapping;
    laddr_t src_base = L_ADDR_NULL;
    laddr_t dest_base = L_ADDR_NULL;
    laddr_t direct_base = L_ADDR_NULL;
  };
  return prepare_data_reservation(
    ctx, d_object_data, object_data.get_reserved_data_len()
  ).si_then([ctx, &object_data](auto) {
    return ctx.tm.reserve_region(
      ctx.t,
      object_data.get_reserved_data_base(),
      object_data.get_reserved_data_len()
    ).handle_error_interruptible(
      write_iertr::pass_further{},
      crimson::ct_error::assert_all{"unexpected error"}
    );
  }).si_then([ctx, &object_data, &d_object_data,
	      this](auto shared_region) {
    return seastar::do_with(
      state_t{
	std::nullopt, std::nullopt, std::nullopt,
	object_data.get_reserved_data_base(),
	d_object_data.get_reserved_data_base(),
	shared_region.get_key()},
      [ctx, &object_data, this](auto &state) {
      return ctx.tm.get_pin(ctx.t, state.src_base
      ).si_then([&state, ctx](auto mapping) {
	state.src_first_mapping = std::move(mapping);
	return ctx.tm.get_pin(ctx.t, state.dest_base);
      }).si_then([&state, ctx](auto mapping) {
	state.dest_first_mapping = std::move(mapping);
	return ctx.tm.get_pin(ctx.t, state.direct_base);
      }).si_then([&state, ctx, &object_data, this](auto mapping) {
	state.first_direct_mapping = std::move(mapping);
	return _clone_range(
	  ctx, state.src_base, state.dest_base, state.direct_base,
	  0, object_data.get_reserved_data_len(),
	  std::move(*state.src_first_mapping),
	  std::move(*state.dest_first_mapping),
	  std::move(*state.first_direct_mapping));
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
      std::optional<LBAMapping> src_first_mapping;
      std::optional<LBAMapping> dest_first_mapping;
      std::optional<LBAMapping> first_direct_mapping;
      laddr_t src_base = L_ADDR_NULL;
      laddr_t dest_base = L_ADDR_NULL;
      laddr_t direct_base = L_ADDR_NULL;
    };
    ceph_assert(!object_data.is_null());
    return prepare_data_reservation(
      ctx, d_object_data, object_data.get_reserved_data_len()
    ).si_then([this, ctx, &object_data](auto) {
      if (ctx.onode.get_shared_region_base() != L_ADDR_NULL) {
	return write_iertr::now();
      }
      return prepare_shared_region(
	ctx, ctx.onode,
	object_data.get_reserved_data_base(),
	object_data.get_reserved_data_len()).discard_result();
    }).si_then([ctx, &object_data, &d_object_data, srcoff, len, this] {
      return seastar::do_with(
	state_t {
	  std::nullopt, std::nullopt, std::nullopt,
	  object_data.get_reserved_data_base(),
	  d_object_data.get_reserved_data_base(),
	  ctx.onode.get_shared_region_base()},
	[ctx, srcoff, len, this](auto &state) {
	auto laddr = (state.src_base + srcoff
	    ).get_aligned_laddr(ctx.tm.get_block_size());
	return ctx.tm.get_containing_pin(ctx.t, laddr
	).si_then([&state, ctx, srcoff](auto mapping) {
	  state.src_first_mapping = std::move(mapping);
	  auto laddr = (state.dest_base + srcoff
	      ).get_aligned_laddr(ctx.tm.get_block_size());
	  return ctx.tm.get_containing_pin(ctx.t, laddr);
	}).si_then([this, &state, ctx, srcoff, len](auto mapping) {
	  state.dest_first_mapping = std::move(mapping);
	  auto laddr = (state.direct_base + srcoff
	      ).get_aligned_laddr(ctx.tm.get_block_size());
	  return ctx.tm.get_containing_pin(ctx.t, laddr
	  ).si_then([this, &state, ctx, srcoff, len](auto mapping) {
	    state.first_direct_mapping = std::move(mapping);
	    return _clone_range(
	      ctx, state.src_base, state.dest_base, state.direct_base,
	      srcoff, len,
	      std::move(*state.src_first_mapping),
	      std::move(*state.dest_first_mapping),
	      std::move(*state.first_direct_mapping));
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
template <> struct fmt::formatter<crimson::os::seastore::overwrite_range_t>
  : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::data_t>
  : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::edge_t>
  : fmt::ostream_formatter {};
#endif
