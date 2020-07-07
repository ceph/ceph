// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

std::ostream &LBAInternalNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", meta=" << get_meta();
}

LBAInternalNode::lookup_range_ret LBAInternalNode::lookup_range(
  op_context_t c,
  laddr_t addr,
  extent_len_t len)
{
  auto [begin, end] = bound(addr, addr + len);
  auto result_up = std::make_unique<lba_pin_list_t>();
  auto &result = *result_up;
  return crimson::do_for_each(
    std::move(begin),
    std::move(end),
    [this, c, &result, addr, len](const auto &val) mutable {
      return get_lba_btree_extent(
	c,
	get_meta().depth - 1,
	val.get_val(),
	get_paddr()).safe_then(
	  [c, &result, addr, len](auto extent) mutable {
	    // TODO: add backrefs to ensure cache residence of parents
	    return extent->lookup_range(
	      c,
	      addr,
	      len).safe_then(
		[&result](auto pin_list) mutable {
		  result.splice(result.end(), pin_list,
				pin_list.begin(), pin_list.end());
		});
	  });
    }).safe_then([result=std::move(result_up)] {
      return lookup_range_ertr::make_ready_future<lba_pin_list_t>(
	std::move(*result));
    });
}

LBAInternalNode::insert_ret LBAInternalNode::insert(
  op_context_t c,
  laddr_t laddr,
  lba_map_val_t val)
{
  auto insertion_pt = get_containing_child(laddr);
  return get_lba_btree_extent(
    c,
    get_meta().depth - 1,
    insertion_pt->get_val(),
    get_paddr()).safe_then(
      [this, insertion_pt, c, laddr, val=std::move(val)](
	auto extent) mutable {
	return extent->at_max_capacity() ?
	  split_entry(c, laddr, insertion_pt, extent) :
	  insert_ertr::make_ready_future<LBANodeRef>(std::move(extent));
      }).safe_then([c, laddr, val=std::move(val)](
		     LBANodeRef extent) mutable {
	return extent->insert(c, laddr, val);
      });
}

LBAInternalNode::mutate_mapping_ret LBAInternalNode::mutate_mapping(
  op_context_t c,
  laddr_t laddr,
  mutate_func_t &&f)
{
  return get_lba_btree_extent(
    c,
    get_meta().depth - 1,
    get_containing_child(laddr)->get_val(),
    get_paddr()
  ).safe_then([this, c, laddr](LBANodeRef extent) {
    if (extent->at_min_capacity()) {
      return merge_entry(
	c,
	laddr,
	get_containing_child(laddr),
	extent);
    } else {
      return merge_ertr::make_ready_future<LBANodeRef>(
	std::move(extent));
    }
  }).safe_then([c, laddr, f=std::move(f)](LBANodeRef extent) mutable {
    return extent->mutate_mapping(c, laddr, std::move(f));
  });
}

LBAInternalNode::find_hole_ret LBAInternalNode::find_hole(
  op_context_t c,
  laddr_t min,
  laddr_t max,
  extent_len_t len)
{
  logger().debug(
    "LBAInternalNode::find_hole min={}, max={}, len={}, *this={}",
    min, max, len, *this);
  auto bounds = bound(min, max);
  return seastar::do_with(
    bounds.first,
    bounds.second,
    L_ADDR_NULL,
    [this, c, len](auto &i, auto &e, auto &ret) {
      return crimson::do_until(
	[this, c, &i, &e, &ret, len] {
	  if (i == e) {
	    return find_hole_ertr::make_ready_future<std::optional<laddr_t>>(
	      std::make_optional<laddr_t>(L_ADDR_NULL));
	  }
	  return get_lba_btree_extent(
	    c,
	    get_meta().depth - 1,
	    i->get_val(),
	    get_paddr()
	  ).safe_then([c, &i, len](auto extent) mutable {
	    logger().debug(
	      "LBAInternalNode::find_hole extent {} lb {} ub {}",
	      *extent,
	      i->get_key(),
	      i->get_next_key_or_max());
	    return extent->find_hole(
	      c,
	      i->get_key(),
	      i->get_next_key_or_max(),
	      len);
	  }).safe_then([&i, &ret](auto addr) mutable {
	    i++;
	    if (addr != L_ADDR_NULL) {
	      ret = addr;
	    }
	    return find_hole_ertr::make_ready_future<std::optional<laddr_t>>(
	      addr == L_ADDR_NULL ? std::nullopt :
	      std::make_optional<laddr_t>(addr));
	  });
	}).safe_then([&ret]() {
	  return ret;
	});
    });
}


void LBAInternalNode::resolve_relative_addrs(paddr_t base) {
  for (auto i: *this) {
    if (i->get_val().is_relative()) {
      auto updated = base.add_relative(i->get_val());
      logger().debug(
	"LBAInternalNode::resolve_relative_addrs {} -> {}",
	i->get_val(),
	updated);
      i->set_val(updated);
    }
  }
}


LBAInternalNode::split_ret
LBAInternalNode::split_entry(
  op_context_t c,
  laddr_t addr,
  internal_iterator_t iter, LBANodeRef entry)
{
  if (!is_pending()) {
    auto mut = c.cache.duplicate_for_write(
      c.trans, this)->cast<LBAInternalNode>();
    auto mut_iter = mut->iter_idx(iter->get_offset());
    return mut->split_entry(c, addr, mut_iter, entry);
  }

  ceph_assert(!at_max_capacity());
  auto [left, right, pivot] = entry->make_split_children(c);

  journal_update(
    iter,
    maybe_generate_relative(left->get_paddr()),
    maybe_get_delta_buffer());
  journal_insert(
    iter + 1,
    pivot,
    maybe_generate_relative(right->get_paddr()),
    maybe_get_delta_buffer());

  c.cache.retire_extent(c.trans, entry);

  logger().debug(
    "LBAInternalNode::split_entry *this {} left {} right {}",
    *this,
    *left,
    *right);

  return split_ertr::make_ready_future<LBANodeRef>(
    pivot > addr ? left : right
  );
}

LBAInternalNode::merge_ret
LBAInternalNode::merge_entry(
  op_context_t c,
  laddr_t addr,
  internal_iterator_t iter, LBANodeRef entry)
{
  if (!is_pending()) {
    auto mut = c.cache.duplicate_for_write(c.trans, this)->cast<LBAInternalNode>();
    auto mut_iter = mut->iter_idx(iter->get_offset());
    return mut->merge_entry(c, addr, mut_iter, entry);
  }

  logger().debug(
    "LBAInternalNode: merge_entry: {}, {}",
    *this,
    *entry);
  auto donor_is_left = (iter + 1) == end();
  auto donor_iter = donor_is_left ? iter - 1 : iter + 1;
  return get_lba_btree_extent(
    c,
    get_meta().depth - 1,
    donor_iter->get_val(),
    get_paddr()
  ).safe_then([this, c, addr, iter, entry, donor_iter, donor_is_left](
		auto donor) mutable {
    auto [l, r] = donor_is_left ?
      std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = donor_is_left ?
      std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (donor->at_min_capacity()) {
      auto replacement = l->make_full_merge(
	c,
	r);

      journal_update(
	liter,
	maybe_generate_relative(replacement->get_paddr()),
	maybe_get_delta_buffer());
      journal_remove(riter, maybe_get_delta_buffer());

      c.cache.retire_extent(c.trans, l);
      c.cache.retire_extent(c.trans, r);
      return split_ertr::make_ready_future<LBANodeRef>(replacement);
    } else {
      logger().debug(
	"LBAInternalEntry::merge_entry balanced l {} r {}",
	*l,
	*r);
      auto [replacement_l, replacement_r, pivot] =
	l->make_balanced(
	  c,
	  r,
	  !donor_is_left);

      journal_update(
	liter,
	maybe_generate_relative(replacement_l->get_paddr()),
	maybe_get_delta_buffer());
      journal_replace(
	riter,
	pivot,
	maybe_generate_relative(replacement_r->get_paddr()),
	maybe_get_delta_buffer());

      c.cache.retire_extent(c.trans, l);
      c.cache.retire_extent(c.trans, r);
      return split_ertr::make_ready_future<LBANodeRef>(
	addr >= pivot ? replacement_r : replacement_l
      );
    }
  });
}


LBAInternalNode::internal_iterator_t
LBAInternalNode::get_containing_child(laddr_t laddr)
{
  // TODO: binary search
  for (auto i = begin(); i != end(); ++i) {
    if (i.contains(laddr))
      return i;
  }
  ceph_assert(0 == "invalid");
  return end();
}

std::ostream &LBALeafNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", meta=" << get_meta();
}

LBALeafNode::lookup_range_ret LBALeafNode::lookup_range(
  op_context_t c,
  laddr_t addr,
  extent_len_t len)
{
  logger().debug(
    "LBALeafNode::lookup_range {}~{}",
    addr,
    len);
  auto ret = lba_pin_list_t();
  auto [i, end] = get_leaf_entries(addr, len);
  for (; i != end; ++i) {
    auto val = (*i).get_val();
    ret.emplace_back(
      std::make_unique<BtreeLBAPin>(
	val.paddr,
	(*i).get_key(),
	val.len));
  }
  return lookup_range_ertr::make_ready_future<lba_pin_list_t>(
    std::move(ret));
}

LBALeafNode::insert_ret LBALeafNode::insert(
  op_context_t c,
  laddr_t laddr,
  lba_map_val_t val)
{
  ceph_assert(!at_max_capacity());

  if (!is_pending()) {
    return c.cache.duplicate_for_write(c.trans, this
    )->cast<LBALeafNode>()->insert(c, laddr, val);
  }

  val.paddr = maybe_generate_relative(val.paddr);
  logger().debug(
    "LBALeafNode::insert: inserting {}~{} -> {}",
    laddr,
    val.len,
    val.paddr);

  auto insert_pt = lower_bound(laddr);
  journal_insert(insert_pt, laddr, val, maybe_get_delta_buffer());

  logger().debug(
    "LBALeafNode::insert: inserted {}~{} -> {}",
    insert_pt.get_key(),
    insert_pt.get_val().len,
    insert_pt.get_val().paddr);
  return insert_ret(
    insert_ertr::ready_future_marker{},
    std::make_unique<BtreeLBAPin>(
      val.paddr,
      laddr,
      val.len));
}

LBALeafNode::mutate_mapping_ret LBALeafNode::mutate_mapping(
  op_context_t c,
  laddr_t laddr,
  mutate_func_t &&f)
{
  if (!is_pending()) {
    return c.cache.duplicate_for_write(c.trans, this)->cast<LBALeafNode>(
    )->mutate_mapping(
      c,
      laddr,
      std::move(f));
  }

  ceph_assert(!at_min_capacity());
  auto mutation_pt = find(laddr);
  if (mutation_pt == end()) {
    ceph_assert(0 == "should be impossible");
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      std::nullopt);
  }

  auto mutated = f(mutation_pt.get_val());
  if (mutated) {
    journal_update(mutation_pt, *mutated, maybe_get_delta_buffer());
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      mutated);
  } else {
    journal_remove(mutation_pt, maybe_get_delta_buffer());
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      mutated);
  }
}

LBALeafNode::find_hole_ret LBALeafNode::find_hole(
  op_context_t c,
  laddr_t min,
  laddr_t max,
  extent_len_t len)
{
  logger().debug(
    "LBALeafNode::find_hole min={} max={}, len={}, *this={}",
    min, max, len, *this);
  for (auto i = begin(); i != end(); ++i) {
    auto ub = i->get_key();
    if (min + len <= ub) {
      return find_hole_ret(
	find_hole_ertr::ready_future_marker{},
	min);
    } else {
      min = i->get_key() + i->get_val().len;
    }
  }
  if (min + len <= max) {
    return find_hole_ret(
      find_hole_ertr::ready_future_marker{},
      min);
  } else {
    return find_hole_ret(
      find_hole_ertr::ready_future_marker{},
      L_ADDR_MAX);
  }
}

void LBALeafNode::resolve_relative_addrs(paddr_t base) {
  for (auto i: *this) {
    if (i->get_val().paddr.is_relative()) {
      auto val = i->get_val();
      val.paddr = base.add_relative(val.paddr);
      logger().debug(
	"LBALeafNode::resolve_relative_addrs {} -> {}",
	i->get_val().paddr,
	val.paddr);
      i->set_val(val);
    }
  }
}

std::pair<LBALeafNode::internal_iterator_t, LBALeafNode::internal_iterator_t>
LBALeafNode::get_leaf_entries(laddr_t addr, extent_len_t len)
{
  return bound(addr, addr + len);
}

Cache::get_extent_ertr::future<LBANodeRef> get_lba_btree_extent(
  op_context_t c,
  depth_t depth,
  paddr_t offset,
  paddr_t base) {
  offset = offset.maybe_relative_to(base);
  ceph_assert(depth > 0);
  if (depth > 1) {
    logger().debug(
      "get_lba_btree_extent: reading internal at offset {}, depth {}",
      offset,
      depth);
    return c.cache.get_extent<LBAInternalNode>(
      c.trans,
      offset,
      LBA_BLOCK_SIZE).safe_then([depth](auto ret) {
	auto meta = ret->get_meta();
	if (ret->get_size()) {
	  ceph_assert(meta.begin <= ret->begin()->get_key());
	  ceph_assert(meta.end > (ret->end() - 1)->get_key());
	}
	return LBANodeRef(ret.detach(), /* add_ref = */ false);
      });
  } else {
    logger().debug(
      "get_lba_btree_extent: reading leaf at offset {}, depth {}",
      offset,
      depth);
    return c.cache.get_extent<LBALeafNode>(
      c.trans,
      offset,
      LBA_BLOCK_SIZE).safe_then([offset, depth](auto ret) {
	logger().debug(
	  "get_lba_btree_extent: read leaf at offset {}",
	  offset);
	auto meta = ret->get_meta();
	if (ret->get_size()) {
	  ceph_assert(meta.begin <= ret->begin()->get_key());
	  ceph_assert(meta.end > (ret->end() - 1)->get_key());
	}
	return LBANodeRef(ret.detach(), /* add_ref = */ false);
      });
  }
}

}
