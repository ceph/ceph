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
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

std::ostream &LBAInternalNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", meta=" << get_meta();
}

LBAInternalNode::lookup_ret LBAInternalNode::lookup(
  op_context_t c,
  laddr_t addr,
  depth_t depth)
{
  auto meta = get_meta();
  if (depth == get_meta().depth) {
    return lookup_ret(
      interruptible::ready_future_marker{},
      this);
  }
  assert(meta.begin <= addr);
  assert(meta.end > addr);

  [[maybe_unused]] auto [iter, biter] = bound(addr, addr + 1);
  assert(iter != biter);
  assert(iter + 1 == biter);
  return get_lba_btree_extent(
    c,
    this,
    meta.depth - 1,
    iter->get_val(),
    get_paddr()).si_then([c, addr, depth](auto child) {
      return child->lookup(c, addr, depth);
    }).finally([ref=LBANodeRef(this)] {});
}

LBAInternalNode::lookup_range_ret LBAInternalNode::lookup_range(
  op_context_t c,
  laddr_t addr,
  extent_len_t len)
{
  auto [begin, end] = bound(addr, addr + len);
  auto result_up = std::make_unique<lba_pin_list_t>();
  auto &result = *result_up;
  return trans_intr::do_for_each(
    std::move(begin),
    std::move(end),
    [this, c, &result, addr, len](const auto &val) {
      return get_lba_btree_extent(
	c,
	this,
	get_meta().depth - 1,
	val.get_val(),
	get_paddr()).si_then(
	  [c, &result, addr, len](auto extent) {
	    return extent->lookup_range(
	      c,
	      addr,
	      len).si_then(
		[&result](auto pin_list) {
		  result.splice(result.end(), pin_list,
				pin_list.begin(), pin_list.end());
		});
	  });
    }).si_then([result=std::move(result_up), ref=LBANodeRef(this)] {
      return lookup_range_iertr::make_ready_future<lba_pin_list_t>(
	std::move(*result));
    });
}

LBAInternalNode::lookup_pin_ret LBAInternalNode::lookup_pin(
  op_context_t c,
  laddr_t addr)
{
  auto iter = get_containing_child(addr);
  return get_lba_btree_extent(
    c,
    this,
    get_meta().depth - 1,
    iter->get_val(),
    get_paddr()
  ).si_then([c, addr](LBANodeRef extent) {
    return extent->lookup_pin(c, addr);
  }).finally([ref=LBANodeRef(this)] {});
}

LBAInternalNode::insert_ret LBAInternalNode::insert(
  op_context_t c,
  laddr_t laddr,
  lba_map_val_t val)
{
  auto insertion_pt = get_containing_child(laddr);
  return get_lba_btree_extent(
    c,
    this,
    get_meta().depth - 1,
    insertion_pt->get_val(),
    get_paddr()).si_then(
      [this, insertion_pt, c, laddr, val=std::move(val)](
	auto extent) mutable {
	return extent->at_max_capacity() ?
	  split_entry(c, laddr, insertion_pt, extent) :
	  insert_iertr::make_ready_future<LBANodeRef>(std::move(extent));
      }).si_then([c, laddr, val=std::move(val)](
		     LBANodeRef extent) mutable {
	return extent->insert(c, laddr, val);
      });
}

LBAInternalNode::mutate_mapping_ret LBAInternalNode::mutate_mapping(
  op_context_t c,
  laddr_t laddr,
  mutate_func_t &&f)
{
  return mutate_mapping_internal(c, laddr, true, std::move(f));
}

LBAInternalNode::mutate_mapping_ret LBAInternalNode::mutate_mapping_internal(
  op_context_t c,
  laddr_t laddr,
  bool is_root,
  mutate_func_t &&f)
{
  auto mutation_pt = get_containing_child(laddr);
  if (mutation_pt == end()) {
    assert(0 == "impossible");
    return crimson::ct_error::enoent::make();
  }
  return get_lba_btree_extent(
    c,
    this,
    get_meta().depth - 1,
    mutation_pt->get_val(),
    get_paddr()
  ).si_then([=](LBANodeRef extent) {
    if (extent->at_min_capacity() && get_size() > 1) {
      return merge_entry(
	c,
	laddr,
	mutation_pt,
	extent,
	is_root);
    } else {
      return merge_iertr::make_ready_future<LBANodeRef>(
	std::move(extent));
    }
  }).si_then([c, laddr, f=std::move(f)](LBANodeRef extent) mutable {
    return extent->mutate_mapping_internal(c, laddr, false, std::move(f));
  });
}

LBAInternalNode::mutate_internal_address_ret LBAInternalNode::mutate_internal_address(
  op_context_t c,
  depth_t depth,
  laddr_t laddr,
  paddr_t paddr)
{
  if (get_meta().depth == (depth + 1)) {
    if (!is_pending()) {
      return c.cache.duplicate_for_write(c.trans, this)->cast<LBAInternalNode>(
      )->mutate_internal_address(
	c,
	depth,
	laddr,
	paddr);
    }
    auto iter = get_containing_child(laddr);
    if (iter->get_key() != laddr) {
      logger().debug(
	"LBAInternalNode::mutate_internal_address laddr {} "
	"not found in extent {}",
	laddr,
	*this);
      return crimson::ct_error::enoent::make();
    }

    auto old_paddr = iter->get_val();

    journal_update(
      iter,
      maybe_generate_relative(paddr),
      maybe_get_delta_buffer());

    return mutate_internal_address_ret(
      interruptible::ready_future_marker{},
      old_paddr
    );
  } else {
    auto iter = get_containing_child(laddr);
    return get_lba_btree_extent(
      c,
      this,
      get_meta().depth - 1,
      iter->get_val(),
      get_paddr()
    ).si_then([=](auto node) {
      return node->mutate_internal_address(
	c,
	depth,
	laddr,
	paddr);
    });
  }
}

LBAInternalNode::find_hole_ret LBAInternalNode::find_hole(
  op_context_t c,
  laddr_t min_addr,
  laddr_t max_addr,
  extent_len_t len)
{
  logger().debug(
    "LBAInternalNode::find_hole min={}, max={}, len={}, *this={}",
    min_addr, max_addr, len, *this);
  auto [begin, end] = bound(min_addr, max_addr);
  return seastar::do_with(
    begin,
    L_ADDR_NULL,
    [this, c, min_addr, len, end=end](auto &i, auto &ret) {
      return trans_intr::repeat([=, &i, &ret]()
        -> find_hole_iertr::future<seastar::stop_iteration> {
	if (i == end) {
	  return seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::yes);
	}
	return get_lba_btree_extent(
	  c,
	  this,
	  get_meta().depth - 1,
	  i->get_val(),
	  get_paddr()
	).si_then([=, &i](auto extent) mutable {
	  auto lb = std::max(min_addr, i->get_key());
	  auto ub = i->get_next_key_or_max();
	  logger().debug("LBAInternalNode::find_hole extent {} lb {} ub {}",
			 *extent, lb, ub);
	  return extent->find_hole(c, lb, ub, len);
	}).si_then([&i, &ret](auto addr) {
	  if (addr == L_ADDR_NULL) {
	    ++i;
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::no);
	  } else {
	    ret = addr;
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::yes);
	  }
	});
	}).si_then([&ret, ref=LBANodeRef(this)]() {
	  return ret;
	});
    });
}

LBAInternalNode::scan_mappings_ret LBAInternalNode::scan_mappings(
  op_context_t c,
  laddr_t begin,
  laddr_t end,
  scan_mappings_func_t &f)
{
  auto [biter, eiter] = bound(begin, end);
  return trans_intr::do_for_each(
    std::move(biter),
    std::move(eiter),
    [=, &f](auto &viter) {
      return get_lba_btree_extent(
	c,
	this,
	get_meta().depth - 1,
	viter->get_val(),
	get_paddr()).si_then([=, &f](auto child) {
	  return child->scan_mappings(c, begin, end, f);
	});
    }).si_then([ref=LBANodeRef(this)] {});
}

LBAInternalNode::scan_mapped_space_ret LBAInternalNode::scan_mapped_space(
  op_context_t c,
  scan_mapped_space_func_t &f)
{
  f(get_paddr(), get_length());
  return trans_intr::do_for_each(
    begin(), end(),
    [=, &f](auto &viter) {
      return get_lba_btree_extent(
	c,
	this,
	get_meta().depth - 1,
	viter->get_val(),
	get_paddr()).si_then([=, &f](auto child) {
	  return child->scan_mapped_space(c, f);
	});
    }).si_then([ref=LBANodeRef(this)]{});
}


void LBAInternalNode::resolve_relative_addrs(paddr_t base)
{
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
    "LBAInternalNode::split_entry *this {} entry {} into left {} right {}",
    *this,
    *entry,
    *left,
    *right);

  return split_iertr::make_ready_future<LBANodeRef>(
    pivot > addr ? left : right
  );
}

LBAInternalNode::merge_ret
LBAInternalNode::merge_entry(
  op_context_t c,
  laddr_t addr,
  internal_iterator_t iter,
  LBANodeRef entry,
  bool is_root)
{
  if (!is_pending()) {
    auto mut = c.cache.duplicate_for_write(c.trans, this)->cast<LBAInternalNode>();
    auto mut_iter = mut->iter_idx(iter->get_offset());
    return mut->merge_entry(c, addr, mut_iter, entry, is_root);
  }

  logger().debug(
    "LBAInternalNode: merge_entry: {}, {}",
    *this,
    *entry);
  auto donor_is_left = (iter + 1) == end();
  auto donor_iter = donor_is_left ? iter - 1 : iter + 1;
  return get_lba_btree_extent(
    c,
    this,
    get_meta().depth - 1,
    donor_iter->get_val(),
    get_paddr()
  ).si_then([=](auto donor) mutable {
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

      if (is_root && get_size() == 1) {
	return c.cache.get_root(c.trans).si_then([=](RootBlockRef croot) {
	  {
	    auto mut_croot = c.cache.duplicate_for_write(c.trans, croot);
	    croot = mut_croot->cast<RootBlock>();
	  }
	  auto new_root_addr = begin()->get_val().maybe_relative_to(get_paddr());
	  croot->get_root().lba_root = lba_root_t{
	    new_root_addr,
	    get_meta().depth - 1};
	  logger().debug(
	    "LBAInternalNode::merge_entry: collapsing root {} to addr {}",
	    *this,
	    new_root_addr);
	  c.cache.retire_extent(c.trans, this);
	  return merge_iertr::make_ready_future<LBANodeRef>(replacement);
	});
      } else {
	return merge_iertr::make_ready_future<LBANodeRef>(replacement);
      }
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
      return merge_iertr::make_ready_future<LBANodeRef>(
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
    auto val = i->get_val();
    auto begin = i->get_key();
    ret.emplace_back(
      std::make_unique<BtreeLBAPin>(
	this,
	val.paddr.maybe_relative_to(get_paddr()),
	lba_node_meta_t{ begin, begin + val.len, 0}));
  }
  return lookup_range_iertr::make_ready_future<lba_pin_list_t>(
    std::move(ret));
}

LBALeafNode::lookup_pin_ret LBALeafNode::lookup_pin(
  op_context_t c,
  laddr_t addr)
{
  logger().debug("LBALeafNode::lookup_pin {}", addr);
  auto iter = find(addr);
  if (iter == end()) {
    return crimson::ct_error::enoent::make();
  }
  auto val = iter->get_val();
  auto begin = iter->get_key();
  return lookup_pin_ret(
    interruptible::ready_future_marker{},
    std::make_unique<BtreeLBAPin>(
      this,
      val.paddr.maybe_relative_to(get_paddr()),
      lba_node_meta_t{ begin, begin + val.len, 0}));
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
  auto begin = insert_pt.get_key();
  return insert_ret(
    interruptible::ready_future_marker{},
    std::make_unique<BtreeLBAPin>(
      this,
      val.paddr.maybe_relative_to(get_paddr()),
      lba_node_meta_t{ begin, begin + val.len, 0}));
}

LBALeafNode::mutate_mapping_ret LBALeafNode::mutate_mapping(
  op_context_t c,
  laddr_t laddr,
  mutate_func_t &&f)
{
  return mutate_mapping_internal(c, laddr, true, std::move(f));
}

LBALeafNode::mutate_mapping_ret LBALeafNode::mutate_mapping_internal(
  op_context_t c,
  laddr_t laddr,
  bool is_root,
  mutate_func_t &&f)
{
  auto mutation_pt = find(laddr);
  if (mutation_pt == end()) {
    return crimson::ct_error::enoent::make();
  }

  if (!is_pending()) {
    return c.cache.duplicate_for_write(c.trans, this)->cast<LBALeafNode>(
    )->mutate_mapping_internal(
      c,
      laddr,
      is_root,
      std::move(f));
  }

  auto cur = mutation_pt.get_val();
  auto mutated = f(cur);

  mutated.paddr = maybe_generate_relative(mutated.paddr);

  logger().debug(
    "{}: mutate addr {}: {} -> {}",
    __func__,
    laddr,
    cur.paddr,
    mutated.paddr);

  if (mutated.refcount > 0) {
    journal_update(mutation_pt, mutated, maybe_get_delta_buffer());
    return mutate_mapping_ret(
      interruptible::ready_future_marker{},
      mutated);
  } else {
    journal_remove(mutation_pt, maybe_get_delta_buffer());
    return mutate_mapping_ret(
      interruptible::ready_future_marker{},
      mutated);
  }
}

LBALeafNode::mutate_internal_address_ret LBALeafNode::mutate_internal_address(
  op_context_t c,
  depth_t depth,
  laddr_t laddr,
  paddr_t paddr)
{
  ceph_assert(0 == "Impossible");
  return mutate_internal_address_ret(
    interruptible::ready_future_marker{},
    paddr);
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
  auto [liter, uiter] = bound(min, max);
  for (auto i = liter; i != uiter; ++i) {
    auto ub = i->get_key();
    if (min + len <= ub) {
      return find_hole_ret(
	interruptible::ready_future_marker{},
	min);
    } else {
      min = i->get_key() + i->get_val().len;
    }
  }
  if (min + len <= max) {
    return find_hole_ret(
      interruptible::ready_future_marker{},
      min);
  } else {
    return find_hole_ret(
      interruptible::ready_future_marker{},
      L_ADDR_MAX);
  }
}

LBALeafNode::scan_mappings_ret LBALeafNode::scan_mappings(
  op_context_t c,
  laddr_t begin,
  laddr_t end,
  scan_mappings_func_t &f)
{
  auto [biter, eiter] = bound(begin, end);
  for (auto i = biter; i != eiter; ++i) {
    auto val = i->get_val();
    f(i->get_key(), val.paddr, val.len);
  }
  return scan_mappings_iertr::now();
}

LBALeafNode::scan_mapped_space_ret LBALeafNode::scan_mapped_space(
  op_context_t c,
  scan_mapped_space_func_t &f)
{
  f(get_paddr(), get_length());
  for (auto i = begin(); i != end(); ++i) {
    auto val = i->get_val();
    f(val.paddr, val.len);
  }
  return scan_mappings_iertr::now();
}


void LBALeafNode::resolve_relative_addrs(paddr_t base)
{
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

get_lba_node_ret get_lba_btree_extent(
  op_context_t c,
  CachedExtentRef parent,
  depth_t depth,
  paddr_t offset,
  paddr_t base)
{
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
      LBA_BLOCK_SIZE).si_then([c, parent](auto ret)
				-> get_lba_node_ret {
	auto meta = ret->get_meta();
	if (ret->get_size()) {
	  ceph_assert(meta.begin <= ret->begin()->get_key());
	  ceph_assert(meta.end > (ret->end() - 1)->get_key());
	}
	if (parent->has_been_invalidated() || ret->has_been_invalidated()) {
	  logger().debug(
	    "get_lba_btree_extent: parent {} or ret {} is invalid, transaction {} is conflicted: {}",
	    *parent,
	    *ret,
	    (void*)&c.trans,
	    c.trans.is_conflicted());
	  assert(!(parent->has_been_invalidated() || ret->has_been_invalidated()));
	}
	if (!ret->is_pending() && !ret->pin.is_linked()) {
	  ret->pin.set_range(meta);
	  c.pins.add_pin(ret->pin);
	}
	return get_lba_node_ret(
	  interruptible::ready_future_marker{},
	  LBANodeRef(ret.detach(), /* add_ref = */ false));
      });
  } else {
    logger().debug(
      "get_lba_btree_extent: reading leaf at offset {}, depth {}",
      offset,
      depth);
    return c.cache.get_extent<LBALeafNode>(
      c.trans,
      offset,
      LBA_BLOCK_SIZE).si_then([offset, c, parent](auto ret)
				-> get_lba_node_ret {
	logger().debug(
	  "get_lba_btree_extent: read leaf at offset {} {}, parent {}",
	  offset,
	  *ret,
	  *parent);
	auto meta = ret->get_meta();
	if (ret->get_size()) {
	  ceph_assert(meta.begin <= ret->begin()->get_key());
	  ceph_assert(meta.end > (ret->end() - 1)->get_key());
	}
	if (parent->has_been_invalidated() || ret->has_been_invalidated()) {
	  logger().debug(
	    "get_lba_btree_extent: parent {} or ret {} is invalid, transaction {} is conflicted: {}",
	    *parent,
	    *ret,
	    (void*)&c.trans,
	    c.trans.is_conflicted());
	  assert(!(parent->has_been_invalidated() || ret->has_been_invalidated()));
	}
	if (!ret->is_pending() && !ret->pin.is_linked()) {
	  ret->pin.set_range(meta);
	  c.pins.add_pin(ret->pin);
	}
	return get_lba_node_ret(
	  interruptible::ready_future_marker{},
	  LBANodeRef(ret.detach(), /* add_ref = */ false));
      });
  }
}

}
