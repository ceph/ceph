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
	     << ", depth=" << depth;
}

LBAInternalNode::lookup_range_ret LBAInternalNode::lookup_range(
  Cache &cache,
  Transaction &t,
  laddr_t addr,
  extent_len_t len)
{
  auto [begin, end] = bound(addr, addr + len);
  auto result_up = std::make_unique<lba_pin_list_t>();
  auto &result = *result_up;
  return crimson::do_for_each(
    std::move(begin),
    std::move(end),
    [this, &cache, &t, &result, addr, len](const auto &val) mutable {
      return get_lba_btree_extent(
	cache,
	t,
	depth-1,
	val.get_val(),
	get_paddr()).safe_then(
	  [&cache, &t, &result, addr, len](auto extent) mutable {
	    // TODO: add backrefs to ensure cache residence of parents
	    return extent->lookup_range(
	      cache,
	      t,
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
  Cache &cache,
  Transaction &t,
  laddr_t laddr,
  lba_map_val_t val)
{
  auto insertion_pt = get_containing_child(laddr);
  return get_lba_btree_extent(
    cache,
    t,
    depth-1,
    insertion_pt->get_val(),
    get_paddr()).safe_then(
      [this, insertion_pt, &cache, &t, laddr, val=std::move(val)](
	auto extent) mutable {
	return extent->at_max_capacity() ?
	  split_entry(cache, t, laddr, insertion_pt, extent) :
	  insert_ertr::make_ready_future<LBANodeRef>(std::move(extent));
      }).safe_then([&cache, &t, laddr, val=std::move(val)](
		     LBANodeRef extent) mutable {
	if (extent->depth == 0) {
	  auto mut_extent = cache.duplicate_for_write(t, extent);
	  extent = mut_extent->cast<LBANode>();
	}
	return extent->insert(cache, t, laddr, val);
      });
}

LBAInternalNode::mutate_mapping_ret LBAInternalNode::mutate_mapping(
  Cache &cache,
  Transaction &t,
  laddr_t laddr,
  mutate_func_t &&f)
{
  return get_lba_btree_extent(
    cache,
    t,
    depth-1,
    get_containing_child(laddr)->get_val(),
    get_paddr()
  ).safe_then([this, &cache, &t, laddr](LBANodeRef extent) {
    if (extent->at_min_capacity()) {
      auto mut_this = cache.duplicate_for_write(
	t, this)->cast<LBAInternalNode>();
      return mut_this->merge_entry(
	cache,
	t,
	laddr,
	mut_this->get_containing_child(laddr),
	extent);
    } else {
      return merge_ertr::make_ready_future<LBANodeRef>(
	std::move(extent));
    }
  }).safe_then([&cache, &t, laddr, f=std::move(f)](LBANodeRef extent) mutable {
    if (extent->depth == 0) {
      auto mut_extent = cache.duplicate_for_write(
	t, extent)->cast<LBANode>();
      return mut_extent->mutate_mapping(cache, t, laddr, std::move(f));
    } else {
      return extent->mutate_mapping(cache, t, laddr, std::move(f));
    }
  });
}

LBAInternalNode::find_hole_ret LBAInternalNode::find_hole(
  Cache &cache,
  Transaction &t,
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
    [this, &cache, &t, len](auto &i, auto &e, auto &ret) {
      return crimson::do_until(
	[this, &cache, &t, &i, &e, &ret, len] {
	  if (i == e) {
	    return find_hole_ertr::make_ready_future<std::optional<laddr_t>>(
	      std::make_optional<laddr_t>(L_ADDR_NULL));
	  }
	  return get_lba_btree_extent(
	    cache,
	    t,
	    depth-1,
	    i->get_val(),
	    get_paddr()
	  ).safe_then([&cache, &t, &i, len](auto extent) mutable {
	    logger().debug(
	      "LBAInternalNode::find_hole extent {} lb {} ub {}",
	      *extent,
	      i->get_key(),
	      i->get_next_key_or_max());
	    return extent->find_hole(
	      cache,
	      t,
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
  Cache &c, Transaction &t, laddr_t addr,
  internal_iterator_t iter, LBANodeRef entry)
{
  ceph_assert(!at_max_capacity());
  auto [left, right, pivot] = entry->make_split_children(c, t);

  journal_remove(iter->get_key());
  journal_insert(iter->get_key(), left->get_paddr());
  journal_insert(pivot, right->get_paddr());

  copy_from_local(iter + 1, iter, end());
  iter->set_val(maybe_generate_relative(left->get_paddr()));
  iter++;
  iter->set_key(pivot);
  iter->set_val(maybe_generate_relative(right->get_paddr()));
  set_size(get_size() + 1);

  c.retire_extent(t, entry);

  logger().debug(
    "LBAInternalNode::split_entry *this {} left {} right {}",
    *this,
    *left,
    *right);

  return split_ertr::make_ready_future<LBANodeRef>(
    pivot > addr ? left : right
  );
}

void LBAInternalNode::journal_remove(
  laddr_t to_remove)
{
  // TODO
}

void LBAInternalNode::journal_insert(
  laddr_t to_insert,
  paddr_t val)
{
  // TODO
}

LBAInternalNode::merge_ret
LBAInternalNode::merge_entry(
  Cache &c, Transaction &t, laddr_t addr,
  internal_iterator_t iter, LBANodeRef entry)
{
  logger().debug(
    "LBAInternalNode: merge_entry: {}, {}",
    *this,
    *entry);
  auto donor_is_left = (iter + 1) == end();
  auto donor_iter = donor_is_left ? iter - 1 : iter + 1;
  return get_lba_btree_extent(
    c,
    t,
    depth - 1,
    donor_iter->get_val(),
    get_paddr()
  ).safe_then([this, &c, &t, addr, iter, entry, donor_iter, donor_is_left](
		auto donor) mutable {
    auto [l, r] = donor_is_left ?
      std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = donor_is_left ?
      std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (donor->at_min_capacity()) {
      auto replacement = l->make_full_merge(
	c,
	t,
	r);

      journal_remove(riter->get_key());
      journal_remove(liter->get_key());
      journal_insert(liter->get_key(), replacement->get_paddr());

      liter->set_val(maybe_generate_relative(replacement->get_paddr()));
      copy_from_local(riter, riter + 1, end());
      set_size(get_size() - 1);

      c.retire_extent(t, l);
      c.retire_extent(t, r);
      return split_ertr::make_ready_future<LBANodeRef>(replacement);
    } else {
      logger().debug(
	"LBAInternalEntry::merge_entry balanced l {} r {}",
	*l,
	*r);
      auto [replacement_l, replacement_r, pivot] =
	l->make_balanced(
	  c,
	  t,
	  r,
	  !donor_is_left);

      journal_remove(liter->get_key());
      journal_remove(riter->get_key());
      journal_insert(liter->get_key(), replacement_l->get_paddr());
      journal_insert(pivot, replacement_r->get_paddr());

      liter->set_val(
	maybe_generate_relative(replacement_l->get_paddr()));
      riter->set_key(pivot);
      riter->set_val(
	maybe_generate_relative(replacement_r->get_paddr()));

      c.retire_extent(t, l);
      c.retire_extent(t, r);
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
	     << ", depth=" << depth;
}

LBALeafNode::lookup_range_ret LBALeafNode::lookup_range(
  Cache &cache,
  Transaction &t,
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
  Cache &cache,
  Transaction &transaction,
  laddr_t laddr,
  lba_map_val_t val)
{
  ceph_assert(!at_max_capacity());
  auto insert_pt = upper_bound(laddr);
  if (insert_pt != end()) {
    copy_from_local(insert_pt + 1, insert_pt, end());
  }
  set_size(get_size() + 1);
  insert_pt.set_key(laddr);
  val.paddr = maybe_generate_relative(val.paddr);
  logger().debug(
    "LBALeafNode::insert: inserting {}~{} -> {}",
    laddr,
    val.len,
    val.paddr);
  insert_pt.set_val(val);
  logger().debug(
    "LBALeafNode::insert: inserted {}~{} -> {}",
    insert_pt.get_key(),
    insert_pt.get_val().len,
    insert_pt.get_val().paddr);
  journal_insertion(laddr, val);
  return insert_ret(
    insert_ertr::ready_future_marker{},
    std::make_unique<BtreeLBAPin>(
      val.paddr,
      laddr,
      val.len));
}

LBALeafNode::mutate_mapping_ret LBALeafNode::mutate_mapping(
  Cache &cache,
  Transaction &transaction,
  laddr_t laddr,
  mutate_func_t &&f)
{
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
    mutation_pt.set_val(*mutated);
    journal_mutated(laddr, *mutated);
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      mutated);
  } else {
    journal_removal(laddr);
    copy_from_local(mutation_pt, mutation_pt + 1, end());
    set_size(get_size() - 1);
    return mutate_mapping_ret(
      mutate_mapping_ertr::ready_future_marker{},
      mutated);
  }
}

void LBALeafNode::journal_mutated(
  laddr_t laddr,
  lba_map_val_t val)
{
  // TODO
}

void LBALeafNode::journal_insertion(
  laddr_t laddr,
  lba_map_val_t val)
{
  // TODO
}

void LBALeafNode::journal_removal(
  laddr_t laddr)
{
  // TODO
}

LBALeafNode::find_hole_ret LBALeafNode::find_hole(
  Cache &cache,
  Transaction &t,
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
  Cache &cache,
  Transaction &t,
  depth_t depth,
  paddr_t offset,
  paddr_t base) {
  offset = offset.maybe_relative_to(base);
  if (depth > 0) {
    logger().debug(
      "get_lba_btree_extent: reading internal at offset {}, depth {}",
      offset,
      depth);
    return cache.get_extent<LBAInternalNode>(
      t,
      offset,
      LBA_BLOCK_SIZE).safe_then([depth](auto ret) {
	ret->set_depth(depth);
	return LBANodeRef(ret.detach(), /* add_ref = */ false);
      });

  } else {
    logger().debug(
      "get_lba_btree_extent: reading leaf at offset {}, depth {}",
      offset,
      depth);
    return cache.get_extent<LBALeafNode>(
      t,
      offset,
      LBA_BLOCK_SIZE).safe_then([offset, depth](auto ret) {
	logger().debug(
	  "get_lba_btree_extent: read leaf at offset {}",
	  offset);
	ret->set_depth(depth);
	return LBANodeRef(ret.detach(), /* add_ref = */ false);
      });
  }
}

}
