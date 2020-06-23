// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node_impl.h"

#include "test/crimson/seastore/test_block.h"   //for temporary
namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::extentmap_manager {

std::ostream &ExtMapInnerNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << depth;
}
ExtMapInnerNode::seek_lextent_ret
ExtMapInnerNode::seek_lextent(Transaction &t, objaddr_t lo, extent_len_t len)
{
  auto [begin, end] = bound(lo, lo + len);
  auto result_up = std::make_unique<extent_map_list_t>();
  auto &result = *result_up;
  return crimson::do_for_each(
    std::move(begin),
    std::move(end),
    [this, &t, &result, lo, len](const auto &val) mutable {
      return extmap_load_extent(tm, t, val.get_val(), depth - 1).safe_then(
	  [&t, &result, lo, len](auto extent) mutable {
	    // TODO: add backrefs to ensure cache residence of parents
	    return extent->seek_lextent(t, lo, len).safe_then(
		[&t, &result, lo, len](auto item_list) mutable {
		  result.splice(result.end(), item_list,
				item_list.begin(), item_list.end());
            });
      });
  }).safe_then([result=std::move(result_up)] {
    return seek_lextent_ret(
           seek_lextent_ertr::ready_future_marker{},
           std::move(*result));
  });
}

ExtMapInnerNode::insert_ret
ExtMapInnerNode::insert(Transaction &t, objaddr_t lo, lext_map_val_t val)
{
  auto insertion_pt = get_containing_child(lo);
  return extmap_load_extent(tm, t, insertion_pt->get_val(), depth-1).safe_then(
    [this, insertion_pt, &t, lo, val=std::move(val)](auto extent) mutable {
      return extent->at_max_capacity() ?
        split_entry(t, lo, insertion_pt, extent) :
        insert_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
    }).safe_then([this, &t, lo, val=std::move(val)](ExtMapNodeRef extent) mutable {
      return extent->insert(t, lo, val);
    });
}

ExtMapInnerNode::punch_lextent_ret
ExtMapInnerNode::punch_lextent(Transaction &t, objaddr_t lo, extent_len_t len)
{
  auto result_up = std::make_unique<extent_map_list_t>();
  auto &result = *result_up;
  auto [begin, end] = bound(lo, lo + len);
  logger().debug("ExtMapInnerNode::punch_lextent {} {}", lo, len);
  return crimson::do_for_each(
    std::move(begin),
    std::move(end),
    [this, &t, &result, lo, len](auto &punch_pt) mutable {
    return extmap_load_extent(tm, t, punch_pt->get_val(), depth-1).safe_then(
      [this, &result, punch_pt, &t, lo, len](auto extent) mutable {
      return extent->at_max_capacity() ?
        split_entry(t, lo, punch_pt, extent) :
        punch_lextent_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
    }).safe_then([this, &result, &t, lo, len](ExtMapNodeRef extent) mutable {
      return extent->punch_lextent(t, lo, len).safe_then(
        [&result](auto item_list) mutable {
        result.splice(result.end(), item_list, item_list.begin(), item_list.end());
      });
    });
  }).safe_then([result=std::move(result_up)] {
    return punch_lextent_ret(
           punch_lextent_ertr::ready_future_marker{},
           std::move(*result));
  });

}

ExtMapInnerNode::rm_lextent_ret
ExtMapInnerNode::rm_lextent(Transaction &t, objaddr_t lo, lext_map_val_t val)
{
  auto rm_pt = get_containing_child(lo);
  return extmap_load_extent(tm, t, rm_pt->get_val(), depth-1).safe_then(
    [this, rm_pt, &t, lo, val=std::move(val)](auto extent) mutable {
    if (extent->at_min_capacity()) {
      return merge_entry(t, lo, get_containing_child(lo), extent);
    } else {
      return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
    }
  }).safe_then([this, &t, lo, val](ExtMapNodeRef extent) mutable {
    return extent->rm_lextent(t, lo, val);
  });
}

ExtMapInnerNode::find_hole_ret
ExtMapInnerNode::find_hole(Transaction &t, objaddr_t lo, extent_len_t len)
{
  logger().debug("ExtMapInnerNode::find_hole for logic_offset={}, len={}, *this={}",
    lo, len, *this);
  auto bounds = bound(lo, lo + len);
  return seastar::do_with(
    bounds.first,
    bounds.second,
    lext_map_val_t{L_ADDR_NULL, 0, 0},
    [this, &t, lo, len](auto &i, auto &e, auto &ret) {
      return crimson::do_until(
	[this, &t, &i, &e, &ret, lo, len] {
	  if (i == e + 1) {
	    return find_hole_ertr::make_ready_future<ExtentRef>(
			    std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0));
	  }
	  return extmap_load_extent(tm, t, i->get_val(), depth-1)
	    .safe_then([this, &t, &i, lo, len](auto extent) mutable {
	    logger().debug(
	      "ExtMapInnerNode::find_hole in {} for lo {} len {} ",
	      *extent, lo, len);
	    return extent->find_hole(t, lo, len);
	  }).safe_then([lo, &i, &ret](auto ext_map) mutable {
	    i++;
	    if (ext_map->laddr != L_ADDR_NULL) {
	      ret.laddr = ext_map->laddr;
	      ret.lextent_offset = ext_map->lextent_offset;
	      ret.length = ext_map->length;
	    }
	    return find_hole_ertr::make_ready_future<ExtentRef>(
	           std::make_unique<Extent>(lo, ret.laddr,
				   ret.lextent_offset, ret.length));
	  });
        }).safe_then([lo, &ret]() {
          return find_hole_ertr::make_ready_future<ExtentRef>(
                 std::make_unique<Extent>(lo, ret.laddr,
                                 ret.lextent_offset, ret.length));
	});
    });
}

ExtMapInnerNode::split_children_ret
ExtMapInnerNode::make_split_children(Transaction &t)
{
  return extmap_alloc_extent<ExtMapInnerNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t] (auto &&left) {
      return extmap_alloc_extent<ExtMapInnerNode>(t,  EXTMAP_BLOCK_SIZE)
        .safe_then([this, left = std::move(left)] (auto &&right) {
          return split_children_ret(
            split_children_ertr::ready_future_marker{},
            std::make_tuple(left, right, split_into(*left, *right)));
      });
  });
}

ExtMapInnerNode::full_merge_ret
ExtMapInnerNode::make_full_merge(Transaction &t, ExtMapNodeRef right)
{
  return extmap_alloc_extent<ExtMapInnerNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<ExtMapInnerNode>());
      return full_merge_ret(
        full_merge_ertr::ready_future_marker{},
        std::move(replacement));
  });
}

ExtMapInnerNode::make_balanced_ret
ExtMapInnerNode::make_balanced(Transaction &t,
                 ExtMapNodeRef _right, bool prefer_left)
{
  ceph_assert(_right->get_type() == type);
  return extmap_alloc_extent<ExtMapInnerNode>(t,  EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t, _right, prefer_left] (auto &&replacement_left){
      return extmap_alloc_extent<ExtMapInnerNode>(t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, _right, prefer_left,
	replacement_left = std::move(replacement_left)]
                   (auto &&replacement_right) {
          auto &right = *_right->cast<ExtMapInnerNode>();
          return make_balanced_ret(
            make_balanced_ertr::ready_future_marker{},
            std::make_tuple(replacement_left, replacement_right,
            balance_into_new_nodes(*this, right, prefer_left,
                                   *replacement_left, *replacement_right)));
      });
  });
}
ExtMapInnerNode::split_entry_ret
ExtMapInnerNode::split_entry(Transaction &t, objaddr_t lo,
  internal_iterator_t iter, ExtMapNodeRef entry)
{
  if (!is_pending()) {
    auto mut = make_duplicate(t, this)->cast<ExtMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_offset());
    return mut->split_entry(t, lo, mut_iter, entry);
  }
  ceph_assert(!at_max_capacity());
  return entry->make_split_children(t)
    .safe_then([this, &t, lo, iter, entry] (auto tuple){
    auto [left, right, pivot] = tuple;
    journal_update(iter, left->get_laddr(), maybe_get_delta_buffer());
    journal_insert(iter + 1, pivot, right->get_laddr(), maybe_get_delta_buffer());
    //retire extent
    return tm->dec_ref(t, entry->get_laddr())
      .safe_then([this, lo, left = std::move(left), right = std::move(right), pivot]
      (auto ret) {
      logger().debug(
        "ExtMapInnerNode::split_entry *this {} left {} right {}",
        *this, *left, *right);

      return split_entry_ertr::make_ready_future<ExtMapNodeRef>(
             pivot > lo ? left : right);
    });
  });
}

ExtMapInnerNode::merge_entry_ret
ExtMapInnerNode::merge_entry(Transaction &t, objaddr_t lo,
  internal_iterator_t iter, ExtMapNodeRef entry)
{
  if (!is_pending()) {
    auto mut = make_duplicate(t, this)->cast<ExtMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_offset());
    return mut->merge_entry(t, lo, mut_iter, entry);
  }
  logger().debug("ExtMapInnerNode: merge_entry: {}, {}", *this, *entry);
  auto is_left = (iter + 1) == end();
  auto donor_iter = is_left ? iter - 1 : iter + 1;
  return extmap_load_extent(tm, t, donor_iter->get_val(), depth-1)
    .safe_then([this, &t, lo, iter, entry, donor_iter, is_left]
      (auto &&donor) mutable {
    auto [l, r] = is_left ?
      std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = is_left ?
      std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (donor->at_min_capacity()) {
      return l->make_full_merge(t, r)
        .safe_then([this, &t, lo, iter, entry, donor_iter, is_left, l, r, liter, riter]
	(auto &&replacement){
        journal_update(liter, replacement->get_laddr(), maybe_get_delta_buffer());
        journal_remove(riter, maybe_get_delta_buffer());

        //retire extent
        return tm->dec_ref(t, l->get_laddr())
          .safe_then([this, r, replacement, &t] (auto ret) {
	  return tm->dec_ref(t, r->get_laddr())
            .safe_then([replacement] (auto ret) {
            return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(replacement);
          });
	});
      });
    } else {
      logger().debug("ExtMapInnerNode::merge_entry balanced l {} r {}",
	*l, *r);
      return l->make_balanced(t, r, !is_left)
	.safe_then([this, &t, lo, iter, entry, donor_iter, is_left, l, r, liter, riter]
	(auto tuple) {
	auto [replacement_l, replacement_r, pivot] = tuple;
        journal_update(liter, replacement_l->get_laddr(), maybe_get_delta_buffer());
        journal_replace(riter, pivot, replacement_r->get_laddr(),
                maybe_get_delta_buffer());


        // retire extent
        return tm->dec_ref(t, l->get_laddr())
          .safe_then([this, &t, lo, r, pivot, replacement_l, replacement_r] (auto ret) {
          return tm->dec_ref(t, r->get_laddr())
            .safe_then([lo, pivot, replacement_l, replacement_r] (auto ret) {
            return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(
               lo >= pivot ? replacement_r : replacement_l);
          });
        });
      });
    }
  });
}


ExtMapInnerNode::internal_iterator_t
ExtMapInnerNode::get_containing_child(objaddr_t lo)
{
  // TODO: binary search
  for (auto i = begin(); i != end(); ++i) {
    if (i.contains(lo))
      return i;
  }
  ceph_assert(0 == "invalid");
  return end();
}

std::ostream &ExtMapLeafNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << depth;
}

ExtMapLeafNode::seek_lextent_ret
ExtMapLeafNode::seek_lextent(Transaction &t, objaddr_t lo, extent_len_t len)
{
  logger().debug(
    "ExtMapLeafNode::seek_lextent {}~{}", lo, len);
  auto ret = extent_map_list_t();
  auto [from, to] = get_leaf_entries(lo, len);
  if (from == to && to != end())
    ++to;
  for (; from != to; ++from) {
    auto val = (*from).get_val();
    ret.emplace_back(
      std::make_unique<Extent>(
	(*from).get_key(),
	val.laddr,
	val.lextent_offset,
	val.length));
    logger().debug("ExtMapLeafNode::seek_lextent find {}~{}", lo, val.laddr);
  }
  return seek_lextent_ertr::make_ready_future<extent_map_list_t>(
    std::move(ret));
}

ExtMapLeafNode::insert_ret
ExtMapLeafNode::insert(Transaction &t, objaddr_t lo, lext_map_val_t val)
{
  ceph_assert(!at_max_capacity());
  if (!is_pending()) {
    auto mut = make_duplicate(t, this)->cast<ExtMapLeafNode>();
    return mut->insert(t, lo, val);
  }
  auto insert_pt = lower_bound(lo);
  journal_insert(insert_pt, lo, val, maybe_get_delta_buffer());

  logger().debug(
    "ExtMapLeafNode::insert: inserted {}->{} {} {}",
    insert_pt.get_key(),
    insert_pt.get_val().laddr,
    insert_pt.get_val().lextent_offset,
    insert_pt.get_val().length);
  return insert_ertr::make_ready_future<ExtentRef>(
    std::make_unique<Extent>(lo, val.laddr, val.lextent_offset, val.length));
}

ExtMapLeafNode::punch_lextent_ret
ExtMapLeafNode::punch_lextent(Transaction &t, objaddr_t lo, extent_len_t len)
{
  if (!is_pending()) {
    auto mut = make_duplicate(t, this)->cast<ExtMapLeafNode>();
    return mut->punch_lextent(t, lo, len);
  }
  auto result_up = std::make_unique<extent_map_list_t>();
  auto &ret = *result_up;
  auto deflist = std::list<laddr_t>();
  auto [punch_pt, punch_end] = get_leaf_entries(lo, len);
  objaddr_t loend = lo + len;
  for (; punch_pt != punch_end; ++punch_pt) {
    if (punch_pt->get_key() >= loend)
      break;
    logger().debug("ExtMapLeafNode::punch_lextent {} {}", lo, len);
    auto val = (*punch_pt).get_val();
    if ( punch_pt->get_key() < lo) {
      if (punch_pt->get_key() + val.length > loend) {
        //split and deref middle
        logger().debug("ExtMapLeafNode::punch_middle {} {}", lo, len);
	uint32_t front = lo - punch_pt->get_key();
	ret.emplace_back(
	  std::make_unique<Extent>(
            lo,
	    val.laddr,
	    val.lextent_offset + front,
	    len));
	lext_map_val_t upval(val.laddr, val.lextent_offset, front);
        journal_update(punch_pt, upval, maybe_get_delta_buffer());
        lext_map_val_t inval(val.laddr, val.lextent_offset + front + len,
	                     val.length - front -len);
        journal_insert(punch_pt + 1, loend, inval, maybe_get_delta_buffer());
        break;
      } else {
        // deref tail
        logger().debug("ExtMapLeafNode::punch_tail {} {}", lo, len);
	ceph_assert(punch_pt->get_key() + val.length > lo);
	uint32_t keep = lo - punch_pt->get_key();
	ret.emplace_back(
          std::make_unique<Extent>(
            lo,
            val.laddr,
            val.lextent_offset + keep,
            val.length - keep));
        lext_map_val_t upval(val.laddr, val.lextent_offset, keep);
	journal_update(punch_pt, upval, maybe_get_delta_buffer());
	continue;
      }
    }
    if (punch_pt->get_key() + punch_pt->get_val().length <= loend){
      //deref whole lextent
      logger().debug("ExtMapLeafNode::punch_whole {} {}", lo, len);
      ret.emplace_back(
        std::make_unique<Extent>(
          punch_pt->get_key(),
          val.laddr,
          val.lextent_offset,
          val.length));
      journal_remove(punch_pt, maybe_get_delta_buffer());
      deflist.push_back(val.laddr);
      continue;
    }
    // deref head
    logger().debug("ExtMapLeafNode::punch_head {} {}", lo, len);
    uint32_t keep = punch_pt->get_key() + val.length - loend;
    ret.emplace_back(
      std::make_unique<Extent>(
        punch_pt->get_key(),
        val.laddr,
        val.lextent_offset,
        val.length - keep));
    lext_map_val_t inval(val.laddr, val.lextent_offset + val.length -keep, keep);
    journal_insert(punch_pt + 1, loend, inval, maybe_get_delta_buffer());
    journal_remove(punch_pt, maybe_get_delta_buffer());
    break;
  }
  return crimson::do_for_each(deflist.begin(), deflist.end(), [this, &t, &ret]
	 (const auto &laddr) {
    return tm->dec_ref(t, laddr).safe_then([](auto i) {
      return punch_lextent_ertr::now();
    });
  }).safe_then ([this, ret = std::move(result_up)] {
    return punch_lextent_ertr::make_ready_future<extent_map_list_t>(std::move(*ret));
  });
}

ExtMapLeafNode::rm_lextent_ret
ExtMapLeafNode::rm_lextent(Transaction &t, objaddr_t lo, lext_map_val_t val)
{
  if(!is_pending()) {
    auto mut =  make_duplicate(t, this)->cast<ExtMapLeafNode>();
    return mut->rm_lextent(t, lo, val);
  }

  auto [rm_pt, rm_end] = get_leaf_entries(lo, val.length);
  if(lo == rm_pt->get_key() && val.laddr == rm_pt->get_val().laddr && val.length == rm_pt->get_val().length){
    journal_remove(rm_pt, maybe_get_delta_buffer());
    logger().debug(
      "ExtMapLeafNode::rm_lextent: removed {}->{} {} {}",
      rm_pt.get_key(),
      rm_pt.get_val().laddr,
      rm_pt.get_val().lextent_offset,
      rm_pt.get_val().length);
      return rm_lextent_ertr::make_ready_future<bool>(true);
  } else {
    return rm_lextent_ertr::make_ready_future<bool>(false);
  }
}

ExtMapLeafNode::find_hole_ret
ExtMapLeafNode::find_hole(Transaction &t, objaddr_t lo, extent_len_t len)
{
  logger().debug("ExtMapLeafNode::find_hole lo={}, len={}, *this={}",
                 lo, len, *this);
  auto [start, end] = bound(lo, lo + len);

  //cross at least one mapping
  if (start != end && start++ != end)
    return find_hole_ertr::make_ready_future<ExtentRef>(
		    std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0));
  //overlap at start tail
  if (start != end && lo < (start->get_key() + start->get_val().length))
    return find_hole_ertr::make_ready_future<ExtentRef>(
                    std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0));
  //overlap at start head
  if (start != end && start->get_key() >= lo)
    return find_hole_ertr::make_ready_future<ExtentRef>(
                    std::make_unique<Extent>(lo, L_ADDR_NULL, 0, 0));

  if (start != begin())
    start = start - 1;

  return tm->read_extents<TestBlock>(t, start->get_val().laddr,
    start->get_val().length).safe_then([this, start, end, &t, lo, len]
    (auto &&extents) mutable {
    [[maybe_unused]] auto [addr, extref] = extents.front();
    auto lextent_end = start.get_key()- start.get_val().lextent_offset + extref->get_length();
    if (lextent_end <= lo)
      start = start + 1;

    return tm->read_extents<TestBlock>(t, start.get_val().laddr,
      start.get_val().length).safe_then([this, start, end, &t, lo, len] (auto &&extents){
      [[maybe_unused]] auto [addr, extref] = extents.front();
      auto keyl = start.get_key();
      auto vall = start.get_val();
      auto lextent_endl = keyl- vall.lextent_offset + extref->get_length();
      auto keyr = end.get_key();
      auto valr = end.get_val();


      laddr_t laddr = L_ADDR_NULL;
      uint32_t lextent_offset = 0;

      if (start != end && vall.laddr == valr.laddr) {  //find hole in middle of one extent
	laddr = vall.laddr;
	lextent_offset = lo- keyl + vall.lextent_offset;
      } else if ((lo + len) <= lextent_endl){  //find hole in begin extent
        laddr = vall.laddr;
	lextent_offset = lo - keyl + vall.lextent_offset;
      } else if (keyr - valr.lextent_offset <= lo) {   //find hole in end extent
        laddr = valr.laddr;
	lextent_offset = valr.lextent_offset - (keyr-lo);
      }
      return find_hole_ertr::make_ready_future<ExtentRef>
	      (std::make_unique<Extent>(lo, laddr, lextent_offset, len));
    });
  });
}

ExtMapLeafNode::split_children_ret
ExtMapLeafNode::make_split_children(Transaction &t)
{
  return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t] (auto &&left) {
      return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, &t, left = std::move(left)] (auto &&right){
           return split_children_ret(
             split_children_ertr::ready_future_marker{},
	     std::make_tuple(left, right, split_into(*left, *right)));
      });
  });
}
ExtMapLeafNode::full_merge_ret
ExtMapLeafNode::make_full_merge(Transaction &t, ExtMapNodeRef right)
{
  return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<ExtMapLeafNode>());
      return full_merge_ret(
	full_merge_ertr::ready_future_marker{},
	std::move(replacement));
  });
}
ExtMapLeafNode::make_balanced_ret
ExtMapLeafNode::make_balanced(Transaction &t,
                              ExtMapNodeRef _right, bool prefer_left)
{
  ceph_assert(_right->get_type() == type);
  return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
    .safe_then([this, &t, _right, prefer_left] (auto &&replacement_left) {
      return extmap_alloc_extent<ExtMapLeafNode>(t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, _right, prefer_left,
          replacement_left = std::move(replacement_left)] (auto &&replacement_right){
          auto &right = *_right->cast<ExtMapLeafNode>();
          return make_balanced_ret(
                 make_balanced_ertr::ready_future_marker{},
                 std::make_tuple(replacement_left, replacement_right,
                     balance_into_new_nodes(*this, right, prefer_left,
                       *replacement_left, *replacement_right)));
    });
  });
}


std::pair<ExtMapLeafNode::internal_iterator_t, ExtMapLeafNode::internal_iterator_t>
ExtMapLeafNode::get_leaf_entries(objaddr_t addr, extent_len_t len)
{
  return bound(addr, addr + len);
}


TransactionManager::read_extent_ertr::future<ExtMapNodeRef>
extmap_load_extent(TransactionManager* tm, Transaction& txn, laddr_t laddr, depth_t depth)
{
  if (depth > 0) {
    return tm->read_extents<ExtMapInnerNode>(txn, laddr, EXTMAP_BLOCK_SIZE).safe_then(
      [tm, depth](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      e->set_depth(depth);
      e->set_tm(tm);
      return TransactionManager::read_extent_ertr::make_ready_future<ExtMapNodeRef>(std::move(e));
    });
  } else {
    return tm->read_extents<ExtMapLeafNode>(txn, laddr, EXTMAP_BLOCK_SIZE).safe_then(
      [tm, depth](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      e->set_depth(depth);
      e->set_tm(tm);
      return TransactionManager::read_extent_ertr::make_ready_future<ExtMapNodeRef>(std::move(e));
    });
  }
}

}

