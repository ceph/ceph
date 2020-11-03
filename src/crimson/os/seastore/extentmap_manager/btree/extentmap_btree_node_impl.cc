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

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::extentmap_manager {

std::ostream &ExtMapInnerNode::print_detail_l(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << get_meta().depth;
}

ExtMapInnerNode::find_lextent_ret
ExtMapInnerNode::find_lextent(ext_context_t ec, objaddr_t lo, extent_len_t len)
{
  auto [begin, end] = bound(lo, lo + len);
  auto result_up = std::make_unique<extent_map_list_t>();
  auto &result = *result_up;
  return crimson::do_for_each(
    std::move(begin),
    std::move(end),
    [this, ec, &result, lo, len](const auto &val) mutable {
      return extmap_load_extent(ec, val.get_val(),  get_meta().depth - 1).safe_then(
        [ec, &result, lo, len](auto extent) mutable {
        return extent->find_lextent(ec, lo, len).safe_then(
          [&result](auto item_list) mutable {
          result.splice(result.end(), item_list,
          item_list.begin(), item_list.end());
        });
      });
  }).safe_then([result=std::move(result_up)] {
    return find_lextent_ret(
           find_lextent_ertr::ready_future_marker{},
           std::move(*result));
  });
}

ExtMapInnerNode::insert_ret
ExtMapInnerNode::insert(ext_context_t ec, objaddr_t lo, lext_map_val_t val)
{
  auto insertion_pt = get_containing_child(lo);
  assert(insertion_pt != end());
  return extmap_load_extent(ec, insertion_pt->get_val(), get_meta().depth - 1).safe_then(
    [this, ec, insertion_pt, lo, val=std::move(val)](auto extent) mutable {
      return extent->at_max_capacity() ?
        split_entry(ec, lo, insertion_pt, extent) :
        insert_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
    }).safe_then([ec, lo, val=std::move(val)](ExtMapNodeRef extent) mutable {
      return extent->insert(ec, lo, val);
    });
}

ExtMapInnerNode::rm_lextent_ret
ExtMapInnerNode::rm_lextent(ext_context_t ec, objaddr_t lo, lext_map_val_t val)
{
  auto rm_pt = get_containing_child(lo);
  return extmap_load_extent(ec, rm_pt->get_val(),  get_meta().depth - 1).safe_then(
    [this, ec, rm_pt, lo, val=std::move(val)](auto extent) mutable {
    if (extent->at_min_capacity() && get_node_size() > 1) {
      return merge_entry(ec, lo, rm_pt, extent);
    } else {
      return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(std::move(extent));
    }
  }).safe_then([ec, lo, val](ExtMapNodeRef extent) mutable {
    return extent->rm_lextent(ec, lo, val);
  });
}

ExtMapInnerNode::split_children_ret
ExtMapInnerNode::make_split_children(ext_context_t ec)
{
  logger().debug("{}: {}", "ExtMapInnerNode", __func__);
  return extmap_alloc_2extents<ExtMapInnerNode>(ec, EXTMAP_BLOCK_SIZE)
    .safe_then([this] (auto &&ext_pair) {
      auto [left, right] = ext_pair;
      return split_children_ret(
             split_children_ertr::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  });
}

ExtMapInnerNode::full_merge_ret
ExtMapInnerNode::make_full_merge(ext_context_t ec, ExtMapNodeRef right)
{
  logger().debug("{}: {}", "ExtMapInnerNode", __func__);
  return extmap_alloc_extent<ExtMapInnerNode>(ec, EXTMAP_BLOCK_SIZE)
    .safe_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<ExtMapInnerNode>());
      return full_merge_ret(
             full_merge_ertr::ready_future_marker{},
             std::move(replacement));
  });
}

ExtMapInnerNode::make_balanced_ret
ExtMapInnerNode::make_balanced(ext_context_t ec, ExtMapNodeRef _right, bool prefer_left)
{
  logger().debug("{}: {}", "ExtMapInnerNode", __func__);
  ceph_assert(_right->get_type() == type);
  return extmap_alloc_2extents<ExtMapInnerNode>(ec, EXTMAP_BLOCK_SIZE)
    .safe_then([this,  _right, prefer_left] (auto &&replacement_pair){
      auto [replacement_left, replacement_right] = replacement_pair;
      auto &right = *_right->cast<ExtMapInnerNode>();
      return make_balanced_ret(
             make_balanced_ertr::ready_future_marker{},
             std::make_tuple(replacement_left, replacement_right,
             balance_into_new_nodes(*this, right, prefer_left,
                                    *replacement_left, *replacement_right)));
  });
}

ExtMapInnerNode::split_entry_ret
ExtMapInnerNode::split_entry(ext_context_t ec, objaddr_t lo,
	                     internal_iterator_t iter, ExtMapNodeRef entry)
{
  logger().debug("{}: {}", "ExtMapInnerNode", __func__);
  if (!is_pending()) {
    auto mut = ec.tm.get_mutable_extent(ec.t, this)->cast<ExtMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_offset());
    return mut->split_entry(ec, lo, mut_iter, entry);
  }
  ceph_assert(!at_max_capacity());
  return entry->make_split_children(ec)
    .safe_then([this, ec, lo, iter, entry] (auto tuple){
    auto [left, right, pivot] = tuple;
    journal_update(iter, left->get_laddr(), maybe_get_delta_buffer());
    journal_insert(iter + 1, pivot, right->get_laddr(), maybe_get_delta_buffer());
    logger().debug(
      "ExtMapInnerNode::split_entry *this {} entry {} into left {} right {}",
      *this, *entry, *left, *right);
    //retire extent
    return ec.tm.dec_ref(ec.t, entry->get_laddr())
      .safe_then([lo, left = left, right = right, pivot = pivot] (auto ret) {
      return split_entry_ertr::make_ready_future<ExtMapNodeRef>(
             pivot > lo ? left : right);
    });
  });
}

ExtMapInnerNode::merge_entry_ret
ExtMapInnerNode::merge_entry(ext_context_t ec, objaddr_t lo,
  internal_iterator_t iter, ExtMapNodeRef entry)
{
  if (!is_pending()) {
    auto mut = ec.tm.get_mutable_extent(ec.t, this)->cast<ExtMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_offset());
    return mut->merge_entry(ec, lo, mut_iter, entry);
  }
  logger().debug("ExtMapInnerNode: merge_entry: {}, {}", *this, *entry);
  auto is_left = (iter + 1) == end();
  auto donor_iter = is_left ? iter - 1 : iter + 1;
  return extmap_load_extent(ec, donor_iter->get_val(),  get_meta().depth - 1)
    .safe_then([this, ec, lo, iter, entry, donor_iter, is_left]
    (auto &&donor) mutable {
    auto [l, r] = is_left ?
                  std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = is_left ?
              std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (donor->at_min_capacity()) {
      return l->make_full_merge(ec, r)
        .safe_then([this, ec, entry, l = l, r = r, liter = liter, riter = riter]
        (auto &&replacement){
        journal_update(liter, replacement->get_laddr(), maybe_get_delta_buffer());
        journal_remove(riter, maybe_get_delta_buffer());
        //retire extent
        std::list<laddr_t> dec_laddrs;
        dec_laddrs.push_back(l->get_laddr());
        dec_laddrs.push_back(r->get_laddr());
        return extmap_retire_node(ec, dec_laddrs)
          .safe_then([replacement] (auto &&ret) {
            return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(replacement);
        });
      });
    } else {
      logger().debug("ExtMapInnerNode::merge_entry balanced l {} r {}",
                    	*l, *r);
      return l->make_balanced(ec, r, !is_left)
	       .safe_then([this, ec, lo,  entry, l = l, r = r, liter = liter, riter = riter]
        (auto tuple) {
        auto [replacement_l, replacement_r, pivot] = tuple;
        journal_update(liter, replacement_l->get_laddr(), maybe_get_delta_buffer());
        journal_replace(riter, pivot, replacement_r->get_laddr(),
                maybe_get_delta_buffer());
        // retire extent
        std::list<laddr_t> dec_laddrs;
        dec_laddrs.push_back(l->get_laddr());
        dec_laddrs.push_back(r->get_laddr());
        return extmap_retire_node(ec, dec_laddrs)
          .safe_then([lo, pivot = pivot, replacement_l = replacement_l, replacement_r = replacement_r] 
            (auto &&ret) {
            return merge_entry_ertr::make_ready_future<ExtMapNodeRef>(
                   lo >= pivot ? replacement_r : replacement_l);
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

std::ostream &ExtMapLeafNode::print_detail_l(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << get_meta().depth;
}

ExtMapLeafNode::find_lextent_ret
ExtMapLeafNode::find_lextent(ext_context_t ec, objaddr_t lo, extent_len_t len)
{
  logger().debug(
    "ExtMapLeafNode::find_lextent {}~{}", lo, len);
  auto ret = extent_map_list_t();
  auto [from, to] = get_leaf_entries(lo, len);
  if (from == to && to != end())
    ++to;
  for (; from != to; ++from) {
    auto val = (*from).get_val();
    ret.emplace_back(
      extent_mapping_t(
      (*from).get_key(),
      val.laddr,
      val.length));
    logger().debug("ExtMapLeafNode::find_lextent find {}~{}", lo, val.laddr);
  }
  return find_lextent_ertr::make_ready_future<extent_map_list_t>(
         std::move(ret));
}

ExtMapLeafNode::insert_ret
ExtMapLeafNode::insert(ext_context_t ec, objaddr_t lo, lext_map_val_t val)
{
  ceph_assert(!at_max_capacity());
  if (!is_pending()) {
    auto mut = ec.tm.get_mutable_extent(ec.t, this)->cast<ExtMapLeafNode>();
    return mut->insert(ec, lo, val);
  }
  auto insert_pt = lower_bound(lo);
  journal_insert(insert_pt, lo, val, maybe_get_delta_buffer());

  logger().debug(
    "ExtMapLeafNode::insert: inserted {}->{} {}",
    insert_pt.get_key(),
    insert_pt.get_val().laddr,
    insert_pt.get_val().length);
  return insert_ertr::make_ready_future<extent_mapping_t>(
         extent_mapping_t(lo, val.laddr, val.length));
}

ExtMapLeafNode::rm_lextent_ret
ExtMapLeafNode::rm_lextent(ext_context_t ec, objaddr_t lo, lext_map_val_t val)
{
  if (!is_pending()) {
    auto mut = ec.tm.get_mutable_extent(ec.t, this)->cast<ExtMapLeafNode>();
    return mut->rm_lextent(ec, lo, val);
  }

  auto [rm_pt, rm_end] = get_leaf_entries(lo, val.length);
  if (lo == rm_pt->get_key() && val.laddr == rm_pt->get_val().laddr
           && val.length == rm_pt->get_val().length) {
    journal_remove(rm_pt, maybe_get_delta_buffer());
    logger().debug(
      "ExtMapLeafNode::rm_lextent: removed {}->{} {}",
      rm_pt.get_key(),
      rm_pt.get_val().laddr,
      rm_pt.get_val().length);
    return rm_lextent_ertr::make_ready_future<bool>(true);
  } else {
    return rm_lextent_ertr::make_ready_future<bool>(false);
  }
}

ExtMapLeafNode::split_children_ret
ExtMapLeafNode::make_split_children(ext_context_t ec)
{
  logger().debug("{}: {}", "ExtMapLeafNode", __func__);
  return extmap_alloc_2extents<ExtMapLeafNode>(ec, EXTMAP_BLOCK_SIZE)
    .safe_then([this] (auto &&ext_pair) {
      auto [left, right] = ext_pair;
      return split_children_ret(
             split_children_ertr::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  });
}

ExtMapLeafNode::full_merge_ret
ExtMapLeafNode::make_full_merge(ext_context_t ec, ExtMapNodeRef right)
{
  logger().debug("{}: {}", "ExtMapLeafNode", __func__);
  return extmap_alloc_extent<ExtMapLeafNode>(ec, EXTMAP_BLOCK_SIZE)
    .safe_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<ExtMapLeafNode>());
      return full_merge_ret(
             full_merge_ertr::ready_future_marker{},
             std::move(replacement));
  });
}
ExtMapLeafNode::make_balanced_ret
ExtMapLeafNode::make_balanced(ext_context_t ec, ExtMapNodeRef _right, bool prefer_left)
{
  logger().debug("{}: {}", "ExtMapLeafNode", __func__);
  ceph_assert(_right->get_type() == type);
  return extmap_alloc_2extents<ExtMapLeafNode>(ec, EXTMAP_BLOCK_SIZE)
    .safe_then([this, _right, prefer_left] (auto &&replacement_pair) {
      auto [replacement_left, replacement_right] = replacement_pair;
      auto &right = *_right->cast<ExtMapLeafNode>();
      return make_balanced_ret(
             make_balanced_ertr::ready_future_marker{},
             std::make_tuple(
               replacement_left, replacement_right,
               balance_into_new_nodes(
                 *this, right, prefer_left,
                 *replacement_left, *replacement_right)));
  });
}


std::pair<ExtMapLeafNode::internal_iterator_t, ExtMapLeafNode::internal_iterator_t>
ExtMapLeafNode::get_leaf_entries(objaddr_t addr, extent_len_t len)
{
  return bound(addr, addr + len);
}


TransactionManager::read_extent_ertr::future<ExtMapNodeRef>
extmap_load_extent(ext_context_t ec, laddr_t laddr, depth_t depth)
{
  ceph_assert(depth > 0);
  if (depth > 1) {
    return ec.tm.read_extents<ExtMapInnerNode>(ec.t, laddr, EXTMAP_BLOCK_SIZE).safe_then(
      [](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      return TransactionManager::read_extent_ertr::make_ready_future<ExtMapNodeRef>(std::move(e));
    });
  } else {
    return ec.tm.read_extents<ExtMapLeafNode>(ec.t, laddr, EXTMAP_BLOCK_SIZE).safe_then(
      [](auto&& extents) {
      assert(extents.size() == 1);
      [[maybe_unused]] auto [laddr, e] = extents.front();
      return TransactionManager::read_extent_ertr::make_ready_future<ExtMapNodeRef>(std::move(e));
    });
  }
}

}
