// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <algorithm>
#include <string.h>
#include "include/buffer.h"
#include "include/byteorder.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "seastar/core/thread.hh"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::omap_manager {

std::ostream &operator<<(std::ostream &out, const omap_inner_key_t &rhs)
{
  return out << "omap_inner_key (" << rhs.key_off<< " - " << rhs.key_len
             << " - " << rhs.laddr << ")";
}

std::ostream &operator<<(std::ostream &out, const omap_leaf_key_t &rhs)
{
  return out << "omap_leaf_key_t (" << rhs.key_off<< " - " << rhs.key_len
             << " - " << rhs.val_len << ")";
}

std::ostream &OMapInnerNode::print_detail_l(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", depth=" << get_meta().depth;
}

using dec_ref_iertr = OMapInnerNode::base_iertr;
using dec_ref_ret = dec_ref_iertr::future<>;
template <typename T>
dec_ref_ret dec_ref(omap_context_t oc, T&& addr) {
  return oc.tm.dec_ref(oc.t, std::forward<T>(addr)).handle_error_interruptible(
    dec_ref_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in OMapInnerNode helper dec_ref"
    }
  ).si_then([](auto &&e) {});
}

/**
 * make_split_insert
 *
 * insert an  entry at iter, with the address of key.
 * will result in a split outcome encoded in the returned mutation_result_t
 */
OMapInnerNode::make_split_insert_ret
OMapInnerNode::make_split_insert(
  omap_context_t oc,
  internal_iterator_t iter,
  std::string key,
  laddr_t laddr)
{
  return make_split_children(oc).si_then([=] (auto tuple) {
    auto [left, right, pivot] = tuple;
    if (pivot > key) {
      auto liter = left->iter_idx(iter.get_index());
      left->journal_inner_insert(liter, laddr, key,
                                 left->maybe_get_delta_buffer());
    } else {  //right
      auto riter = right->iter_idx(iter.get_index() - left->get_node_size());
      right->journal_inner_insert(riter, laddr, key,
                                  right->maybe_get_delta_buffer());
    }
    return make_split_insert_ret(
           interruptible::ready_future_marker{},
           mutation_result_t(mutation_status_t::WAS_SPLIT, tuple, std::nullopt));
  });

}


OMapInnerNode::handle_split_ret
OMapInnerNode::handle_split(
  omap_context_t oc,
  internal_iterator_t iter,
  mutation_result_t mresult)
{
  logger().debug("OMapInnerNode: {}",  __func__);
  if (!is_pending()) {
    auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter.get_index());
    return mut->handle_split(oc, mut_iter, mresult);
  }
  auto [left, right, pivot] = *(mresult.split_tuple);
  //update operation will not cause node overflow, so we can do it first.
  journal_inner_update(iter, left->get_laddr(), maybe_get_delta_buffer());
  bool overflow = extent_will_overflow(pivot.size(), std::nullopt);
  if (!overflow) {
    journal_inner_insert(iter + 1, right->get_laddr(), pivot,
                         maybe_get_delta_buffer());
    return insert_ret(
           interruptible::ready_future_marker{},
           mutation_result_t(mutation_status_t::SUCCESS, std::nullopt, std::nullopt));
  } else {
    return make_split_insert(oc, iter + 1, pivot, right->get_laddr())
      .si_then([this, oc] (auto m_result) {
       return dec_ref(oc, get_laddr())
         .si_then([m_result = std::move(m_result)] {
          return insert_ret(
                 interruptible::ready_future_marker{},
                 m_result);
       });
   });
  }
}

OMapInnerNode::get_value_ret
OMapInnerNode::get_value(
  omap_context_t oc,
  const std::string &key)
{
  logger().debug("OMapInnerNode: {} key = {}",  __func__, key);
  auto child_pt = get_containing_child(key);
  assert(child_pt != iter_end());
  auto laddr = child_pt->get_val();
  return omap_load_extent(oc, laddr, get_meta().depth - 1).si_then(
    [oc, &key] (auto extent) {
    return extent->get_value(oc, key);
  }).finally([ref = OMapNodeRef(this)] {});
}

OMapInnerNode::insert_ret
OMapInnerNode::insert(
  omap_context_t oc,
  const std::string &key,
  const ceph::bufferlist &value)
{
  logger().debug("OMapInnerNode: {}  {}->{}",  __func__, key, value);
  auto child_pt = get_containing_child(key);
  assert(child_pt != iter_end());
  auto laddr = child_pt->get_val();
  return omap_load_extent(oc, laddr, get_meta().depth - 1).si_then(
    [oc, &key, &value] (auto extent) {
    return extent->insert(oc, key, value);
  }).si_then([this, oc, child_pt] (auto mresult) {
    if (mresult.status == mutation_status_t::SUCCESS) {
      return insert_iertr::make_ready_future<mutation_result_t>(mresult);
    } else if (mresult.status == mutation_status_t::WAS_SPLIT) {
      return handle_split(oc, child_pt, mresult);
    } else {
     return insert_ret(
            interruptible::ready_future_marker{},
            mutation_result_t(mutation_status_t::SUCCESS, std::nullopt, std::nullopt));
    }
  });
}

OMapInnerNode::rm_key_ret
OMapInnerNode::rm_key(omap_context_t oc, const std::string &key)
{
  logger().debug("OMapInnerNode: {}", __func__);
  auto child_pt = get_containing_child(key);
  assert(child_pt != iter_end());
  auto laddr = child_pt->get_val();
  return omap_load_extent(oc, laddr, get_meta().depth - 1).si_then(
    [this, oc, &key, child_pt] (auto extent) {
    return extent->rm_key(oc, key)
      .si_then([this, oc, child_pt, extent = std::move(extent)] (auto mresult) {
      switch (mresult.status) {
        case mutation_status_t::SUCCESS:
        case mutation_status_t::FAIL:
          return rm_key_iertr::make_ready_future<mutation_result_t>(mresult);
        case mutation_status_t::NEED_MERGE: {
          if (get_node_size() >1)
            return merge_entry(oc, child_pt, *(mresult.need_merge));
          else
            return rm_key_ret(
                   interruptible::ready_future_marker{},
                   mutation_result_t(mutation_status_t::SUCCESS,
                                     std::nullopt, std::nullopt));
        }
        case mutation_status_t::WAS_SPLIT:
          return handle_split(oc, child_pt, mresult);
        default:
          return rm_key_iertr::make_ready_future<mutation_result_t>(mresult);
      }
    });
  });
}

OMapInnerNode::list_ret
OMapInnerNode::list(
  omap_context_t oc,
  const std::optional<std::string> &start,
  omap_list_config_t config)
{
  logger().debug("OMapInnerNode: {}", __func__);

  auto child_iter = start ?
    get_containing_child(*start) :
    iter_cbegin();
  assert(child_iter != iter_cend());

  return seastar::do_with(
    child_iter,
    iter_cend(),
    list_bare_ret(false, {}),
    [=, &start](auto &biter, auto &eiter, auto &ret) {
      auto &complete = std::get<0>(ret);
      auto &result = std::get<1>(ret);
      return trans_intr::repeat(
	[&, config, oc, this]() -> list_iertr::future<seastar::stop_iteration> {
	  if (biter == eiter  || result.size() == config.max_result_size) {
	    complete = biter == eiter;
	    return list_iertr::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::yes);
	  }
	  auto laddr = biter->get_val();
	  return omap_load_extent(
	    oc, laddr,
	    get_meta().depth - 1
	  ).si_then([&, config, oc] (auto &&extent) {
	    return extent->list(
	      oc,
	      start,
	      config.with_reduced_max(result.size())
	    ).si_then([&, config](auto &&child_ret) mutable {
	      auto &[child_complete, child_result] = child_ret;
	      if (result.size() && child_result.size()) {
		assert(child_result.begin()->first > result.rbegin()->first);
	      }
	      if (child_result.size() && start) {
		assert(child_result.begin()->first > *start);
	      }
	      result.merge(std::move(child_result));
	      ++biter;
	      assert(child_complete || result.size() == config.max_result_size);
	      return list_iertr::make_ready_future<seastar::stop_iteration>(
		seastar::stop_iteration::no);
	    });
	  });
	}).si_then([&ret, ref = OMapNodeRef(this)] {
	  return list_iertr::make_ready_future<list_bare_ret>(std::move(ret));
	});
    });
}

OMapInnerNode::clear_ret
OMapInnerNode::clear(omap_context_t oc)
{
  logger().debug("OMapInnerNode: {}", __func__);
  return trans_intr::do_for_each(iter_begin(), iter_end(), [this, oc] (auto iter) {
    auto laddr = iter->get_val();
    return omap_load_extent(oc, laddr, get_meta().depth - 1).si_then(
      [oc] (auto &&extent) {
      return extent->clear(oc);
    }).si_then([oc, laddr] {
      return dec_ref(oc, laddr);
    }).si_then([ref = OMapNodeRef(this)] {
      return clear_iertr::now();
    });
  });
}

OMapInnerNode::split_children_ret
OMapInnerNode:: make_split_children(omap_context_t oc)
{
  logger().debug("OMapInnerNode: {}", __func__);
  return oc.tm.alloc_extents<OMapInnerNode>(oc.t, L_ADDR_MIN, OMAP_BLOCK_SIZE, 2)
    .si_then([this] (auto &&ext_pair) {
      auto left = ext_pair.front();
      auto right = ext_pair.back();
      return split_children_ret(
             interruptible::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  });
}

OMapInnerNode::full_merge_ret
OMapInnerNode::make_full_merge(omap_context_t oc, OMapNodeRef right)
{
  logger().debug("OMapInnerNode: {}", __func__);
  return oc.tm.alloc_extent<OMapInnerNode>(oc.t, L_ADDR_MIN, OMAP_BLOCK_SIZE)
    .si_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<OMapInnerNode>());
      return full_merge_ret(
        interruptible::ready_future_marker{},
        std::move(replacement));
  });
}

OMapInnerNode::make_balanced_ret
OMapInnerNode::make_balanced(omap_context_t oc, OMapNodeRef _right)
{
  logger().debug("OMapInnerNode: {}", __func__);
  ceph_assert(_right->get_type() == type);
  return oc.tm.alloc_extents<OMapInnerNode>(oc.t, L_ADDR_MIN, OMAP_BLOCK_SIZE, 2)
    .si_then([this, _right] (auto &&replacement_pair){
      auto replacement_left = replacement_pair.front();
      auto replacement_right = replacement_pair.back();
      auto &right = *_right->cast<OMapInnerNode>();
      return make_balanced_ret(
             interruptible::ready_future_marker{},
             std::make_tuple(replacement_left, replacement_right,
                             balance_into_new_nodes(*this, right,
                               *replacement_left, *replacement_right)));
  });
}

OMapInnerNode::merge_entry_ret
OMapInnerNode::merge_entry(
  omap_context_t oc,
  internal_iterator_t iter,
  OMapNodeRef entry)
{
  logger().debug("OMapInnerNode: {}", __func__);
  if (!is_pending()) {
    auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_index());
    return mut->merge_entry(oc, mut_iter, entry);
  }
  auto is_left = (iter + 1) == iter_end();
  auto donor_iter = is_left ? iter - 1 : iter + 1;
  return omap_load_extent(oc, donor_iter->get_val(), get_meta().depth - 1)
    .si_then([=] (auto &&donor) mutable {
    auto [l, r] = is_left ?
      std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = is_left ?
      std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (donor->extent_is_below_min()) {
      logger().debug("{}::merge_entry make_full_merge l {} r {}", __func__, *l, *r);
      assert(entry->extent_is_below_min());
      return l->make_full_merge(oc, r).si_then([liter=liter, riter=riter,
                                                  l=l, r=r, oc, this] (auto &&replacement){
        journal_inner_update(liter, replacement->get_laddr(), maybe_get_delta_buffer());
        journal_inner_remove(riter, maybe_get_delta_buffer());
        //retire extent
        std::vector<laddr_t> dec_laddrs {l->get_laddr(), r->get_laddr()};
        return dec_ref(oc, dec_laddrs).si_then([this] {
          if (extent_is_below_min()) {
            return merge_entry_ret(
                   interruptible::ready_future_marker{},
                   mutation_result_t(mutation_status_t::NEED_MERGE, std::nullopt,
                                    this));
          } else {
            return merge_entry_ret(
                   interruptible::ready_future_marker{},
                   mutation_result_t(mutation_status_t::SUCCESS, std::nullopt, std::nullopt));
          }
        });
      });
    } else {
      logger().debug("{}::merge_entry balanced l {} r {}", __func__, *l, *r);
      return l->make_balanced(oc, r).si_then([liter=liter, riter=riter,
                                                l=l, r=r, oc, this] (auto tuple) {
        auto [replacement_l, replacement_r, replacement_pivot] = tuple;
        //update operation will not cuase node overflow, so we can do it first
        journal_inner_update(liter, replacement_l->get_laddr(), maybe_get_delta_buffer());
        bool overflow = extent_will_overflow(replacement_pivot.size(), std::nullopt);
        if (!overflow) {
          journal_inner_remove(riter, maybe_get_delta_buffer());
          journal_inner_insert(
	    riter,
	    replacement_r->get_laddr(),
	    replacement_pivot,
	    maybe_get_delta_buffer());
          std::vector<laddr_t> dec_laddrs{l->get_laddr(), r->get_laddr()};
          return dec_ref(oc, dec_laddrs).si_then([] {
            return merge_entry_ret(
                   interruptible::ready_future_marker{},
                   mutation_result_t(mutation_status_t::SUCCESS, std::nullopt, std::nullopt));
          });
        } else {
          logger().debug("{}::merge_entry balanced and split {} r {}", __func__, *l, *r);
          //use remove and insert to instead of replace,
          //remove operation will not cause node split, so we can do it first
          journal_inner_remove(riter, maybe_get_delta_buffer());
          return make_split_insert(oc, riter, replacement_pivot, replacement_r->get_laddr())
            .si_then([this, oc, l = l, r = r] (auto mresult) {
            std::vector<laddr_t> dec_laddrs{l->get_laddr(), r->get_laddr(), get_laddr()};
            return dec_ref(oc, dec_laddrs)
              .si_then([mresult = std::move(mresult)] {
              return merge_entry_ret(
                     interruptible::ready_future_marker{},
                     mresult);
            });
          });
        }
      });
    }
  });

}

OMapInnerNode::internal_iterator_t
OMapInnerNode::get_containing_child(const std::string &key)
{
  auto iter = std::find_if(iter_begin(), iter_end(),
       [&key](auto it) { return it.contains(key); });
  return iter;
}

std::ostream &OMapLeafNode::print_detail_l(std::ostream &out) const
{
  return out << ", size=" << get_size()
         << ", depth=" << get_meta().depth;
}

OMapLeafNode::get_value_ret
OMapLeafNode::get_value(omap_context_t oc, const std::string &key)
{
  logger().debug("OMapLeafNode: {} key = {}", __func__, key);
  auto ite = find_string_key(key);
  if (ite != iter_end()) {
    auto value = ite->get_val();
    return get_value_ret(
      interruptible::ready_future_marker{},
      value);
  } else {
    return get_value_ret(
      interruptible::ready_future_marker{},
      std::nullopt);
  }
}

OMapLeafNode::insert_ret
OMapLeafNode::insert(
  omap_context_t oc,
  const std::string &key,
  const ceph::bufferlist &value)
{
  logger().debug("OMapLeafNode: {}, {} -> {}", __func__, key, value);
  bool overflow = extent_will_overflow(key.size(), value.length());
  if (!overflow) {
    if (!is_pending()) {
      auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapLeafNode>();
      return mut->insert(oc, key, value);
    }
    auto replace_pt = find_string_key(key);
    if (replace_pt != iter_end()) {
      journal_leaf_update(replace_pt, key, value, maybe_get_delta_buffer());
    } else {
      auto insert_pt = string_lower_bound(key);
      journal_leaf_insert(insert_pt, key, value, maybe_get_delta_buffer());

      logger().debug(
        "{}: {} inserted {}"," OMapLeafNode",  __func__,
        insert_pt.get_key());
    }
    return insert_ret(
           interruptible::ready_future_marker{},
           mutation_result_t(mutation_status_t::SUCCESS, std::nullopt, std::nullopt));
  } else {
    return make_split_children(oc).si_then([this, oc, &key, &value] (auto tuple) {
      auto [left, right, pivot] = tuple;
      auto replace_pt = find_string_key(key);
      if (replace_pt != iter_end()) {
        if (key < pivot) {  //left
          auto mut_iter = left->iter_idx(replace_pt->get_index());
          left->journal_leaf_update(mut_iter, key, value, left->maybe_get_delta_buffer());
        } else if (key >= pivot) { //right
          auto mut_iter = right->iter_idx(replace_pt->get_index() - left->get_node_size());
          right->journal_leaf_update(mut_iter, key, value, right->maybe_get_delta_buffer());
        }
      } else {
        auto insert_pt = string_lower_bound(key);
        if (key < pivot) {  //left
          auto mut_iter = left->iter_idx(insert_pt->get_index());
          left->journal_leaf_insert(mut_iter, key, value, left->maybe_get_delta_buffer());
        } else {
          auto mut_iter = right->iter_idx(insert_pt->get_index() - left->get_node_size());
          right->journal_leaf_insert(mut_iter, key, value, right->maybe_get_delta_buffer());
        }
      }
      return dec_ref(oc, get_laddr())
        .si_then([tuple = std::move(tuple)] {
        return insert_ret(
               interruptible::ready_future_marker{},
               mutation_result_t(mutation_status_t::WAS_SPLIT, tuple, std::nullopt));
      });
    });
  }
}

OMapLeafNode::rm_key_ret
OMapLeafNode::rm_key(omap_context_t oc, const std::string &key)
{
  logger().debug("OMapLeafNode: {} : {}", __func__, key);
  if(!is_pending()) {
    auto mut =  oc.tm.get_mutable_extent(oc.t, this)->cast<OMapLeafNode>();
    return mut->rm_key(oc, key);
  }

  auto rm_pt = find_string_key(key);
  if (rm_pt != iter_end()) {
    journal_leaf_remove(rm_pt, maybe_get_delta_buffer());
    if (extent_is_below_min()) {
      return rm_key_ret(
	interruptible::ready_future_marker{},
	mutation_result_t(mutation_status_t::NEED_MERGE, std::nullopt,
			  this->cast<OMapNode>()));
    } else {
      return rm_key_ret(
	interruptible::ready_future_marker{},
	mutation_result_t(mutation_status_t::SUCCESS, std::nullopt, std::nullopt));
    }
  } else {
    return rm_key_ret(
      interruptible::ready_future_marker{},
      mutation_result_t(mutation_status_t::FAIL, std::nullopt, std::nullopt));
  }

}

OMapLeafNode::list_ret
OMapLeafNode::list(
  omap_context_t oc,
  const std::optional<std::string> &start,
  omap_list_config_t config)
{
  logger().debug(
    "OMapLeafNode::{} start {} max_result_size {} inclusive {}",
    __func__,
    start ? start->c_str() : "",
    config.max_result_size,
    config.inclusive
  );
  auto ret = list_bare_ret(false, {});
  auto &[complete, result] = ret;
  auto iter = start ?
    (config.inclusive ?
     string_lower_bound(*start) :
     string_upper_bound(*start)) :
    iter_begin();

  for (; iter != iter_end() && result.size() < config.max_result_size; iter++) {
    result.emplace(std::make_pair(iter->get_key(), iter->get_val()));
  }

  complete = (iter == iter_end());

  return list_iertr::make_ready_future<list_bare_ret>(
    std::move(ret));
}

OMapLeafNode::clear_ret
OMapLeafNode::clear(omap_context_t oc)
{
  return clear_iertr::now();
}

OMapLeafNode::split_children_ret
OMapLeafNode::make_split_children(omap_context_t oc)
{
  logger().debug("OMapLeafNode: {}", __func__);
  return oc.tm.alloc_extents<OMapLeafNode>(oc.t, L_ADDR_MIN, OMAP_BLOCK_SIZE, 2)
    .si_then([this] (auto &&ext_pair) {
      auto left = ext_pair.front();
      auto right = ext_pair.back();
      return split_children_ret(
             interruptible::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  });
}

OMapLeafNode::full_merge_ret
OMapLeafNode::make_full_merge(omap_context_t oc, OMapNodeRef right)
{
  ceph_assert(right->get_type() == type);
  logger().debug("OMapLeafNode: {}", __func__);
  return oc.tm.alloc_extent<OMapLeafNode>(oc.t, L_ADDR_MIN, OMAP_BLOCK_SIZE)
    .si_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<OMapLeafNode>());
      return full_merge_ret(
        interruptible::ready_future_marker{},
        std::move(replacement));
  });
}

OMapLeafNode::make_balanced_ret
OMapLeafNode::make_balanced(omap_context_t oc, OMapNodeRef _right)
{
  ceph_assert(_right->get_type() == type);
  logger().debug("OMapLeafNode: {}",  __func__);
  return oc.tm.alloc_extents<OMapLeafNode>(oc.t, L_ADDR_MIN, OMAP_BLOCK_SIZE, 2)
    .si_then([this, _right] (auto &&replacement_pair) {
      auto replacement_left = replacement_pair.front();
      auto replacement_right = replacement_pair.back();
      auto &right = *_right->cast<OMapLeafNode>();
      return make_balanced_ret(
             interruptible::ready_future_marker{},
             std::make_tuple(
               replacement_left, replacement_right,
               balance_into_new_nodes(
                 *this, right,
                 *replacement_left, *replacement_right)));
  });
}


omap_load_extent_iertr::future<OMapNodeRef>
omap_load_extent(omap_context_t oc, laddr_t laddr, depth_t depth)
{
  ceph_assert(depth > 0);
  if (depth > 1) {
    return oc.tm.read_extent<OMapInnerNode>(oc.t, laddr, OMAP_BLOCK_SIZE
    ).handle_error_interruptible(
      omap_load_extent_iertr::pass_further{},
      crimson::ct_error::assert_all{ "Invalid error in omap_load_extent" }
    ).si_then(
      [](auto&& e) {
      return seastar::make_ready_future<OMapNodeRef>(std::move(e));
    });
  } else {
    return oc.tm.read_extent<OMapLeafNode>(oc.t, laddr, OMAP_BLOCK_SIZE
    ).handle_error_interruptible(
      omap_load_extent_iertr::pass_further{},
      crimson::ct_error::assert_all{ "Invalid error in omap_load_extent" }
    ).si_then(
      [](auto&& e) {
      return seastar::make_ready_future<OMapNodeRef>(std::move(e));
    });
  }
}
}
