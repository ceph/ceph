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

SET_SUBSYS(seastore_omap);

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
  return oc.tm.remove(oc.t, std::forward<T>(addr)).handle_error_interruptible(
    dec_ref_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in OMapInnerNode helper dec_ref"
    }
  ).discard_result();
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
  LOG_PREFIX(OMapInnerNode::make_split_insert);
  DEBUGT("this: {}, key: {}", oc.t, *this, key);
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
    ++(oc.t.get_omap_tree_stats().extents_num_delta);
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
  LOG_PREFIX(OMapInnerNode::handle_split);
  DEBUGT("this: {}",  oc.t, *this);
  if (!is_mutable()) {
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
  LOG_PREFIX(OMapInnerNode::get_value);
  DEBUGT("key = {}, this: {}", oc.t, key, *this);
  auto child_pt = get_containing_child(key);
  assert(child_pt != iter_cend());
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
  LOG_PREFIX(OMapInnerNode::insert);
  DEBUGT("{}->{}, this: {}",  oc.t, key, value, *this);
  auto child_pt = get_containing_child(key);
  assert(child_pt != iter_cend());
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
  LOG_PREFIX(OMapInnerNode::rm_key);
  DEBUGT("key={}, this: {}", oc.t, key, *this);
  auto child_pt = get_containing_child(key);
  assert(child_pt != iter_cend());
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
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  omap_list_config_t config)
{
  LOG_PREFIX(OMapInnerNode::list);
  if (first && last) {
    DEBUGT("first: {}, last: {}, this: {}", oc.t, *first, *last, *this);
    assert(*first <= *last);
  } else if (first) {
    DEBUGT("first: {}, this: {}", oc.t, *first, *this);
  } else if (last) {
    DEBUGT("last: {}, this: {}", oc.t, *last, *this);
  } else {
    DEBUGT("this: {}", oc.t, *this);
  }

  auto first_iter = first ?
    get_containing_child(*first) :
    iter_cbegin();
  auto last_iter = last ?
    get_containing_child(*last) + 1:
    iter_cend();
  assert(first_iter != iter_cend());

  return seastar::do_with(
    first_iter,
    last_iter,
    iter_t(first_iter),
    list_bare_ret(false, {}),
    [this, &first, &last, oc, config](
      auto &fiter,
      auto &liter,
      auto &iter,
      auto &ret)
    {
      auto &complete = std::get<0>(ret);
      auto &result = std::get<1>(ret);
      return trans_intr::repeat(
        [&, config, oc, this]() -> list_iertr::future<seastar::stop_iteration>
      {
        if (iter == liter) {
          complete = true;
          return list_iertr::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
	assert(result.size() < config.max_result_size);
        auto laddr = iter->get_val();
        return omap_load_extent(
          oc, laddr,
          get_meta().depth - 1
        ).si_then([&, config, oc](auto &&extent) {
	  return seastar::do_with(
	    iter == fiter ? first : std::optional<std::string>(std::nullopt),
	    iter == liter - 1 ? last : std::optional<std::string>(std::nullopt),
	    [&result, extent = std::move(extent), config, oc](
	      auto &nfirst,
	      auto &nlast) {
	    return extent->list(
	      oc,
	      nfirst,
	      nlast,
	      config.with_reduced_max(result.size()));
	  }).si_then([&, config](auto &&child_ret) mutable {
	    boost::ignore_unused(config);   // avoid clang warning;
            auto &[child_complete, child_result] = child_ret;
            if (result.size() && child_result.size()) {
              assert(child_result.begin()->first > result.rbegin()->first);
            }
            if (child_result.size() && first && iter == fiter) {
	      if (config.first_inclusive) {
		assert(child_result.begin()->first >= *first);
	      } else {
		assert(child_result.begin()->first > *first);
	      }
            }
            if (child_result.size() && last && iter == liter - 1) {
	      auto biter = --(child_result.end());
	      if (config.last_inclusive) {
		assert(biter->first <= *last);
	      } else {
		assert(biter->first < *last);
	      }
            }
            result.merge(std::move(child_result));
	    if (result.size() == config.max_result_size) {
	      return list_iertr::make_ready_future<seastar::stop_iteration>(
		seastar::stop_iteration::yes);
	    }
            ++iter;
            assert(child_complete);
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
  LOG_PREFIX(OMapInnerNode::clear);
  DEBUGT("this: {}", oc.t, *this);
  return trans_intr::do_for_each(iter_begin(), iter_end(),
    [oc, this](auto iter) {
    auto laddr = iter->get_val();
    auto ndepth = get_meta().depth - 1;
    if (ndepth > 1) {
      return omap_load_extent(oc, laddr, ndepth
      ).si_then([oc](auto &&extent) {
	return extent->clear(oc);
      }).si_then([oc, laddr] {
	return dec_ref(oc, laddr);
      }).si_then([ref = OMapNodeRef(this)] {
	return clear_iertr::now();
      });
    } else {
      assert(ndepth == 1);
      return dec_ref(oc, laddr
      ).si_then([ref = OMapNodeRef(this)] {
	return clear_iertr::now();
      });
    }
  });
}

OMapInnerNode::split_children_ret
OMapInnerNode:: make_split_children(omap_context_t oc)
{
  LOG_PREFIX(OMapInnerNode::make_split_children);
  DEBUGT("this: {}", oc.t, *this);
  return oc.tm.alloc_extents<OMapInnerNode>(oc.t, oc.hint,
    OMAP_INNER_BLOCK_SIZE, 2)
    .si_then([this, oc] (auto &&ext_pair) {
      LOG_PREFIX(OMapInnerNode::make_split_children);
      auto left = ext_pair.front();
      auto right = ext_pair.back();
      DEBUGT("this: {}, split into: l {} r {}", oc.t, *this, *left, *right);
      return split_children_ret(
             interruptible::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    split_children_iertr::pass_further{}
  );
}

OMapInnerNode::full_merge_ret
OMapInnerNode::make_full_merge(omap_context_t oc, OMapNodeRef right)
{
  LOG_PREFIX(OMapInnerNode::make_full_merge);
  DEBUGT("", oc.t);
  return oc.tm.alloc_non_data_extent<OMapInnerNode>(oc.t, oc.hint,
    OMAP_INNER_BLOCK_SIZE)
    .si_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<OMapInnerNode>());
      return full_merge_ret(
        interruptible::ready_future_marker{},
        std::move(replacement));
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    full_merge_iertr::pass_further{}
  );
}

OMapInnerNode::make_balanced_ret
OMapInnerNode::make_balanced(omap_context_t oc, OMapNodeRef _right)
{
  LOG_PREFIX(OMapInnerNode::make_balanced);
  DEBUGT("l: {}, r: {}", oc.t, *this, *_right);
  ceph_assert(_right->get_type() == TYPE);
  return oc.tm.alloc_extents<OMapInnerNode>(oc.t, oc.hint,
    OMAP_INNER_BLOCK_SIZE, 2)
    .si_then([this, _right] (auto &&replacement_pair){
      auto replacement_left = replacement_pair.front();
      auto replacement_right = replacement_pair.back();
      auto &right = *_right->cast<OMapInnerNode>();
      return make_balanced_ret(
             interruptible::ready_future_marker{},
             std::make_tuple(replacement_left, replacement_right,
                             balance_into_new_nodes(*this, right,
                               *replacement_left, *replacement_right)));
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    make_balanced_iertr::pass_further{}
  );
}

OMapInnerNode::merge_entry_ret
OMapInnerNode::merge_entry(
  omap_context_t oc,
  internal_iterator_t iter,
  OMapNodeRef entry)
{
  LOG_PREFIX(OMapInnerNode::merge_entry);
  DEBUGT("{}, parent: {}", oc.t, *entry, *this);
  if (!is_mutable()) {
    auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapInnerNode>();
    auto mut_iter = mut->iter_idx(iter->get_index());
    return mut->merge_entry(oc, mut_iter, entry);
  }
  auto is_left = (iter + 1) == iter_cend();
  auto donor_iter = is_left ? iter - 1 : iter + 1;
  return omap_load_extent(oc, donor_iter->get_val(), get_meta().depth - 1
  ).si_then([=, this](auto &&donor) mutable {
    LOG_PREFIX(OMapInnerNode::merge_entry);
    auto [l, r] = is_left ?
      std::make_pair(donor, entry) : std::make_pair(entry, donor);
    auto [liter, riter] = is_left ?
      std::make_pair(donor_iter, iter) : std::make_pair(iter, donor_iter);
    if (l->can_merge(r)) {
      DEBUGT("make_full_merge l {} r {}", oc.t, *l, *r);
      assert(entry->extent_is_below_min());
      return l->make_full_merge(oc, r
      ).si_then([liter=liter, riter=riter, l=l, r=r, oc, this]
		(auto &&replacement) {
	LOG_PREFIX(OMapInnerNode::merge_entry);
	DEBUGT("to update parent: {}", oc.t, *this);
        journal_inner_update(
	  liter,
	  replacement->get_laddr(),
	  maybe_get_delta_buffer());
        journal_inner_remove(riter, maybe_get_delta_buffer());
        //retire extent
        std::vector<laddr_t> dec_laddrs {l->get_laddr(), r->get_laddr()};
        return dec_ref(oc, dec_laddrs
	).si_then([this, oc] {
	  --(oc.t.get_omap_tree_stats().extents_num_delta);
          if (extent_is_below_min()) {
            return merge_entry_ret(
                   interruptible::ready_future_marker{},
                   mutation_result_t(mutation_status_t::NEED_MERGE,
		     std::nullopt, this));
          } else {
            return merge_entry_ret(
                   interruptible::ready_future_marker{},
                   mutation_result_t(mutation_status_t::SUCCESS,
		     std::nullopt, std::nullopt));
          }
        });
      });
    } else {
      DEBUGT("balanced l {} r {}", oc.t, *l, *r);
      return l->make_balanced(oc, r
      ).si_then([liter=liter, riter=riter, l=l, r=r, oc, this](auto tuple) {
	LOG_PREFIX(OMapInnerNode::merge_entry);
	DEBUGT("to update parent: {}", oc.t, *this);
        auto [replacement_l, replacement_r, replacement_pivot] = tuple;
        //update operation will not cuase node overflow, so we can do it first
        journal_inner_update(
	  liter,
	  replacement_l->get_laddr(),
	  maybe_get_delta_buffer());
        bool overflow = extent_will_overflow(replacement_pivot.size(),
	  std::nullopt);
        if (!overflow) {
          journal_inner_remove(riter, maybe_get_delta_buffer());
          journal_inner_insert(
	    riter,
	    replacement_r->get_laddr(),
	    replacement_pivot,
	    maybe_get_delta_buffer());
          std::vector<laddr_t> dec_laddrs{l->get_laddr(), r->get_laddr()};
          return dec_ref(oc, dec_laddrs
	  ).si_then([] {
            return merge_entry_ret(
                   interruptible::ready_future_marker{},
                   mutation_result_t(mutation_status_t::SUCCESS,
		     std::nullopt, std::nullopt));
          });
        } else {
          DEBUGT("balanced and split {} r {}", oc.t, *l, *r);
          //use remove and insert to instead of replace,
          //remove operation will not cause node split, so we can do it first
          journal_inner_remove(riter, maybe_get_delta_buffer());
          return make_split_insert(oc, riter, replacement_pivot,
	    replacement_r->get_laddr()
	  ).si_then([this, oc, l = l, r = r](auto mresult) {
	    std::vector<laddr_t> dec_laddrs{
	      l->get_laddr(),
	      r->get_laddr(),
	      get_laddr()};
	    return dec_ref(oc, dec_laddrs
	    ).si_then([mresult = std::move(mresult)] {
	      return merge_entry_ret(
		     interruptible::ready_future_marker{}, mresult);
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
  LOG_PREFIX(OMapLeafNode::get_value);
  DEBUGT("key = {}, this: {}", oc.t, *this, key);
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
  LOG_PREFIX(OMapLeafNode::insert);
  DEBUGT("{} -> {}, this: {}", oc.t, key, value, *this);
  bool overflow = extent_will_overflow(key.size(), value.length());
  if (!overflow) {
    if (!is_mutable()) {
      auto mut = oc.tm.get_mutable_extent(oc.t, this)->cast<OMapLeafNode>();
      return mut->insert(oc, key, value);
    }
    auto replace_pt = find_string_key(key);
    if (replace_pt != iter_end()) {
      ++(oc.t.get_omap_tree_stats().num_updates);
      journal_leaf_update(replace_pt, key, value, maybe_get_delta_buffer());
    } else {
      ++(oc.t.get_omap_tree_stats().num_inserts);
      auto insert_pt = string_lower_bound(key);
      journal_leaf_insert(insert_pt, key, value, maybe_get_delta_buffer());

      DEBUGT("inserted {}, this: {}", oc.t, insert_pt.get_key(), *this);
    }
    return insert_ret(
           interruptible::ready_future_marker{},
           mutation_result_t(mutation_status_t::SUCCESS, std::nullopt, std::nullopt));
  } else {
    return make_split_children(oc).si_then([this, oc, &key, &value] (auto tuple) {
      auto [left, right, pivot] = tuple;
      auto replace_pt = find_string_key(key);
      if (replace_pt != iter_end()) {
        ++(oc.t.get_omap_tree_stats().num_updates);
        if (key < pivot) {  //left
          auto mut_iter = left->iter_idx(replace_pt->get_index());
          left->journal_leaf_update(mut_iter, key, value, left->maybe_get_delta_buffer());
        } else if (key >= pivot) { //right
          auto mut_iter = right->iter_idx(replace_pt->get_index() - left->get_node_size());
          right->journal_leaf_update(mut_iter, key, value, right->maybe_get_delta_buffer());
        }
      } else {
        ++(oc.t.get_omap_tree_stats().num_inserts);
        auto insert_pt = string_lower_bound(key);
        if (key < pivot) {  //left
          auto mut_iter = left->iter_idx(insert_pt->get_index());
          left->journal_leaf_insert(mut_iter, key, value, left->maybe_get_delta_buffer());
        } else {
          auto mut_iter = right->iter_idx(insert_pt->get_index() - left->get_node_size());
          right->journal_leaf_insert(mut_iter, key, value, right->maybe_get_delta_buffer());
        }
      }
      ++(oc.t.get_omap_tree_stats().extents_num_delta);
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
  LOG_PREFIX(OMapLeafNode::rm_key);
  DEBUGT("{}, this: {}", oc.t, key, *this);
  auto rm_pt = find_string_key(key);
  if (!is_mutable() && rm_pt != iter_end()) {
    auto mut =  oc.tm.get_mutable_extent(oc.t, this)->cast<OMapLeafNode>();
    return mut->rm_key(oc, key);
  }

  if (rm_pt != iter_end()) {
    ++(oc.t.get_omap_tree_stats().num_erases);
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
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  omap_list_config_t config)
{
  LOG_PREFIX(OMapLeafNode::list);
  DEBUGT(
    "first {} last {}  max_result_size {} first_inclusive {} \
    last_inclusive {}, this: {}",
    oc.t,
    first ? first->c_str() : "",
    last ? last->c_str() : "",
    config.max_result_size,
    config.first_inclusive,
    config.last_inclusive,
    *this
  );
  auto ret = list_bare_ret(false, {});
  auto &[complete, result] = ret;
  auto iter = first ?
    (config.first_inclusive ?
     string_lower_bound(*first) :
     string_upper_bound(*first)) :
    iter_begin();
  auto liter = last ?
    (config.last_inclusive ?
     string_upper_bound(*last) :
     string_lower_bound(*last)) :
    iter_end();

  for (; iter != liter && result.size() < config.max_result_size; iter++) {
    result.emplace(std::make_pair(iter->get_key(), iter->get_val()));
    DEBUGT("found key {}", oc.t, iter->get_key());
  }

  complete = (iter == liter);

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
  LOG_PREFIX(OMapLeafNode::make_split_children);
  DEBUGT("this: {}", oc.t, *this);
  return oc.tm.alloc_extents<OMapLeafNode>(oc.t, oc.hint, OMAP_LEAF_BLOCK_SIZE, 2)
    .si_then([this] (auto &&ext_pair) {
      auto left = ext_pair.front();
      auto right = ext_pair.back();
      return split_children_ret(
             interruptible::ready_future_marker{},
             std::make_tuple(left, right, split_into(*left, *right)));
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    split_children_iertr::pass_further{}
  );
}

OMapLeafNode::full_merge_ret
OMapLeafNode::make_full_merge(omap_context_t oc, OMapNodeRef right)
{
  ceph_assert(right->get_type() == TYPE);
  LOG_PREFIX(OMapLeafNode::make_full_merge);
  DEBUGT("this: {}", oc.t, *this);
  return oc.tm.alloc_non_data_extent<OMapLeafNode>(oc.t, oc.hint, OMAP_LEAF_BLOCK_SIZE)
    .si_then([this, right] (auto &&replacement) {
      replacement->merge_from(*this, *right->cast<OMapLeafNode>());
      return full_merge_ret(
        interruptible::ready_future_marker{},
        std::move(replacement));
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    full_merge_iertr::pass_further{}
  );
}

OMapLeafNode::make_balanced_ret
OMapLeafNode::make_balanced(omap_context_t oc, OMapNodeRef _right)
{
  ceph_assert(_right->get_type() == TYPE);
  LOG_PREFIX(OMapLeafNode::make_balanced);
  DEBUGT("this: {}",  oc.t, *this);
  return oc.tm.alloc_extents<OMapLeafNode>(oc.t, oc.hint, OMAP_LEAF_BLOCK_SIZE, 2)
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
  }).handle_error_interruptible(
    crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
    make_balanced_iertr::pass_further{}
  );
}


omap_load_extent_iertr::future<OMapNodeRef>
omap_load_extent(omap_context_t oc, laddr_t laddr, depth_t depth)
{
  ceph_assert(depth > 0);
  if (depth > 1) {
    return oc.tm.read_extent<OMapInnerNode>(oc.t, laddr,
      OMAP_INNER_BLOCK_SIZE)
      .handle_error_interruptible(
      omap_load_extent_iertr::pass_further{},
      crimson::ct_error::assert_all{ "Invalid error in omap_load_extent" }
    ).si_then(
      [](auto&& e) {
      return seastar::make_ready_future<OMapNodeRef>(std::move(e));
    });
  } else {
    return oc.tm.read_extent<OMapLeafNode>(oc.t, laddr, OMAP_LEAF_BLOCK_SIZE
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
