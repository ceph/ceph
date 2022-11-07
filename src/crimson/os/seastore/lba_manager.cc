// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

namespace crimson::os::seastore {

LBAManager::update_mappings_ret
LBAManager::update_mappings(
  Transaction& t,
  const std::list<LogicalCachedExtentRef>& extents,
  const std::vector<paddr_t>& original_paddrs)
{
  assert(extents.size() == original_paddrs.size());
  auto extents_end = extents.end();
  return seastar::do_with(
      extents.begin(),
      original_paddrs.begin(),
      [this, extents_end, &t](auto& iter_extents,
                              auto& iter_original_paddrs) {
    return trans_intr::repeat(
      [this, extents_end, &t, &iter_extents, &iter_original_paddrs]
    {
      if (extents_end == iter_extents) {
        return update_mappings_iertr::make_ready_future<
          seastar::stop_iteration>(seastar::stop_iteration::yes);
      }
      assert((*iter_extents)->get_parent_tracker());
      assert((*iter_extents)->get_parent_tracker()->get_parent());
#ifndef NDEBUG
      auto leaf = (*iter_extents)->get_parent_tracker()->template get_parent<
	  lba_manager::btree::LBALeafNode<true>>();
      auto it = leaf->mutate_state.pending_children.find(
	  (*iter_extents)->get_laddr(),
	  typename lba_manager::btree::LBALeafNode<true>::pending_child_tracker_t::cmp_t());
      assert(it != leaf->mutate_state.pending_children.end()
	&& it->op != lba_manager::btree::LBALeafNode<true>::op_t::REMOVE);
#endif
      return update_mapping(
          t,
          (*iter_extents)->get_laddr(),
          *iter_original_paddrs,
          (*iter_extents)->get_paddr(),
	  iter_extents->get()
      ).si_then([&iter_extents, &iter_original_paddrs] {
        ++iter_extents;
        ++iter_original_paddrs;
        return seastar::stop_iteration::no;
      });
    });
  });
}

template <bool leaf_has_children>
LBAManagerRef lba_manager::create_lba_manager(Cache &cache) {
  return LBAManagerRef(new btree::BtreeLBAManager<leaf_has_children>(cache));
}

template  LBAManagerRef lba_manager::create_lba_manager<true>(Cache &cache);
template  LBAManagerRef lba_manager::create_lba_manager<false>(Cache &cache);

}
