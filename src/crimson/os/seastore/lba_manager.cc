// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/cache.h"
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
      return update_mapping(
          t,
          (*iter_extents)->get_laddr(),
          *iter_original_paddrs,
          (*iter_extents)->get_paddr()
      ).si_then([&iter_extents, &iter_original_paddrs] {
        ++iter_extents;
        ++iter_original_paddrs;
        return seastar::stop_iteration::no;
      });
    });
  });
}

LBAManagerRef lba_manager::create_lba_manager(
  SegmentManager &segment_manager,
  Cache &cache) {
  return LBAManagerRef(new btree::BtreeLBAManager(segment_manager, cache));
}

}
