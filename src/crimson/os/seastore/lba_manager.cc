// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

namespace crimson::os::seastore {

LBAManager::update_mappings_ret
LBAManager::update_mappings(
  Transaction& t,
  const std::list<LogicalCachedExtentRef>& extents)
{
  return trans_intr::do_for_each(extents,
				 [this, &t](auto &extent) {
    return update_mapping(
      t,
      extent->get_laddr(),
      extent->get_length(),
      extent->get_prior_paddr_and_reset(),
      extent->get_length(),
      extent->get_paddr(),
      nullptr	// all the extents should have already been
		// added to the fixed_kv_btree
    );
  });
}

LBAManagerRef lba_manager::create_lba_manager(Cache &cache) {
  return LBAManagerRef(new btree::BtreeLBAManager(cache));
}

}
