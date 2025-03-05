// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

namespace crimson::os::seastore {

LBAManager::update_mappings_ret
LBAManager::update_mappings(
  Transaction& t,
  const std::list<LogicalChildNodeRef>& extents)
{
  return trans_intr::do_for_each(extents,
				 [this, &t](auto &extent) {
    return get_mapping(t, *extent
    ).si_then([this, &t, &extent](auto mapping) {
      return update_mapping(
	t,
	std::move(mapping),
	extent->get_length(),
	extent->get_prior_paddr_and_reset(),
	extent->get_length(),
	extent->get_paddr(),
	extent->get_last_committed_crc(),
	nullptr	// all the extents should have already been
		  // added to the fixed_kv_btree
      ).discard_result();
    }).handle_error_interruptible(
      update_mappings_iertr::pass_further{},
      crimson::ct_error::assert_all{"unexpected enoent"}
    );
  });
}

LBAManagerRef lba_manager::create_lba_manager(Cache &cache) {
  return LBAManagerRef(new btree::BtreeLBAManager(cache));
}

}
