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
  return seastar::do_with(
    LBAIter{},
    [this, &t, &extents](LBAIter &iter) {
      return trans_intr::do_for_each(
	extents,
	[this, &t, &iter](auto &extent) {
	  return make_iterator(t, extent
	  ).si_then([this, &t, &iter, &extent](LBAIter i) {
	    iter = i;
	    auto val = iter.val;
	    ceph_assert(extent->get_laddr() == iter.key);
	    ceph_assert(val.pladdr.get_paddr() ==
			extent->get_prior_paddr_and_reset());
	    val.pladdr = extent->get_paddr();
	    val.len = extent->get_length();
	    val.checksum = extent->get_last_committed_crc();
	    return change_mapping(t, iter, iter.key, val, nullptr)
		.discard_result();
	  });
	});
  });
}

LBAManagerRef lba_manager::create_lba_manager(Cache &cache) {
  return LBAManagerRef(new btree::BtreeLBAManager(cache));
}

}
