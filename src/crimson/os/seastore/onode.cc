// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "onode.h"
#include <iostream>
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager/log/log_manager.h"

namespace crimson::os::seastore {

std::shared_ptr<OMapManager> Onode::get_manager(TransactionManager& tm) {
  /* 
   * if LOG is set, root should be initialized by set_alloc_hint
   * before accessing object, otherwise BtreeOMapManager is used
   */
  auto log_root = get_root(omap_type_t::LOG);
  if (!mgr) {
    if (log_root.is_null()) {
      mgr = std::make_unique<crimson::os::seastore::omap_manager::BtreeOMapManager>(tm);
    } else {  
      mgr = std::make_unique<crimson::os::seastore::log_manager::LogManager>(tm);
    }
  }
  return mgr;
}

std::ostream& operator<<(std::ostream &out, const Onode &rhs)
{
  auto &layout = rhs.get_layout();
  return out << "Onode("
	     << "hobj=" << rhs.hobj << ", "
             << "size=0x" << std::hex << static_cast<uint32_t>(layout.size) << std::dec
             << ")";
}

}

