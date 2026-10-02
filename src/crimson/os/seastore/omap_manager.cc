// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"

namespace crimson::os::seastore::omap_manager {

OMapManagerRef create_omap_manager(TransactionManager &trans_manager) {
  return OMapManagerRef(new BtreeOMapManager(trans_manager));
}

}
