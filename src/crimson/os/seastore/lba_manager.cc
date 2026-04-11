// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/lba/btree_lba_manager.h"

namespace crimson::os::seastore {

LBAManagerRef lba::create_lba_manager(Cache &cache) {
  return LBAManagerRef(new lba::BtreeLBAManager(cache));
}

}
