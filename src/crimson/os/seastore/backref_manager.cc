// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/backref_manager.h"
#include "crimson/os/seastore/backref/btree_backref_manager.h"

namespace crimson::os::seastore {

BackrefManagerRef create_backref_manager(
  SegmentManagerGroup &sm_group,
  Cache &cache)
{
  return BackrefManagerRef(
    new backref::BtreeBackrefManager(
      sm_group, cache));
}

} // namespace crimson::os::seastore::backref

