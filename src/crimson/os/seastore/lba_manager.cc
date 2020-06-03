// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

namespace crimson::os::seastore::lba_manager {

LBAManagerRef create_lba_manager(
  SegmentManager &segment_manager,
  Cache &cache) {
  return LBAManagerRef(new btree::BtreeLBAManager(segment_manager, cache));
}

}

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const LBAPin &rhs)
{
  return out << "LBAPin(" << rhs.get_laddr() << "~" << rhs.get_length()
	     << "->" << rhs.get_paddr();
}

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (auto &i: rhs) {
    out << (first ? "" : ",") << *i;
    first = false;
  }
  return out << ']';
}

};
