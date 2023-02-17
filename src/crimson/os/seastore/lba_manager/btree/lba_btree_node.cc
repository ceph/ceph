// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <memory>
#include <string.h>

#include "include/buffer.h"
#include "include/byteorder.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_lba);

namespace crimson::os::seastore::lba_manager::btree {

std::ostream& operator<<(std::ostream& out, const lba_map_val_t& v)
{
  return out << "lba_map_val_t("
             << v.paddr
             << "~" << v.len
             << ", refcount=" << v.refcount
             << ", checksum=" << v.checksum
             << ")";
}

std::ostream &LBALeafNode::print_detail(std::ostream &out) const
{
  return out << ", size=" << get_size()
	     << ", meta=" << get_meta();
}

void LBALeafNode::resolve_relative_addrs(paddr_t base)
{
  LOG_PREFIX(LBALeafNode::resolve_relative_addrs);
  for (auto i: *this) {
    if (i->get_val().paddr.is_relative()) {
      auto val = i->get_val();
      val.paddr = base.add_relative(val.paddr);
      TRACE("{} -> {}", i->get_val().paddr, val.paddr);
      i->set_val(val);
    }
  }
}

}
