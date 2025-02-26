// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/btree_types.h"

namespace crimson::os::seastore {

namespace lba_manager::btree {

std::ostream& operator<<(std::ostream& out, const lba_map_val_t& v)
{
  return out << "lba_map_val_t("
             << v.pladdr
             << "~0x" << std::hex << v.len
             << ", checksum=0x" << v.checksum
             << ", refcount=" << std::dec << v.refcount
             << ")";
}

} // namespace lba_manager::btree

namespace backref {

std::ostream& operator<<(std::ostream &out, const backref_map_val_t& val) {
  return out << "backref_map_val_t("
	     << val.laddr
	     << "~0x" << std::hex << val.len << std::dec
	     << ")";
}

} // namespace backref
} // namespace crimson::os::seastore
