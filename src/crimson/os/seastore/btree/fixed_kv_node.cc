// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const LBAPin &rhs)
{
  return out << "LBAPin(" << rhs.get_key() << "~" << rhs.get_length()
	     << "->" << rhs.get_val();
}

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs)
{
  bool first = true;
  out << '[';
  for (const auto &i: rhs) {
    out << (first ? "" : ",") << *i;
    first = false;
  }
  return out << ']';
}

} // namespace crimson::os::seastore
