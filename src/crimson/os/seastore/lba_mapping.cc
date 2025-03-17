// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "lba_mapping.h"

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs)
{
  out << "LBAMapping(" << rhs.get_key()
      << "~0x" << std::hex << rhs.get_length() << std::dec
      << "->" << rhs.get_val();
  if (rhs.is_indirect()) {
    out << ",indirect(" << rhs.get_intermediate_base()
        << "~0x" << std::hex << rhs.get_intermediate_length()
        << "@0x" << rhs.get_intermediate_offset() << std::dec
        << ")";
  }
  out << ")";
  return out;
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

LBAMappingRef LBAMapping::duplicate() const {
  auto ret = _duplicate(ctx);
  ret->range = range;
  ret->value = value;
  ret->parent = parent;
  ret->len = len;
  ret->pos = pos;
  return ret;
}

} // namespace crimson::os::seastore
