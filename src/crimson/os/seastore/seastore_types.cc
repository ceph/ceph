// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

std::ostream &segment_to_stream(std::ostream &out, const segment_id_t &t)
{
  if (t == NULL_SEG_ID)
    return out << "NULL_SEG";
  else if (t == BLOCK_REL_SEG_ID)
    return out << "BLOCK_REL_SEG";
  else if (t == RECORD_REL_SEG_ID)
    return out << "RECORD_REL_SEG";
  else
    return out << t;
}

std::ostream &offset_to_stream(std::ostream &out, const segment_off_t &t)
{
  if (t == NULL_SEG_OFF)
    return out << "NULL_OFF";
  else
    return out << t;
}

std::ostream &operator<<(std::ostream &out, const paddr_t &rhs)
{
  out << "paddr_t<";
  segment_to_stream(out, rhs.segment);
  out << ", ";
  offset_to_stream(out, rhs.offset);
  return out << ">";
}

std::ostream &operator<<(std::ostream &out, extent_types_t t)
{
  switch (t) {
  case extent_types_t::ROOT_LOCATION:
    return out << "ROOT_LOCATION";
  case extent_types_t::ROOT:
    return out << "ROOT";
  case extent_types_t::LADDR_INTERNAL:
    return out << "LADDR_INTERNAL";
  case extent_types_t::LADDR_LEAF:
    return out << "LADDR_LEAF";
  case extent_types_t::TEST_BLOCK:
    return out << "TEST_BLOCK";
  case extent_types_t::NONE:
    return out << "NONE";
  default:
    return out << "UNKNOWN";
  }
}

std::ostream &operator<<(std::ostream &out, const laddr_list_t &rhs)
{
  bool first = false;
  for (auto &i: rhs) {
    out << (first ? '[' : ',') << '(' << i.first << ',' << i.second << ')';
    first = true;
  }
  return out << ']';
}
std::ostream &operator<<(std::ostream &out, const paddr_list_t &rhs)
{
  bool first = false;
  for (auto &i: rhs) {
    out << (first ? '[' : ',') << '(' << i.first << ',' << i.second << ')';
    first = true;
  }
  return out << ']';
}

}
