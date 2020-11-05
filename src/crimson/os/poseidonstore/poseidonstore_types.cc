// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/poseidonstore/poseidonstore_types.h"

namespace crimson::os::poseidonstore {
std::ostream &operator<<(std::ostream &out, const wal_seq_t &seq)
{
  return out << "wal_seq_t(addr="
             << seq.addr << ", id="
             << seq.id
             << ", length="
             << seq.length
             << ")";
}

std::ostream &operator<<(std::ostream &out, ce_types_t t)
{
  switch (t) {
    case ce_types_t::TEST_BLOCK:
      return out << "TEST_BLOCK";
    case ce_types_t::NONE:
      return out << "NONE";
    default:
      return out << "UNKNOWN";
  }
}
 
}
