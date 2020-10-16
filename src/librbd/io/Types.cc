// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/Types.h"
#include <iostream>

namespace librbd {
namespace io {

const WriteReadSnapIds INITIAL_WRITE_READ_SNAP_IDS{0, 0};

std::ostream& operator<<(std::ostream& os, SparseExtentState state) {
  switch (state) {
  case SPARSE_EXTENT_STATE_DNE:
    os << "dne";
    break;
  case SPARSE_EXTENT_STATE_ZEROED:
    os << "zeroed";
    break;
  case SPARSE_EXTENT_STATE_DATA:
    os << "data";
    break;
  default:
    ceph_abort();
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const SparseExtent& se) {
  os << "["
     << "state=" << se.state << ", "
     << "length=" << se.length << "]";
  return os;
}

} // namespace io
} // namespace librbd
