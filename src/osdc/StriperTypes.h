// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OSDC_STRIPER_TYPES_H
#define CEPH_OSDC_STRIPER_TYPES_H

#include "include/types.h"
#include <boost/container/small_vector.hpp>
#include <ios>
#include <utility>

namespace striper {

// off -> len extents in (striped) buffer being mapped
typedef std::pair<uint64_t,uint64_t> BufferExtent;
typedef boost::container::small_vector<
    BufferExtent, 4> LightweightBufferExtents;

struct LightweightObjectExtent {
  LightweightObjectExtent() = delete;
  LightweightObjectExtent(uint64_t object_no, uint64_t offset,
                          uint64_t length, uint64_t truncate_size)
    : object_no(object_no), offset(offset), length(length),
      truncate_size(truncate_size) {
  }

  uint64_t object_no;
  uint64_t offset;        // in-object
  uint64_t length;        // in-object
  uint64_t truncate_size; // in-object
  LightweightBufferExtents buffer_extents;
};

typedef boost::container::small_vector<
    LightweightObjectExtent, 4> LightweightObjectExtents;

inline std::ostream& operator<<(std::ostream& os,
                                const LightweightObjectExtent& ex) {
  return os << "extent("
            << ex.object_no << " "
            << ex.offset << "~" << ex.length
            << " -> " << ex.buffer_extents
            << ")";
}

} // namespace striper

#endif // CEPH_OSDC_STRIPER_TYPES_H
