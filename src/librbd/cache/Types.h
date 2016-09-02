// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_TYPES
#define CEPH_LIBRBD_CACHE_TYPES

#include "include/int_types.h"
#include <vector>

namespace librbd {
namespace cache {

enum PolicyMapResult {
  POLICY_MAP_RESULT_HIT,    // block is already in cache
  POLICY_MAP_RESULT_MISS,   // block not in cache, do not promote
  POLICY_MAP_RESULT_NEW,    // block not in cache, promote
  POLICY_MAP_RESULT_REPLACE // block not in cache, demote other block first
};

enum IOType {
  IO_TYPE_READ,
  IO_TYPE_WRITE,
  IO_TYPE_DISCARD
};

typedef std::vector<uint64_t> BufferOffsets;
typedef std::vector<std::pair<uint64_t, uint64_t> > ImageExtents;
typedef std::vector<std::pair<uint32_t, uint32_t> > BlockExtents;

} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_TYPES
