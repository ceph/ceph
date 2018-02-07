// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_TYPES
#define CEPH_LIBRBD_CACHE_TYPES

#include "include/int_types.h"
#include "include/lru.h"
#include <vector>

namespace librbd {
namespace cache {

#define MAX_BLOCK_ID         0X3FFFFFFF
#define LOCATE_IN_BASE_CACHE 0X01
#define LOCATE_IN_CACHE      0X02
#define NOT_IN_CACHE         0X03

class Entry : public LRUObject {
public:
  uint64_t on_disk_id;
  Entry() : on_disk_id(0) {}
  Entry(const Entry& other) {
    on_disk_id = other.on_disk_id;
  }
};

enum PolicyMapResult {
  POLICY_MAP_RESULT_HIT,    // block is already in cache
  POLICY_MAP_RESULT_HIT_IN_BASE,    // block is already in base cache
  POLICY_MAP_RESULT_MISS,   // block not in cache, do not promote
  POLICY_MAP_RESULT_NEW,    // block not in cache, promote
  POLICY_MAP_RESULT_REPLACE // block not in cache, demote other block first
};

enum IOType {
  IO_TYPE_WRITE   = 0,
  IO_TYPE_DISCARD = 1,
  IO_TYPE_FLUSH   = 2,
  IO_TYPE_READ    = 3
};

typedef std::vector<uint64_t> BufferOffsets;
typedef std::vector<std::pair<uint64_t, uint64_t> > ImageExtents;
typedef std::vector<std::pair<uint32_t, uint32_t> > BlockExtents;

} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_TYPES
