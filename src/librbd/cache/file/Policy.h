// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_POLICY
#define CEPH_LIBRBD_CACHE_FILE_POLICY

#include "include/buffer_fwd.h"
#include "include/int_types.h"

namespace librbd {
namespace cache {
namespace file {

/**
 * Cache-replacement policy for image store
 */
class Policy {
public:
  enum MapResult {
    MAP_RESULT_HIT,    // block is already in cache
    MAP_RESULT_MISS,   // block not in cache, do not promote
    MAP_RESULT_NEW,    // block not in cache, promote
    MAP_RESULT_REPLACE // block not in cache, demote other block first
  };

  enum OpType {
    OP_TYPE_READ,
    OP_TYPE_WRITE,
    OP_TYPE_DISCARD
  };

  virtual ~Policy() {
  }

  virtual int map(OpType op_type, uint64_t block, bool partial_block,
                  MapResult *map_result, uint64_t *replace_cache_block) = 0;
  virtual void tick() = 0;

};

} // namespace file
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_FILE_POLICY
