// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_POLICY
#define CEPH_LIBRBD_CACHE_FILE_POLICY

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "librbd/cache/Types.h"

namespace librbd {
namespace cache {
namespace file {

/**
 * Cache-replacement policy for image store
 */
class Policy {
public:
  virtual ~Policy() {
  }

  virtual void set_block_count(uint64_t block_count) = 0;

  virtual int invalidate(uint64_t block) = 0;

  virtual int map(IOType io_type, uint64_t block, bool partial_block,
                  PolicyMapResult *policy_map_result,
                  uint64_t *replace_cache_block) = 0;
  virtual void tick() = 0;

};

} // namespace file
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_FILE_POLICY
