// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
#define CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY

#include "librbd/cache/file/Policy.h"
#include "include/lru.h"
#include "librbd/cache/file/Types.h"

namespace librbd {

struct ImageCtx;

namespace cache {
namespace file {

/**
 * Stupid LRU-style policy
 */
template <typename ImageCtxT>
class StupidPolicy : public Policy {
public:

  virtual int map(OpType op_type, uint64_t block, bool partial_block,
                  MapResult *map_result, uint64_t *replace_cache_block);
  virtual void tick();

private:

  struct Entry : public LRUObject {
    uint64_t block;
    bool dirty;
  };

};

} // namespace file
} // namespace cache
} // namespace librbd

extern template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_FILE_STUPID_POLICY
