// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/file/StupidPolicy.h"

namespace librbd {
namespace cache {
namespace file {

template <typename I>
int StupidPolicy<I>::map(OpType op_type, uint64_t block, bool partial_block,
                         MapResult *map_result, uint64_t *replace_cache_block) {
  // TODO
  *map_result = MAP_RESULT_MISS;
  return 0;
}

template <typename I>
void StupidPolicy<I>::tick() {
  // TODO
}

} // namespace file
} // namespace cache
} // namespace librbd

template class librbd::cache::file::StupidPolicy<librbd::ImageCtx>;
