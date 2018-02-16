// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_STACKING_IMAGE_CACHE
#define CEPH_LIBRBD_CACHE_STACKING_IMAGE_CACHE

#include "include/buffer_fwd.h"
#include "include/int_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/ImageCache.h"
#include <vector>

class Context;

namespace librbd {
namespace cache {

/**
 * Stacking Image caches
 */
template <typename ImageCtxT = librbd::ImageCtx>
struct StackingImageCache : public ImageCache {
  using typename ImageCache::Extent;
  using typename ImageCache::Extents;
  using typename ImageCache::CacheMode;
};

} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_STACKING_IMAGE_CACHE
