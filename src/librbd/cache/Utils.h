// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_UTILS_H
#define CEPH_LIBRBD_CACHE_UTILS_H

namespace librbd {

namespace cache {
namespace util {

template <typename T>
bool is_rwl_enabled(T& image_ctx) {
#if defined(WITH_RBD_RWL)
  return image_ctx.config.template get_val<bool>("rbd_rwl_enabled");
#else
  return false;
#endif
}

} // namespace util
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_UTILS_H
