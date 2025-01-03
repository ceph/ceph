// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_UTILS_H
#define CEPH_LIBRBD_CACHE_UTILS_H

#include "acconfig.h"
#include <string>

class Context;

namespace librbd {

struct ImageCtx;

namespace cache {
namespace util {

template <typename T>
bool is_pwl_enabled(T& image_ctx) {
#if defined(WITH_RBD_RWL) || defined(WITH_RBD_SSD_CACHE)
  auto value = image_ctx.config.template get_val<std::string>("rbd_persistent_cache_mode");
  return value == "disabled" ? false : true;
#else
  return false;
#endif // WITH_RBD_RWL
}

} // namespace util
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_UTILS_H
