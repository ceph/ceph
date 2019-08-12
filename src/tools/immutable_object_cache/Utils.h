// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CACHE_UTILS_H
#define CEPH_CACHE_UTILS_H

#include "include/rados/librados.hpp"
#include "include/Context.h"

namespace ceph {
namespace immutable_obj_cache {
namespace detail {

template <typename T, void(T::*MF)(int)>
void rados_callback(rados_completion_t c, void *arg) {
  T *obj = reinterpret_cast<T*>(arg);
  int r = rados_aio_get_return_value(c);
  (obj->*MF)(r);
}

}  // namespace detail

template <typename T, void(T::*MF)(int)=&T::complete>
librados::AioCompletion *create_rados_callback(T *obj) {
  return librados::Rados::aio_create_completion(
    obj, &detail::rados_callback<T, MF>, nullptr);
}

}  // namespace immutable_obj_cache
}  // namespace ceph
#endif  // CEPH_CACHE_UTILS_H
