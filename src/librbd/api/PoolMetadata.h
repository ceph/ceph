// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_POOL_METADATA_H
#define CEPH_LIBRBD_API_POOL_METADATA_H

#include "include/buffer_fwd.h"
#include "include/rados/librados_fwd.hpp"

#include <cstdint>
#include <map>
#include <string>

namespace librbd {

class ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
class PoolMetadata {
public:
  static int get(librados::IoCtx& io_ctx, const std::string &key,
                 std::string *value);
  static int set(librados::IoCtx& io_ctx, const std::string &key,
                 const std::string &value);
  static int remove(librados::IoCtx& io_ctx, const std::string &key);
  static int list(librados::IoCtx& io_ctx, const std::string &start,
                  uint64_t max, std::map<std::string, ceph::bufferlist> *pairs);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::PoolMetadata<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_POOL_METADATA_H
