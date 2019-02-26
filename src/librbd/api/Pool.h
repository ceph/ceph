// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_POOL_H
#define CEPH_LIBRBD_API_POOL_H

#include "include/int_types.h"
#include "include/rados/librados_fwd.hpp"
#include "include/rbd/librbd.h"
#include <map>

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
class Pool {
public:
  typedef std::map<rbd_pool_stat_option_t, uint64_t*> StatOptions;

  static int init(librados::IoCtx& io_ctx, bool force);

  static int add_stat_option(StatOptions* stat_options,
                             rbd_pool_stat_option_t option,
                             uint64_t* value);

  static int get_stats(librados::IoCtx& io_ctx, StatOptions* stat_options);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Pool<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_POOL_H
