// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_PERF_H
#define CEPH_LIBRBD_API_PERF_H

#include "librbd/Types.h"

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Perf {

  static int perf_reset(ImageCtxT *ictx);
  static int perf_dump(ImageCtxT *ictx, const std::string &format, bufferlist *outbl);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Perf<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_PERF_H
