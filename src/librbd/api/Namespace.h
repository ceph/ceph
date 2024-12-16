// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_NAMESPACE_H
#define CEPH_LIBRBD_API_NAMESPACE_H

#include "include/rados/librados_fwd.hpp"
#include "include/rbd/librbd.hpp"
#include <string>
#include <vector>

namespace librbd {

struct ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
struct Namespace {

  static int create(librados::IoCtx& io_ctx, const std::string& name);
  static int remove(librados::IoCtx& io_ctx, const std::string& name);
  static int list(librados::IoCtx& io_ctx, std::vector<std::string>* names);
  static int exists(librados::IoCtx& io_ctx, const std::string& name, bool *exists);

};

} // namespace api
} // namespace librbd

extern template class librbd::api::Namespace<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_NAMESPACE_H
