// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_API_CONFIG_H
#define CEPH_LIBRBD_API_CONFIG_H

#include "include/rbd/librbd.hpp"

namespace librados {

class IoCtx;

}

namespace librbd {

class ImageCtx;

namespace api {

template <typename ImageCtxT = librbd::ImageCtx>
class Config {
public:
  static bool is_option_name(librados::IoCtx& io_ctx, const std::string &name);
  static int list(librados::IoCtx& io_ctx,
                  std::vector<config_option_t> *options);

  static bool is_option_name(ImageCtxT *image_ctx, const std::string &name);
  static int list(ImageCtxT *image_ctx, std::vector<config_option_t> *options);
};

} // namespace api
} // namespace librbd

extern template class librbd::api::Config<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_API_CONFIG_H
