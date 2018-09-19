// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_DETACH_PARENT_REQUEST_H
#define CEPH_LIBRBD_IMAGE_DETACH_PARENT_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/Types.h"

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class DetachParentRequest {
public:
  static DetachParentRequest* create(ImageCtxT& image_ctx, Context* on_finish) {
    return new DetachParentRequest(image_ctx, on_finish);
  }

  DetachParentRequest(ImageCtxT& image_ctx, Context* on_finish)
    : m_image_ctx(image_ctx), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |    * * * * * *
   *    |    *         * -EOPNOTSUPP
   *    v    v         *
   * DETACH_PARENT * * *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT& m_image_ctx;
  Context* m_on_finish;

  bool m_legacy_parent = false;

  void detach_parent();
  void handle_detach_parent(int r);

  void finish(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::DetachParentRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_DETACH_PARENT_REQUEST_H
