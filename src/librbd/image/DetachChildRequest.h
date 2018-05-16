// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_DETACH_CHILD_REQUEST_H
#define CEPH_LIBRBD_IMAGE_DETACH_CHILD_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/Types.h"

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class DetachChildRequest {
public:
  static DetachChildRequest* create(ImageCtxT& image_ctx, Context* on_finish) {
    return new DetachChildRequest(image_ctx, on_finish);
  }

  DetachChildRequest(ImageCtxT& image_ctx, Context* on_finish)
    : m_image_ctx(image_ctx), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v (skip if v1)
   * CHILD_DETACH
   *    |
   *    v (skip if v2)
   * REMOVE_CHILD
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT& m_image_ctx;
  Context* m_on_finish;

  librados::IoCtx m_parent_io_ctx;
  ParentSpec m_parent_spec;

  ceph::bufferlist m_out_bl;

  void clone_v2_child_detach();
  void handle_clone_v2_child_detach(int r);

  void clone_v1_remove_child();
  void handle_clone_v1_remove_child(int r);

  void finish(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::DetachChildRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_DETACH_CHILD_REQUEST_H
