// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_ATTACH_PARENT_REQUEST_H
#define CEPH_LIBRBD_IMAGE_ATTACH_PARENT_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/Types.h"

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class AttachParentRequest {
public:
  static AttachParentRequest* create(ImageCtxT& image_ctx,
                                     const cls::rbd::ParentImageSpec& pspec,
                                     uint64_t parent_overlap,
                                     bool reattach,
                                     Context* on_finish) {
    return new AttachParentRequest(image_ctx, pspec, parent_overlap, reattach,
                                   on_finish);
  }

  AttachParentRequest(ImageCtxT& image_ctx,
                      const cls::rbd::ParentImageSpec& pspec,
                      uint64_t parent_overlap, bool reattach,
                      Context* on_finish)
    : m_image_ctx(image_ctx), m_parent_image_spec(pspec),
      m_parent_overlap(parent_overlap), m_reattach(reattach),
      m_on_finish(on_finish) {
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
   * ATTACH_PARENT * * *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT& m_image_ctx;
  cls::rbd::ParentImageSpec m_parent_image_spec;
  uint64_t m_parent_overlap;
  bool m_reattach;
  Context* m_on_finish;

  bool m_legacy_parent = false;

  void attach_parent();
  void handle_attach_parent(int r);

  void finish(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::AttachParentRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_ATTACH_PARENT_REQUEST_H
