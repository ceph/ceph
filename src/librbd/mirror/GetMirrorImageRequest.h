// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GET_MIRROR_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GET_MIRROR_IMAGE_REQUEST_H

#include "include/buffer.h"
#include "include/common_fwd.h"
#include "include/rados/librados.hpp"
#include <string>

struct Context;

namespace cls { namespace rbd { struct MirrorImage; } }

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GetMirrorImageRequest {
public:
  static GetMirrorImageRequest *create(
      librados::IoCtx &io_ctx, const std::string &image_id,
      cls::rbd::MirrorImage *mirror_image, Context *on_finish) {
    return new GetMirrorImageRequest(io_ctx, image_id, mirror_image,
                                     on_finish);
  }

  GetMirrorImageRequest(librados::IoCtx& io_ctx,
                        const std::string &image_id,
                        cls::rbd::MirrorImage *mirror_image,
                        Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   *       <start>
   *          |
   *          v
   *   GET_MIRROR_IMAGE
   *          |
   *          |
   *          v
   *       <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_image_id;
  cls::rbd::MirrorImage *m_mirror_image;
  Context *m_on_finish;

  CephContext *m_cct;

  bufferlist m_out_bl;

  void get_mirror_image();
  void handle_get_mirror_image(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GetMirrorImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GET_MIRROR_IMAGE_REQUEST_H

