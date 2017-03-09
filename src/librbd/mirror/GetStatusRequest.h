// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GET_STATUS_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GET_STATUS_REQUEST_H

#include "include/buffer.h"
#include "librbd/mirror/Types.h"
#include <string>

struct Context;
namespace cls { namespace rbd { struct MirrorImage; } }
namespace cls { namespace rbd { struct MirrorImageStatus; } }

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GetStatusRequest {
public:
  static GetStatusRequest *create(ImageCtxT &image_ctx,
                                  cls::rbd::MirrorImageStatus *status,
                                  cls::rbd::MirrorImage *mirror_image,
                                  PromotionState *promotion_state,
                                  Context *on_finish) {
    return new GetStatusRequest(image_ctx, status, mirror_image,
                                promotion_state, on_finish);
  }

  GetStatusRequest(ImageCtxT &image_ctx, cls::rbd::MirrorImageStatus *status,
                   cls::rbd::MirrorImage *mirror_image,
                   PromotionState *promotion_state, Context *on_finish)
    : m_image_ctx(image_ctx), m_mirror_image_status(status),
      m_mirror_image(mirror_image), m_promotion_state(promotion_state),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_INFO
   *    |
   *    v
   * GET_STATUS
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT &m_image_ctx;
  cls::rbd::MirrorImageStatus *m_mirror_image_status;
  cls::rbd::MirrorImage *m_mirror_image;
  PromotionState *m_promotion_state;
  Context *m_on_finish;

  bufferlist m_out_bl;

  void get_info();
  void handle_get_info(int r);

  void get_status();
  void handle_get_status(int r);

  void finish(int r);

};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GetStatusRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GET_STATUS_REQUEST_H

