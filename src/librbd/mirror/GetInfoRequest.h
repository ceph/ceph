// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GET_INFO_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GET_INFO_REQUEST_H

#include "include/buffer.h"
#include "librbd/mirror/Types.h"
#include <string>

struct Context;
namespace cls { namespace rbd { struct MirrorImage; } }

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GetInfoRequest {
public:
  static GetInfoRequest *create(ImageCtxT &image_ctx,
                                cls::rbd::MirrorImage *mirror_image,
                                PromotionState *promotion_state,
                                Context *on_finish) {
    return new GetInfoRequest(image_ctx, mirror_image, promotion_state,
                              on_finish);
  }

  GetInfoRequest(ImageCtxT &image_ctx, cls::rbd::MirrorImage *mirror_image,
                 PromotionState *promotion_state, Context *on_finish)
    : m_image_ctx(image_ctx), m_mirror_image(mirror_image),
      m_promotion_state(promotion_state), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * REFRESH
   *    |
   *    v
   * GET_MIRROR_IMAGE
   *    |
   *    v
   * GET_TAG_OWNER
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT &m_image_ctx;
  cls::rbd::MirrorImage *m_mirror_image;
  PromotionState *m_promotion_state;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_mirror_uuid;

  void refresh_image();
  void handle_refresh_image(int r);

  void get_mirror_image();
  void handle_get_mirror_image(int r);

  void get_tag_owner();
  void handle_get_tag_owner(int r);

  void finish(int r);

};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GetInfoRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GET_INFO_REQUEST_H

