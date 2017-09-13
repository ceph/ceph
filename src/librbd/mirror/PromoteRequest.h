// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_PROMOTE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_PROMOTE_REQUEST_H

#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class PromoteRequest {
public:
  static PromoteRequest *create(ImageCtxT &image_ctx, bool force,
                                Context *on_finish) {
    return new PromoteRequest(image_ctx, force, on_finish);
  }

  PromoteRequest(ImageCtxT &image_ctx, bool force, Context *on_finish)
    : m_image_ctx(image_ctx), m_force(force), m_on_finish(on_finish) {
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
   * GET_TAG_OWNER
   *    |
   *    v
   * PROMOTE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT &m_image_ctx;
  bool m_force;
  Context *m_on_finish;

  cls::rbd::MirrorImage m_mirror_image;
  PromotionState m_promotion_state = PROMOTION_STATE_PRIMARY;

  void get_info();
  void handle_get_info(int r);

  void promote();
  void handle_promote(int r);

  void finish(int r);

};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::PromoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_PROMOTE_REQUEST_H
