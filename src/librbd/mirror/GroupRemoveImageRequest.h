// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H

#include "include/Context.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = ImageCtx>
class GroupRemoveImageRequest {
public:
  static GroupRemoveImageRequest* create(ImageCtxT* image_ctx,
                                         Context* on_finish) {
    return new GroupRemoveImageRequest(image_ctx, on_finish);
  }

  GroupRemoveImageRequest(ImageCtxT* image_ctx, Context* on_finish);

  void send();

private:

/**
 * @verbatim
 *
 *            <start>
 *               |
 *               v
 *         GET_MIRROR_INFO
 *               |
 *               | not mirrored
 *               |--------------------------> <finish>
 *               |
 *               | mirrored
 *               v
 *  SET_MIRROR_IMAGE_DISABLING
 *               |
 *               v
 *  REMOVE_GLOBAL_MIRROR_IMAGE_ENTRY
 *               |
 *               v
 *           <finish>
 *
 * @endverbatim
 */

  ImageCtxT* m_image_ctx;
  Context* m_on_finish;

  CephContext* m_cct;

  // mirror state
  cls::rbd::MirrorImage m_mirror_image;
  PromotionState m_promotion_state = PROMOTION_STATE_UNKNOWN;
  std::string m_mirror_uuid;

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void set_mirror_image_disabling();
  void handle_set_mirror_image_disabling(int r);

  void remove_global_mirror_image_entry();
  void handle_remove_global_mirror_image_entry(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class
librbd::mirror::GroupRemoveImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H
