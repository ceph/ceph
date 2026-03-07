// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H

#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = ImageCtx>
class GroupRemoveImageRequest {
public:
  static GroupRemoveImageRequest* create(ImageCtxT* image_ctx,
                                         const std::string& group_id,
                                         librados::IoCtx& group_io_ctx,
                                         bool force, Context* on_finish) {
    return new GroupRemoveImageRequest(image_ctx, group_id, group_io_ctx,
                                       force, on_finish);
  }

  GroupRemoveImageRequest(ImageCtxT* image_ctx, const std::string& group_id,
                          librados::IoCtx& group_io_ctx, bool force,
                          Context* on_finish);

  void send();

private:

  /**
   * @verbatim
   *
   *   <start>
   *      |
   *      v
   *    GET_MIRROR_INFO
   *      |
   *      *** mirror info missing ***********************
   *      |                                             *
   *      v                                             *
   *    SET_MIRROR_IMAGE_DISABLING                      *
   *      |                                             *
   *      *** mirror already removed ********************
   *      |                                             *
   *      v                                    (if required cleanup)
   *    PROMOTE_IF_NEEDED                               *
   *      |                                             *
   *      v                                             *
   *    REFRESH_IMAGE                                   *
   *      |                                             *
   *      ***********************************************
   *      |
   *      v
   *    REMOVE_GROUP_FROM_IMAGE
   *      |
   *      v
   *    REMOVE_IMAGE_FROM_GROUP
   *      |
   *      v
   *    REMOVE_SNAPSHOT_MIRROR_METADATA
   *      |
   *      v
   *    REMOVE_GLOBAL_MIRROR_IMAGE_ENTRY
   *      |
   *      v
   *    CLOSE_IMAGE
   *      |
   *      v
   *   <finish>
   *
   * @endverbatim
   */

  ImageCtxT* m_image_ctx;
  std::string m_group_id;
  librados::IoCtx& m_group_io_ctx;
  bool m_force;
  Context* m_on_finish;

  CephContext* m_cct;

  // mirror state
  cls::rbd::MirrorImage m_mirror_image;
  PromotionState m_promotion_state = PROMOTION_STATE_UNKNOWN;
  std::string m_mirror_uuid;
  bool m_is_primary = false;

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void set_mirror_image_disabling();
  void handle_set_mirror_image_disabling(int r);

  void promote_if_needed();
  void handle_promote(int r);

  void refresh_image();
  void handle_refresh(int r);

  void remove_group_from_image();
  void handle_remove_group_from_image(int r);

  void remove_image_from_group();
  void handle_remove_image_from_group(int r);

  void remove_snapshot_mirror_metadata();

  void remove_global_mirror_image_entry();
  void handle_remove_global_mirror_image_entry(int r);

  void close_image();
  void handle_close(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class
librbd::mirror::GroupRemoveImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H
