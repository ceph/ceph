// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_ATTACH_CHILD_REQUEST_H
#define CEPH_LIBRBD_IMAGE_ATTACH_CHILD_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"

class CephContext;
class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class AttachChildRequest {
public:
  static AttachChildRequest* create(ImageCtxT *image_ctx,
                                    ImageCtxT *parent_image_ctx,
                                    const librados::snap_t &parent_snap_id,
                                    ImageCtxT *old_parent_image_ctx,
                                    const librados::snap_t &old_parent_snap_id,
                                    uint32_t clone_format,
                                    Context* on_finish) {
      return new AttachChildRequest(image_ctx, parent_image_ctx, parent_snap_id,
                                    old_parent_image_ctx, old_parent_snap_id,
                                    clone_format, on_finish);
  }

  AttachChildRequest(ImageCtxT *image_ctx,
                     ImageCtxT *parent_image_ctx,
                     const librados::snap_t &parent_snap_id,
                     ImageCtxT *old_parent_image_ctx,
                     const librados::snap_t &old_parent_snap_id,
                     uint32_t clone_format,
                     Context* on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   *                     <start>
   *    (clone v1)          |        (clone v2)
   *      /----------------/ \---------------\
   *      |                                  |
   *      v                                  v
   *  V1 ADD CHILD                       V2 SET CLONE
   *      |                                  |
   *      v                                  v
   *  V1 VALIDATE PROTECTED              V2 ATTACH CHILD
   *      |                                  |
   *      |                                  v
   *  V1 REMOVE CHILD FROM OLD PARENT    V2 DETACH CHILD FROM OLD PARENT
   *      |                                  |
   *      \----------------\ /---------------/
   *                        |
   *                        v
   *                     <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  ImageCtxT *m_parent_image_ctx;
  librados::snap_t m_parent_snap_id;
  ImageCtxT *m_old_parent_image_ctx;
  librados::snap_t m_old_parent_snap_id;
  uint32_t m_clone_format;
  Context* m_on_finish;

  CephContext *m_cct;

  void v1_add_child();
  void handle_v1_add_child(int r);

  void v1_refresh();
  void handle_v1_refresh(int r);

  void v1_remove_child_from_old_parent();
  void handle_v1_remove_child_from_old_parent(int r);

  void v2_set_op_feature();
  void handle_v2_set_op_feature(int r);

  void v2_child_attach();
  void handle_v2_child_attach(int r);

  void v2_child_detach_from_old_parent();
  void handle_v2_child_detach_from_old_parent(int r);

  void finish(int r);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::AttachChildRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_ATTACH_CHILD_REQUEST_H
