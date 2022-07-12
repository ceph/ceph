// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_DETACH_CHILD_REQUEST_H
#define CEPH_LIBRBD_IMAGE_DETACH_CHILD_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "librbd/Types.h"
#include "librbd/internal.h"

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
  ~DetachChildRequest();

  void send();

private:
  /**
   * @verbatim
   *
   *                 <start>
   *                    |
   *     (v1)           | (v2)
   *    /--------------/ \--------------\
   *    |                               |
   *    v                               v
   * REMOVE_CHILD                   CHILD_DETACH
   *    |                               |
   *    |                               v
   *    |                           GET_SNAPSHOT
   *    |  (snapshot in-use)          . |
   *    |/. . . . . . . . . . . . . . . |
   *    |                               v
   *    |                           OPEN_PARENT
   *    |                               |
   *    |                               v           (has more children)
   *    |                           REMOVE_SNAPSHOT ---------------\
   *    |                               |                          |
   *    |                               v                  (noent) |
   *    |     (auto-delete when     GET_PARENT_TRASH_ENTRY . . . .\|
   *    |      last child detached)     |                          |
   *    |                               v                          v
   *    |                           REMOVE_PARENT_FROM_TRASH   CLOSE_PARENT
   *    |                               |                          |
   *    |/------------------------------/--------------------------/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT& m_image_ctx;
  Context* m_on_finish;

  librados::IoCtx m_parent_io_ctx;
  cls::rbd::ParentImageSpec m_parent_spec;
  std::string m_parent_header_name;

  cls::rbd::SnapshotNamespace m_parent_snap_namespace;
  std::string m_parent_snap_name;

  ImageCtxT* m_parent_image_ctx = nullptr;

  ceph::bufferlist m_out_bl;
  NoOpProgressContext m_no_op;

  void clone_v2_child_detach();
  void handle_clone_v2_child_detach(int r);

  void clone_v2_get_snapshot();
  void handle_clone_v2_get_snapshot(int r);

  void clone_v2_open_parent();
  void handle_clone_v2_open_parent(int r);

  void clone_v2_remove_snapshot();
  void handle_clone_v2_remove_snapshot(int r);

  void clone_v2_get_parent_trash_entry();
  void handle_clone_v2_get_parent_trash_entry(int r);

  void clone_v2_remove_parent_from_trash();
  void handle_clone_v2_remove_parent_from_trash(int r);

  void clone_v2_close_parent();
  void handle_clone_v2_close_parent(int r);

  void clone_v1_remove_child();
  void handle_clone_v1_remove_child(int r);

  void finish(int r);

};

} // namespace image
} // namespace librbd

extern template class librbd::image::DetachChildRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_DETACH_CHILD_REQUEST_H
