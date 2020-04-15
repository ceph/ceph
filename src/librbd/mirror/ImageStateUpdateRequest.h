// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_IMAGE_STATE_UPDATE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_IMAGE_STATE_UPDATE_REQUEST_H

#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace mirror {

template <typename ImageCtxT = ImageCtx>
class ImageStateUpdateRequest {
public:
  static ImageStateUpdateRequest *create(
      librados::IoCtx& io_ctx,
      const std::string& image_id,
      cls::rbd::MirrorImageState mirror_image_state,
      const cls::rbd::MirrorImage& mirror_image,
      Context* on_finish) {
    return new ImageStateUpdateRequest(
      io_ctx, image_id, mirror_image_state, mirror_image, on_finish);
  }

  ImageStateUpdateRequest(
      librados::IoCtx& io_ctx,
      const std::string& image_id,
      cls::rbd::MirrorImageState mirror_image_state,
      const cls::rbd::MirrorImage& mirror_image,
      Context* on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v (skip if provided)
   * GET_MIRROR_IMAGE
   *    |
   *    v
   * SET_MIRROR_IMAGE
   *    |
   *    v
   * NOTIFY_MIRRORING_WATCHER
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx& m_io_ctx;
  std::string m_image_id;
  cls::rbd::MirrorImageState m_mirror_image_state;
  cls::rbd::MirrorImage m_mirror_image;
  Context* m_on_finish;

  CephContext* m_cct;
  bufferlist m_out_bl;

  void get_mirror_image();
  void handle_get_mirror_image(int r);

  void set_mirror_image();
  void handle_set_mirror_image(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void finish(int r);

};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::ImageStateUpdateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_IMAGE_STATE_UPDATE_REQUEST_H
