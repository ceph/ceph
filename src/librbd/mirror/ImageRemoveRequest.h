// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_IMAGE_REMOVE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_IMAGE_REMOVE_REQUEST_H

#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"

#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace mirror {

template <typename ImageCtxT = ImageCtx>
class ImageRemoveRequest {
public:
  static ImageRemoveRequest *create(librados::IoCtx& io_ctx,
                                    const std::string& global_image_id,
                                    const std::string& image_id,
                                    Context* on_finish) {
    return new ImageRemoveRequest(io_ctx, global_image_id, image_id, on_finish);
  }

  ImageRemoveRequest(librados::IoCtx& io_ctx,
                     const std::string& global_image_id,
                     const std::string& image_id,
                     Context* on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   * REMOVE_MIRROR_IMAGE
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
  std::string m_global_image_id;
  std::string m_image_id;
  Context* m_on_finish;

  CephContext* m_cct;

  void remove_mirror_image();
  void handle_remove_mirror_image(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void finish(int r);

};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::ImageRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_IMAGE_REMOVE_REQUEST_H
