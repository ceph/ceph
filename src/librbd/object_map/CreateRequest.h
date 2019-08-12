// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_CREATE_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_CREATE_REQUEST_H

#include "include/buffer.h"
#include <map>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace object_map {

template <typename ImageCtxT = ImageCtx>
class CreateRequest {
public:
  static CreateRequest *create(ImageCtxT *image_ctx, Context *on_finish) {
    return new CreateRequest(image_ctx, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |         .  .  .
   *    v         v     .
   * OBJECT_MAP_RESIZE  . (for every snapshot)
   *    |         .     .
   *    v         .  .  .
   * <finis>
   *
   * @endverbatim
   */

  CreateRequest(ImageCtxT *image_ctx, Context *on_finish);

  ImageCtxT *m_image_ctx;
  Context *m_on_finish;

  std::vector<uint64_t> m_snap_ids;

  void send_object_map_resize();
  Context *handle_object_map_resize(int *result);
};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::CreateRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_CREATE_REQUEST_H
