// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_UNLOCK_REQUEST_H
#define CEPH_LIBRBD_OBJECT_MAP_UNLOCK_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/Context.h"
#include <map>

namespace librbd {

class ImageCtx;

namespace object_map {

template <typename ImageCtxT = ImageCtx>
class UnlockRequest {
public:
  UnlockRequest(ImageCtxT &image_ctx, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start> ----> UNLOCK ----> <finish>
   *
   * @endverbatim
   */

  ImageCtxT &m_image_ctx;
  Context *m_on_finish;

  void send_unlock();
  Context* handle_unlock(int *ret_val);
};

} // namespace object_map
} // namespace librbd

extern template class librbd::object_map::UnlockRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_OBJECT_MAP_UNLOCK_REQUEST_H
