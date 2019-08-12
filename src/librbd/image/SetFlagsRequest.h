// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_SET_FLAGS_REQUEST_H
#define CEPH_LIBRBD_IMAGE_SET_FLAGS_REQUEST_H

#include "include/buffer.h"
#include <map>
#include <string>

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class SetFlagsRequest {
public:
  static SetFlagsRequest *create(ImageCtxT *image_ctx, uint64_t flags,
				 uint64_t mask, Context *on_finish) {
    return new SetFlagsRequest(image_ctx, flags, mask, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |   .  .  .
   *    v   v     .
   * SET_FLAGS    . (for every snapshot)
   *    |   .     .
   *    v   .  .  .
   * <finis>
   *
   * @endverbatim
   */

  SetFlagsRequest(ImageCtxT *image_ctx, uint64_t flags, uint64_t mask,
		  Context *on_finish);

  ImageCtxT *m_image_ctx;
  uint64_t m_flags;
  uint64_t m_mask;
  Context *m_on_finish;

  void send_set_flags();
  Context *handle_set_flags(int *result);
};

} // namespace image
} // namespace librbd

extern template class librbd::image::SetFlagsRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_SET_FLAGS_REQUEST_H
