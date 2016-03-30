// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_CLOSE_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_CLOSE_IMAGE_REQUEST_H

#include "include/int_types.h"
#include "librbd/ImageCtx.h"
#include <string>

class Context;
class ContextWQ;
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class CloseImageRequest {
public:
  static CloseImageRequest* create(ImageCtxT **image_ctx, ContextWQ *work_queue,
                                   bool destroy_only, Context *on_finish) {
    return new CloseImageRequest(image_ctx, work_queue, destroy_only,
                                 on_finish);
  }

  CloseImageRequest(ImageCtxT **image_ctx, ContextWQ *work_queue,
                    bool destroy_only, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * CLOSE_IMAGE (skip if not needed)
   *    |
   *    v
   * SWITCH_CONTEXT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */
  ImageCtxT **m_image_ctx;
  ContextWQ *m_work_queue;
  bool m_destroy_only;
  Context *m_on_finish;

  void close_image();
  void handle_close_image(int r);

  void switch_thread_context();
  void handle_switch_thread_context(int r);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::CloseImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_CLOSE_IMAGE_REQUEST_H
