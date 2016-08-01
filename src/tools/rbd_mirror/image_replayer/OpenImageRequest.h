// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_OPEN_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_OPEN_IMAGE_REQUEST_H

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
class OpenImageRequest {
public:
  static OpenImageRequest* create(librados::IoCtx &io_ctx,
                                  ImageCtxT **image_ctx,
                                  const std::string &image_id,
                                  bool read_only, ContextWQ *work_queue,
                                  Context *on_finish) {
    return new OpenImageRequest(io_ctx, image_ctx, image_id, read_only,
                                work_queue, on_finish);
  }

  OpenImageRequest(librados::IoCtx &io_ctx, ImageCtxT **image_ctx,
                   const std::string &image_id, bool read_only,
                   ContextWQ *m_work_queue, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN_IMAGE * * * * * * * *
   *    |                     *
   *    v                     v
   * <finish> <---------- CLOSE_IMAGE
   *
   * @endverbatim
   */
  librados::IoCtx &m_io_ctx;
  ImageCtxT **m_image_ctx;
  std::string m_image_id;
  bool m_read_only;
  ContextWQ *m_work_queue;
  Context *m_on_finish;

  int m_ret_val = 0;

  void send_open_image();
  void handle_open_image(int r);

  void send_close_image(int r);
  void handle_close_image(int r);

  void finish(int r);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::OpenImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_OPEN_IMAGE_REQUEST_H
