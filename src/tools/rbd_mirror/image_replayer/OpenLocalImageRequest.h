// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_OPEN_LOCAL_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_OPEN_LOCAL_IMAGE_REQUEST_H

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
class OpenLocalImageRequest {
public:
  static OpenLocalImageRequest* create(librados::IoCtx &local_io_ctx,
                                       ImageCtxT **local_image_ctx,
                                       const std::string &local_image_name,
                                       const std::string &local_image_id,
                                       ContextWQ *work_queue,
                                       Context *on_finish) {
    return new OpenLocalImageRequest(local_io_ctx, local_image_ctx,
                                     local_image_name, local_image_id,
                                     work_queue, on_finish);
  }

  OpenLocalImageRequest(librados::IoCtx &local_io_ctx,
                        ImageCtxT **local_image_ctx,
                        const std::string &local_image_name,
                        const std::string &local_image_id,
                        ContextWQ *m_work_queue,
                        Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * OPEN_IMAGE * * * * * * *
   *    |                   *
   *    v                   v
   * LOCK_IMAGE * * * > CLOSE_IMAGE
   *    |                   |
   *    v                   |
   * <finish> <-------------/
   *
   * @endverbatim
   */
  librados::IoCtx &m_local_io_ctx;
  ImageCtxT **m_local_image_ctx;
  std::string m_local_image_name;
  std::string m_local_image_id;
  ContextWQ *m_work_queue;
  Context *m_on_finish;

  int m_ret_val = 0;

  void send_open_image();
  void handle_open_image(int r);

  void send_lock_image();
  void handle_lock_image(int r);

  void send_close_image(int r);
  void handle_close_image(int r);

  void finish(int r);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::OpenLocalImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_OPEN_LOCAL_IMAGE_REQUEST_H
