// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_CLOSE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_CLOSE_REQUEST_H

#include "include/int_types.h"
#include "librbd/ImageCtx.h"

class Context;

namespace librbd {

class ImageCtx;

namespace image {

template <typename ImageCtxT = ImageCtx>
class CloseRequest {
public:
  static CloseRequest *create(ImageCtxT *image_ctx, Context *on_finish) {
    return new CloseRequest(image_ctx, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UNREGISTER_IMAGE_WATCHER
   *    |
   *    v
   * SHUT_DOWN_AIO_WORK_QUEUE . . .
   *    |                         .
   *    v                         .
   * SHUT_DOWN_EXCLUSIVE_LOCK     . (exclusive lock
   *    |                         .  disabled)
   *    v                         v
   * FLUSH  < . . . . . . . . . . .
   *    |
   *    v
   * FLUSH_READAHEAD
   *    |
   *    v
   * SHUTDOWN_CACHE
   *    |
   *    v
   * FLUSH_OP_WORK_QUEUE . . . . .
   *    |                        .
   *    v                        .
   * CLOSE_PARENT                . (no parent)
   *    |                        .
   *    v                        .
   * FLUSH_IMAGE_WATCHER < . . . .
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  CloseRequest(ImageCtxT *image_ctx, Context *on_finish);

  ImageCtxT *m_image_ctx;
  Context *m_on_finish;

  int m_error_result;

  decltype(m_image_ctx->exclusive_lock) m_exclusive_lock;

  void send_unregister_image_watcher();
  void handle_unregister_image_watcher(int r);

  void send_shut_down_aio_queue();
  void handle_shut_down_aio_queue(int r);

  void send_shut_down_exclusive_lock();
  void handle_shut_down_exclusive_lock(int r);

  void send_flush();
  void handle_flush(int r);

  void send_flush_readahead();
  void handle_flush_readahead(int r);

  void send_shut_down_cache();
  void handle_shut_down_cache(int r);

  void send_flush_op_work_queue();
  void handle_flush_op_work_queue(int r);

  void send_close_parent();
  void handle_close_parent(int r);

  void send_flush_image_watcher();
  void handle_flush_image_watcher(int r);

  void finish();

  void save_result(int result) {
    if (m_error_result == 0 && result < 0) {
      m_error_result = result;
    }
  }
};

} // namespace image
} // namespace librbd

extern template class librbd::image::CloseRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_CLOSE_REQUEST_H
