// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_CLOSE_REQUEST_H
#define CEPH_LIBRBD_IMAGE_CLOSE_REQUEST_H

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
   * BLOCK_IMAGE_WATCHER (skip if R/O)
   *    |
   *    v
   * SHUT_DOWN_UPDATE_WATCHERS
   *    |
   *    v
   * SHUT_DOWN_AIO_WORK_QUEUE . . .
   *    |                         . (exclusive lock disabled)
   *    v                         v
   * SHUT_DOWN_EXCLUSIVE_LOCK   FLUSH
   *    |                         .
   *    |     . . . . . . . . . . .
   *    |     .
   *    v     v
   * UNREGISTER_IMAGE_WATCHER (skip if R/O)
   *    |
   *    v
   * FLUSH_READAHEAD
   *    |
   *    v
   * SHUT_DOWN_OBJECT_DISPATCHER
   *    |
   *    v
   * FLUSH_OP_WORK_QUEUE
   *    |
   *    v (skip if no parent)
   * CLOSE_PARENT
   *    |
   *    v
   * FLUSH_IMAGE_WATCHER
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

  void send_block_image_watcher();
  void handle_block_image_watcher(int r);

  void send_shut_down_update_watchers();
  void handle_shut_down_update_watchers(int r);

  void send_shut_down_io_queue();
  void handle_shut_down_io_queue(int r);

  void send_shut_down_exclusive_lock();
  void handle_shut_down_exclusive_lock(int r);

  void send_flush();
  void handle_flush(int r);

  void send_unregister_image_watcher();
  void handle_unregister_image_watcher(int r);

  void send_flush_readahead();
  void handle_flush_readahead(int r);

  void send_shut_down_object_dispatcher();
  void handle_shut_down_object_dispatcher(int r);

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
