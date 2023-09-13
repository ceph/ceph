// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_PRE_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_PRE_ACQUIRE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "librbd/ImageCtx.h"
#include "msg/msg_types.h"
#include <string>

class Context;

namespace librbd {

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class PreAcquireRequest {
public:
  static PreAcquireRequest* create(ImageCtxT &image_ctx, Context *on_finish);

  ~PreAcquireRequest();
  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * PREPARE_LOCK
   *    |
   *    v
   * FLUSH_NOTIFIES
   *    |
   *    |
   *    |
        v
   * <finish>
   *
   * @endverbatim
   */

  PreAcquireRequest(ImageCtxT &image_ctx, Context *on_finish);

  ImageCtxT &m_image_ctx;
  Context *m_on_finish;

  int m_error_result;

  void send_prepare_lock();
  void handle_prepare_lock(int r);

  void send_flush_notifies();
  void handle_flush_notifies(int r);

  void finish();

  void save_result(int result) {
    if (m_error_result == 0 && result < 0) {
      m_error_result = result;
    }
  }
};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::PreAcquireRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
