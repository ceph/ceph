// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_PRE_RELEASE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_PRE_RELEASE_REQUEST_H

#include "librbd/ImageCtx.h"
#include <string>

class AsyncOpTracker;
class Context;

namespace librbd {

struct ImageCtx;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class PreReleaseRequest {
public:
  static PreReleaseRequest* create(ImageCtxT &image_ctx, bool shutting_down,
                                   AsyncOpTracker &async_op_tracker,
                                   Context *on_finish);

  ~PreReleaseRequest();
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
   * CANCEL_OP_REQUESTS
   *    |
   *    v
   * BLOCK_WRITES
   *    |
   *    v
   * WAIT_FOR_OPS
   *    |
   *    v
   * INVALIDATE_CACHE
   *    |
   *    v
   * FLUSH_NOTIFIES . . . . . . . . . . . . . .
   *    |                                     .
   *    v                                     .
   * CLOSE_JOURNAL                            .
   *    |                (journal disabled,   .
   *    v                 object map enabled) .
   * CLOSE_OBJECT_MAP < . . . . . . . . . . . .
   *    |                                     .
   *    v               (object map disabled) .
   * <finish> < . . . . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  PreReleaseRequest(ImageCtxT &image_ctx, bool shutting_down,
                    AsyncOpTracker &async_op_tracker, Context *on_finish);

  ImageCtxT &m_image_ctx;
  bool m_shutting_down;
  AsyncOpTracker &m_async_op_tracker;
  Context *m_on_finish;

  int m_error_result = 0;

  decltype(m_image_ctx.object_map) m_object_map = nullptr;
  decltype(m_image_ctx.journal) m_journal = nullptr;

  void send_prepare_lock();
  void handle_prepare_lock(int r);

  void send_cancel_op_requests();
  void handle_cancel_op_requests(int r);

  void send_block_writes();
  void handle_block_writes(int r);

  void send_wait_for_ops();
  void handle_wait_for_ops(int r);

  void send_invalidate_cache();
  void handle_invalidate_cache(int r);

  void send_flush_notifies();
  void handle_flush_notifies(int r);

  void send_close_journal();
  void handle_close_journal(int r);

  void send_close_object_map();
  void handle_close_object_map(int r);

  void send_unlock();

  void finish();

  void save_result(int result) {
    if (m_error_result == 0 && result < 0) {
      m_error_result = result;
    }
  }

};

} // namespace exclusive_lock
} // namespace librbd

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_PRE_RELEASE_REQUEST_H
