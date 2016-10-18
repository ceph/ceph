// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_RELEASE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_RELEASE_REQUEST_H

#include "librbd/ImageCtx.h"
#include <string>

class Context;

namespace librbd {

namespace managed_lock{
class LockWatcher;
}

struct ImageCtx;
template <typename> class Lock;
typedef Lock<librbd::managed_lock::LockWatcher> LockT;
template <typename> class Journal;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class ReleaseRequest {
public:
  static ReleaseRequest* create(ImageCtxT &image_ctx, LockT *managed_lock,
                                Context *on_finish, bool shutting_down);

  ~ReleaseRequest();
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
   * FLUSH_NOTIFIES . . . . . . . . . . . . . .
   *    |                                     .
   *    v                                     .
   * CLOSE_JOURNAL                            .
   *    |                (journal disabled,   .
   *    v                 object map enabled) .
   * CLOSE_OBJECT_MAP < . . . . . . . . . . . .
   *    |                                     .
   *    v               (object map disabled) .
   * UNLOCK < . . . . . . . . . . . . . . . . .
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ReleaseRequest(ImageCtxT &image_ctx, LockT *managed_lock, Context *on_finish,
                 bool shutting_down);

  ImageCtxT &m_image_ctx;
  LockT *m_managed_lock;
  Context *m_on_finish;
  bool m_shutting_down;

  int m_error_result;

  decltype(m_image_ctx.object_map) m_object_map;
  decltype(m_image_ctx.journal) m_journal;

  void send_prepare_lock();
  void handle_prepare_lock(int r);

  void send_cancel_op_requests();
  void handle_cancel_op_requests(int r);

  void send_block_writes();
  void handle_block_writes(int r);

  void send_image_flush_notifies();
  void handle_image_flush_notifies(int r);

  void send_close_journal();
  void handle_close_journal(int r);

  void send_close_object_map();
  void handle_close_object_map(int r);

  void send_unlock();
  void handle_unlock(int r);

  void finish();

  void save_result(int result) {
    if (m_error_result == 0 && result < 0) {
      m_error_result = result;
    }
  }

};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::ReleaseRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_RELEASE_REQUEST_H
