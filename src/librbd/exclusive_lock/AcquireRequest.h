// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "librbd/ImageCtx.h"
#include "msg/msg_types.h"
#include <string>

class Context;

namespace librbd {

namespace managed_lock{
class LockWatcher;
}

template <typename> class Journal;
template <typename> class Lock;
typedef Lock<librbd::managed_lock::LockWatcher> LockT;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class AcquireRequest {
public:
  static AcquireRequest* create(ImageCtxT &image_ctx, LockT *managed_lock,
                                Context *on_finish, bool try_lock);

  ~AcquireRequest();
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
   *    \--> LOCK_IMAGE
   *              |
   *              v
   *         REFRESH (skip if not
   *              |   needed)
   *              v
   *         OPEN_OBJECT_MAP (skip if
   *              |           disabled)
   *              v
   *         OPEN_JOURNAL (skip if
   *              |   *     disabled)
   *              |   *
   *              |   * * * * * * * *
   *              v                 *
   *          ALLOCATE_JOURNAL_TAG  *
   *              |            *    *
   *              |            *    *
   *              |            v    v
   *              |         CLOSE_JOURNAL
   *              |               |
   *              |               v
   *              |         CLOSE_OBJECT_MAP
   *              |               |
   *              |               v
   *              |         UNLOCK_IMAGE
   *              |               |
   *              v               |
   *          <finish> <----------/
   *
   * @endverbatim
   */

  AcquireRequest(ImageCtxT &image_ctx, LockT *managed_lock, Context *on_finish,
                 bool try_lock);

  ImageCtxT &m_image_ctx;
  LockT *m_managed_lock;
  Context *m_on_finish;
  bool m_try_lock;

  bufferlist m_out_bl;

  decltype(m_image_ctx.object_map) m_object_map;
  decltype(m_image_ctx.journal) m_journal;

  int m_error_result;
  bool m_prepare_lock_completed = false;

  void send_prepare_lock();
  Context *handle_prepare_lock(int *ret_val);

  void send_flush_notifies();
  Context *handle_flush_notifies(int *ret_val);

  void send_lock();
  Context *handle_lock(int *ret_val);

  Context *send_refresh();
  Context *handle_refresh(int *ret_val);

  Context *send_open_journal();
  Context *handle_open_journal(int *ret_val);

  void send_allocate_journal_tag();
  Context *handle_allocate_journal_tag(int *ret_val);

  Context *send_open_object_map();
  Context *handle_open_object_map(int *ret_val);

  void send_close_journal();
  Context *handle_close_journal(int *ret_val);

  void send_close_object_map();
  Context *handle_close_object_map(int *ret_val);

  void send_unlock();
  Context *handle_unlock(int *ret_val);

  void send_get_lockers();
  Context *handle_get_lockers(int *ret_val);

  void send_get_watchers();
  Context *handle_get_watchers(int *ret_val);

  void send_blacklist();
  Context *handle_blacklist(int *ret_val);

  void send_break_lock();
  Context *handle_break_lock(int *ret_val);

  void apply();
  void revert(int *ret_val);
};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::AcquireRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
