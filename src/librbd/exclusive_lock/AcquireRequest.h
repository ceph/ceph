// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "librbd/ImageCtx.h"
#include "librbd/exclusive_lock/Types.h"
#include <string>

class Context;

namespace librbd {

template <typename> class Journal;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class AcquireRequest {
public:
  static AcquireRequest* create(ImageCtxT &image_ctx, const std::string &cookie,
                                Context *on_acquire, Context *on_finish);

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
   *    v
   * GET_LOCKERS <--------------------------------------\
   *    |     ^                                         |
   *    |     . (EBUSY && no cached locker)             |
   *    |     .                                         |
   *    |     .          (EBUSY && cached locker)       |
   *    \--> LOCK_IMAGE * * * * * * * * > BREAK_LOCK ---/
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

  AcquireRequest(ImageCtxT &image_ctx, const std::string &cookie,
                 Context *on_acquire, Context *on_finish);

  ImageCtxT &m_image_ctx;
  std::string m_cookie;
  Context *m_on_acquire;
  Context *m_on_finish;

  decltype(m_image_ctx.object_map) m_object_map;
  decltype(m_image_ctx.journal) m_journal;

  Locker m_locker;

  int m_error_result;
  bool m_prepare_lock_completed = false;

  void send_prepare_lock();
  Context *handle_prepare_lock(int *ret_val);

  void send_flush_notifies();
  Context *handle_flush_notifies(int *ret_val);

  void send_get_locker();
  Context *handle_get_locker(int *ret_val);

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

  void send_break_lock();
  Context *handle_break_lock(int *ret_val);

  void apply();
  void revert(int *ret_val);
};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::AcquireRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
