// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_POST_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_POST_ACQUIRE_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "librbd/ImageCtx.h"
#include "msg/msg_types.h"
#include <string>

class Context;

namespace librbd {

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class PostAcquireRequest {
public:
  static PostAcquireRequest* create(ImageCtxT &image_ctx, Context *on_acquire,
                                    Context *on_finish);

  ~PostAcquireRequest();
  void send();

private:

  /**
   * @verbatim
   *
   *    <start>
   *      |
   *      |
   *      v
   * REFRESH (skip if not
   *      |   needed)
   *      v
   * OPEN_OBJECT_MAP (skip if
   *      |           disabled)
   *      v
   * OPEN_JOURNAL (skip if
   *      |   *     disabled)
   *      |   *
   *      |   * * * * * * * *
   *      v                 *
   *  ALLOCATE_JOURNAL_TAG  *
   *      |            *    *
   *      |            *    *
   *      |            v    v
   *      |         CLOSE_JOURNAL
   *      |               |
   *      |               v
   *      |         CLOSE_OBJECT_MAP
   *      |               |
   *      v               |
   *  <finish> <----------/
   *
   * @endverbatim
   */

  PostAcquireRequest(ImageCtxT &image_ctx, Context *on_acquire,
                     Context *on_finish);

  ImageCtxT &m_image_ctx;
  Context *m_on_acquire;
  Context *m_on_finish;

  decltype(m_image_ctx.object_map) m_object_map;
  decltype(m_image_ctx.journal) m_journal;

  bool m_prepare_lock_completed = false;
  int m_error_result;

  void send_refresh();
  void handle_refresh(int r);

  void send_open_journal();
  void handle_open_journal(int r);

  void send_allocate_journal_tag();
  void handle_allocate_journal_tag(int r);

  void send_open_object_map();
  void handle_open_object_map(int r);

  void send_close_journal();
  void handle_close_journal(int r);

  void send_close_object_map();
  void handle_close_object_map(int r);

  void apply();
  void revert();

  void finish();

  void save_result(int result) {
    if (m_error_result == 0 && result < 0) {
      m_error_result = result;
    }
  }
};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::PostAcquireRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_POST_ACQUIRE_REQUEST_H
