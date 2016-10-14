// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_RELEASE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_RELEASE_REQUEST_H

#include "librbd/ImageCtx.h"
#include <string>

class Context;

namespace librbd {

class ImageCtx;
template <typename> class Journal;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class ReleaseRequest {
public:
  static ReleaseRequest* create(ImageCtxT &image_ctx, const std::string &cookie,
                                Context *on_releasing, Context *on_finish,
                                bool shutting_down);

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

  ReleaseRequest(ImageCtxT &image_ctx, const std::string &cookie,
                 Context *on_releasing, Context *on_finish,
                 bool shutting_down);

  ImageCtxT &m_image_ctx;
  std::string m_cookie;
  Context *m_on_releasing;
  Context *m_on_finish;
  bool m_shutting_down;

  decltype(m_image_ctx.object_map) m_object_map;
  decltype(m_image_ctx.journal) m_journal;

  void send_prepare_lock();
  Context *handle_prepare_lock(int *ret_val);

  void send_cancel_op_requests();
  Context *handle_cancel_op_requests(int *ret_val);

  void send_block_writes();
  Context *handle_block_writes(int *ret_val);

  void send_flush_notifies();
  Context *handle_flush_notifies(int *ret_val);

  void send_close_journal();
  Context *handle_close_journal(int *ret_val);

  void send_close_object_map();
  Context *handle_close_object_map(int *ret_val);

  void send_unlock();
  Context *handle_unlock(int *ret_val);

};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::ReleaseRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_RELEASE_REQUEST_H
