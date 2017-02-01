// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_BREAK_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_BREAK_REQUEST_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "msg/msg_types.h"
#include "librbd/ImageCtx.h"
#include <list>
#include <string>
#include <boost/optional.hpp>

class Context;

namespace librbd {

template <typename> class Journal;

namespace exclusive_lock {

struct Locker;

template <typename ImageCtxT = ImageCtx>
class BreakRequest {
public:
  static BreakRequest* create(ImageCtxT &image_ctx, const Locker &locker,
                              bool blacklist_locker, bool force_break_lock,
                              Context *on_finish) {
    return new BreakRequest(image_ctx, locker, blacklist_locker,
                            force_break_lock, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_WATCHERS
   *    |
   *    v
   * BLACKLIST (skip if disabled)
   *    |
   *    v
   * BREAK_LOCK
   *    |
   *    v
   * <finish>
   *
   * @endvertbatim
   */

  ImageCtxT &m_image_ctx;
  const Locker &m_locker;
  bool m_blacklist_locker;
  bool m_force_break_lock;
  Context *m_on_finish;

  bufferlist m_out_bl;

  std::list<obj_watch_t> m_watchers;
  int m_watchers_ret_val;

  BreakRequest(ImageCtxT &image_ctx, const Locker &locker,
               bool blacklist_locker, bool force_break_lock,
               Context *on_finish)
    : m_image_ctx(image_ctx), m_locker(locker),
      m_blacklist_locker(blacklist_locker),
      m_force_break_lock(force_break_lock), m_on_finish(on_finish) {
  }

  void send_get_watchers();
  void handle_get_watchers(int r);

  void send_blacklist();
  void handle_blacklist(int r);

  void send_break_lock();
  void handle_break_lock(int r);

  void finish(int r);

};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::BreakRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_BREAK_REQUEST_H
