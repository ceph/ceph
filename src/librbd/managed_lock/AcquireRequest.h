// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_ACQUIRE_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/int_types.h"
#include "include/buffer.h"
#include "msg/msg_types.h"
#include "librbd/managed_lock/Types.h"
#include "librbd/watcher/Types.h"
#include <string>

class Context;

namespace librbd {

class Watcher;
namespace asio { struct ContextWQ; }

namespace managed_lock {

template <typename ImageCtxT>
class AcquireRequest {
private:
  typedef watcher::Traits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Watcher Watcher;

public:
  static AcquireRequest* create(librados::IoCtx& ioctx, Watcher *watcher,
                                asio::ContextWQ *work_queue,
                                const std::string& oid,
                                const std::string& cookie,
                                bool exclusive,
                                bool blacklist_on_break_lock,
                                uint32_t blacklist_expire_seconds,
                                Context *on_finish);

  ~AcquireRequest();
  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_LOCKER
   *    |     ^
   *    |     . (EBUSY && no cached locker)
   *    |     .
   *    |     .          (EBUSY && cached locker)
   *    \--> LOCK_IMAGE * * * * * * * * > BREAK_LOCK . . . . .
   *            |   ^                         |              .
   *            |   |                         | (success)    .
   *            |   \-------------------------/              .
   *            v                                            .
   *         <finish>  < . . . . . . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  AcquireRequest(librados::IoCtx& ioctx, Watcher *watcher,
                 asio::ContextWQ *work_queue, const std::string& oid,
                 const std::string& cookie, bool exclusive,
                 bool blacklist_on_break_lock,
                 uint32_t blacklist_expire_seconds, Context *on_finish);

  librados::IoCtx& m_ioctx;
  Watcher *m_watcher;
  CephContext *m_cct;
  asio::ContextWQ *m_work_queue;
  std::string m_oid;
  std::string m_cookie;
  bool m_exclusive;
  bool m_blacklist_on_break_lock;
  uint32_t m_blacklist_expire_seconds;
  Context *m_on_finish;

  bufferlist m_out_bl;

  Locker m_locker;

  void send_get_locker();
  void handle_get_locker(int r);

  void send_lock();
  void handle_lock(int r);

  void send_break_lock();
  void handle_break_lock(int r);

  void finish(int r);
};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_ACQUIRE_REQUEST_H
