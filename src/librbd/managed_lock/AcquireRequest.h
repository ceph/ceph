// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_ACQUIRE_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/int_types.h"
#include "include/buffer.h"
#include "msg/msg_types.h"
#include "librbd/ManagedLock.h"
#include "librbd/watcher/Types.h"
#include <string>

class Context;
class ContextWQ;

namespace librbd {

class Watcher;

namespace managed_lock {

template <typename ImageCtxT>
class AcquireRequest {
private:
  typedef watcher::Traits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Watcher Watcher;

public:
  static AcquireRequest* create(librados::IoCtx& ioctx, Watcher *watcher,
                                ContextWQ *work_queue, const std::string& oid,
                                const std::string& cookie, Context *on_finish);

  ~AcquireRequest();
  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |
   *    |
   *    |     /-----------------------------------------------------------\
   *    |     |                                                           |
   *    |     |             (no lockers)                                  |
   *    |     |   . . . . . . . . . . . . . . . . . . . . . .             |
   *    |     |   .                                         .             |
   *    |     v   v      (EBUSY)                            .             |
   *    \--> LOCK_IMAGE * * * * * * * * > GET_LOCK_OWNER  . .             |
   *              |                         |                             |
   *              |                         v                             |
   *              |                       BREAK_LOCK                      |
   *              |                         |                             |
   *              |                         \-----------------------------/
   *              v
   *          <finish>
   *
   * @endverbatim
   */

  AcquireRequest(librados::IoCtx& ioctx, Watcher *watcher,
                 ContextWQ *work_queue, const std::string& oid,
                 const std::string& cookie, Context *on_finish);

  librados::IoCtx& m_ioctx;
  Watcher *m_watcher;
  CephContext *m_cct;
  ContextWQ *m_work_queue;
  std::string m_oid;
  std::string m_cookie;
  Context *m_on_finish;

  bufferlist m_out_bl;

  std::list<obj_watch_t> m_watchers;
  int m_watchers_ret_val;

  typename ManagedLock<ImageCtxT>::LockOwner m_lock_owner;

  int m_error_result;

  void send_lock();
  void handle_lock(int r);

  void send_unlock();
  void handle_unlock(int r);

  void send_get_lock_owner();
  void handle_get_lock_owner(int r);

  void send_break_lock();
  void handle_break_lock(int r);

  void finish();

  void save_result(int r) {
    if (m_error_result == 0 && r < 0) {
      m_error_result = r;
    }
  }
};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_ACQUIRE_REQUEST_H
