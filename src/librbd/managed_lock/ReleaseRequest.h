// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_LOCK_RELEASE_REQUEST_H
#define CEPH_LIBRBD_LOCK_RELEASE_REQUEST_H

#include "include/rados/librados.hpp"
#include <string>

class Context;
class ContextWQ;

namespace librbd {

namespace managed_lock {

class LockWatcher;

class ReleaseRequest {
public:
  static ReleaseRequest* create(librados::IoCtx& ioctx,
                                ContextWQ *work_queue,
                                LockWatcher *watcher,
                                const std::string& m_oid,
                                const std::string &cookie,
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
   * FLUSH_NOTIFIES
   *    |
   *    v
   * UNLOCK
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ReleaseRequest(librados::IoCtx& ioctx, ContextWQ *work_queue,
                 LockWatcher *watcher, const std::string& oid,
                 const std::string &cookie, Context *on_releasing,
                 Context *on_finish, bool shutting_down);

  librados::IoCtx& m_ioctx;
  ContextWQ *m_work_queue;
  LockWatcher *m_watcher;
  std::string m_oid;
  std::string m_cookie;
  Context *m_on_releasing;
  Context *m_on_finish;
  bool m_shutting_down;

  void send_flush_notifies();
  Context *handle_flush_notifies(int *ret_val);

  void send_unlock();
  Context *handle_unlock(int *ret_val);

};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_LOCK_RELEASE_REQUEST_H
