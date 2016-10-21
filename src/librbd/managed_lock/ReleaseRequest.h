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

template <typename LockWatcherT = LockWatcher>
class ReleaseRequest {
public:
  static ReleaseRequest* create(librados::IoCtx& ioctx,
                                LockWatcherT *watcher,
                                const std::string& m_oid,
                                const std::string &cookie,
                                Context *on_finish);

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

  ReleaseRequest(librados::IoCtx& ioctx, LockWatcherT *watcher,
                 const std::string& oid, const std::string &cookie,
                 Context *on_finish);

  librados::IoCtx& m_ioctx;
  LockWatcherT *m_watcher;
  std::string m_oid;
  std::string m_cookie;
  Context *m_on_finish;

  void send_flush_notifies();
  void handle_flush_notifies(int r);

  void send_unlock();
  void handle_unlock(int r);

  void finish();

};

} // namespace managed_lock
} // namespace librbd

extern template class librbd::managed_lock::ReleaseRequest<
                                            librbd::managed_lock::LockWatcher>;

#endif // CEPH_LIBRBD_LOCK_RELEASE_REQUEST_H
