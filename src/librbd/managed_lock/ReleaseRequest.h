// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_RELEASE_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_RELEASE_REQUEST_H

#include "include/rados/librados.hpp"
#include "librbd/watcher/Types.h"
#include <string>

class Context;
class ContextWQ;

namespace librbd {

class Watcher;
namespace asio { struct ContextWQ; }

namespace managed_lock {

template <typename ImageCtxT>
class ReleaseRequest {
private:
  typedef watcher::Traits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Watcher Watcher;

public:
  static ReleaseRequest* create(librados::IoCtx& ioctx, Watcher *watcher,
                                asio::ContextWQ *work_queue,
                                const std::string& oid,
                                const std::string& cookie,
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
   * UNLOCK
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ReleaseRequest(librados::IoCtx& ioctx, Watcher *watcher,
                 asio::ContextWQ *work_queue, const std::string& oid,
                 const std::string& cookie, Context *on_finish);

  librados::IoCtx& m_ioctx;
  Watcher *m_watcher;
  std::string m_oid;
  std::string m_cookie;
  Context *m_on_finish;

  void send_unlock();
  void handle_unlock(int r);

  void finish();

};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_RELEASE_REQUEST_H
