// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_GET_LOCK_OWNER_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_GET_LOCK_OWNER_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/int_types.h"
#include "include/buffer.h"
#include "msg/msg_types.h"
#include "librbd/ManagedLock.h"
#include <string>

class Context;

namespace librbd {

namespace managed_lock {

template <typename ImageCtxT>
class GetLockOwnerRequest {
public:
  static GetLockOwnerRequest* create(librados::IoCtx& ioctx,
				     const std::string& oid,
                                     typename ManagedLock<ImageCtxT>::LockOwner *owner,
				     Context *on_finish);
  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v         (no lockers or error)
   * GET_LOCKERS . . . . . . . .
   *    |                      .
   *    v                      .
   * GET_WATCHERS              .
   *    |                      .
   *    v                      .
   * <finish>  < . . . . . . . .
   *
   * @endverbatim
   */

  GetLockOwnerRequest(librados::IoCtx& ioctx, const std::string& oid,
                      typename ManagedLock<ImageCtxT>::LockOwner *lock_owner,
                      Context *on_finish);

  librados::IoCtx& m_ioctx;
  CephContext *m_cct;
  std::string m_oid;
  typename ManagedLock<ImageCtxT>::LockOwner *m_lock_owner;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::list<obj_watch_t> m_watchers;
  int m_watchers_ret_val;

  void send_get_lockers();
  void handle_get_lockers(int r);

  void send_get_watchers();
  void handle_get_watchers(int r);

  void finish(int r);
};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_GET_LOCK_OWNER_REQUEST_H
