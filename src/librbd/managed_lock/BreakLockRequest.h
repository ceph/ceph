// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_BREAK_LOCK_REQUEST_H
#define CEPH_LIBRBD_MANAGED_LOCK_BREAK_LOCK_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/int_types.h"
#include "include/buffer.h"
#include "msg/msg_types.h"
#include "librbd/ManagedLock.h"
#include <string>

class Context;
class ContextWQ;

namespace librbd {

namespace managed_lock {

template <typename ImageCtxT>
class BreakLockRequest {
public:
  static BreakLockRequest* create(librados::IoCtx& ioctx,
				  ContextWQ *work_queue, const std::string& oid,
                                  const typename ManagedLock<ImageCtxT>::LockOwner &owner,
                                  bool blacklist_lock_owner,
                                  Context *on_finish);
  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v       (blacklist disabled or error)
   * BLACKLIST . . . . . . . . .
   *    |                      .
   *    v                      .
   * BREAK_LOCK                .
   *    |                      .
   *    v                      .
   * <finish>  < . . . . . . . .
   *
   * @endverbatim
   */

  BreakLockRequest(librados::IoCtx& ioctx,
                   ContextWQ *work_queue, const std::string& oid,
                   const typename ManagedLock<ImageCtxT>::LockOwner &lock_owner,
		   bool blacklist_lock_owner, Context *on_finish);

  librados::IoCtx& m_ioctx;
  CephContext *m_cct;
  ContextWQ *m_work_queue;
  std::string m_oid;
  typename ManagedLock<ImageCtxT>::LockOwner m_lock_owner;
  bool m_blacklist_lock_owner;
  Context *m_on_finish;

  void send_blacklist();
  void handle_blacklist(int r);

  void send_break_lock();
  void handle_break_lock(int r);

  void finish(int r);
};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_BREAK_LOCK_REQUEST_H
