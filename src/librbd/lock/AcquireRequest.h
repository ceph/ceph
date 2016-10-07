// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H

#include "include/rados/librados.hpp"
#include "include/int_types.h"
#include "include/buffer.h"
#include "msg/msg_types.h"
#include <string>

class Context;
class ContextWQ;

namespace librbd {
namespace lock {

class LockWatcher;

class AcquireRequest {
public:
  static AcquireRequest* create(librados::IoCtx& ioctx,
                                ContextWQ *work_queue,
                                LockWatcher *watcher,
                                const std::string& m_oid,
                                const std::string &cookie,
                                Context *on_acquire, Context *on_finish);

  ~AcquireRequest();
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
   *    |     /-----------------------------------------------------------\
   *    |     |                                                           |
   *    |     |             (no lockers)                                  |
   *    |     |   . . . . . . . . . . . . . . . . . . . . . .             |
   *    |     |   .                                         .             |
   *    |     v   v      (EBUSY)                            .             |
   *    \--> LOCK_IMAGE * * * * * * * * > GET_LOCKERS . . . .             |
   *              |                         |                             |
   *              |                         v                             |
   *              |                       GET_WATCHERS                    |
   *              |                         |                             |
   *              |                         v                             |
   *              |                       BLACKLIST (skip if blacklist    |
   *              |                         |        disabled)            |
   *              |                         v                             |
   *              |                       BREAK_LOCK                      |
   *              |                         |                             |
   *              |                         \-----------------------------/
   *              v
   *          <finish>
   *
   * @endverbatim
   */

  AcquireRequest(librados::IoCtx& ioctx, ContextWQ *work_queue,
                 LockWatcher *watcher, const std::string& m_oid,
                 const std::string &cookie, Context *on_acquire,
                 Context *on_finish);

  librados::IoCtx& m_ioctx;
  ContextWQ *m_work_queue;
  LockWatcher *m_watcher;
  CephContext *m_cct;
  std::string m_oid;
  std::string m_cookie;
  Context *m_on_acquire;
  Context *m_on_finish;

  bufferlist m_out_bl;

  std::list<obj_watch_t> m_watchers;
  int m_watchers_ret_val;

  entity_name_t m_locker_entity;
  std::string m_locker_cookie;
  std::string m_locker_address;
  uint64_t m_locker_handle;

  int m_error_result;

  void send_flush_notifies();
  Context *handle_flush_notifies(int *ret_val);

  void send_lock();
  Context *handle_lock(int *ret_val);

  void send_unlock();
  Context *handle_unlock(int *ret_val);

  void send_get_lockers();
  Context *handle_get_lockers(int *ret_val);

  void send_get_watchers();
  Context *handle_get_watchers(int *ret_val);

  void send_blacklist();
  Context *handle_blacklist(int *ret_val);

  void send_break_lock();
  Context *handle_break_lock(int *ret_val);
};

} // namespace exclusive_lock
} // namespace librbd

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_ACQUIRE_REQUEST_H
