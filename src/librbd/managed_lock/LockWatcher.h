// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_LOCK_WATCHER_H
#define CEPH_LIBRBD_LOCK_WATCHER_H

#include "librbd/watcher/Watcher.h"
#include "librbd/managed_lock/LockWatcherTypes.h"
#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include <boost/variant.hpp>

namespace ceph {
class Formatter;
}

namespace librbd {

template <typename> class Lock;

namespace managed_lock {

class LockWatcher : public watcher::Watcher<LockPayload> {
public:
  static const std::string WATCHER_LOCK_TAG;

  LockWatcher(Lock<LockWatcher> *managed_lock);
  virtual ~LockWatcher();

  void notify_acquired_lock();
  void notify_released_lock();
  void notify_request_lock();

  inline ContextWQ *work_queue() {
    return m_work_queue;
  }

  static bool decode_lock_cookie(const std::string &tag, uint64_t *handle);
  std::string encode_lock_cookie() const;

private:
  static const watcher::TaskCode TASK_CODE_REQUEST_LOCK;

  Lock<LockWatcher> *m_managed_lock;
  CephContext *m_cct;
  Mutex m_owner_client_id_lock;
  ClientId m_owner_client_id;

  void schedule_request_lock(bool use_timer, int timer_delayi = -1);

  void set_owner_client_id(const ClientId &client_id);
  ClientId get_client_id();

  void handle_request_lock(int r);

  bool handle_payload(const AcquiredLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const ReleasedLockPayload& payload,
                      C_NotifyAck *ctx);
  bool handle_payload(const RequestLockPayload& payload,
                      C_NotifyAck *ctx);

  virtual void handle_error(uint64_t handle, int err);

  void notify_lock_owner(bufferlist &&bl, Context *on_finish);

  virtual LockPayload decode_payload_type(uint32_t notify_op) {
    // select the correct payload variant based upon the encoded op
    switch (notify_op) {
    case NOTIFY_OP_ACQUIRED_LOCK:
      return AcquiredLockPayload();
    case NOTIFY_OP_RELEASED_LOCK:
      return ReleasedLockPayload();
    case NOTIFY_OP_REQUEST_LOCK:
      return RequestLockPayload();
    default:
      return UnknownPayload();
    }
  }

};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_LOCK_WATCHER_H
