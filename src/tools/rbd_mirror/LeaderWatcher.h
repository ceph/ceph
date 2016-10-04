// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_LEADER_WATCHER_H
#define CEPH_RBD_MIRROR_LEADER_WATCHER_H

#include "librbd/Watcher.h"
#include "tools/rbd_mirror/leader_watcher/Types.h"

namespace rbd {
namespace mirror {

class LeaderWatcher : public librbd::Watcher {
public:
  LeaderWatcher(librados::IoCtx &io_ctx, ContextWQ *work_queue);

  std::string get_oid() const {
    return m_oid;
  }

  int init();
  void shut_down();

  void notify_heartbeat(Context *on_finish);
  void notify_lock_acquired(Context *on_finish);
  void notify_lock_released(Context *on_finish);

  virtual void handle_heartbeat(Context *on_ack) = 0;
  virtual void handle_lock_acquired(Context *on_ack) = 0;
  virtual void handle_lock_released(Context *on_ack) = 0;

protected:
  virtual void handle_notify(uint64_t notify_id, uint64_t handle,
                             uint64_t notifier_id, bufferlist &bl);

private:
  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    LeaderWatcher *leader_watcher;
    Context *on_notify_ack;

    HandlePayloadVisitor(LeaderWatcher *leader_watcher, Context *on_notify_ack)
      : leader_watcher(leader_watcher), on_notify_ack(on_notify_ack) {
    }

    template <typename Payload>
    inline void operator()(const Payload &payload) const {
      leader_watcher->handle_payload(payload, on_notify_ack);
    }
  };

  uint64_t m_notifier_id;

  void handle_payload(const leader_watcher::HeartbeatPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::LockAcquiredPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::LockReleasedPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::UnknownPayload &payload,
                      Context *on_notify_ack);

};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_LEADER_WATCHER_H
