// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_LEADER_WATCHER_TYPES_H
#define RBD_MIRROR_LEADER_WATCHER_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include <string>
#include <variant>
#include <vector>

struct Context;

namespace ceph { class Formatter; }

namespace rbd {
namespace mirror {
namespace leader_watcher {

struct Listener {
  typedef std::vector<std::string> InstanceIds;

  virtual ~Listener() {
  }

  virtual void post_acquire_handler(Context *on_finish) = 0;
  virtual void pre_release_handler(Context *on_finish) = 0;

  virtual void update_leader_handler(
    const std::string &leader_instance_id) = 0;

  virtual void handle_instances_added(const InstanceIds& instance_ids) = 0;
  virtual void handle_instances_removed(const InstanceIds& instance_ids) = 0;
};

enum NotifyOp {
  NOTIFY_OP_HEARTBEAT        = 0,
  NOTIFY_OP_LOCK_ACQUIRED    = 1,
  NOTIFY_OP_LOCK_RELEASED    = 2,
};

struct HeartbeatPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_HEARTBEAT;

  HeartbeatPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct LockAcquiredPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_LOCK_ACQUIRED;

  LockAcquiredPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct LockReleasedPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_LOCK_RELEASED;

  LockReleasedPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);

  UnknownPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

typedef std::variant<HeartbeatPayload,
		     LockAcquiredPayload,
		     LockReleasedPayload,
		     UnknownPayload> Payload;

struct NotifyMessage {
  NotifyMessage(const Payload &payload = UnknownPayload()) : payload(payload) {
  }

  Payload payload;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<NotifyMessage *> &o);
};

WRITE_CLASS_ENCODER(NotifyMessage);

std::ostream &operator<<(std::ostream &out, const NotifyOp &op);

} // namespace leader_watcher
} // namespace mirror
} // namespace librbd

using rbd::mirror::leader_watcher::encode;
using rbd::mirror::leader_watcher::decode;

#endif // RBD_MIRROR_LEADER_WATCHER_TYPES_H
