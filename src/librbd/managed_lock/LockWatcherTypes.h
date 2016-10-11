// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_LOCK_WATCHER_TYPES_H
#define CEPH_LIBRBD_LOCK_WATCHER_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include <boost/variant.hpp>

namespace ceph {
class Formatter;
}

namespace librbd {
namespace managed_lock {

struct ClientId {
  uint64_t gid;
  uint64_t handle;

  ClientId() : gid(0), handle(0) {}
  ClientId(uint64_t gid_, uint64_t handle_) : gid(gid_), handle(handle_) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  inline bool is_valid() const {
    return (*this != ClientId());
  }

  inline bool operator==(const ClientId &rhs) const {
    return (gid == rhs.gid && handle == rhs.handle);
  }
  inline bool operator!=(const ClientId &rhs) const {
    return !(*this == rhs);
  }
  inline bool operator<(const ClientId &rhs) const {
    if (gid != rhs.gid) {
      return gid < rhs.gid;
    } else {
      return handle < rhs.handle;
    }
  }
};

enum NotifyOp {
  NOTIFY_OP_ACQUIRED_LOCK      = 0,
  NOTIFY_OP_RELEASED_LOCK      = 1,
  NOTIFY_OP_REQUEST_LOCK       = 2,
};

struct AcquiredLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_ACQUIRED_LOCK;

  ClientId client_id;

  AcquiredLockPayload() {}
  AcquiredLockPayload(const ClientId &client_id_) : client_id(client_id_) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ReleasedLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_RELEASED_LOCK;

  ClientId client_id;

  ReleasedLockPayload() {}
  ReleasedLockPayload(const ClientId &client_id_) : client_id(client_id_) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct RequestLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_REQUEST_LOCK;

  ClientId client_id;
  bool force = false;

  RequestLockPayload() {}
  RequestLockPayload(const ClientId &client_id_, bool force_)
    : client_id(client_id_), force(force_) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

typedef boost::variant<AcquiredLockPayload,
                       ReleasedLockPayload,
                       RequestLockPayload,
                       UnknownPayload> LockPayload;

} // namespace managed_lock
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::managed_lock::NotifyOp &op);
std::ostream &operator<<(std::ostream &out,
                         const librbd::managed_lock::ClientId &client);

WRITE_CLASS_ENCODER(librbd::managed_lock::ClientId);

#endif // CEPH_LIBRBD_LOCK_WATCHER_TYPES_H
