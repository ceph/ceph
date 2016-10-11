// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/LockWatcherTypes.h"
#include "common/Formatter.h"

namespace librbd {
namespace managed_lock {

void ClientId::encode(bufferlist &bl) const {
  ::encode(gid, bl);
  ::encode(handle, bl);
}

void ClientId::decode(bufferlist::iterator &iter) {
  ::decode(gid, iter);
  ::decode(handle, iter);
}

void ClientId::dump(Formatter *f) const {
  f->dump_unsigned("gid", gid);
  f->dump_unsigned("handle", handle);
}

void AcquiredLockPayload::encode(bufferlist &bl) const {
  ::encode(client_id, bl);
}

void AcquiredLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
  if (version >= 2) {
    ::decode(client_id, iter);
  }
}

void AcquiredLockPayload::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void ReleasedLockPayload::encode(bufferlist &bl) const {
  ::encode(client_id, bl);
}

void ReleasedLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
  if (version >= 2) {
    ::decode(client_id, iter);
  }
}

void ReleasedLockPayload::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void RequestLockPayload::encode(bufferlist &bl) const {
  ::encode(client_id, bl);
  ::encode(force, bl);
}

void RequestLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
  if (version >= 2) {
    ::decode(client_id, iter);
  }
  if (version >= 3) {
    ::decode(force, iter);
  }
}

void RequestLockPayload::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
  f->dump_bool("force", force);
}

void UnknownPayload::encode(bufferlist &bl) const {
  assert(false);
}

void UnknownPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void UnknownPayload::dump(Formatter *f) const {
}

} // namespace managed_lock
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::managed_lock::NotifyOp &op) {
  using namespace librbd::managed_lock;

  switch (op) {
  case NOTIFY_OP_ACQUIRED_LOCK:
    out << "AcquiredLock";
    break;
  case NOTIFY_OP_RELEASED_LOCK:
    out << "ReleasedLock";
    break;
  case NOTIFY_OP_REQUEST_LOCK:
    out << "RequestLock";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

std::ostream &operator<<(std::ostream &out,
                         const librbd::managed_lock::ClientId &client_id) {
  out << "[" << client_id.gid << "," << client_id.handle << "]";
  return out;
}


