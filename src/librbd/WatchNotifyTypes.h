// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef LIBRBD_WATCH_NOTIFY_TYPES_H
#define LIBRBD_WATCH_NOTIFY_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include <iosfwd>
#include <list>
#include <string>
#include <boost/variant.hpp>

namespace ceph {
class Formatter;
}

namespace librbd {
namespace watch_notify {

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

struct AsyncRequestId {
  ClientId client_id;
  uint64_t request_id;

  AsyncRequestId() : request_id() {}
  AsyncRequestId(const ClientId &client_id_, uint64_t request_id_)
    : client_id(client_id_), request_id(request_id_) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  inline bool operator<(const AsyncRequestId &rhs) const {
    if (client_id != rhs.client_id) {
      return client_id < rhs.client_id;
    } else {
      return request_id < rhs.request_id;
    }
  }
  inline bool operator!=(const AsyncRequestId &rhs) const {
    return (client_id != rhs.client_id || request_id != rhs.request_id);
  }
};

enum NotifyOp {
  NOTIFY_OP_ACQUIRED_LOCK      = 0,
  NOTIFY_OP_RELEASED_LOCK      = 1,
  NOTIFY_OP_REQUEST_LOCK       = 2,
  NOTIFY_OP_HEADER_UPDATE      = 3,
  NOTIFY_OP_ASYNC_PROGRESS     = 4,
  NOTIFY_OP_ASYNC_COMPLETE     = 5,
  NOTIFY_OP_FLATTEN            = 6,
  NOTIFY_OP_RESIZE             = 7,
  NOTIFY_OP_SNAP_CREATE        = 8,
  NOTIFY_OP_SNAP_REMOVE        = 9,
  NOTIFY_OP_REBUILD_OBJECT_MAP = 10,
  NOTIFY_OP_SNAP_RENAME        = 11,
  NOTIFY_OP_SNAP_PROTECT       = 12,
  NOTIFY_OP_SNAP_UNPROTECT     = 13,
  NOTIFY_OP_RENAME             = 14
};

struct AcquiredLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_ACQUIRED_LOCK;
  static const bool CHECK_FOR_REFRESH = true;

  ClientId client_id;

  AcquiredLockPayload() {}
  AcquiredLockPayload(const ClientId &client_id_) : client_id(client_id_) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ReleasedLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_RELEASED_LOCK;
  static const bool CHECK_FOR_REFRESH = true;

  ClientId client_id;

  ReleasedLockPayload() {}
  ReleasedLockPayload(const ClientId &client_id_) : client_id(client_id_) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct RequestLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_REQUEST_LOCK;
  static const bool CHECK_FOR_REFRESH = true;

  ClientId client_id;

  RequestLockPayload() {}
  RequestLockPayload(const ClientId &client_id_) : client_id(client_id_) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct HeaderUpdatePayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_HEADER_UPDATE;
  static const bool CHECK_FOR_REFRESH = false;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct AsyncRequestPayloadBase {
public:
  AsyncRequestId async_request_id;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;

protected:
  AsyncRequestPayloadBase() {}
  AsyncRequestPayloadBase(const AsyncRequestId &id) : async_request_id(id) {}
};

struct AsyncProgressPayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_ASYNC_PROGRESS;
  static const bool CHECK_FOR_REFRESH = false;

  AsyncProgressPayload() : offset(0), total(0) {}
  AsyncProgressPayload(const AsyncRequestId &id, uint64_t offset_, uint64_t total_)
    : AsyncRequestPayloadBase(id), offset(offset_), total(total_) {}

  uint64_t offset;
  uint64_t total;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct AsyncCompletePayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_ASYNC_COMPLETE;
  static const bool CHECK_FOR_REFRESH = false;

  AsyncCompletePayload() {}
  AsyncCompletePayload(const AsyncRequestId &id, int r)
    : AsyncRequestPayloadBase(id), result(r) {}

  int result;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct FlattenPayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_FLATTEN;
  static const bool CHECK_FOR_REFRESH = true;

  FlattenPayload() {}
  FlattenPayload(const AsyncRequestId &id) : AsyncRequestPayloadBase(id) {}
};

struct ResizePayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_RESIZE;
  static const bool CHECK_FOR_REFRESH = true;

  ResizePayload() : size(0) {}
  ResizePayload(uint64_t size_, const AsyncRequestId &id)
    : AsyncRequestPayloadBase(id), size(size_) {}

  uint64_t size;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct SnapPayloadBase {
public:
  static const bool CHECK_FOR_REFRESH = true;

  std::string snap_name;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;

protected:
  SnapPayloadBase() {}
  SnapPayloadBase(const std::string &name) : snap_name(name) {}
};

struct SnapCreatePayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_CREATE;

  SnapCreatePayload() {}
  SnapCreatePayload(const std::string &name) : SnapPayloadBase(name) {}
};

struct SnapRenamePayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_RENAME;

  SnapRenamePayload() {}
  SnapRenamePayload(const uint64_t &src_snap_id, const std::string &dst_name)
    : SnapPayloadBase(dst_name), snap_id(src_snap_id) {}

  uint64_t snap_id;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct SnapRemovePayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_REMOVE;

  SnapRemovePayload() {}
  SnapRemovePayload(const std::string &name) : SnapPayloadBase(name) {}
};

struct SnapProtectPayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_PROTECT;

  SnapProtectPayload() {}
  SnapProtectPayload(const std::string &name) : SnapPayloadBase(name) {}
};

struct SnapUnprotectPayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_UNPROTECT;

  SnapUnprotectPayload() {}
  SnapUnprotectPayload(const std::string &name) : SnapPayloadBase(name) {}
};

struct RebuildObjectMapPayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_REBUILD_OBJECT_MAP;
  static const bool CHECK_FOR_REFRESH = true;

  RebuildObjectMapPayload() {}
  RebuildObjectMapPayload(const AsyncRequestId &id)
    : AsyncRequestPayloadBase(id) {}
};

struct RenamePayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_RENAME;
  static const bool CHECK_FOR_REFRESH = true;

  RenamePayload() {}
  RenamePayload(const std::string _image_name) : image_name(_image_name) {}

  std::string image_name;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);
  static const bool CHECK_FOR_REFRESH = false;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

typedef boost::variant<AcquiredLockPayload,
                       ReleasedLockPayload,
                       RequestLockPayload,
                       HeaderUpdatePayload,
                       AsyncProgressPayload,
                       AsyncCompletePayload,
                       FlattenPayload,
                       ResizePayload,
                       SnapCreatePayload,
                       SnapRemovePayload,
                       SnapRenamePayload,
                       SnapProtectPayload,
                       SnapUnprotectPayload,
                       RebuildObjectMapPayload,
                       RenamePayload,
                       UnknownPayload> Payload;

struct NotifyMessage {
  NotifyMessage() : payload(UnknownPayload()) {}
  NotifyMessage(const Payload &payload_) : payload(payload_) {}

  Payload payload;

  bool check_for_refresh() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<NotifyMessage *> &o);
};

struct ResponseMessage {
  ResponseMessage() : result(0) {}
  ResponseMessage(int result_) : result(result_) {}

  int result;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ResponseMessage *> &o);
};

} // namespace watch_notify
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::watch_notify::NotifyOp &op);
std::ostream &operator<<(std::ostream &out,
                         const librbd::watch_notify::ClientId &client);
std::ostream &operator<<(std::ostream &out,
                         const librbd::watch_notify::AsyncRequestId &request);

WRITE_CLASS_ENCODER(librbd::watch_notify::ClientId);
WRITE_CLASS_ENCODER(librbd::watch_notify::AsyncRequestId);
WRITE_CLASS_ENCODER(librbd::watch_notify::NotifyMessage);
WRITE_CLASS_ENCODER(librbd::watch_notify::ResponseMessage);

#endif // LIBRBD_WATCH_NOTIFY_TYPES_H
