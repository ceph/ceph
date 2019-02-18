// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef LIBRBD_WATCH_NOTIFY_TYPES_H
#define LIBRBD_WATCH_NOTIFY_TYPES_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "librbd/watcher/Types.h"
#include <iosfwd>
#include <list>
#include <string>
#include <boost/variant.hpp>

namespace ceph {
class Formatter;
}

namespace librbd {
namespace watch_notify {

using librbd::watcher::ClientId;

WRITE_CLASS_ENCODER(ClientId);

struct AsyncRequestId {
  ClientId client_id;
  uint64_t request_id;

  AsyncRequestId() : request_id() {}
  AsyncRequestId(const ClientId &client_id_, uint64_t request_id_)
    : client_id(client_id_), request_id(request_id_) {}

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
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
  NOTIFY_OP_RENAME             = 14,
  NOTIFY_OP_UPDATE_FEATURES    = 15,
  NOTIFY_OP_MIGRATE            = 16,
  NOTIFY_OP_SPARSIFY           = 17,
};

struct AcquiredLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_ACQUIRED_LOCK;
  static const bool CHECK_FOR_REFRESH = false;

  ClientId client_id;

  AcquiredLockPayload() {}
  AcquiredLockPayload(const ClientId &client_id_) : client_id(client_id_) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct ReleasedLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_RELEASED_LOCK;
  static const bool CHECK_FOR_REFRESH = false;

  ClientId client_id;

  ReleasedLockPayload() {}
  ReleasedLockPayload(const ClientId &client_id_) : client_id(client_id_) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct RequestLockPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_REQUEST_LOCK;
  static const bool CHECK_FOR_REFRESH = false;

  ClientId client_id;
  bool force = false;

  RequestLockPayload() {}
  RequestLockPayload(const ClientId &client_id_, bool force_)
    : client_id(client_id_), force(force_) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct HeaderUpdatePayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_HEADER_UPDATE;
  static const bool CHECK_FOR_REFRESH = false;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct AsyncRequestPayloadBase {
public:
  AsyncRequestId async_request_id;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
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
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct AsyncCompletePayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_ASYNC_COMPLETE;
  static const bool CHECK_FOR_REFRESH = false;

  AsyncCompletePayload() : result(0) {}
  AsyncCompletePayload(const AsyncRequestId &id, int r)
    : AsyncRequestPayloadBase(id), result(r) {}

  int result;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
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

  ResizePayload() : size(0), allow_shrink(true) {}
  ResizePayload(uint64_t size_, bool allow_shrink_, const AsyncRequestId &id)
    : AsyncRequestPayloadBase(id), size(size_), allow_shrink(allow_shrink_) {}

  uint64_t size;
  bool allow_shrink;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct SnapPayloadBase {
public:
  static const bool CHECK_FOR_REFRESH = true;

  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;

protected:
  SnapPayloadBase() {}
  SnapPayloadBase(const cls::rbd::SnapshotNamespace& _snap_namespace,
		  const std::string &name)
    : snap_namespace(_snap_namespace), snap_name(name) {}
};

struct SnapCreatePayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_CREATE;

  SnapCreatePayload() {}
  SnapCreatePayload(const cls::rbd::SnapshotNamespace &_snap_namespace,
		    const std::string &name)
    : SnapPayloadBase(_snap_namespace, name) {}

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct SnapRenamePayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_RENAME;

  SnapRenamePayload() {}
  SnapRenamePayload(const uint64_t &src_snap_id,
		    const std::string &dst_name)
    : SnapPayloadBase(cls::rbd::UserSnapshotNamespace(), dst_name), snap_id(src_snap_id) {}

  uint64_t snap_id = 0;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct SnapRemovePayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_REMOVE;

  SnapRemovePayload() {}
  SnapRemovePayload(const cls::rbd::SnapshotNamespace& snap_namespace,
		    const std::string &name)
    : SnapPayloadBase(snap_namespace, name) {}
};

struct SnapProtectPayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_PROTECT;

  SnapProtectPayload() {}
  SnapProtectPayload(const cls::rbd::SnapshotNamespace& snap_namespace,
		     const std::string &name)
    : SnapPayloadBase(snap_namespace, name) {}
};

struct SnapUnprotectPayload : public SnapPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SNAP_UNPROTECT;

  SnapUnprotectPayload() {}
  SnapUnprotectPayload(const cls::rbd::SnapshotNamespace& snap_namespace,
		       const std::string &name)
    : SnapPayloadBase(snap_namespace, name) {}
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
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct UpdateFeaturesPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_UPDATE_FEATURES;
  static const bool CHECK_FOR_REFRESH = true;

  UpdateFeaturesPayload() : features(0), enabled(false) {}
  UpdateFeaturesPayload(uint64_t features_, bool enabled_)
    : features(features_), enabled(enabled_) {}

  uint64_t features;
  bool enabled;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct MigratePayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_MIGRATE;
  static const bool CHECK_FOR_REFRESH = true;

  MigratePayload() {}
  MigratePayload(const AsyncRequestId &id) : AsyncRequestPayloadBase(id) {}
};

struct SparsifyPayload : public AsyncRequestPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SPARSIFY;
  static const bool CHECK_FOR_REFRESH = true;

  SparsifyPayload() {}
  SparsifyPayload(const AsyncRequestId &id, size_t sparse_size)
    : AsyncRequestPayloadBase(id), sparse_size(sparse_size) {}

  size_t sparse_size = 0;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);
  static const bool CHECK_FOR_REFRESH = false;

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
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
                       UpdateFeaturesPayload,
                       MigratePayload,
                       SparsifyPayload,
                       UnknownPayload> Payload;

struct NotifyMessage {
  NotifyMessage() : payload(UnknownPayload()) {}
  NotifyMessage(const Payload &payload_) : payload(payload_) {}

  Payload payload;

  bool check_for_refresh() const;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;
  NotifyOp get_notify_op() const;

  static void generate_test_instances(std::list<NotifyMessage *> &o);
};

struct ResponseMessage {
  ResponseMessage() : result(0) {}
  ResponseMessage(int result_) : result(result_) {}

  int result;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::const_iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<ResponseMessage *> &o);
};

std::ostream &operator<<(std::ostream &out,
                         const NotifyOp &op);
std::ostream &operator<<(std::ostream &out,
                         const AsyncRequestId &request);

WRITE_CLASS_ENCODER(AsyncRequestId);
WRITE_CLASS_ENCODER(NotifyMessage);
WRITE_CLASS_ENCODER(ResponseMessage);

} // namespace watch_notify
} // namespace librbd


#endif // LIBRBD_WATCH_NOTIFY_TYPES_H
