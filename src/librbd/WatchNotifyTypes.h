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
#include <memory>
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
  inline operator bool() const {
    return (*this != AsyncRequestId());
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
  NOTIFY_OP_QUIESCE            = 18,
  NOTIFY_OP_UNQUIESCE          = 19,
};

struct Payload {
  virtual ~Payload() {}

  virtual NotifyOp get_notify_op() const = 0;
  virtual bool check_for_refresh() const = 0;

  virtual void encode(bufferlist &bl) const = 0;
  virtual void decode(__u8 version, bufferlist::const_iterator &iter) = 0;
  virtual void dump(Formatter *f) const = 0;
};

struct AcquiredLockPayload : public Payload {
  ClientId client_id;

  AcquiredLockPayload() {}
  AcquiredLockPayload(const ClientId &client_id) : client_id(client_id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_ACQUIRED_LOCK;
  }
  bool check_for_refresh() const override {
    return false;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct ReleasedLockPayload : public Payload {
  ClientId client_id;

  ReleasedLockPayload() {}
  ReleasedLockPayload(const ClientId &client_id) : client_id(client_id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_RELEASED_LOCK;
  }
  bool check_for_refresh() const override {
    return false;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct RequestLockPayload : public Payload {
  ClientId client_id;
  bool force = false;

  RequestLockPayload() {}
  RequestLockPayload(const ClientId &client_id, bool force)
    : client_id(client_id), force(force) {
  }

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_REQUEST_LOCK;
  }
  bool check_for_refresh() const override {
    return false;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct HeaderUpdatePayload : public Payload {
  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_HEADER_UPDATE;
  }
  bool check_for_refresh() const override {
    return false;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct AsyncRequestPayloadBase : public Payload {
public:
  AsyncRequestId async_request_id;

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;

protected:
  AsyncRequestPayloadBase() {}
  AsyncRequestPayloadBase(const AsyncRequestId &id) : async_request_id(id) {}
};

struct AsyncProgressPayload : public AsyncRequestPayloadBase {
  uint64_t offset = 0;
  uint64_t total = 0;

  AsyncProgressPayload() {}
  AsyncProgressPayload(const AsyncRequestId &id, uint64_t offset, uint64_t total)
    : AsyncRequestPayloadBase(id), offset(offset), total(total) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_ASYNC_PROGRESS;
  }
  bool check_for_refresh() const override {
    return false;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct AsyncCompletePayload : public AsyncRequestPayloadBase {
  int result = 0;

  AsyncCompletePayload() {}
  AsyncCompletePayload(const AsyncRequestId &id, int r)
    : AsyncRequestPayloadBase(id), result(r) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_ASYNC_COMPLETE;
  }
  bool check_for_refresh() const override {
    return false;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct FlattenPayload : public AsyncRequestPayloadBase {
  FlattenPayload() {}
  FlattenPayload(const AsyncRequestId &id) : AsyncRequestPayloadBase(id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_FLATTEN;
  }
  bool check_for_refresh() const override {
    return true;
  }
};

struct ResizePayload : public AsyncRequestPayloadBase {
  uint64_t size = 0;
  bool allow_shrink = true;

  ResizePayload() {}
  ResizePayload(uint64_t size, bool allow_shrink, const AsyncRequestId &id)
    : AsyncRequestPayloadBase(id), size(size), allow_shrink(allow_shrink) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_RESIZE;
  }
  bool check_for_refresh() const override {
    return true;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct SnapPayloadBase : public Payload {
public:
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string snap_name;

  bool check_for_refresh() const override {
    return true;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;

protected:
  SnapPayloadBase() {}
  SnapPayloadBase(const cls::rbd::SnapshotNamespace& snap_namespace,
		  const std::string &name)
    : snap_namespace(snap_namespace), snap_name(name) {}
};

struct SnapCreatePayload : public SnapPayloadBase {
  AsyncRequestId async_request_id;
  uint64_t flags = 0;

  SnapCreatePayload() {}
  SnapCreatePayload(const AsyncRequestId &id,
                    const cls::rbd::SnapshotNamespace &snap_namespace,
		    const std::string &name, uint64_t flags)
    : SnapPayloadBase(snap_namespace, name), async_request_id(id),
      flags(flags) {
  }

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_SNAP_CREATE;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct SnapRenamePayload : public SnapPayloadBase {
  uint64_t snap_id = 0;

  SnapRenamePayload() {}
  SnapRenamePayload(const uint64_t &src_snap_id,
		    const std::string &dst_name)
    : SnapPayloadBase(cls::rbd::UserSnapshotNamespace(), dst_name), snap_id(src_snap_id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_SNAP_RENAME;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct SnapRemovePayload : public SnapPayloadBase {
  SnapRemovePayload() {}
  SnapRemovePayload(const cls::rbd::SnapshotNamespace& snap_namespace,
		    const std::string &name)
    : SnapPayloadBase(snap_namespace, name) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_SNAP_REMOVE;
  }
};

struct SnapProtectPayload : public SnapPayloadBase {
  SnapProtectPayload() {}
  SnapProtectPayload(const cls::rbd::SnapshotNamespace& snap_namespace,
		     const std::string &name)
    : SnapPayloadBase(snap_namespace, name) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_SNAP_PROTECT;
  }
};

struct SnapUnprotectPayload : public SnapPayloadBase {
  SnapUnprotectPayload() {}
  SnapUnprotectPayload(const cls::rbd::SnapshotNamespace& snap_namespace,
		       const std::string &name)
    : SnapPayloadBase(snap_namespace, name) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_SNAP_UNPROTECT;
  }
};

struct RebuildObjectMapPayload : public AsyncRequestPayloadBase {
  RebuildObjectMapPayload() {}
  RebuildObjectMapPayload(const AsyncRequestId &id)
    : AsyncRequestPayloadBase(id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_REBUILD_OBJECT_MAP;
  }
  bool check_for_refresh() const override {
    return true;
  }
};

struct RenamePayload : public Payload {
  std::string image_name;

  RenamePayload() {}
  RenamePayload(const std::string _image_name) : image_name(_image_name) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_RENAME;
  }
  bool check_for_refresh() const override {
    return true;
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::const_iterator &iter);
  void dump(Formatter *f) const;
};

struct UpdateFeaturesPayload : public Payload {
  uint64_t features = 0;
  bool enabled = false;

  UpdateFeaturesPayload() {}
  UpdateFeaturesPayload(uint64_t features, bool enabled)
    : features(features), enabled(enabled) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_UPDATE_FEATURES;
  }
  bool check_for_refresh() const override {
    return true;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct MigratePayload : public AsyncRequestPayloadBase {
  MigratePayload() {}
  MigratePayload(const AsyncRequestId &id) : AsyncRequestPayloadBase(id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_MIGRATE;
  }
  bool check_for_refresh() const override {
    return true;
  }
};

struct SparsifyPayload : public AsyncRequestPayloadBase {
  size_t sparse_size = 0;

  SparsifyPayload() {}
  SparsifyPayload(const AsyncRequestId &id, size_t sparse_size)
    : AsyncRequestPayloadBase(id), sparse_size(sparse_size) {
  }

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_SPARSIFY;
  }
  bool check_for_refresh() const override {
    return true;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct QuiescePayload : public AsyncRequestPayloadBase {
  QuiescePayload() {}
  QuiescePayload(const AsyncRequestId &id) : AsyncRequestPayloadBase(id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_QUIESCE;
  }
  bool check_for_refresh() const override {
    return false;
  }
};

struct UnquiescePayload : public AsyncRequestPayloadBase {
  UnquiescePayload() {}
  UnquiescePayload(const AsyncRequestId &id) : AsyncRequestPayloadBase(id) {}

  NotifyOp get_notify_op() const override {
    return NOTIFY_OP_UNQUIESCE;
  }
  bool check_for_refresh() const override {
    return false;
  }
};

struct UnknownPayload : public Payload {
  NotifyOp get_notify_op() const override {
    return static_cast<NotifyOp>(-1);
  }
  bool check_for_refresh() const override {
    return false;
  }

  void encode(bufferlist &bl) const override;
  void decode(__u8 version, bufferlist::const_iterator &iter) override;
  void dump(Formatter *f) const override;
};

struct NotifyMessage {
  NotifyMessage() : payload(new UnknownPayload()) {}
  NotifyMessage(Payload *payload) : payload(payload) {}

  std::unique_ptr<Payload> payload;

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
