// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "librbd/WatchNotifyTypes.h"

namespace librbd {
namespace watch_notify {

void AsyncRequestId::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(client_id, bl);
  encode(request_id, bl);
}

void AsyncRequestId::decode(bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(client_id, iter);
  decode(request_id, iter);
}

void AsyncRequestId::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
  f->dump_unsigned("request_id", request_id);
}

void AcquiredLockPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(client_id, bl);
}

void AcquiredLockPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  if (version >= 2) {
    decode(client_id, iter);
  }
}

void AcquiredLockPayload::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void ReleasedLockPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(client_id, bl);
}

void ReleasedLockPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  if (version >= 2) {
    decode(client_id, iter);
  }
}

void ReleasedLockPayload::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void RequestLockPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(client_id, bl);
  encode(force, bl);
}

void RequestLockPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  if (version >= 2) {
    decode(client_id, iter);
  }
  if (version >= 3) {
    decode(force, iter);
  }
}

void RequestLockPayload::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
  f->dump_bool("force", force);
}

void HeaderUpdatePayload::encode(bufferlist &bl) const {
}

void HeaderUpdatePayload::decode(__u8 version, bufferlist::const_iterator &iter) {
}

void HeaderUpdatePayload::dump(Formatter *f) const {
}

void AsyncRequestPayloadBase::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(async_request_id, bl);
}

void AsyncRequestPayloadBase::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(async_request_id, iter);
}

void AsyncRequestPayloadBase::dump(Formatter *f) const {
  f->open_object_section("async_request_id");
  async_request_id.dump(f);
  f->close_section();
}

void AsyncProgressPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  AsyncRequestPayloadBase::encode(bl);
  encode(offset, bl);
  encode(total, bl);
}

void AsyncProgressPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  AsyncRequestPayloadBase::decode(version, iter);
  decode(offset, iter);
  decode(total, iter);
}

void AsyncProgressPayload::dump(Formatter *f) const {
  AsyncRequestPayloadBase::dump(f);
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("total", total);
}

void AsyncCompletePayload::encode(bufferlist &bl) const {
  using ceph::encode;
  AsyncRequestPayloadBase::encode(bl);
  encode(result, bl);
}

void AsyncCompletePayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  AsyncRequestPayloadBase::decode(version, iter);
  decode(result, iter);
}

void AsyncCompletePayload::dump(Formatter *f) const {
  AsyncRequestPayloadBase::dump(f);
  f->dump_int("result", result);
}

void ResizePayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(size, bl);
  AsyncRequestPayloadBase::encode(bl);
  encode(allow_shrink, bl);
}

void ResizePayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(size, iter);
  AsyncRequestPayloadBase::decode(version, iter);

  if (version >= 4) {
    decode(allow_shrink, iter);
  }
}

void ResizePayload::dump(Formatter *f) const {
  f->dump_unsigned("size", size);
  f->dump_bool("allow_shrink", allow_shrink);
  AsyncRequestPayloadBase::dump(f);
}

void SnapPayloadBase::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(snap_name, bl);
  encode(snap_namespace, bl);
}

void SnapPayloadBase::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(snap_name, iter);
  if (version >= 6) {
    decode(snap_namespace, iter);
  }
}

void SnapPayloadBase::dump(Formatter *f) const {
  f->dump_string("snap_name", snap_name);
  snap_namespace.dump(f);
}

void SnapCreatePayload::encode(bufferlist &bl) const {
  using ceph::encode;
  SnapPayloadBase::encode(bl);
  encode(async_request_id, bl);
  encode(flags, bl);
}

void SnapCreatePayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  SnapPayloadBase::decode(version, iter);
  if (version == 5) {
    decode(snap_namespace, iter);
  }
  if (version >= 7) {
    decode(async_request_id, iter);
    decode(flags, iter);
  }
}

void SnapCreatePayload::dump(Formatter *f) const {
  f->open_object_section("async_request_id");
  async_request_id.dump(f);
  f->close_section();
  SnapPayloadBase::dump(f);
  f->dump_unsigned("flags", flags);
}

void SnapRenamePayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(snap_id, bl);
  SnapPayloadBase::encode(bl);
}

void SnapRenamePayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(snap_id, iter);
  SnapPayloadBase::decode(version, iter);
}

void SnapRenamePayload::dump(Formatter *f) const {
  f->dump_unsigned("src_snap_id", snap_id);
  SnapPayloadBase::dump(f);
}

void RenamePayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(image_name, bl);
}

void RenamePayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(image_name, iter);
}

void RenamePayload::dump(Formatter *f) const {
  f->dump_string("image_name", image_name);
}

void UpdateFeaturesPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(features, bl);
  encode(enabled, bl);
}

void UpdateFeaturesPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(features, iter);
  decode(enabled, iter);
}

void UpdateFeaturesPayload::dump(Formatter *f) const {
  f->dump_unsigned("features", features);
  f->dump_bool("enabled", enabled);
}

void SparsifyPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  AsyncRequestPayloadBase::encode(bl);
  encode(sparse_size, bl);
}

void SparsifyPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  AsyncRequestPayloadBase::decode(version, iter);
  decode(sparse_size, iter);
}

void SparsifyPayload::dump(Formatter *f) const {
  AsyncRequestPayloadBase::dump(f);
  f->dump_unsigned("sparse_size", sparse_size);
}

void UnknownPayload::encode(bufferlist &bl) const {
  ceph_abort();
}

void UnknownPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
}

void UnknownPayload::dump(Formatter *f) const {
}

bool NotifyMessage::check_for_refresh() const {
  return payload->check_for_refresh();
}

void NotifyMessage::encode(bufferlist& bl) const {
  ENCODE_START(7, 1, bl);
  encode(static_cast<uint32_t>(payload->get_notify_op()), bl);
  payload->encode(bl);
  ENCODE_FINISH(bl);
}

void NotifyMessage::decode(bufferlist::const_iterator& iter) {
  DECODE_START(1, iter);

  uint32_t notify_op;
  decode(notify_op, iter);

  // select the correct payload variant based upon the encoded op
  switch (notify_op) {
  case NOTIFY_OP_ACQUIRED_LOCK:
    payload.reset(new AcquiredLockPayload());
    break;
  case NOTIFY_OP_RELEASED_LOCK:
    payload.reset(new ReleasedLockPayload());
    break;
  case NOTIFY_OP_REQUEST_LOCK:
    payload.reset(new RequestLockPayload());
    break;
  case NOTIFY_OP_HEADER_UPDATE:
    payload.reset(new HeaderUpdatePayload());
    break;
  case NOTIFY_OP_ASYNC_PROGRESS:
    payload.reset(new AsyncProgressPayload());
    break;
  case NOTIFY_OP_ASYNC_COMPLETE:
    payload.reset(new AsyncCompletePayload());
    break;
  case NOTIFY_OP_FLATTEN:
    payload.reset(new FlattenPayload());
    break;
  case NOTIFY_OP_RESIZE:
    payload.reset(new ResizePayload());
    break;
  case NOTIFY_OP_SNAP_CREATE:
    payload.reset(new SnapCreatePayload());
    break;
  case NOTIFY_OP_SNAP_REMOVE:
    payload.reset(new SnapRemovePayload());
    break;
  case NOTIFY_OP_SNAP_RENAME:
    payload.reset(new SnapRenamePayload());
    break;
  case NOTIFY_OP_SNAP_PROTECT:
    payload.reset(new SnapProtectPayload());
    break;
  case NOTIFY_OP_SNAP_UNPROTECT:
    payload.reset(new SnapUnprotectPayload());
    break;
  case NOTIFY_OP_REBUILD_OBJECT_MAP:
    payload.reset(new RebuildObjectMapPayload());
    break;
  case NOTIFY_OP_RENAME:
    payload.reset(new RenamePayload());
    break;
  case NOTIFY_OP_UPDATE_FEATURES:
    payload.reset(new UpdateFeaturesPayload());
    break;
  case NOTIFY_OP_MIGRATE:
    payload.reset(new MigratePayload());
    break;
  case NOTIFY_OP_SPARSIFY:
    payload.reset(new SparsifyPayload());
    break;
  case NOTIFY_OP_QUIESCE:
    payload.reset(new QuiescePayload());
    break;
  case NOTIFY_OP_UNQUIESCE:
    payload.reset(new UnquiescePayload());
    break;
  }

  payload->decode(struct_v, iter);
  DECODE_FINISH(iter);
}

void NotifyMessage::dump(Formatter *f) const {
  payload->dump(f);
}

NotifyOp NotifyMessage::get_notify_op() const {
  return payload->get_notify_op();
}

void NotifyMessage::generate_test_instances(std::list<NotifyMessage *> &o) {
  o.push_back(new NotifyMessage(new AcquiredLockPayload(ClientId(1, 2))));
  o.push_back(new NotifyMessage(new ReleasedLockPayload(ClientId(1, 2))));
  o.push_back(new NotifyMessage(new RequestLockPayload(ClientId(1, 2), true)));
  o.push_back(new NotifyMessage(new HeaderUpdatePayload()));
  o.push_back(new NotifyMessage(new AsyncProgressPayload(AsyncRequestId(ClientId(0, 1), 2), 3, 4)));
  o.push_back(new NotifyMessage(new AsyncCompletePayload(AsyncRequestId(ClientId(0, 1), 2), 3)));
  o.push_back(new NotifyMessage(new FlattenPayload(AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(new ResizePayload(123, true, AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(new SnapCreatePayload(AsyncRequestId(ClientId(0, 1), 2),
                                                      cls::rbd::UserSnapshotNamespace(),
                                                      "foo", 1)));
  o.push_back(new NotifyMessage(new SnapRemovePayload(cls::rbd::UserSnapshotNamespace(), "foo")));
  o.push_back(new NotifyMessage(new SnapProtectPayload(cls::rbd::UserSnapshotNamespace(), "foo")));
  o.push_back(new NotifyMessage(new SnapUnprotectPayload(cls::rbd::UserSnapshotNamespace(), "foo")));
  o.push_back(new NotifyMessage(new RebuildObjectMapPayload(AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(new RenamePayload("foo")));
  o.push_back(new NotifyMessage(new UpdateFeaturesPayload(1, true)));
  o.push_back(new NotifyMessage(new MigratePayload(AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(new SparsifyPayload(AsyncRequestId(ClientId(0, 1), 2), 1)));
  o.push_back(new NotifyMessage(new QuiescePayload(AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(new UnquiescePayload(AsyncRequestId(ClientId(0, 1), 2))));
}

void ResponseMessage::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  encode(result, bl);
  ENCODE_FINISH(bl);
}

void ResponseMessage::decode(bufferlist::const_iterator& iter) {
  DECODE_START(1, iter);
  decode(result, iter);
  DECODE_FINISH(iter);
}

void ResponseMessage::dump(Formatter *f) const {
  f->dump_int("result", result);
}

void ResponseMessage::generate_test_instances(std::list<ResponseMessage *> &o) {
  o.push_back(new ResponseMessage(1));
}

std::ostream &operator<<(std::ostream &out,
                         const librbd::watch_notify::NotifyOp &op) {
  using namespace librbd::watch_notify;

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
  case NOTIFY_OP_HEADER_UPDATE:
    out << "HeaderUpdate";
    break;
  case NOTIFY_OP_ASYNC_PROGRESS:
    out << "AsyncProgress";
    break;
  case NOTIFY_OP_ASYNC_COMPLETE:
    out << "AsyncComplete";
    break;
  case NOTIFY_OP_FLATTEN:
    out << "Flatten";
    break;
  case NOTIFY_OP_RESIZE:
    out << "Resize";
    break;
  case NOTIFY_OP_SNAP_CREATE:
    out << "SnapCreate";
    break;
  case NOTIFY_OP_SNAP_REMOVE:
    out << "SnapRemove";
    break;
  case NOTIFY_OP_SNAP_RENAME:
    out << "SnapRename";
    break;
  case NOTIFY_OP_SNAP_PROTECT:
    out << "SnapProtect";
    break;
  case NOTIFY_OP_SNAP_UNPROTECT:
    out << "SnapUnprotect";
    break;
  case NOTIFY_OP_REBUILD_OBJECT_MAP:
    out << "RebuildObjectMap";
    break;
  case NOTIFY_OP_RENAME:
    out << "Rename";
    break;
  case NOTIFY_OP_UPDATE_FEATURES:
    out << "UpdateFeatures";
    break;
  case NOTIFY_OP_MIGRATE:
    out << "Migrate";
    break;
  case NOTIFY_OP_SPARSIFY:
    out << "Sparsify";
    break;
  case NOTIFY_OP_QUIESCE:
    out << "Quiesce";
    break;
  case NOTIFY_OP_UNQUIESCE:
    out << "Unquiesce";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

std::ostream &operator<<(std::ostream &out,
                         const librbd::watch_notify::AsyncRequestId &request) {
  out << "[" << request.client_id.gid << "," << request.client_id.handle << ","
      << request.request_id << "]";
  return out;
}
} // namespace watch_notify
} // namespace librbd
