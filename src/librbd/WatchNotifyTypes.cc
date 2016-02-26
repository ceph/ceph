// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/WatchNotifyTypes.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace librbd {
namespace watch_notify {

namespace {

class CheckForRefreshVisitor  : public boost::static_visitor<bool> {
public:
  template <typename Payload>
  inline bool operator()(const Payload &payload) const {
    return Payload::CHECK_FOR_REFRESH;
  }
};

class EncodePayloadVisitor : public boost::static_visitor<void> {
public:
  explicit EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
    ::encode(static_cast<uint32_t>(Payload::NOTIFY_OP), m_bl);
    payload.encode(m_bl);
  }

private:
  bufferlist &m_bl;
};

class DecodePayloadVisitor : public boost::static_visitor<void> {
public:
  DecodePayloadVisitor(__u8 version, bufferlist::iterator &iter)
    : m_version(version), m_iter(iter) {}

  template <typename Payload>
  inline void operator()(Payload &payload) const {
    payload.decode(m_version, m_iter);
  }

private:
  __u8 m_version;
  bufferlist::iterator &m_iter;
};

class DumpPayloadVisitor : public boost::static_visitor<void> {
public:
  explicit DumpPayloadVisitor(Formatter *formatter) : m_formatter(formatter) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
    NotifyOp notify_op = Payload::NOTIFY_OP;
    m_formatter->dump_string("notify_op", stringify(notify_op));
    payload.dump(m_formatter);
  }

private:
  ceph::Formatter *m_formatter;
};

}

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

void AsyncRequestId::encode(bufferlist &bl) const {
  ::encode(client_id, bl);
  ::encode(request_id, bl);
}

void AsyncRequestId::decode(bufferlist::iterator &iter) {
  ::decode(client_id, iter);
  ::decode(request_id, iter);
}

void AsyncRequestId::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
  f->dump_unsigned("request_id", request_id);
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
}

void RequestLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
  if (version >= 2) {
    ::decode(client_id, iter);
  }
}

void RequestLockPayload::dump(Formatter *f) const {
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void HeaderUpdatePayload::encode(bufferlist &bl) const {
}

void HeaderUpdatePayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void HeaderUpdatePayload::dump(Formatter *f) const {
}

void AsyncRequestPayloadBase::encode(bufferlist &bl) const {
  ::encode(async_request_id, bl);
}

void AsyncRequestPayloadBase::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(async_request_id, iter);
}

void AsyncRequestPayloadBase::dump(Formatter *f) const {
  f->open_object_section("async_request_id");
  async_request_id.dump(f);
  f->close_section();
}

void AsyncProgressPayload::encode(bufferlist &bl) const {
  AsyncRequestPayloadBase::encode(bl);
  ::encode(offset, bl);
  ::encode(total, bl);
}

void AsyncProgressPayload::decode(__u8 version, bufferlist::iterator &iter) {
  AsyncRequestPayloadBase::decode(version, iter);
  ::decode(offset, iter);
  ::decode(total, iter);
}

void AsyncProgressPayload::dump(Formatter *f) const {
  AsyncRequestPayloadBase::dump(f);
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("total", total);
}

void AsyncCompletePayload::encode(bufferlist &bl) const {
  AsyncRequestPayloadBase::encode(bl);
  ::encode(result, bl);
}

void AsyncCompletePayload::decode(__u8 version, bufferlist::iterator &iter) {
  AsyncRequestPayloadBase::decode(version, iter);
  ::decode(result, iter);
}

void AsyncCompletePayload::dump(Formatter *f) const {
  AsyncRequestPayloadBase::dump(f);
  f->dump_int("result", result);
}

void ResizePayload::encode(bufferlist &bl) const {
  ::encode(size, bl);
  AsyncRequestPayloadBase::encode(bl);
}

void ResizePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(size, iter);
  AsyncRequestPayloadBase::decode(version, iter);
}

void ResizePayload::dump(Formatter *f) const {
  f->dump_unsigned("size", size);
  AsyncRequestPayloadBase::dump(f);
}

void SnapPayloadBase::encode(bufferlist &bl) const {
  ::encode(snap_name, bl);
}

void SnapPayloadBase::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(snap_name, iter);
}

void SnapPayloadBase::dump(Formatter *f) const {
  f->dump_string("snap_name", snap_name);
}

void SnapRenamePayload::encode(bufferlist &bl) const {
  ::encode(snap_id, bl);
  SnapPayloadBase::encode(bl);
}

void SnapRenamePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(snap_id, iter);
  SnapPayloadBase::decode(version, iter);
}

void SnapRenamePayload::dump(Formatter *f) const {
  f->dump_unsigned("src_snap_id", snap_id);
  SnapPayloadBase::dump(f);
}

void RenamePayload::encode(bufferlist &bl) const {
  ::encode(image_name, bl);
}

void RenamePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(image_name, iter);
}

void RenamePayload::dump(Formatter *f) const {
  f->dump_string("image_name", image_name);
}

void UnknownPayload::encode(bufferlist &bl) const {
  assert(false);
}

void UnknownPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void UnknownPayload::dump(Formatter *f) const {
}

bool NotifyMessage::check_for_refresh() const {
  return boost::apply_visitor(CheckForRefreshVisitor(), payload);
}

void NotifyMessage::encode(bufferlist& bl) const {
  ENCODE_START(2, 1, bl);
  boost::apply_visitor(EncodePayloadVisitor(bl), payload);
  ENCODE_FINISH(bl);
}

void NotifyMessage::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);

  uint32_t notify_op;
  ::decode(notify_op, iter);

  // select the correct payload variant based upon the encoded op
  switch (notify_op) {
  case NOTIFY_OP_ACQUIRED_LOCK:
    payload = AcquiredLockPayload();
    break;
  case NOTIFY_OP_RELEASED_LOCK:
    payload = ReleasedLockPayload();
    break;
  case NOTIFY_OP_REQUEST_LOCK:
    payload = RequestLockPayload();
    break;
  case NOTIFY_OP_HEADER_UPDATE:
    payload = HeaderUpdatePayload();
    break;
  case NOTIFY_OP_ASYNC_PROGRESS:
    payload = AsyncProgressPayload();
    break;
  case NOTIFY_OP_ASYNC_COMPLETE:
    payload = AsyncCompletePayload();
    break;
  case NOTIFY_OP_FLATTEN:
    payload = FlattenPayload();
    break;
  case NOTIFY_OP_RESIZE:
    payload = ResizePayload();
    break;
  case NOTIFY_OP_SNAP_CREATE:
    payload = SnapCreatePayload();
    break;
  case NOTIFY_OP_SNAP_REMOVE:
    payload = SnapRemovePayload();
    break;
  case NOTIFY_OP_SNAP_RENAME:
    payload = SnapRenamePayload();
    break;
  case NOTIFY_OP_SNAP_PROTECT:
    payload = SnapProtectPayload();
    break;
  case NOTIFY_OP_SNAP_UNPROTECT:
    payload = SnapUnprotectPayload();
    break;
  case NOTIFY_OP_REBUILD_OBJECT_MAP:
    payload = RebuildObjectMapPayload();
    break;
  case NOTIFY_OP_RENAME:
    payload = RenamePayload();
    break;
  default:
    payload = UnknownPayload();
    break;
  }

  apply_visitor(DecodePayloadVisitor(struct_v, iter), payload);
  DECODE_FINISH(iter);
}

void NotifyMessage::dump(Formatter *f) const {
  apply_visitor(DumpPayloadVisitor(f), payload);
}

void NotifyMessage::generate_test_instances(std::list<NotifyMessage *> &o) {
  o.push_back(new NotifyMessage(AcquiredLockPayload(ClientId(1, 2))));
  o.push_back(new NotifyMessage(ReleasedLockPayload(ClientId(1, 2))));
  o.push_back(new NotifyMessage(RequestLockPayload(ClientId(1, 2))));
  o.push_back(new NotifyMessage(HeaderUpdatePayload()));
  o.push_back(new NotifyMessage(AsyncProgressPayload(AsyncRequestId(ClientId(0, 1), 2), 3, 4)));
  o.push_back(new NotifyMessage(AsyncCompletePayload(AsyncRequestId(ClientId(0, 1), 2), 3)));
  o.push_back(new NotifyMessage(FlattenPayload(AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(ResizePayload(123, AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(SnapCreatePayload("foo")));
  o.push_back(new NotifyMessage(SnapRemovePayload("foo")));
  o.push_back(new NotifyMessage(SnapProtectPayload("foo")));
  o.push_back(new NotifyMessage(SnapUnprotectPayload("foo")));
  o.push_back(new NotifyMessage(RebuildObjectMapPayload(AsyncRequestId(ClientId(0, 1), 2))));
  o.push_back(new NotifyMessage(RenamePayload("foo")));
}

void ResponseMessage::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(result, bl);
  ENCODE_FINISH(bl);
}

void ResponseMessage::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);
  ::decode(result, iter);
  DECODE_FINISH(iter);
}

void ResponseMessage::dump(Formatter *f) const {
  f->dump_int("result", result);
}

void ResponseMessage::generate_test_instances(std::list<ResponseMessage *> &o) {
  o.push_back(new ResponseMessage(1));
}

} // namespace watch_notify
} // namespace librbd

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
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

std::ostream &operator<<(std::ostream &out,
                         const librbd::watch_notify::ClientId &client_id) {
  out << "[" << client_id.gid << "," << client_id.handle << "]";
  return out;
}

std::ostream &operator<<(std::ostream &out,
                         const librbd::watch_notify::AsyncRequestId &request) {
  out << "[" << request.client_id.gid << "," << request.client_id.handle << ","
      << request.request_id << "]";
  return out;
}
