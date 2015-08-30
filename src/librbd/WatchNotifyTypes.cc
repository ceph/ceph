// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/WatchNotifyTypes.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace librbd {
namespace WatchNotify {

namespace {

class EncodePayloadVisitor : public boost::static_visitor<void> {
public:
  EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
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
  DumpPayloadVisitor(Formatter *formatter) : m_formatter(formatter) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
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
  ::encode(static_cast<uint32_t>(NOTIFY_OP_ACQUIRED_LOCK), bl);
  ::encode(client_id, bl);
}

void AcquiredLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
  if (version >= 2) {
    ::decode(client_id, iter);
  }
}

void AcquiredLockPayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_ACQUIRED_LOCK));
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void ReleasedLockPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_RELEASED_LOCK), bl);
  ::encode(client_id, bl);
}

void ReleasedLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
  if (version >= 2) {
    ::decode(client_id, iter);
  }
}

void ReleasedLockPayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_RELEASED_LOCK));
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void RequestLockPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_REQUEST_LOCK), bl);
  ::encode(client_id, bl);
}

void RequestLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
  if (version >= 2) {
    ::decode(client_id, iter);
  }
}

void RequestLockPayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_REQUEST_LOCK));
  f->open_object_section("client_id");
  client_id.dump(f);
  f->close_section();
}

void HeaderUpdatePayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_HEADER_UPDATE), bl);
}

void HeaderUpdatePayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void HeaderUpdatePayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_HEADER_UPDATE));
}

void AsyncProgressPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_ASYNC_PROGRESS), bl);
  ::encode(async_request_id, bl);
  ::encode(offset, bl);
  ::encode(total, bl);
}

void AsyncProgressPayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(async_request_id, iter);
  ::decode(offset, iter);
  ::decode(total, iter);
}

void AsyncProgressPayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_ASYNC_PROGRESS));
  f->open_object_section("async_request_id");
  async_request_id.dump(f);
  f->close_section();
  f->dump_unsigned("offset", offset);
  f->dump_unsigned("total", total);
}

void AsyncCompletePayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_ASYNC_COMPLETE), bl);
  ::encode(async_request_id, bl);
  ::encode(result, bl);
}

void AsyncCompletePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(async_request_id, iter);
  ::decode(result, iter);
}

void AsyncCompletePayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_ASYNC_COMPLETE));
  f->open_object_section("async_request_id");
  async_request_id.dump(f);
  f->close_section();
  f->dump_int("result", result);
}

void FlattenPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_FLATTEN), bl);
  ::encode(async_request_id, bl);
}

void FlattenPayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(async_request_id, iter);
}

void FlattenPayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_FLATTEN));
  f->open_object_section("async_request_id");
  async_request_id.dump(f);
  f->close_section();
}

void ResizePayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_RESIZE), bl);
  ::encode(size, bl);
  ::encode(async_request_id, bl);
}

void ResizePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(size, iter);
  ::decode(async_request_id, iter);
}

void ResizePayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_RESIZE));
  f->dump_unsigned("size", size);
  f->open_object_section("async_request_id");
  async_request_id.dump(f);
  f->close_section();
}

void SnapCreatePayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_SNAP_CREATE), bl);
  ::encode(snap_name, bl);
}

void SnapCreatePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(snap_name, iter);
}

void SnapCreatePayload::dump(Formatter *f) const {
  f->dump_string("notify_op", stringify(NOTIFY_OP_SNAP_CREATE));
  f->dump_string("snap_name", snap_name);
}

void UnknownPayload::encode(bufferlist &bl) const {
  assert(false);
}

void UnknownPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void UnknownPayload::dump(Formatter *f) const {
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

} // namespace WatchNotify
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::WatchNotify::NotifyOp &op) {
  using namespace librbd::WatchNotify;

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
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

std::ostream &operator<<(std::ostream &out,
                         const librbd::WatchNotify::ClientId &client_id) {
  out << "[" << client_id.gid << "," << client_id.handle << "]";
  return out;
}

std::ostream &operator<<(std::ostream &out,
                         const librbd::WatchNotify::AsyncRequestId &request) {
  out << "[" << request.client_id.gid << "," << request.client_id.handle << ","
      << request.request_id << "]";
  return out;
}
