// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/WatchNotifyTypes.h"
#include "include/assert.h"

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

}

void ClientId::encode(bufferlist &bl) const {
  ::encode(gid, bl);
  ::encode(handle, bl);
}

void ClientId::decode(bufferlist::iterator &iter) {
  ::decode(gid, iter);
  ::decode(handle, iter);
}

void AsyncRequestId::encode(bufferlist &bl) const {
  ::encode(client_id, bl);
  ::encode(request_id, bl);
}

void AsyncRequestId::decode(bufferlist::iterator &iter) {
  ::decode(client_id, iter);
  ::decode(request_id, iter);
}

void AcquiredLockPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_ACQUIRED_LOCK), bl);
}

void AcquiredLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void ReleasedLockPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_RELEASED_LOCK), bl);
}

void ReleasedLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void RequestLockPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_REQUEST_LOCK), bl);
}

void RequestLockPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void HeaderUpdatePayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_HEADER_UPDATE), bl);
}

void HeaderUpdatePayload::decode(__u8 version, bufferlist::iterator &iter) {
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

void AsyncCompletePayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_ASYNC_COMPLETE), bl);
  ::encode(async_request_id, bl);
  ::encode(result, bl);
}

void AsyncCompletePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(async_request_id, iter);
  ::decode(result, iter);
}

void FlattenPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_FLATTEN), bl);
  ::encode(async_request_id, bl);
}

void FlattenPayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(async_request_id, iter);
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

void SnapCreatePayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(NOTIFY_OP_SNAP_CREATE), bl);
  ::encode(snap_name, bl);
}

void SnapCreatePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(snap_name, iter);
}

void UnknownPayload::encode(bufferlist &bl) const {
  assert(false);
}

void UnknownPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void NotifyMessage::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
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

} // namespace WatchNotify
} // namespace librbd

std::ostream &operator<<(std::ostream &out,
                         const librbd::WatchNotify::AsyncRequestId &request) {
  out << "[" << request.client_id.gid << "," << request.client_id.handle << ","
      << request.request_id << "]";
  return out;
}
