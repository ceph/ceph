// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace rbd {
namespace mirror {
namespace leader_watcher {

namespace {

class EncodePayloadVisitor : public boost::static_visitor<void> {
public:
  explicit EncodePayloadVisitor(bufferlist &bl) : m_bl(bl) {}

  template <typename Payload>
  inline void operator()(const Payload &payload) const {
    using ceph::encode;
    encode(static_cast<uint32_t>(Payload::NOTIFY_OP), m_bl);
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

} // anonymous namespace

void HeartbeatPayload::encode(bufferlist &bl) const {
}

void HeartbeatPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void HeartbeatPayload::dump(Formatter *f) const {
}

void LockAcquiredPayload::encode(bufferlist &bl) const {
}

void LockAcquiredPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void LockAcquiredPayload::dump(Formatter *f) const {
}

void LockReleasedPayload::encode(bufferlist &bl) const {
}

void LockReleasedPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void LockReleasedPayload::dump(Formatter *f) const {
}

void UnknownPayload::encode(bufferlist &bl) const {
  ceph_abort();
}

void UnknownPayload::decode(__u8 version, bufferlist::iterator &iter) {
}

void UnknownPayload::dump(Formatter *f) const {
}

void NotifyMessage::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  boost::apply_visitor(EncodePayloadVisitor(bl), payload);
  ENCODE_FINISH(bl);
}

void NotifyMessage::decode(bufferlist::iterator& iter) {
  DECODE_START(1, iter);

  uint32_t notify_op;
  decode(notify_op, iter);

  // select the correct payload variant based upon the encoded op
  switch (notify_op) {
  case NOTIFY_OP_HEARTBEAT:
    payload = HeartbeatPayload();
    break;
  case NOTIFY_OP_LOCK_ACQUIRED:
    payload = LockAcquiredPayload();
    break;
  case NOTIFY_OP_LOCK_RELEASED:
    payload = LockReleasedPayload();
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
  o.push_back(new NotifyMessage(HeartbeatPayload()));
  o.push_back(new NotifyMessage(LockAcquiredPayload()));
  o.push_back(new NotifyMessage(LockReleasedPayload()));
}

std::ostream &operator<<(std::ostream &out, const NotifyOp &op) {
  switch (op) {
  case NOTIFY_OP_HEARTBEAT:
    out << "Heartbeat";
    break;
  case NOTIFY_OP_LOCK_ACQUIRED:
    out << "LockAcquired";
    break;
  case NOTIFY_OP_LOCK_RELEASED:
    out << "LockReleased";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

} // namespace leader_watcher
} // namespace mirror
} // namespace librbd
