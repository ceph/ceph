// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "Types.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace rbd {
namespace mirror {
namespace instance_watcher {

namespace {

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

} // anonymous namespace

void PayloadBase::encode(bufferlist &bl) const {
  ::encode(request_id, bl);
}

void PayloadBase::decode(__u8 version, bufferlist::iterator &iter) {
  ::decode(request_id, iter);
}

void PayloadBase::dump(Formatter *f) const {
  f->dump_unsigned("request_id", request_id);
}

void ImagePayloadBase::encode(bufferlist &bl) const {
  PayloadBase::encode(bl);
  ::encode(global_image_id, bl);
  ::encode(peer_mirror_uuid, bl);
  ::encode(peer_image_id, bl);
}

void ImagePayloadBase::decode(__u8 version, bufferlist::iterator &iter) {
  PayloadBase::decode(version, iter);
  ::decode(global_image_id, iter);
  ::decode(peer_mirror_uuid, iter);
  ::decode(peer_image_id, iter);
}

void ImagePayloadBase::dump(Formatter *f) const {
  PayloadBase::dump(f);
  f->dump_string("global_image_id", global_image_id);
  f->dump_string("peer_mirror_uuid", peer_mirror_uuid);
  f->dump_string("peer_image_id", peer_image_id);
}

void ImageReleasePayload::encode(bufferlist &bl) const {
  ImagePayloadBase::encode(bl);
  ::encode(schedule_delete, bl);
}

void ImageReleasePayload::decode(__u8 version, bufferlist::iterator &iter) {
  ImagePayloadBase::decode(version, iter);
  ::decode(schedule_delete, iter);
}

void ImageReleasePayload::dump(Formatter *f) const {
  ImagePayloadBase::dump(f);
  f->dump_bool("schedule_delete", schedule_delete);
}

void SyncPayloadBase::encode(bufferlist &bl) const {
  PayloadBase::encode(bl);
  ::encode(sync_id, bl);
}

void SyncPayloadBase::decode(__u8 version, bufferlist::iterator &iter) {
  PayloadBase::decode(version, iter);
  ::decode(sync_id, iter);
}

void SyncPayloadBase::dump(Formatter *f) const {
  PayloadBase::dump(f);
  f->dump_string("sync_id", sync_id);
}

void UnknownPayload::encode(bufferlist &bl) const {
  assert(false);
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
  ::decode(notify_op, iter);

  // select the correct payload variant based upon the encoded op
  switch (notify_op) {
  case NOTIFY_OP_IMAGE_ACQUIRE:
    payload = ImageAcquirePayload();
    break;
  case NOTIFY_OP_IMAGE_RELEASE:
    payload = ImageReleasePayload();
    break;
  case NOTIFY_OP_SYNC_REQUEST:
    payload = SyncRequestPayload();
    break;
  case NOTIFY_OP_SYNC_START:
    payload = SyncStartPayload();
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
  o.push_back(new NotifyMessage(ImageAcquirePayload()));
  o.push_back(new NotifyMessage(ImageAcquirePayload(1, "gid", "uuid", "id")));

  o.push_back(new NotifyMessage(ImageReleasePayload()));
  o.push_back(new NotifyMessage(ImageReleasePayload(1, "gid", "uuid", "id",
                                                    true)));

  o.push_back(new NotifyMessage(SyncRequestPayload()));
  o.push_back(new NotifyMessage(SyncRequestPayload(1, "sync_id")));

  o.push_back(new NotifyMessage(SyncStartPayload()));
  o.push_back(new NotifyMessage(SyncStartPayload(1, "sync_id")));
}

std::ostream &operator<<(std::ostream &out, const NotifyOp &op) {
  switch (op) {
  case NOTIFY_OP_IMAGE_ACQUIRE:
    out << "ImageAcquire";
    break;
  case NOTIFY_OP_IMAGE_RELEASE:
    out << "ImageRelease";
    break;
  case NOTIFY_OP_SYNC_REQUEST:
    out << "SyncRequest";
    break;
  case NOTIFY_OP_SYNC_START:
    out << "SyncStart";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

void NotifyAckPayload::encode(bufferlist &bl) const {
  ::encode(instance_id, bl);
  ::encode(request_id, bl);
  ::encode(ret_val, bl);
}

void NotifyAckPayload::decode(bufferlist::iterator &iter) {
  ::decode(instance_id, iter);
  ::decode(request_id, iter);
  ::decode(ret_val, iter);
}

void NotifyAckPayload::dump(Formatter *f) const {
  f->dump_string("instance_id", instance_id);
  f->dump_unsigned("request_id", request_id);
  f->dump_int("request_id", ret_val);
}

} // namespace instance_watcher
} // namespace mirror
} // namespace rbd
