// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/mirroring_watcher/Types.h"
#include "include/assert.h"
#include "include/stringify.h"
#include "common/Formatter.h"

namespace librbd {
namespace mirroring_watcher {

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

void ModeUpdatedPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(mirror_mode), bl);
}

void ModeUpdatedPayload::decode(__u8 version, bufferlist::iterator &iter) {
  uint32_t mirror_mode_decode;
  ::decode(mirror_mode_decode, iter);
  mirror_mode = static_cast<cls::rbd::MirrorMode>(mirror_mode_decode);
}

void ModeUpdatedPayload::dump(Formatter *f) const {
  f->dump_stream("mirror_mode") << mirror_mode;
}

void ImageUpdatedPayload::encode(bufferlist &bl) const {
  ::encode(static_cast<uint32_t>(mirror_image_state), bl);
  ::encode(image_id, bl);
  ::encode(global_image_id, bl);
}

void ImageUpdatedPayload::decode(__u8 version, bufferlist::iterator &iter) {
  uint32_t mirror_image_state_decode;
  ::decode(mirror_image_state_decode, iter);
  mirror_image_state = static_cast<cls::rbd::MirrorImageState>(
    mirror_image_state_decode);
  ::decode(image_id, iter);
  ::decode(global_image_id, iter);
}

void ImageUpdatedPayload::dump(Formatter *f) const {
  f->dump_stream("mirror_image_state") << mirror_image_state;
  f->dump_string("image_id", image_id);
  f->dump_string("global_image_id", global_image_id);
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
  case NOTIFY_OP_MODE_UPDATED:
    payload = ModeUpdatedPayload();
    break;
  case NOTIFY_OP_IMAGE_UPDATED:
    payload = ImageUpdatedPayload();
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
  o.push_back(new NotifyMessage(ModeUpdatedPayload(cls::rbd::MIRROR_MODE_DISABLED)));
  o.push_back(new NotifyMessage(ImageUpdatedPayload(cls::rbd::MIRROR_IMAGE_STATE_DISABLING,
                                                    "image id", "global image id")));
}

std::ostream &operator<<(std::ostream &out, const NotifyOp &op) {
  switch (op) {
  case NOTIFY_OP_MODE_UPDATED:
    out << "ModeUpdated";
    break;
  case NOTIFY_OP_IMAGE_UPDATED:
    out << "ImageUpdated";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

} // namespace mirroring_watcher
} // namespace librbd
