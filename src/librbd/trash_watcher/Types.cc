// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "include/stringify.h"
#include "librbd/trash_watcher/Types.h"
#include "librbd/watcher/Utils.h"

namespace librbd {
namespace trash_watcher {

namespace {

class DumpPayloadVisitor {
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

void ImageAddedPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(image_id, bl);
  encode(trash_image_spec, bl);
}

void ImageAddedPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(image_id, iter);
  decode(trash_image_spec, iter);
}

void ImageAddedPayload::dump(Formatter *f) const {
  f->dump_string("image_id", image_id);
  f->open_object_section("trash_image_spec");
  trash_image_spec.dump(f);
  f->close_section();
}

void ImageRemovedPayload::encode(bufferlist &bl) const {
  using ceph::encode;
  encode(image_id, bl);
}

void ImageRemovedPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
  using ceph::decode;
  decode(image_id, iter);
}

void ImageRemovedPayload::dump(Formatter *f) const {
  f->dump_string("image_id", image_id);
}

void UnknownPayload::encode(bufferlist &bl) const {
  ceph_abort();
}

void UnknownPayload::decode(__u8 version, bufferlist::const_iterator &iter) {
}

void UnknownPayload::dump(Formatter *f) const {
}

void NotifyMessage::encode(bufferlist& bl) const {
  ENCODE_START(1, 1, bl);
  std::visit(watcher::util::EncodePayloadVisitor(bl), payload);
  ENCODE_FINISH(bl);
}

void NotifyMessage::decode(bufferlist::const_iterator& iter) {
  DECODE_START(1, iter);

  uint32_t notify_op;
  decode(notify_op, iter);

  // select the correct payload variant based upon the encoded op
  switch (notify_op) {
  case NOTIFY_OP_IMAGE_ADDED:
    payload = ImageAddedPayload();
    break;
  case NOTIFY_OP_IMAGE_REMOVED:
    payload = ImageRemovedPayload();
    break;
  default:
    payload = UnknownPayload();
    break;
  }

  std::visit(watcher::util::DecodePayloadVisitor(struct_v, iter), payload);
  DECODE_FINISH(iter);
}

void NotifyMessage::dump(Formatter *f) const {
  std::visit(DumpPayloadVisitor(f), payload);
}

void NotifyMessage::generate_test_instances(std::list<NotifyMessage *> &o) {
  o.push_back(new NotifyMessage{ImageAddedPayload{
    "id", {cls::rbd::TRASH_IMAGE_SOURCE_USER, "name", {}, {}}}});
  o.push_back(new NotifyMessage{ImageRemovedPayload{"id"}});
}

std::ostream &operator<<(std::ostream &out, const NotifyOp &op) {
  switch (op) {
  case NOTIFY_OP_IMAGE_ADDED:
    out << "ImageAdded";
    break;
  case NOTIFY_OP_IMAGE_REMOVED:
    out << "ImageRemoved";
    break;
  default:
    out << "Unknown (" << static_cast<uint32_t>(op) << ")";
    break;
  }
  return out;
}

} // namespace trash_watcher
} // namespace librbd
