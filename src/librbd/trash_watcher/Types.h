// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_TRASH_WATCHER_TYPES_H
#define CEPH_LIBRBD_TRASH_WATCHER_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "cls/rbd/cls_rbd_types.h"
#include <iosfwd>
#include <list>
#include <string>
#include <boost/variant.hpp>


namespace librbd {
namespace trash_watcher {

enum NotifyOp {
  NOTIFY_OP_IMAGE_ADDED = 0,
  NOTIFY_OP_IMAGE_REMOVED = 1
};

struct ImageAddedPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_ADDED;

  std::string image_id;
  cls::rbd::TrashImageSpec trash_image_spec;

  ImageAddedPayload() {
  }
  ImageAddedPayload(const std::string& image_id,
                    const cls::rbd::TrashImageSpec& trash_image_spec)
    : image_id(image_id), trash_image_spec(trash_image_spec) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ImageRemovedPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_REMOVED;

  std::string image_id;

  ImageRemovedPayload() {
  }
  ImageRemovedPayload(const std::string& image_id)
    : image_id(image_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);

  UnknownPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

typedef boost::variant<ImageAddedPayload,
                       ImageRemovedPayload,
                       UnknownPayload> Payload;

struct NotifyMessage {
  NotifyMessage(const Payload &payload = UnknownPayload()) : payload(payload) {
  }

  Payload payload;

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<NotifyMessage *> &o);
};

WRITE_CLASS_ENCODER(NotifyMessage);

std::ostream &operator<<(std::ostream &out, const NotifyOp &op);

} // namespace trash_watcher
} // namespace librbd

using librbd::trash_watcher::encode;
using librbd::trash_watcher::decode;

#endif // CEPH_LIBRBD_TRASH_WATCHER_TYPES_H
