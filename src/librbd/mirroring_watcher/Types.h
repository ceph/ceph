// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRRORING_WATCHER_TYPES_H
#define CEPH_LIBRBD_MIRRORING_WATCHER_TYPES_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "cls/rbd/cls_rbd_types.h"
#include <iosfwd>
#include <list>
#include <string>
#include <boost/variant.hpp>

namespace ceph { class Formatter; }

namespace librbd {
namespace mirroring_watcher {

enum NotifyOp {
  NOTIFY_OP_MODE_UPDATED  = 0,
  NOTIFY_OP_IMAGE_UPDATED = 1
};

struct ModeUpdatedPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_MODE_UPDATED;

  cls::rbd::MirrorMode mirror_mode = cls::rbd::MIRROR_MODE_DISABLED;

  ModeUpdatedPayload() {
  }
  ModeUpdatedPayload(cls::rbd::MirrorMode mirror_mode)
    : mirror_mode(mirror_mode) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ImageUpdatedPayload {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_UPDATED;

  cls::rbd::MirrorImageState mirror_image_state =
    cls::rbd::MIRROR_IMAGE_STATE_ENABLED;
  std::string image_id;
  std::string global_image_id;

  ImageUpdatedPayload() {
  }
  ImageUpdatedPayload(cls::rbd::MirrorImageState mirror_image_state,
                      const std::string &image_id,
                      const std::string &global_image_id)
    : mirror_image_state(mirror_image_state), image_id(image_id),
      global_image_id(global_image_id) {
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

typedef boost::variant<ModeUpdatedPayload,
                       ImageUpdatedPayload,
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

} // namespace mirroring_watcher
} // namespace librbd

using librbd::mirroring_watcher::encode;
using librbd::mirroring_watcher::decode;

#endif // CEPH_LIBRBD_MIRRORING_WATCHER_TYPES_H
