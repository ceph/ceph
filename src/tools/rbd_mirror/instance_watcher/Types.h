// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_INSTANCE_WATCHER_TYPES_H
#define RBD_MIRROR_INSTANCE_WATCHER_TYPES_H

#include <string>
#include <set>
#include <boost/variant.hpp>

#include "include/buffer_fwd.h"
#include "include/encoding.h"
#include "include/int_types.h"

namespace ceph { class Formatter; }

namespace rbd {
namespace mirror {
namespace instance_watcher {

enum NotifyOp {
  NOTIFY_OP_IMAGE_ACQUIRE  = 0,
  NOTIFY_OP_IMAGE_RELEASE  = 1,
  NOTIFY_OP_SYNC_REQUEST   = 2,
  NOTIFY_OP_SYNC_START     = 3,
};

struct PayloadBase {
  uint64_t request_id;

  PayloadBase() : request_id(0) {
  }

  PayloadBase(uint64_t request_id) : request_id(request_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ImagePayloadBase : public PayloadBase {
  std::string global_image_id;
  std::string peer_mirror_uuid;
  std::string peer_image_id;

  ImagePayloadBase() : PayloadBase() {
  }

  ImagePayloadBase(uint64_t request_id, const std::string &global_image_id,
                   const std::string &peer_mirror_uuid,
                   const std::string &peer_image_id)
    : PayloadBase(request_id), global_image_id(global_image_id),
      peer_mirror_uuid(peer_mirror_uuid), peer_image_id(peer_image_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct ImageAcquirePayload : public ImagePayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_ACQUIRE;

  ImageAcquirePayload() : ImagePayloadBase() {
  }

  ImageAcquirePayload(uint64_t request_id, const std::string &global_image_id,
                      const std::string &peer_mirror_uuid,
                      const std::string &peer_image_id)
    : ImagePayloadBase(request_id, global_image_id, peer_mirror_uuid,
                       peer_image_id) {
  }
};

struct ImageReleasePayload : public ImagePayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_IMAGE_RELEASE;

  bool schedule_delete;

  ImageReleasePayload() : ImagePayloadBase(), schedule_delete(false) {
  }

  ImageReleasePayload(uint64_t request_id, const std::string &global_image_id,
                      const std::string &peer_mirror_uuid,
                      const std::string &peer_image_id, bool schedule_delete)
    : ImagePayloadBase(request_id, global_image_id, peer_mirror_uuid,
                       peer_image_id),
      schedule_delete(schedule_delete) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct SyncPayloadBase : public PayloadBase {
  std::string sync_id;

  SyncPayloadBase() : PayloadBase() {
  }

  SyncPayloadBase(uint64_t request_id, const std::string &sync_id)
    : PayloadBase(request_id), sync_id(sync_id) {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

struct SyncRequestPayload : public SyncPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SYNC_REQUEST;

  SyncRequestPayload() : SyncPayloadBase() {
  }

  SyncRequestPayload(uint64_t request_id, const std::string &sync_id)
    : SyncPayloadBase(request_id, sync_id) {
  }
};

struct SyncStartPayload : public SyncPayloadBase {
  static const NotifyOp NOTIFY_OP = NOTIFY_OP_SYNC_START;

  SyncStartPayload() : SyncPayloadBase() {
  }

  SyncStartPayload(uint64_t request_id, const std::string &sync_id)
    : SyncPayloadBase(request_id, sync_id) {
  }
};

struct UnknownPayload {
  static const NotifyOp NOTIFY_OP = static_cast<NotifyOp>(-1);

  UnknownPayload() {
  }

  void encode(bufferlist &bl) const;
  void decode(__u8 version, bufferlist::iterator &iter);
  void dump(Formatter *f) const;
};

typedef boost::variant<ImageAcquirePayload,
                       ImageReleasePayload,
                       SyncRequestPayload,
                       SyncStartPayload,
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

struct NotifyAckPayload {
  std::string instance_id;
  uint64_t request_id;
  int ret_val;

  NotifyAckPayload() : request_id(0), ret_val(0) {
  }

  NotifyAckPayload(const std::string &instance_id, uint64_t request_id,
                   int ret_val)
    : instance_id(instance_id), request_id(request_id), ret_val(ret_val) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

WRITE_CLASS_ENCODER(NotifyAckPayload);

} // namespace instance_watcher
} // namespace mirror
} // namespace librbd

using rbd::mirror::instance_watcher::encode;
using rbd::mirror::instance_watcher::decode;

#endif // RBD_MIRROR_INSTANCE_WATCHER_TYPES_H
