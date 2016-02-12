// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "common/Formatter.h"

namespace cls {
namespace rbd {

void MirrorPeer::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(uuid, bl);
  ::encode(cluster_name, bl);
  ::encode(client_name, bl);
  ::encode(pool_id, bl);
  ENCODE_FINISH(bl);
}

void MirrorPeer::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(uuid, it);
  ::decode(cluster_name, it);
  ::decode(client_name, it);
  ::decode(pool_id, it);
  DECODE_FINISH(it);
}

void MirrorPeer::dump(Formatter *f) const {
  f->dump_string("uuid", uuid);
  f->dump_string("cluster_name", cluster_name);
  f->dump_string("client_name", client_name);
  f->dump_int("pool_id", pool_id);
}

void MirrorPeer::generate_test_instances(std::list<MirrorPeer*> &o) {
  o.push_back(new MirrorPeer());
  o.push_back(new MirrorPeer("uuid-123", "cluster name", "client name", 123));
}

bool MirrorPeer::operator==(const MirrorPeer &rhs) const {
  return (uuid == rhs.uuid &&
          cluster_name == rhs.cluster_name &&
          client_name == rhs.client_name &&
          pool_id == rhs.pool_id);
}

std::ostream& operator<<(std::ostream& os, const MirrorMode& mirror_mode) {
  switch (mirror_mode) {
  case MIRROR_MODE_DISABLED:
    os << "disabled";
    break;
  case MIRROR_MODE_IMAGE:
    os << "image";
    break;
  case MIRROR_MODE_POOL:
    os << "pool";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(mirror_mode) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer) {
  os << "["
     << "uuid=" << peer.uuid << ", "
     << "cluster_name=" << peer.cluster_name << ", "
     << "client_name=" << peer.client_name;
  if (peer.pool_id != -1) {
    os << ", pool_id=" << peer.pool_id;
  }
  os << "]";
  return os;
}

void MirrorImage::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(global_image_id, bl);
  ::encode(static_cast<uint8_t>(state), bl);
  ENCODE_FINISH(bl);
}

void MirrorImage::decode(bufferlist::iterator &it) {
  uint8_t int_state;
  DECODE_START(1, it);
  ::decode(global_image_id, it);
  ::decode(int_state, it);
  state = static_cast<MirrorImageState>(int_state);
  DECODE_FINISH(it);
}

void MirrorImage::dump(Formatter *f) const {
  f->dump_string("global_image_id", global_image_id);
  f->dump_int("state", state);
}

void MirrorImage::generate_test_instances(std::list<MirrorImage*> &o) {
  o.push_back(new MirrorImage());
  o.push_back(new MirrorImage("uuid-123", MIRROR_IMAGE_STATE_ENABLED));
  o.push_back(new MirrorImage("uuid-abc", MIRROR_IMAGE_STATE_DISABLING));
}

bool MirrorImage::operator==(const MirrorImage &rhs) const {
  return global_image_id == rhs.global_image_id && state == rhs.state;
}

std::ostream& operator<<(std::ostream& os, const MirrorImageState& mirror_state) {
  switch (mirror_state) {
  case MIRROR_IMAGE_STATE_DISABLING:
    os << "disabling";
    break;
  case MIRROR_IMAGE_STATE_ENABLED:
    os << "enabled";
    break;
  default:
    os << "unknown (" << static_cast<uint32_t>(mirror_state) << ")";
    break;
  }
  return os;
}

std::ostream& operator<<(std::ostream& os, const MirrorImage& mirror_image) {
  os << "["
     << "global_image_id=" << mirror_image.global_image_id << ", "
     << "state=" << mirror_image.state << "]";
  return os;
}

} // namespace rbd
} // namespace cls
