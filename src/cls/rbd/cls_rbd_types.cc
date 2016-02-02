// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "cls/rbd/cls_rbd_types.h"
#include "common/Formatter.h"

namespace cls {
namespace rbd {

void MirrorPeer::encode(bufferlist &bl) const {
  ENCODE_START(1, 1, bl);
  ::encode(cluster_uuid, bl);
  ::encode(cluster_name, bl);
  ::encode(client_name, bl);
  ENCODE_FINISH(bl);
}

void MirrorPeer::decode(bufferlist::iterator &it) {
  DECODE_START(1, it);
  ::decode(cluster_uuid, it);
  ::decode(cluster_name, it);
  ::decode(client_name, it);
  DECODE_FINISH(it);
}

void MirrorPeer::dump(Formatter *f) const {
  f->dump_string("cluster_uuid", cluster_uuid);
  f->dump_string("cluster_name", cluster_name);
  f->dump_string("client_name", client_name);
}

void MirrorPeer::generate_test_instances(std::list<MirrorPeer*> &o) {
  o.push_back(new MirrorPeer());
  o.push_back(new MirrorPeer("uuid-123", "cluster name", "client name"));
}

bool MirrorPeer::operator==(const MirrorPeer &rhs) const {
  return (cluster_uuid == rhs.cluster_uuid &&
          cluster_name == rhs.cluster_name &&
          client_name == rhs.client_name);
}

std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer) {
  os << "["
     << "cluster_uuid=" << peer.cluster_uuid << ", "
     << "cluster_name=" << peer.cluster_name << ", "
     << "client_name=" << peer.client_name << "]";
  return os;
}

} // namespace rbd
} // namespace cls
