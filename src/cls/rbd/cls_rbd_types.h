// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_CLS_RBD_TYPES_H
#define CEPH_CLS_RBD_TYPES_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include <iosfwd>
#include <string>

namespace ceph { class Formatter; }

namespace cls {
namespace rbd {

struct MirrorPeer {
  MirrorPeer() {
  }
  MirrorPeer(const std::string &cluster_uuid, const std::string &cluster_name,
             const std::string &client_name)
    : cluster_uuid(cluster_uuid), cluster_name(cluster_name),
      client_name(client_name) {
  }

  std::string cluster_uuid;
  std::string cluster_name;
  std::string client_name;

  void encode(bufferlist &bl) const;
  void decode(bufferlist::iterator &it);
  void dump(Formatter *f) const;

  static void generate_test_instances(std::list<MirrorPeer*> &o);

  bool operator==(const MirrorPeer &rhs) const;
};

std::ostream& operator<<(std::ostream& os, const MirrorPeer& peer);

WRITE_CLASS_ENCODER(MirrorPeer);

} // namespace rbd
} // namespace cls

using cls::rbd::encode;
using cls::rbd::decode;

#endif // CEPH_CLS_RBD_TYPES_H
