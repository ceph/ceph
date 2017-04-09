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

struct PeerImageId {
  std::string mirror_uuid;
  std::string image_id;

  inline bool operator<(const PeerImageId &rhs) const {
    return mirror_uuid < rhs.mirror_uuid;
  }

  inline bool operator==(const PeerImageId &rhs) const {
    return (mirror_uuid == rhs.mirror_uuid && image_id == rhs.image_id);
  }

  void encode(bufferlist& bl) const;
  void decode(bufferlist::iterator& it);
  void dump(Formatter *f) const;
};

WRITE_CLASS_ENCODER(PeerImageId);

typedef std::set<PeerImageId> PeerImageIds;

} // namespace instance_watcher
} // namespace mirror
} // namespace librbd

#endif // RBD_MIRROR_INSTANCE_WATCHER_TYPES_H
