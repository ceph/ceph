// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "tools/rbd_mirror/Types.h"

namespace rbd {
namespace mirror {

std::ostream &operator<<(std::ostream &os, const MirrorEntityType &type) {
  switch (type) {
  case MIRROR_ENTITY_TYPE_IMAGE:
    os << "Image";
    break;
  case MIRROR_ENTITY_TYPE_GROUP:
    os << "Group";
    break;
  default:
    os << "Unknown (" << static_cast<uint32_t>(type) << ")";
    break;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os, const MirrorEntity &entity) {
  return os << "type=" << entity.type << ", global_id=" << entity.global_id
            << ", size=" << entity.size;
}

std::ostream& operator<<(std::ostream& lhs,
                         const LocalPoolMeta& rhs) {
  return lhs << "mirror_uuid=" << rhs.mirror_uuid;
}

std::ostream& operator<<(std::ostream& lhs,
                         const RemotePoolMeta& rhs) {
  return lhs << "mirror_uuid=" << rhs.mirror_uuid << ", "
                "mirror_peer_uuid=" << rhs.mirror_peer_uuid;
}

std::ostream& operator<<(std::ostream& lhs, const PeerSpec &peer) {
  return lhs << "uuid: " << peer.uuid
	     << " cluster: " << peer.cluster_name
	     << " client: " << peer.client_name;
}

std::ostream& operator<<(std::ostream& lhs, const GroupCtx &group_ctx) {
  return lhs << "name: " << group_ctx.name
	     << ", group_id: " << group_ctx.group_id
	     << ", global_group_id: " << group_ctx.global_group_id;
}

} // namespace mirror
} // namespace rbd
