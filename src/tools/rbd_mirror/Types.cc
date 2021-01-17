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
            << ", count=" << entity.count;
}

std::ostream& operator<<(std::ostream& os,
                         const LocalPoolMeta& local_pool_meta) {
  return os << "mirror_uuid=" << local_pool_meta.mirror_uuid;
}

std::ostream& operator<<(std::ostream& os,
                         const RemotePoolMeta& remote_pool_meta) {
  return os << "mirror_uuid=" << remote_pool_meta.mirror_uuid
            << ", mirror_peer_uuid=" << remote_pool_meta.mirror_peer_uuid;
}

std::ostream& operator<<(std::ostream& os, const PeerSpec &peer) {
  return os << "uuid: " << peer.uuid
            << " cluster: " << peer.cluster_name
            << " client: " << peer.client_name;
}

std::ostream& operator<<(std::ostream& os, const GroupCtx &group_ctx) {
  return os << "name: " << group_ctx.name
            << ", group_id: " << group_ctx.group_id
            << ", global_group_id: " << group_ctx.global_group_id;
}

} // namespace mirror
} // namespace rbd
