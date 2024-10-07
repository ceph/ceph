// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd_mirror/Types.h"

namespace rbd {
namespace mirror {

std::ostream &operator<<(std::ostream &os, const ImageId &image_id) {
  return os << "global id=" << image_id.global_id << ", "
            << "id=" << image_id.id;
}

std::ostream& operator<<(std::ostream& os,
                         const LocalPoolMeta& local_pool_meta) {
  return os << "mirror_uuid=" << local_pool_meta.mirror_uuid;
}

std::ostream& operator<<(std::ostream& os,
                         const RemotePoolMeta& remote_pool_meta) {
  return os << "mirror_uuid=" << remote_pool_meta.mirror_uuid << ", "
                "mirror_peer_uuid=" << remote_pool_meta.mirror_peer_uuid;
}

std::ostream& operator<<(std::ostream& os, const PeerSpec &peer) {
  return os << "uuid: " << peer.uuid
	     << " cluster: " << peer.cluster_name
	     << " client: " << peer.client_name;
}

} // namespace mirror
} // namespace rbd
