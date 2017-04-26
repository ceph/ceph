// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "types.h"

namespace rbd {
namespace mirror {

std::ostream &operator<<(std::ostream &os, const ImageId &image_id) {
  return os << "global id=" << image_id.global_id << ", "
            << "id=" << image_id.id;
}

std::ostream& operator<<(std::ostream& lhs, const peer_t &peer) {
  return lhs << "uuid: " << peer.uuid
	     << " cluster: " << peer.cluster_name
	     << " client: " << peer.client_name;
}

} // namespace mirror
} // namespace rbd
