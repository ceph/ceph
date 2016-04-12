// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "types.h"

std::ostream& operator<<(std::ostream& lhs, const rbd::mirror::peer_t &peer)
{
  return lhs << "uuid: " << peer.uuid
	     << " cluster: " << peer.cluster_name
	     << " client: " << peer.client_name;
}
