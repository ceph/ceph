// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "types.h"

std::ostream& operator<<(std::ostream& lhs, const rbd::mirror::peer_t &peer)
{
  return lhs << "name: " << peer.cluster_name
	     << " uuid: " << peer.cluster_uuid
	     << " client: " << peer.client_name;
}
