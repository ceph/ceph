// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/acked_peers.h"

namespace crimson::osd {

std::ostream& operator<<(std::ostream &out, const  peer_shard_t &pshard) {
  return out << " {shard=" << pshard.shard << " lcod="
	     << pshard.last_complete_ondisk << "} ";
}

}
