// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "crimson/os/seastore/segment_manager/ephemeral.h"

namespace crimson::os::seastore::segment_manager {

SegmentManagerRef create_ephemeral(ephemeral_config_t config) {
  return SegmentManagerRef{new EphemeralSegmentManager(config)};
}

std::ostream &operator<<(std::ostream &lhs, const ephemeral_config_t &c) {
  return lhs << "ephemeral_config_t(size=" << c.size << ", block_size=" << c.block_size
	     << ", segment_size=" << c.segment_size << ")";
}

}
