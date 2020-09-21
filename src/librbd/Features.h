// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <string>
#include <ostream>

namespace librbd {

  std::string rbd_features_to_string(uint64_t features,
				     std::ostream *err);
  uint64_t rbd_features_from_string(const std::string& value,
				    std::ostream *err);

} // librbd
