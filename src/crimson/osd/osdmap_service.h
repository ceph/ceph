// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/smart_ptr/local_shared_ptr.hpp>

#include "include/types.h"

class OSDMap;

class OSDMapService {
public:
  using cached_map_t = boost::local_shared_ptr<OSDMap>;
  virtual ~OSDMapService() = default;
  virtual seastar::future<cached_map_t> get_map(epoch_t e) = 0;
  /// get the latest map
  virtual cached_map_t get_map() const = 0;
};
