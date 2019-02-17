// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/shared_ptr.hh>

#include "include/types.h"

class OSDMap;

class OSDMapService {
public:
  virtual ~OSDMapService() = default;
  virtual seastar::future<seastar::lw_shared_ptr<OSDMap>>
  get_map(epoch_t e) = 0;
  /// get the latest map
  virtual seastar::lw_shared_ptr<OSDMap> get_map() const = 0;
  virtual void pre_publish_map(seastar::lw_shared_ptr<OSDMap>)  = 0;
  virtual seastar::lw_shared_ptr<OSDMap> get_nextmap_reserved() = 0;
  virtual void release_map(seastar::lw_shared_ptr<OSDMap> osdmap) = 0;
};
