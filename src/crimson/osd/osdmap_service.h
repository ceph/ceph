// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/types.h"
#include "osd/OSDMap.h"

class OSDMap;

class OSDMapService {
public:
  using cached_map_t = OSDMapRef;
  using local_cached_map_t = LocalOSDMapRef;
  enum class encoded_osdmap_type_t {
    FULLMAP,
    INCMAP
  };
  using bls_pair = std::pair<encoded_osdmap_type_t, bufferlist>;
  using bls_map_pair_t = std::pair<epoch_t, bls_pair>;
  using bls_map_t = std::map<epoch_t, bls_pair>;

  virtual ~OSDMapService() = default;
  virtual seastar::future<cached_map_t> get_map(epoch_t e) = 0;
  /// get the latest map
  virtual cached_map_t get_map() const = 0;
  virtual epoch_t get_up_epoch() const = 0;
};
