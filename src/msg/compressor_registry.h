// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <vector>

#include "compressor/Compressor.h"
#include "common/ceph_mutex.h"
#include "common/ceph_context.h"
#include "common/config_cacher.h"

class CompressorRegistry : public md_config_obs_t {
public:
  CompressorRegistry(CephContext *cct);
  ~CompressorRegistry();

  void refresh_config() {
    std::scoped_lock l(lock);
    _refresh_config();
  }

  std::vector<std::string> get_tracked_keys() const noexcept override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed) override;

  TOPNSPC::Compressor::CompressionAlgorithm pick_method(uint32_t peer_type,
					       const std::vector<uint32_t>& preferred_methods);

  TOPNSPC::Compressor::CompressionMode get_mode(uint32_t peer_type, bool is_secure);

  const std::vector<uint32_t> get_methods(uint32_t peer_type) { 
    std::scoped_lock l(lock);
    switch (peer_type) {
      case CEPH_ENTITY_TYPE_OSD:
        return ms_osd_compression_methods;
      default:
        return {};
    }
   }

  uint64_t get_min_compression_size(uint32_t peer_type) const {
    std::scoped_lock l(lock);
    switch (peer_type) {
      case CEPH_ENTITY_TYPE_OSD:
        return ms_osd_compress_min_size;
      default:
        return 0;
    }
  }

  bool get_is_compress_secure() const { 
    std::scoped_lock l(lock);
    return ms_compress_secure; 
  }

private:
  CephContext *cct;
  mutable ceph::mutex lock = ceph::make_mutex("CompressorRegistry::lock");

  uint32_t ms_osd_compress_mode;
  bool ms_compress_secure;
  std::uint64_t ms_osd_compress_min_size;
  std::vector<uint32_t> ms_osd_compression_methods;

  void _refresh_config();
  std::vector<uint32_t> _parse_method_list(const std::string& s);
};
