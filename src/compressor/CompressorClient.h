// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <vector>
#include "include/buffer_fwd.h"
#include "CompressorRegistry.h"
#include "auth/Auth.h"

class CompConnectionMeta;
class Connection;

class CompressorClient {
  CompressorRegistry comp_registry;
public:
  CompressorClient(CephContext *cct) : comp_registry(cct) {
    comp_registry.refresh_config();
  } 
  virtual ~CompressorClient() {}

  /// Build a compression request to begin the handshake
  void get_comp_request(Connection *con, CompConnectionMeta *comp_meta,
    AuthConnectionMeta *auth_meta, std::vector<uint32_t> *preferred_methods) {
    entity_type_t peer_type = con->get_peer_type();
    comp_meta->con_mode = static_cast<Compressor::CompressionMode>(comp_registry.get_mode(peer_type, auth_meta->is_mode_secure()));
    *preferred_methods = comp_registry.get_methods(peer_type);
  }

  void handle_comp_done(CompConnectionMeta *comp_meta, bool is_compress, uint32_t method) {
    comp_meta->con_method = static_cast<Compressor::CompressionAlgorithm>(method);
    if (comp_meta->is_compress() != is_compress) {
      comp_meta->con_mode = Compressor::COMP_NONE;
    }
  }

  uint64_t get_min_compress_size(uint32_t peer_type) const {
    return comp_registry.get_min_compression_size(peer_type);
  }
};
