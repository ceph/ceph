// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "CompressorRegistry.h"
#include "Compressor.h"
#include "include/common_fwd.h"
#include "auth/Auth.h"

#include <vector>

class Connection;

class CompressorServer {
public:
  CompressorServer(CephContext *cct) : comp_registry(cct) { 
    comp_registry.refresh_config();
  } 
    
  virtual ~CompressorServer() {}

  void handle_comp_request(Connection *con, CompConnectionMeta *comp_meta, 
    AuthConnectionMeta *auth_meta, bool peer_is_compress, std::vector<uint32_t>& peer_methods) {
    uint32_t peer_type = con->get_peer_type();
    uint32_t mode = comp_registry.get_mode(peer_type, auth_meta->is_mode_secure());

    uint32_t method = Compressor::COMP_ALG_NONE;
    if ((mode != Compressor::COMP_NONE) && peer_is_compress) {
      method = comp_registry.pick_method(peer_type, mode, peer_methods);
      comp_meta->con_mode = static_cast<Compressor::CompressionMode>(mode);
    } else {
      comp_meta->con_mode = Compressor::COMP_NONE;
    }

    comp_meta->con_method = static_cast<Compressor::CompressionAlgorithm>(method);
  }

private:
  CompressorRegistry comp_registry;
};
