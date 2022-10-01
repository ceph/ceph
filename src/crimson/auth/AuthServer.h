// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <cstdint>
#include <utility>
#include <vector>
#include "crimson/net/Fwd.h"

struct AuthAuthorizeHandler;

namespace crimson::auth {

class AuthServer {
public:
  virtual ~AuthServer() {}

  // Get authentication methods and connection modes for the given peer type
  virtual std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
  get_supported_auth_methods(int peer_type) = 0;
  // Get support connection modes for the given peer type and auth method
  virtual uint32_t pick_con_mode(
    int peer_type,
    uint32_t auth_method,
    const std::vector<uint32_t>& preferred_modes) = 0;
  // return an AuthAuthorizeHandler for the given peer type and auth method
  virtual AuthAuthorizeHandler* get_auth_authorize_handler(
    int peer_type,
    int auth_method) = 0;
  // Handle an authentication request on an incoming connection
  virtual int handle_auth_request(
    crimson::net::ConnectionRef conn,
    AuthConnectionMetaRef auth_meta,
    bool more,           //< true if this is not the first part of the handshake
    uint32_t auth_method,
    const bufferlist& bl,
    bufferlist *reply) = 0;
};

} // namespace crimson::auth
