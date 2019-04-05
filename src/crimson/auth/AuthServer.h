// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include "auth/AuthAuthorizeHandler.h"
#include "crimson/net/Fwd.h"

namespace ceph::auth {

// TODO: revisit interfaces for non-dummy implementations
class AuthServer {
public:
  // TODO:
  // AuthRegistry auth_registry;

  AuthServer() {}
  virtual ~AuthServer() {}

  // Get authentication methods and connection modes for the given peer type
  virtual std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
  get_supported_auth_methods(
    int peer_type) {
    // std::vector<uint32_t> methods;
    // std::vector<uint32_t> modes;
    // auth_registry.get_supported_methods(peer_type, &methods, &modes);
    return {{CEPH_AUTH_NONE}, {CEPH_AUTH_NONE}};
  }

  // Get support connection modes for the given peer type and auth method
  virtual void get_supported_con_modes(
    int peer_type,
    uint32_t auth_method,
    std::vector<uint32_t> *modes) {
    // auth_registry.get_supported_modes(peer_type, auth_method, modes);
    *modes = { CEPH_CON_MODE_CRC };
  }

  // Get support connection modes for the given peer type and auth method
  virtual uint32_t pick_con_mode(
    int peer_type,
    uint32_t auth_method,
    const std::vector<uint32_t>& preferred_modes) {
    // return auth_registry.pick_mode(peer_type, auth_method, preferred_modes);
    ceph_assert(auth_method == CEPH_AUTH_NONE);
    ceph_assert(preferred_modes.size() &&
                preferred_modes[0] == CEPH_CON_MODE_CRC);
    return CEPH_CON_MODE_CRC;
  }

  // return an AuthAuthorizeHandler for the given peer type and auth method
  AuthAuthorizeHandler *get_auth_authorize_handler(
    int peer_type,
    int auth_method) {
    // return auth_registry.get_handler(peer_type, auth_method);
    return nullptr;
  }

  // Handle an authentication request on an incoming connection
  virtual int handle_auth_request(
    ceph::net::ConnectionRef conn,
    AuthConnectionMetaRef auth_meta,
    bool more,           //< true if this is not the first part of the handshake
    uint32_t auth_method,
    const bufferlist& bl,
    bufferlist *reply) = 0;
};

} // namespace ceph::auth
