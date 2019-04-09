// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include "common/ceph_context.h"
#include "auth/AuthAuthorizeHandler.h"
#include "auth/AuthRegistry.h"
#include "crimson/net/Fwd.h"

namespace ceph::auth {

// TODO: revisit interfaces for non-dummy implementations
class AuthServer {
public:
  // TODO:
  AuthServer()
    : auth_registry{&cct}
  {}
  virtual ~AuthServer() {}

  // Get authentication methods and connection modes for the given peer type
  virtual std::pair<std::vector<uint32_t>, std::vector<uint32_t>>
  get_supported_auth_methods(int peer_type) {
    std::vector<uint32_t> methods;
    std::vector<uint32_t> modes;
    auth_registry.get_supported_methods(peer_type, &methods, &modes);
    return {methods, modes};
  }
  // Get support connection modes for the given peer type and auth method
  virtual uint32_t pick_con_mode(
    int peer_type,
    uint32_t auth_method,
    const std::vector<uint32_t>& preferred_modes) {
    return auth_registry.pick_mode(peer_type, auth_method, preferred_modes);
  }
  // return an AuthAuthorizeHandler for the given peer type and auth method
  AuthAuthorizeHandler *get_auth_authorize_handler(
    int peer_type,
    int auth_method) {
    return auth_registry.get_handler(peer_type, auth_method);
  }
  // Handle an authentication request on an incoming connection
  virtual int handle_auth_request(
    ceph::net::ConnectionRef conn,
    AuthConnectionMetaRef auth_meta,
    bool more,           //< true if this is not the first part of the handshake
    uint32_t auth_method,
    const bufferlist& bl,
    bufferlist *reply) = 0;

private:
  CephContext cct; // for auth_registry
  AuthRegistry auth_registry;
};

} // namespace ceph::auth
