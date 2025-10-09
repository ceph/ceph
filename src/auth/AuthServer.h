// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "AuthRegistry.h"
#include "include/common_fwd.h"

#include <vector>

class Connection;

class AuthServer {
public:
  AuthRegistry auth_registry;

  AuthServer(CephContext *cct) : auth_registry(cct) {}
  virtual ~AuthServer() {}

  /// Get authentication methods for the given peer type
  virtual void get_supported_auth_methods(
    int peer_type,
    std::vector<uint32_t> *methods) {
    auth_registry.get_supported_methods(peer_type, methods, nullptr);
  }

  /// Get supported connection modes for the given peer type and auth method
  virtual void get_supported_con_modes(
    int peer_type,
    uint32_t auth_method,
    std::vector<uint32_t> *modes) {
    auth_registry.get_supported_modes(peer_type, auth_method, modes);
  }

  /// Choose a connection mode for the given peer type and auth method
  virtual uint32_t pick_con_mode(
    int peer_type,
    uint32_t auth_method,
    const std::vector<uint32_t>& preferred_modes) {
    return auth_registry.pick_mode(peer_type, auth_method, preferred_modes);
  }

  /// return an AuthAuthorizeHandler for the given peer type and auth method
  AuthAuthorizeHandler *get_auth_authorize_handler(
    int peer_type,
    int auth_method) {
    return auth_registry.get_handler(peer_type, auth_method);
  }

  /// Handle an authentication request on an incoming connection
  virtual int handle_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    bool more,           ///< true if this is not the first part of the handshake
    uint32_t auth_method,
    const ceph::buffer::list& bl,
    ceph::buffer::list *reply) = 0;
};
