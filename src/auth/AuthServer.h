// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "AuthRegistry.h"

#include <vector>

class CephContext;
class Connection;

class AuthServer {
public:
  AuthRegistry auth_registry;

  AuthServer(CephContext *cct) : auth_registry(cct) {}
  virtual ~AuthServer() {}

  /// Get authentication methods and connection modes for the given peer type
  virtual void get_supported_auth_methods(
    int peer_type,
    std::vector<uint32_t> *methods,
    std::vector<uint32_t> *modes = nullptr) {
    auth_registry.get_supported_methods(peer_type, methods, modes);
  }

  /// Get support connection modes for the given peer type and auth method
  virtual void get_supported_con_modes(
    int peer_type,
    uint32_t auth_method,
    std::vector<uint32_t> *modes) {
    auth_registry.get_supported_modes(peer_type, auth_method, modes);
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
    const bufferlist& bl,
    bufferlist *reply) = 0;
};
