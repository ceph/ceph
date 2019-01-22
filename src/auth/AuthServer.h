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

  virtual void get_supported_auth_methods(
    int peer_type,
    std::vector<uint32_t> *methods) {
    auth_registry.get_supported_methods(peer_type, methods);
  }

  AuthAuthorizeHandler *get_auth_authorize_handler(
    int peer_type,
    int auth_method) {
    return auth_registry.get_handler(peer_type, auth_method);
  }

  virtual int handle_auth_request(
    Connection *con,
    bool more,
    uint32_t auth_method,
    const bufferlist& bl,
    bufferlist *reply) = 0;
};
