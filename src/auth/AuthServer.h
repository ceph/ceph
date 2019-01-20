// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "AuthAuthorizeHandler.h"

#include <vector>

class CephContext;
class Connection;

class AuthServer {
private:
  std::unique_ptr<AuthAuthorizeHandlerRegistry> auth_ah_service_registry;
  std::unique_ptr<AuthAuthorizeHandlerRegistry> auth_ah_cluster_registry;
public:
  AuthServer(CephContext *cct);
  virtual ~AuthServer() {}

  AuthAuthorizeHandler *get_auth_authorize_handler(
    int peer_type,
    int auth_method);

  virtual void get_supported_auth_methods(
    int peer_type,
    std::vector<uint32_t> *methods);

  virtual int handle_auth_request(
    Connection *con,
    bool more,
    uint32_t auth_method,
    const bufferlist& bl,
    bufferlist *reply) = 0;
};
