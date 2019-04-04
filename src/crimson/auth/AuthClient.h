// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>
#include "crimson/net/Fwd.h"

class CryptoKey;

namespace ceph::auth {

// TODO: revisit interfaces for non-dummy implementations
class AuthClient {
public:
  virtual ~AuthClient() {}

  // Build an authentication request to begin the handshake
  virtual int get_auth_request(
    ceph::net::ConnectionRef conn,
    AuthConnectionMetaRef auth_meta,
    uint32_t *method,
    std::vector<uint32_t> *preferred_modes,
    bufferlist *out) = 0;

  // Handle server's request to continue the handshake
  virtual int handle_auth_reply_more(
    ceph::net::ConnectionRef conn,
    AuthConnectionMetaRef auth_meta,
    const bufferlist& bl,
    bufferlist *reply) = 0;

  // Handle server's indication that authentication succeeded
  virtual int handle_auth_done(
    ceph::net::ConnectionRef conn,
    AuthConnectionMetaRef auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const bufferlist& bl,
    CryptoKey *session_key,
    std::string *connection_secret) = 0;

  // Handle server's indication that the previous auth attempt failed
  virtual int handle_auth_bad_method(
    ceph::net::ConnectionRef conn,
    AuthConnectionMetaRef auth_meta,
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes) = 0;
};

} // namespace ceph::auth
