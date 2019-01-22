// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <vector>

class EntityName;
class CryptoKey;

class AuthClient {
public:
  virtual ~AuthClient() {}

  virtual int get_auth_request(
    Connection *con,
    uint32_t *method,
    std::vector<uint32_t> *preferred_modes,
    bufferlist *out) = 0;
  virtual int handle_auth_reply_more(
    Connection *con,
    const bufferlist& bl,
    bufferlist *reply) = 0;
  virtual int handle_auth_done(
    Connection *con,
    uint64_t global_id,
    uint32_t con_mode,
    const bufferlist& bl,
    CryptoKey *session_key,
    CryptoKey *connection_key) = 0;
  virtual int handle_auth_bad_method(
    Connection *con,
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes) = 0;
};
