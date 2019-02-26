// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "AuthClient.h"
#include "AuthServer.h"

class DummyAuthClientServer : public AuthClient,
			      public AuthServer {
public:
  DummyAuthClientServer(CephContext *cct) : AuthServer(cct) {}

  // client
  int get_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t *method,
    std::vector<uint32_t> *preferred_modes,
    bufferlist *out) override {
    *method = CEPH_AUTH_NONE;
    *preferred_modes = { CEPH_CON_MODE_CRC };
    return 0;
  }

  int handle_auth_reply_more(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    const bufferlist& bl,
    bufferlist *reply) override {
    ceph_abort();
  }

  int handle_auth_done(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint64_t global_id,
    uint32_t con_mode,
    const bufferlist& bl,
    CryptoKey *session_key,
    std::string *connection_secret) {
    return 0;
  }

  int handle_auth_bad_method(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    uint32_t old_auth_method,
    int result,
    const std::vector<uint32_t>& allowed_methods,
    const std::vector<uint32_t>& allowed_modes) override {
    ceph_abort();
  }

  // server
  int handle_auth_request(
    Connection *con,
    AuthConnectionMeta *auth_meta,
    bool more,
    uint32_t auth_method,
    const bufferlist& bl,
    bufferlist *reply) override {
    return 1;
  }
};
