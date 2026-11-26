// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <set>
#include <string>

#include "auth/cephx/CephxKeyServer.h"
#include "auth/cephx/CephxProtocol.h"
#include "auth/cephx/CephxServiceHandler.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

namespace {

class TestKeyServer : public KeyServer {
public:
  std::set<int> allowed{CEPH_CRYPTO_AES256KRB5};

  explicit TestKeyServer(CephContext* cct) : KeyServer(cct, nullptr) {}

  bool is_cipher_allowed(int cipher) const override {
    return allowed.contains(cipher);
  }
};

struct LoginResult {
  int ret;
  CryptoKey session_key;
};

class CephxPendingKey : public ::testing::Test {
protected:
  CephContext* cct = g_ceph_context;
  TestKeyServer ks{cct};
  EntityName name;

  void SetUp() override {
    ASSERT_TRUE(name.from_str("client.foo"));
    // a good login needs an auth service secret to build its ticket
    KeyServerData::Incremental inc;
    inc.op = KeyServerData::AUTH_INC_SET_ROTATING;
    ASSERT_TRUE(ks.prepare_rotating_update(inc.rotating_bl, false));
    ks.apply_data_incremental(inc);
  }

  CryptoKey make_key(int type) {
    CryptoKey k;
    EXPECT_EQ(0, k.create(cct, type));
    return k;
  }

  void add_entity(const CryptoKey& key, const CryptoKey& pending_key) {
    EntityAuth auth;
    auth.key = key;
    auth.pending_key = pending_key;
    encode(std::string("allow r"), auth.caps["mon"]);
    ks.add_auth(name, auth);
  }

  // run the cephx handshake for name, proving possession of client_key
  LoginResult login(CryptoKey client_key) {
    CephxServiceHandler handler(cct, &ks);
    ceph::bufferlist challenge_bl;
    AuthCapsInfo caps;
    EXPECT_EQ(0, handler.start_session(name, 1, true, &challenge_bl, &caps));
    CephXServerChallenge challenge;
    auto p = challenge_bl.cbegin();
    decode(challenge, p);

    CephXAuthenticate req;
    req.client_challenge = 0x1234;
    std::string error;
    cephx_calc_client_server_challenge(cct, client_key,
				       challenge.server_challenge,
				       req.client_challenge, &req.key, error);
    EXPECT_TRUE(error.empty()) << error;

    ceph::bufferlist in;
    CephXRequestHeader header{CEPHX_GET_AUTH_SESSION_KEY};
    encode(header, in);
    encode(req, in);
    auto q = in.cbegin();
    ceph::bufferlist out;
    LoginResult r;
    std::string connection_secret;
    r.ret = handler.handle_request(q, 0, &out, &caps, &r.session_key,
				   &connection_secret);
    return r;
  }
};

} // anonymous namespace

// handle_request() returns 1 when the client proved its key
constexpr int login_ok = 1;

// a failed login must not retire the active key a client may still depend on
TEST_F(CephxPendingKey, wrong_key_does_not_commit_pending)
{
  add_entity(make_key(CEPH_CRYPTO_AES256KRB5), make_key(CEPH_CRYPTO_AES256KRB5));
  EXPECT_EQ(-EACCES, login(make_key(CEPH_CRYPTO_AES256KRB5)).ret);
  EXPECT_TRUE(ks.get_used_pending_keys().empty());
}

// a client can move to an allowed cipher after the old one was disabled
TEST_F(CephxPendingKey, pending_key_with_allowed_cipher)
{
  const auto key = make_key(CEPH_CRYPTO_AES);
  const auto pending = make_key(CEPH_CRYPTO_AES256KRB5);
  add_entity(key, pending);
  EXPECT_EQ(-EACCES, login(key).ret);
  auto r = login(pending);
  EXPECT_EQ(login_ok, r.ret);
  EXPECT_EQ(CEPH_CRYPTO_AES256KRB5, (int)r.session_key.get_type());
  EXPECT_EQ(1u, ks.get_used_pending_keys().size());
}

TEST_F(CephxPendingKey, pending_key_with_disallowed_cipher)
{
  const auto pending = make_key(CEPH_CRYPTO_AES);
  add_entity(make_key(CEPH_CRYPTO_AES256KRB5), pending);
  EXPECT_EQ(-EACCES, login(pending).ret);
  EXPECT_TRUE(ks.get_used_pending_keys().empty());
}
