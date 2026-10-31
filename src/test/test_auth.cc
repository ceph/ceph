// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "include/types.h"
#include "include/stringify.h"
#include "auth/Auth.h"
#include "gtest/gtest.h"
#include "common/ceph_context.h"
#include "global/global_context.h"
#include "auth/AuthRegistry.h"
#include "auth/KeyRing.h"
#include "auth/RotatingKeyRing.h"
#include "auth/cephx/CephxClientHandler.h"
#include "auth/cephx/CephxProtocol.h"
#include "common/Clock.h"

#include <chrono>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

TEST(AuthRegistry, con_modes)
{
  auto cct = g_ceph_context;
  AuthRegistry reg(cct);
  std::vector<uint32_t> modes;

  const std::vector<uint32_t> crc_secure = { CEPH_CON_MODE_CRC,
					     CEPH_CON_MODE_SECURE };
  const std::vector<uint32_t> secure_crc = { CEPH_CON_MODE_SECURE,
					     CEPH_CON_MODE_CRC };
  const std::vector<uint32_t> secure = { CEPH_CON_MODE_SECURE };

  cct->_conf.set_val(
    "enable_experimental_unrecoverable_data_corrupting_features", "*");

  // baseline: everybody agrees
  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  cct->_conf.set_val("ms_cluster_mode", "crc secure");
  cct->_conf.set_val("ms_service_mode", "crc secure");
  cct->_conf.set_val("ms_client_mode", "crc secure");
  cct->_conf.set_val("ms_mon_cluster_mode", "crc secure");
  cct->_conf.set_val("ms_mon_service_mode", "crc secure");
  cct->_conf.set_val("ms_mon_client_mode", "crc secure");
  cct->_conf.apply_changes(NULL);

  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);
  ASSERT_EQ((uint32_t)CEPH_CON_MODE_CRC, reg.pick_mode(CEPH_ENTITY_TYPE_OSD,
						       CEPH_AUTH_CEPHX,
						       crc_secure));

  // what mons prefer secure, internal to mon cluster only
  cct->_conf.set_val("ms_mon_cluster_mode", "secure");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_OSD);

  /* mon/mgr are treated the same, and relevant config is ms_mon_cluster_mode */
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MON);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  // how all cluster -> mon connections secure?
  cct->_conf.set_val("ms_mon_service_mode", "secure");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_OSD);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MON);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MDS, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);


  // how about client -> mon connections?
  cct->_conf.set_val("ms_mon_client_mode", "secure");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  //  ms_mon)client_mode doesn't does't affect daemons, though...
  cct->_conf.set_val("ms_mon_service_mode", "crc secure");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MON);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MDS, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  // how about all internal cluster connection secure?
  cct->_conf.set_val("ms_cluster_mode", "secure");
  cct->_conf.set_val("ms_mon_service_mode", "secure");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_OSD);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_CLIENT, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MGR);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MDS, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_CLIENT, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MDS);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_CLIENT, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MON);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_CLIENT, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  // how about all connections to the cluster?
  cct->_conf.set_val("ms_service_mode", "secure");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MDS, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, crc_secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_OSD);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_CLIENT, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MGR);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_CLIENT, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MDS, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  cct->_set_module_type(CEPH_ENTITY_TYPE_MDS);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_CLIENT, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  // client forcing things?
  cct->_conf.set_val("ms_cluster_mode", "crc secure");
  cct->_conf.set_val("ms_service_mode", "crc secure");
  cct->_conf.set_val("ms_client_mode", "secure");
  cct->_conf.set_val("ms_mon_cluster_mode", "crc secure");
  cct->_conf.set_val("ms_mon_service_mode", "crc secure");
  cct->_conf.set_val("ms_mon_client_mode", "secure");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MDS, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure);

  // client *preferring* secure?
  cct->_conf.set_val("ms_cluster_mode", "crc secure");
  cct->_conf.set_val("ms_service_mode", "crc secure");
  cct->_conf.set_val("ms_client_mode", "secure crc");
  cct->_conf.set_val("ms_mon_cluster_mode", "crc secure");
  cct->_conf.set_val("ms_mon_service_mode", "crc secure");
  cct->_conf.set_val("ms_mon_client_mode", "secure crc");
  cct->_conf.apply_changes(NULL);

  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MON, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure_crc);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MGR, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure_crc);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_OSD, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure_crc);
  reg.get_supported_modes(CEPH_ENTITY_TYPE_MDS, CEPH_AUTH_CEPHX, &modes);
  ASSERT_EQ(modes, secure_crc);

  // back to normalish, for the benefit of the next test(s)
  cct->_set_module_type(CEPH_ENTITY_TYPE_CLIENT);  
}

namespace {

// AES-CBC block size, i.e. strlen(CEPH_AES_IV)
constexpr size_t aes_block_len = 16;

constexpr uint32_t svc = CEPH_ENTITY_TYPE_OSD;
constexpr uint64_t gid = 42;
// this length lands the garbled block on created and expires alone
constexpr char eid[] = "client-tamper";
constexpr int sealing_key_lifetime = 7200;

// where the ticket fields land in the plaintext encode_encrypt_enc_bl seals
struct Offsets {
  size_t auid;
  size_t stamps;
  size_t allow_all;
};

Offsets
plaintext_offsets()
{
  using ceph::encode;
  EntityName name;
  name.set(CEPH_ENTITY_TYPE_CLIENT, eid);
  ceph::bufferlist bl;
  __u8 v = 1;
  uint64_t u = 0;
  utime_t t;
  encode(v, bl); // encode_encrypt_enc_bl struct_v
  encode(u, bl); // AUTH_ENC_MAGIC
  encode(v, bl); // CephXServiceTicketInfo struct_v
  v = 2;
  encode(v, bl); // AuthTicket struct_v
  encode(name, bl);
  encode(u, bl); // global_id
  Offsets off;
  off.auid = bl.length();
  encode(u, bl); // auid
  off.stamps = bl.length();
  encode(t, bl); // created
  encode(t, bl); // expires
  v = 1;
  encode(v, bl); // AuthCapsInfo struct_v
  off.allow_all = bl.length();
  return off;
}

// mon side: mint a ticket sealed by the service's fixed secret
CephXTicketHandler
mint_fixed(
    CephContext* cct,
    KeyRing& keys,
    bool allow_all = false,
    const ceph::bufferlist* caps = nullptr,
    int ttl = 3600)
{
  using ceph::encode;
  CephXSessionAuthInfo info;
  info.service_id = svc;
  info.secret_id = (uint64_t)-1; // general secret, not rotating
  info.ticket.name.set(CEPH_ENTITY_TYPE_CLIENT, eid);
  info.ticket.global_id = gid;
  info.ticket.init_timestamps(ceph_clock_now(), ttl);
  info.ticket.caps.allow_all = allow_all;
  if (caps)
    info.ticket.caps.caps = *caps;
  else
    encode(std::string("allow rw pool=foo"), info.ticket.caps.caps);
  EXPECT_EQ(0, info.session_key.create(cct, CEPH_CRYPTO_AES));
  EXPECT_EQ(0, info.service_secret.create(cct, CEPH_CRYPTO_AES));

  EntityName service_name;
  service_name.set_type(svc);
  keys.add(service_name, info.service_secret);

  CephXTicketHandler h(cct, svc);
  EXPECT_TRUE(cephx_build_service_ticket_blob(cct, info, h.ticket));
  h.session_key = info.session_key;
  return h;
}

struct RotatingTicketParams {
  int created_off;
  int ttl;
  uint32_t created_nsec;
  uint32_t expires_nsec;
  uint32_t session_nsec;
};

// Use historical timestamps so the tests cannot depend on the verifier's clock.
CephXTicketHandler
mint_rotating(
    CephContext* cct,
    RotatingKeyRing& keys,
    RotatingTicketParams params)
{
  using ceph::encode;
  const utime_t base(1700000000, 0);
  ExpiringCryptoKey sealing;
  const ceph::bufferptr service_secret("fedcba9876543210", aes_block_len);
  EXPECT_EQ(0, sealing.key.set_secret(CEPH_CRYPTO_AES, service_secret, base));
  sealing.expiration = utime_t(base.sec() + sealing_key_lifetime, 0);

  RotatingSecrets secrets;
  secrets.add(sealing); // secret_id 1, the sealing secret
  keys.set_secrets(std::move(secrets));

  CephXSessionAuthInfo info;
  info.service_id = svc;
  info.secret_id = 1;
  info.ticket.name.set(CEPH_ENTITY_TYPE_CLIENT, eid);
  info.ticket.global_id = gid;
  info.ticket.created = utime_t(base.sec() + params.created_off, 0);
  info.ticket.expires = utime_t(info.ticket.created.sec() + params.ttl, 0);
  // nsec_ref() bypasses the utime_t constructor, which would normalize a
  // carry away and hide the out-of-range nanoseconds these tests set.
  info.ticket.created.nsec_ref() = params.created_nsec;
  info.ticket.expires.nsec_ref() = params.expires_nsec;
  encode(std::string("allow rw pool=foo"), info.ticket.caps.caps);
  const ceph::bufferptr session_secret(
      "0123456789abcdef", aes_block_len);
  utime_t session_created = base;
  session_created.nsec_ref() = params.session_nsec;
  EXPECT_EQ(
      0,
      info.session_key.set_secret(
          CEPH_CRYPTO_AES,
          session_secret,
          session_created));
  info.service_secret = sealing.key;

  CephXTicketHandler h(cct, svc);
  EXPECT_TRUE(cephx_build_service_ticket_blob(cct, info, h.ticket));
  h.session_key = info.session_key;
  return h;
}

bool
verify_authorizer(
    CephContext* cct,
    const KeyStore& keys,
    const CephXTicketHandler& handler)
{
  std::unique_ptr<CephXAuthorizer> a(handler.build_authorizer(gid));
  EXPECT_TRUE(a);
  if (!a)
    return false;
  auto p = a->bl.cbegin();
  CephXServiceTicketInfo ticket_info;
  ceph::bufferlist reply;
  return cephx_verify_authorizer(
      cct, keys, p, 0, ticket_info, nullptr, nullptr, &reply);
}

} // anonymous namespace

class CephxServiceTicket : public ::testing::Test {
protected:
  CephContext* cct = g_ceph_context;
  KeyRing keys;
};

class CephxRotatingServiceTicket : public ::testing::Test {
protected:
  CephContext* cct = g_ceph_context;
  KeyRing backing;
  RotatingKeyRing keys{cct, svc, &backing};

  bool verifies(RotatingTicketParams params) {
    return verify_authorizer(
        cct, keys, mint_rotating(cct, keys, std::move(params)));
  }
};

TEST_F(CephxServiceTicket, verify_good_ticket)
{
  ASSERT_TRUE(verify_authorizer(cct, keys, mint_fixed(cct, keys)));
}

TEST_F(CephxServiceTicket, accept_long_lifetime)
{
  ASSERT_TRUE(verify_authorizer(
      cct, keys, mint_fixed(cct, keys, false, nullptr, 30 * 24 * 3600)));
}

// a ticket carrying allow_all is tampered even though it decrypts cleanly
TEST_F(CephxServiceTicket, reject_allow_all)
{
  ASSERT_FALSE(
      verify_authorizer(cct, keys, mint_fixed(cct, keys, true)));
}

// a CBC bit-flip in block K sets a chosen byte of block K+1 while garbling
// only block K; flip allow_all on and check the ticket is still refused.
TEST_F(CephxServiceTicket, reject_bit_flipped_allow_all)
{
  CephXTicketHandler m = mint_fixed(cct, keys);
  const Offsets off = plaintext_offsets();

  // flip one block ahead of allow_all; the garbled block must start at created
  const size_t flip_off = off.allow_all - aes_block_len;
  ASSERT_LE(off.stamps, (flip_off / aes_block_len) * aes_block_len);
  ASSERT_LT(flip_off, m.ticket.blob.length());

  m.ticket.blob.c_str()[flip_off] ^= 0x01;

  ASSERT_FALSE(verify_authorizer(cct, keys, m));
}

// Both ticket timestamps are historical; verification uses the session key.
TEST_F(CephxRotatingServiceTicket, verify_good_rotating_ticket)
{
  ASSERT_TRUE(verifies({.created_off = 60, .ttl = 3600}));
}

TEST_F(CephxRotatingServiceTicket, accept_long_lifetime)
{
  // TTL changes and the verifier's keyring do not impose a lifetime limit.
  EXPECT_TRUE(verifies({.created_off = 0, .ttl = 30 * 24 * 3600}));
}

TEST_F(CephxRotatingServiceTicket, reject_reversed_timestamps)
{
  EXPECT_FALSE(verifies({.created_off = 0, .ttl = -1}));
}

TEST_F(CephxRotatingServiceTicket, enforce_creation_slop)
{
  EXPECT_TRUE(verifies({.created_off = -300, .ttl = 3600}));
  EXPECT_TRUE(verifies({.created_off = 300, .ttl = 3600}));
  EXPECT_FALSE(verifies({.created_off = -301, .ttl = 3600}));
  EXPECT_FALSE(verifies({.created_off = 301, .ttl = 3600}));
}

// a different uid means tampering. the encoder writes the constant whatever
// the ticket holds, so corrupt the plaintext and reseal
TEST_F(CephxServiceTicket, reject_bad_auid)
{
  CephXTicketHandler m = mint_fixed(cct, keys);

  EntityName service_name;
  service_name.set_type(svc);
  CryptoKey sk;
  ASSERT_TRUE(keys.get_secret(service_name, sk));

  ceph::bufferlist plain;
  std::string error;
  ASSERT_EQ(
      0, sk.decrypt_ext(
             cct, CEPHX_KEY_USAGE_TICKET_INFO, m.ticket.blob, plain, &error));
  const size_t auid = plaintext_offsets().auid;
  ASSERT_LT(auid, plain.length());
  plain.c_str()[auid] ^= 0x01;
  m.ticket.blob.clear();
  ASSERT_EQ(
      0, sk.encrypt_ext(
             cct, CEPHX_KEY_USAGE_TICKET_INFO, plain, m.ticket.blob, &error));

  ASSERT_FALSE(verify_authorizer(cct, keys, m));
}

TEST_F(CephxServiceTicket, reject_corrupted_timestamps)
{
  CephXTicketHandler m = mint_fixed(cct, keys);
  EntityName service_name;
  service_name.set_type(svc);
  CryptoKey sk;
  ASSERT_TRUE(keys.get_secret(service_name, sk));

  ceph::bufferlist plain;
  std::string error;
  ASSERT_EQ(0, sk.decrypt_ext(
      cct, CEPHX_KEY_USAGE_TICKET_INFO, m.ticket.blob, plain, &error));
  const size_t stamps = plaintext_offsets().stamps;
  ASSERT_EQ(0u, stamps % aes_block_len);
  ASSERT_LE(stamps + aes_block_len, plain.length());
  // Model the garbled timestamp block without changing caps or the session
  // key. Use valid nanoseconds and ordering to isolate the creation check.
  ceph::bufferlist times;
  encode(utime_t(1, 0), times);
  encode(utime_t(3601, 0), times);
  auto p = plain.begin();
  p.seek(stamps);
  p.copy_in(times.length(), times.c_str());
  m.ticket.blob.clear();
  ASSERT_EQ(0, sk.encrypt_ext(
      cct, CEPHX_KEY_USAGE_TICKET_INFO, plain, m.ticket.blob, &error));
  ASSERT_FALSE(verify_authorizer(cct, keys, m));
}

TEST_F(CephxRotatingServiceTicket, enforce_nsec_edges)
{
  EXPECT_TRUE(verifies(
      {.created_off = 60, .ttl = 3600, .created_nsec = 1000000000}));
  EXPECT_TRUE(verifies(
      {.created_off = 60, .ttl = 3600, .expires_nsec = 1000000000}));
  EXPECT_FALSE(verifies(
      {.created_off = 60, .ttl = 3600, .created_nsec = 1000000001}));
  EXPECT_FALSE(verifies(
      {.created_off = 60, .ttl = 3600, .expires_nsec = 1000000001}));
  EXPECT_TRUE(verifies(
      {.created_off = 60, .ttl = 3600, .session_nsec = 1000000000}));
  EXPECT_FALSE(verifies(
      {.created_off = 60, .ttl = 3600, .session_nsec = 0xffffffff}));
}

// shortening the caps length prefix truncates the string in place, dropping a
// clause off the grant; nothing after it moves, so it otherwise verifies
TEST_F(CephxServiceTicket, reject_truncated_caps)
{
  using ceph::encode;

  const std::string str = "allow rwx pool=foo";
  ceph::bufferlist caps;
  encode((uint32_t)9, caps); // length of "allow rwx", the unscoped prefix
  caps.append(str.data(), str.size());

  ASSERT_FALSE(
      verify_authorizer(cct, keys, mint_fixed(cct, keys, false, &caps)));
}

TEST_F(CephxServiceTicket, reject_undecodable_caps)
{
  using ceph::encode;

  ceph::bufferlist caps;
  encode((uint32_t)1, caps);

  ASSERT_FALSE(
      verify_authorizer(cct, keys, mint_fixed(cct, keys, false, &caps)));
}

namespace {

CephXSessionAuthInfo
session_info(CephContext* cct, uint32_t service_id, int validity)
{
  CephXSessionAuthInfo info;
  info.service_id = service_id;
  info.secret_id = 1;
  info.ticket.name.set(CEPH_ENTITY_TYPE_CLIENT, eid);
  info.ticket.global_id = gid;
  info.ticket.init_timestamps(ceph_clock_now(), validity);
  info.validity = utime_t(validity, 0);
  EXPECT_EQ(0, info.session_key.create(cct, CEPH_CRYPTO_AES));
  EXPECT_EQ(0, info.service_secret.create(cct, CEPH_CRYPTO_AES));
  return info;
}

// mon side: a service ticket reply sealed for the holder of 'principal'
void
build_reply(
    CephContext* cct,
    CryptoKey& principal,
    std::vector<CephXSessionAuthInfo> infos,
    ceph::bufferlist& bl)
{
  CryptoKey unused;
  EXPECT_TRUE(cephx_build_service_ticket_reply(
      cct, principal, std::move(infos), false, unused, bl));
}

} // anonymous namespace

// a later renew_after for the MGR ticket could leave it due on its own
TEST(CephxTicketManager, tickets_in_one_reply_come_due_together)
{
  CephContext* cct = g_ceph_context;
  CryptoKey principal;
  ASSERT_EQ(0, principal.create(cct, CEPH_CRYPTO_AES));
  ceph::bufferlist bl;
  build_reply(cct, principal,
	      {session_info(cct, CEPH_ENTITY_TYPE_MON, 3600),
	       session_info(cct, CEPH_ENTITY_TYPE_OSD, 3600),
	       session_info(cct, CEPH_ENTITY_TYPE_MGR, 3600)},
	      bl);

  CephXTicketManager tickets(cct);
  auto p = bl.cbegin();
  ASSERT_TRUE(tickets.verify_service_ticket_reply(principal, p));
  const utime_t mon = tickets.get_handler(CEPH_ENTITY_TYPE_MON).renew_after;
  EXPECT_EQ(mon, tickets.get_handler(CEPH_ENTITY_TYPE_OSD).renew_after);
  EXPECT_EQ(mon, tickets.get_handler(CEPH_ENTITY_TYPE_MGR).renew_after);
}

class CephxClientTickets : public ::testing::Test {
protected:
  CephContext* cct = g_ceph_context;
  KeyRing keyring;
  RotatingKeyRing rotating{cct, CEPH_ENTITY_TYPE_CLIENT, &keyring};
  std::unique_ptr<CephxClientHandler> client;
  CryptoKey auth_session_key;

  int respond(uint16_t type, CryptoKey& principal,
	      std::vector<CephXSessionAuthInfo> infos) {
    using ceph::encode;
    ceph::bufferlist bl;
    CephXResponseHeader header;
    header.request_type = type;
    header.status = 0;
    encode(header, bl);
    build_reply(cct, principal, std::move(infos), bl);
    auto p = bl.cbegin();
    return client->handle_response(0, p, nullptr, nullptr);
  }

  // authenticate, then get service tickets back with the given lifetimes
  void get_tickets(std::vector<std::pair<uint32_t, int>> services) {
    using ceph::encode;
    CryptoKey secret;
    ASSERT_EQ(0, secret.create(cct, CEPH_CRYPTO_AES));
    keyring.add(cct->_conf->name, secret);
    client = std::make_unique<CephxClientHandler>(cct, &rotating);
    client->set_want_keys(CEPH_ENTITY_TYPE_MON | CEPH_ENTITY_TYPE_OSD |
			  CEPH_ENTITY_TYPE_MGR);

    ceph::bufferlist bl;
    CephXServerChallenge challenge;
    challenge.server_challenge = 1;
    encode(challenge, bl);
    auto p = bl.cbegin();
    ASSERT_EQ(-EAGAIN, client->handle_response(0, p, nullptr, nullptr));

    CephXSessionAuthInfo auth = session_info(cct, CEPH_ENTITY_TYPE_AUTH, 3600);
    auth_session_key = auth.session_key;
    ASSERT_EQ(-EAGAIN, respond(CEPHX_GET_AUTH_SESSION_KEY, secret, {auth}));

    std::vector<CephXSessionAuthInfo> infos;
    for (auto& [service_id, validity] : services)
      infos.push_back(session_info(cct, service_id, validity));
    ASSERT_EQ(0, respond(CEPHX_GET_PRINCIPAL_SESSION_KEY, auth_session_key,
			 infos));
  }
};

// a held MGR ticket left unrenewed outlives its rotating secret
TEST_F(CephxClientTickets, renew_mgr_ticket_due_alone)
{
  // validity 2s: renew_after is 1.5s out (validity/4 before expiry)
  ASSERT_NO_FATAL_FAILURE(get_tickets({{CEPH_ENTITY_TYPE_MON, 3600},
				       {CEPH_ENTITY_TYPE_OSD, 3600},
				       {CEPH_ENTITY_TYPE_MGR, 2}}));
  ASSERT_FALSE(client->need_tickets());
  std::this_thread::sleep_for(std::chrono::milliseconds(1700));
  ASSERT_TRUE(client->need_tickets());

  // the renewal asks for the MGR ticket alone
  client->prepare_build_request();
  ceph::bufferlist req;
  ASSERT_EQ(0, client->build_request(req));
  auto p = req.cbegin();
  CephXRequestHeader header;
  header.decode(p);
  ASSERT_EQ(CEPHX_GET_PRINCIPAL_SESSION_KEY, header.request_type);
  p = req.cbegin();
  p += req.length() - 5; // ends with the CephXServiceTicketRequest
  CephXServiceTicketRequest ticket_req;
  ticket_req.decode(p);
  ASSERT_TRUE(p.end());
  ASSERT_EQ((uint32_t)CEPH_ENTITY_TYPE_MGR, ticket_req.keys);

  // and once it arrives, nothing is due
  ASSERT_EQ(0, respond(CEPHX_GET_PRINCIPAL_SESSION_KEY, auth_session_key,
		       {session_info(cct, CEPH_ENTITY_TYPE_MGR, 3600)}));
  ASSERT_FALSE(client->need_tickets());
}

// mons that never issue a MGR ticket must not be asked for one in a loop
TEST_F(CephxClientTickets, skip_mgr_ticket_never_issued)
{
  ASSERT_NO_FATAL_FAILURE(get_tickets(
      {{CEPH_ENTITY_TYPE_MON, 3600}, {CEPH_ENTITY_TYPE_OSD, 3600}}));
  ASSERT_FALSE(client->need_tickets());
}

// likewise for a MGR ticket issued without an expiry (a zero ttl)
TEST_F(CephxClientTickets, skip_mgr_ticket_without_expiry)
{
  ASSERT_NO_FATAL_FAILURE(get_tickets({{CEPH_ENTITY_TYPE_MON, 3600},
				       {CEPH_ENTITY_TYPE_OSD, 3600},
				       {CEPH_ENTITY_TYPE_MGR, 0}}));
  ASSERT_FALSE(client->need_tickets());
}
