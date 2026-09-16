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
#include "auth/cephx/CephxProtocol.h"
#include "common/Clock.h"

#include <sstream>
#include <string>
#include <string_view>

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
static constexpr size_t aes_block_len = 16;

// mon side: mint a service ticket and register its service secret in *keys.
void build_service_ticket(CephContext *cct,
              uint32_t service_id,
              uint64_t global_id,
              std::string_view entity_id,
              bool allow_all,
              utime_t created,
              double ttl,
              KeyRing *keys,
              CephXTicketBlob *blob,
              CryptoKey *session_key,
              const ceph::bufferlist *caps = nullptr)
{
  using ceph::encode;
  CephXSessionAuthInfo info;
  info.service_id = service_id;
  // -1 makes the verifier ask for the service's general secret via
  // KeyStore::get_secret() rather than for a rotating one
  info.secret_id = (uint64_t)-1;
  info.ticket.name.set(CEPH_ENTITY_TYPE_CLIENT, entity_id);
  info.ticket.global_id = global_id;
  info.ticket.init_timestamps(created, ttl);
  info.ticket.caps.allow_all = allow_all;
  if (caps)
    info.ticket.caps.caps = *caps;
  else
    encode(std::string("allow rw pool=foo"), info.ticket.caps.caps);
  ASSERT_EQ(0, info.session_key.create(cct, CEPH_CRYPTO_AES));
  ASSERT_EQ(0, info.service_secret.create(cct, CEPH_CRYPTO_AES));

  EntityName service_name;
  service_name.set_type(service_id);
  keys->get_keys()[service_name].key = info.service_secret;

  ASSERT_TRUE(cephx_build_service_ticket_blob(cct, info, *blob));
  *session_key = info.session_key;
}

// client side: wrap a ticket blob in an authorizer.
void build_authorizer(CephContext *cct,
              uint32_t service_id,
              uint64_t global_id,
              const CephXTicketBlob& blob,
              const CryptoKey& session_key,
              ceph::bufferlist *bl)
{
  using ceph::encode;
  __u8 authorizer_v = 1;  // AUTH_MODE_AUTHORIZER
  encode(authorizer_v, *bl);
  encode(global_id, *bl);
  encode(service_id, *bl);
  encode(blob, *bl);

  CephXAuthorize msg;
  msg.nonce = 0x1234;
  std::string error;
  ASSERT_EQ(0, encode_encrypt(cct, msg, session_key, *bl, error));
  ASSERT_TRUE(error.empty());
}

bool verify_authorizer(CephContext *cct,
               const KeyRing& keys,
               const ceph::bufferlist& authorizer)
{
  auto p = authorizer.cbegin();
  CephXServiceTicketInfo ticket_info;
  ceph::bufferlist reply;
  return cephx_verify_authorizer(cct, keys, p, 0, ticket_info,
                 nullptr, nullptr, &reply);
}

} // anonymous namespace

TEST(CephxServiceTicket, verify_good_ticket)
{
  auto cct = g_ceph_context;
  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, 42, "esc", false,
               ceph_clock_now(), 3600, &keys, &blob, &session_key);

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob, session_key,
           &authorizer);
  ASSERT_TRUE(verify_authorizer(cct, keys, authorizer));
}

// the mon never sets allow_all when it builds a service ticket, so a ticket
// bearing it has been tampered with, no matter that it decrypts cleanly.
TEST(CephxServiceTicket, reject_allow_all)
{
  auto cct = g_ceph_context;
  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, 42, "esc", true,
               ceph_clock_now(), 3600, &keys, &blob, &session_key);

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob, session_key,
           &authorizer);
  ASSERT_FALSE(verify_authorizer(cct, keys, authorizer));
}

// a CBC bit-flip in block K sets a chosen byte of block K+1 while garbling
// only block K; flip allow_all on and check the ticket is still refused.
TEST(CephxServiceTicket, reject_bit_flipped_allow_all)
{
  using ceph::encode;
  auto cct = g_ceph_context;
  const uint64_t global_id = 42;
  EntityName name;
  name.set(CEPH_ENTITY_TYPE_CLIENT, "esc");

  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, global_id, name.get_id(),
               false, ceph_clock_now(), 3600, &keys, &blob, &session_key);

  // locate allow_all in the plaintext by encoding everything ahead of it
  ceph::bufferlist prefix;
  __u8 v = 1;
  uint64_t u = 0;
  utime_t t;
  encode(v, prefix);        // encode_encrypt_enc_bl struct_v
  encode(u, prefix);        // AUTH_ENC_MAGIC
  encode(v, prefix);        // CephXServiceTicketInfo struct_v
  v = 2;
  encode(v, prefix);        // AuthTicket struct_v
  encode(name, prefix);
  const size_t global_id_off = prefix.length();
  encode(u, prefix);        // global_id
  encode(u, prefix);        // auid
  encode(t, prefix);        // created
  encode(t, prefix);        // expires
  v = 1;
  encode(v, prefix);        // AuthCapsInfo struct_v
  const size_t allow_all_off = prefix.length();

  // flip a byte one block ahead of allow_all; the garbled block must not
  // overlap global_id, which the verifier checks.
  const size_t flip_off = allow_all_off - aes_block_len;
  ASSERT_LE(global_id_off + sizeof(uint64_t),
        (flip_off / aes_block_len) * aes_block_len);
  ASSERT_LT(flip_off, blob.blob.length());

  ceph::bufferlist ct = blob.blob;
  std::string bytes(ct.c_str(), ct.length());
  bytes[flip_off] ^= 0x01;
  CephXTicketBlob tampered = blob;
  tampered.blob.clear();
  tampered.blob.append(bytes.data(), bytes.size());

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, global_id, tampered,
           session_key, &authorizer);
  ASSERT_FALSE(verify_authorizer(cct, keys, authorizer));
}

// a ticket claiming to live far longer than auth_service_ticket_ttl is forged.
TEST(CephxServiceTicket, reject_long_ttl)
{
  auto cct = g_ceph_context;
  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, 42, "esc", false,
               ceph_clock_now(), 24 * 3600, &keys, &blob, &session_key);

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob, session_key,
           &authorizer);
  ASSERT_FALSE(verify_authorizer(cct, keys, authorizer));
}

// a ticket right at the slack ceiling is honest and must still verify.
TEST(CephxServiceTicket, accept_max_ttl)
{
  auto cct = g_ceph_context;
  const double ttl =
      2 * cct->_conf.get_val<double>("auth_service_ticket_ttl") + 300;
  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, 42, "esc", false,
               ceph_clock_now(), ttl, &keys, &blob, &session_key);

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob, session_key,
           &authorizer);
  ASSERT_TRUE(verify_authorizer(cct, keys, authorizer));
}

// timestamps far from the verifier's clock are forged, even when the interval
// between them looks legitimate.
TEST(CephxServiceTicket, reject_stale_ticket)
{
  auto cct = g_ceph_context;
  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, 42, "esc", false,
               ceph_clock_now() - utime_t(100000, 0), 3600,
               &keys, &blob, &session_key);

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob, session_key,
           &authorizer);
  ASSERT_FALSE(verify_authorizer(cct, keys, authorizer));
}

// the key server writes the same uid into every ticket, so a different one
// means the block carrying it was scrambled. sealed by hand because the
// encoder writes the constant, which is exactly why it makes a good check.
TEST(CephxServiceTicket, reject_bad_auid)
{
  using ceph::encode;
  auto cct = g_ceph_context;
  const std::string id = "esc";

  CephXServiceTicketInfo ticket_info;
  ticket_info.ticket.name.set(CEPH_ENTITY_TYPE_CLIENT, id);
  ticket_info.ticket.global_id = 42;
  ticket_info.ticket.init_timestamps(ceph_clock_now(), 3600);
  encode(std::string("allow rw pool=foo"), ticket_info.ticket.caps.caps);
  ASSERT_EQ(0, ticket_info.session_key.create(cct, CEPH_CRYPTO_AES));

  CryptoKey service_secret;
  ASSERT_EQ(0, service_secret.create(cct, CEPH_CRYPTO_AES));
  KeyRing keys;
  EntityName service_name;
  service_name.set_type(CEPH_ENTITY_TYPE_OSD);
  keys.get_keys()[service_name].key = service_secret;

  ceph::bufferlist plain;
  __u8 struct_v = 1;
  uint64_t magic = AUTH_ENC_MAGIC;
  encode(struct_v, plain);
  encode(magic, plain);
  encode(ticket_info, plain);
  plain.c_str()[27 + id.size()] ^= 0xff;

  CephXTicketBlob blob;
  blob.secret_id = (uint64_t)-1;
  std::string error;
  ASSERT_EQ(0, service_secret.encrypt(cct, plain, blob.blob, &error));

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob,
           ticket_info.session_key, &authorizer);
  ASSERT_FALSE(verify_authorizer(cct, keys, authorizer));
}

// a minted timestamp carries at most a full second of nanoseconds, since
// utime_t normalizes anything past that into the seconds field. a larger one
// is a scrambled block reading as a near enough pair of times.
TEST(CephxServiceTicket, reject_out_of_range_nsec)
{
  auto cct = g_ceph_context;
  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  utime_t created = ceph_clock_now();
  created.nsec_ref() = 2000000000u;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, 42, "esc", false,
               created, 3600, &keys, &blob, &session_key);

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob, session_key,
           &authorizer);
  ASSERT_FALSE(verify_authorizer(cct, keys, authorizer));
}

// the caps string carries its own length inside the bufferlist AuthCapsInfo
// wraps it in, and shortening that one truncates the string where it sits,
// dropping the clause that scopes the grant. nothing after the caps moves, so
// the session key survives and the ticket would otherwise verify.
TEST(CephxServiceTicket, reject_truncated_caps)
{
  using ceph::encode;
  auto cct = g_ceph_context;

  const std::string str = "allow rwx pool=foo";
  ceph::bufferlist caps;
  encode((uint32_t)9, caps);   // length of "allow rwx", the unscoped prefix
  caps.append(str.data(), str.size());

  KeyRing keys;
  CephXTicketBlob blob;
  CryptoKey session_key;
  build_service_ticket(cct, CEPH_ENTITY_TYPE_OSD, 42, "esc", false,
               ceph_clock_now(), 3600, &keys, &blob, &session_key, &caps);

  ceph::bufferlist authorizer;
  build_authorizer(cct, CEPH_ENTITY_TYPE_OSD, 42, blob, session_key,
           &authorizer);
  ASSERT_FALSE(verify_authorizer(cct, keys, authorizer));
}
