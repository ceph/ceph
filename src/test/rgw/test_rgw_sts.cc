// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "common/dout.h"
#include "global/global_init.h"
#include "include/buffer.h"

#include "rgw_b64.h"
#include "rgw_sts.h"
#include "rgw_sts_keyring.h"

namespace {

using rgw::sts::StsKeyring;
using rgw::sts::sts_aead_key;

// build a raw key (20-byte id, 32-byte key) distinct from the parsed test keys
sts_aead_key make_raw_key(size_t index)
{
  sts_aead_key key;
  key.id.assign(20, static_cast<char>(0x10 + index));
  key.key.assign(32, static_cast<char>(0x20 + index));
  return key;
}

std::string make_sts_key_id(size_t index)
{
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(40) << index;
  return out.str();
}

std::string make_sts_key(size_t index)
{
  return rgw::to_base64(std::string(32, static_cast<char>(index)));
}

std::string make_sts_keyring(size_t count)
{
  std::ostringstream out;
  for (size_t i = 0; i < count; ++i) {
    if (i > 0) {
      out << ' ';
    }
    out << make_sts_key_id(i) << '=' << make_sts_key(i);
  }
  return out.str();
}

int parse(std::string_view value, std::string& error)
{
  StsKeyring keyring;
  return StsKeyring::parse(value, keyring, error);
}

} // anonymous namespace

TEST(sts_keyring, accepts_valid_keyrings)
{
  std::string error;
  EXPECT_EQ(0, parse(make_sts_keyring(1), error));
  EXPECT_EQ(0, parse(make_sts_keyring(16), error));

  auto multiline_keyring = make_sts_keyring(2);
  multiline_keyring.replace(multiline_keyring.find(' '), 1, "\n");
  EXPECT_EQ(0, parse(multiline_keyring, error));
}

TEST(sts_keyring, rejects_empty_keyrings)
{
  std::string error;
  EXPECT_EQ(-EINVAL, parse("", error));
  EXPECT_EQ(-EINVAL, parse(" \n\t", error));
}

TEST(sts_keyring, rejects_malformed_entries)
{
  const auto id = make_sts_key_id(0);
  const auto key = make_sts_key(0);
  const std::vector<std::string> invalid = {
    id,
    id + "=",
    id.substr(1) + '=' + key,
    std::string(40, 'g') + '=' + key,
    id + "=!!!!",
  };

  for (const auto& value : invalid) {
    std::string error;
    EXPECT_EQ(-EINVAL, parse(value, error)) << value;
  }

  std::string error;
  EXPECT_EQ(-EINVAL, parse(id + "==", error));
  EXPECT_NE(std::string::npos, error.find("does not decode to 32 bytes"));
}

TEST(sts_keyring, requires_canonical_base64)
{
  const auto id = make_sts_key_id(0);
  const auto key = make_sts_key(0);
  auto noncanonical_pad_bits = key;
  noncanonical_pad_bits[noncanonical_pad_bits.size() - 2] = 'B';

  const std::vector<std::string> invalid = {
    key.substr(0, key.size() - 1),
    key + '=',
    noncanonical_pad_bits,
    rgw::to_base64(std::string(31, '\0')),
    rgw::to_base64(std::string(33, '\0')),
  };

  for (const auto& encoded : invalid) {
    std::string error;
    EXPECT_EQ(-EINVAL, parse(id + '=' + encoded, error)) << encoded;
  }
}

TEST(sts_keyring, rejects_duplicate_ids_and_keys)
{
  const std::string mixed_case_id =
    "0123456789abcdef0123456789abcdef01234567";
  auto uppercase_id = mixed_case_id;
  std::transform(uppercase_id.begin(), uppercase_id.end(),
                 uppercase_id.begin(),
                 [](unsigned char c) { return std::toupper(c); });

  const auto key0 = make_sts_key(0);
  const auto key1 = make_sts_key(1);
  std::string error;
  EXPECT_EQ(-EINVAL,
            parse(mixed_case_id + '=' + key0 + ' ' +
                  uppercase_id + '=' + key1,
                  error));
  EXPECT_EQ(-EINVAL,
            parse(make_sts_key_id(0) + '=' + key0 + ' ' +
                  make_sts_key_id(1) + '=' + key0,
                  error));
}

TEST(sts_keyring, limits_key_count)
{
  std::string error;
  EXPECT_EQ(0, parse(make_sts_keyring(16), error));
  EXPECT_EQ(-EINVAL, parse(make_sts_keyring(17), error));
  EXPECT_NE(std::string::npos, error.find("at most 16"));
}

TEST(sts_keyring, format_round_trips)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(3), keyring, error));

  StsKeyring reparsed;
  ASSERT_EQ(0, StsKeyring::parse(keyring.format(), reparsed, error));

  ASSERT_EQ(keyring.size(), reparsed.size());
  for (size_t i = 0; i < keyring.size(); ++i) {
    EXPECT_EQ(keyring.entries()[i].id, reparsed.entries()[i].id);
    EXPECT_EQ(keyring.entries()[i].key, reparsed.entries()[i].key);
  }
}

TEST(sts_keyring, errors_do_not_echo_secrets)
{
  const std::string secret = "not-a-valid-secret-key";
  std::string error;
  EXPECT_EQ(-EINVAL, parse(make_sts_key_id(0) + '=' + secret, error));
  EXPECT_EQ(std::string::npos, error.find(secret));
}

TEST(sts_keyring, sealing_key_is_the_first_entry)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(3), keyring, error));
  EXPECT_EQ(&keyring.sealing_key(), &keyring.entries().front());
}

TEST(sts_keyring, find_locates_keys_by_id)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(3), keyring, error));
  const std::string id1 = keyring.entries()[1].id;
  ASSERT_NE(nullptr, keyring.find(id1));
  EXPECT_EQ(id1, keyring.find(id1)->id);
  EXPECT_EQ(nullptr, keyring.find(std::string(20, '\xff')));
}

TEST(sts_keyring, prepend_adds_a_sealing_key)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(2), keyring, error));
  const std::string old_seal = keyring.sealing_key().id;

  ASSERT_EQ(0, keyring.prepend(make_raw_key(9), error));
  EXPECT_EQ(3u, keyring.size());
  EXPECT_NE(old_seal, keyring.sealing_key().id);

  EXPECT_EQ(-EINVAL, keyring.prepend(make_raw_key(9), error));
}

TEST(sts_keyring, prepend_rejects_a_full_keyring)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(16), keyring, error));
  EXPECT_EQ(-EINVAL, keyring.prepend(make_raw_key(99), error));
  EXPECT_NE(std::string::npos, error.find("more than 16"));
}

TEST(sts_keyring, remove_drops_a_key)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(3), keyring, error));
  const std::string id1 = keyring.entries()[1].id;
  ASSERT_EQ(0, keyring.remove(id1, error));
  EXPECT_EQ(2u, keyring.size());
  EXPECT_EQ(nullptr, keyring.find(id1));
  EXPECT_EQ(-ENOENT, keyring.remove(std::string(20, '\xff'), error));
}

TEST(sts_keyring, remove_refuses_the_only_key)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(1), keyring, error));
  EXPECT_EQ(-EINVAL, keyring.remove(keyring.sealing_key().id, error));
  EXPECT_EQ(1u, keyring.size());
}

TEST(sts_keyring, trim_drops_the_oldest_keeping_the_sealing_key)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(5), keyring, error));
  const std::string seal = keyring.sealing_key().id;

  const auto removed = keyring.trim(2);
  EXPECT_EQ(3u, removed.size());
  EXPECT_EQ(2u, keyring.size());
  EXPECT_EQ(seal, keyring.sealing_key().id);
}

TEST(sts_seal, round_trips)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(3), keyring, error));
  const NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);

  const std::string body = "a session token body";
  bufferlist plaintext;
  plaintext.append(body);

  std::string token;
  ASSERT_EQ(0, STS::seal_session_token(&dpp, g_ceph_context,
                                       keyring.sealing_key(), plaintext, token));
  EXPECT_EQ(0u, token.rfind("v2.", 0));

  bufferlist opened;
  ASSERT_EQ(0, STS::unseal_session_token(&dpp, keyring, token, opened));
  EXPECT_EQ(body, opened.to_str());
}

TEST(sts_seal, verifies_tokens_sealed_under_an_older_key)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(1), keyring, error));
  const NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);

  const std::string body = "older key body";
  bufferlist plaintext;
  plaintext.append(body);
  std::string token;
  ASSERT_EQ(0, STS::seal_session_token(&dpp, g_ceph_context,
                                       keyring.sealing_key(), plaintext, token));

  // rotate: a new key seals, the old one stays only to verify
  ASSERT_EQ(0, keyring.prepend(make_raw_key(5), error));
  ASSERT_NE(keyring.sealing_key().id, keyring.entries().back().id);

  bufferlist opened;
  ASSERT_EQ(0, STS::unseal_session_token(&dpp, keyring, token, opened));
  EXPECT_EQ(body, opened.to_str());
}

TEST(sts_seal, rejects_a_tampered_tag)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(1), keyring, error));
  const NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);

  bufferlist plaintext;
  plaintext.append(std::string("payload"));
  std::string token;
  ASSERT_EQ(0, STS::seal_session_token(&dpp, g_ceph_context,
                                       keyring.sealing_key(), plaintext, token));

  std::string envelope = rgw::from_base64(token.substr(3));
  envelope[envelope.size() - 1] ^= 0x01;
  const std::string tampered = "v2." + rgw::to_base64(envelope);

  bufferlist opened;
  EXPECT_EQ(-EPERM, STS::unseal_session_token(&dpp, keyring, tampered, opened));
}

TEST(sts_seal, rejects_an_unknown_key_id)
{
  StsKeyring sealer;
  StsKeyring other;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(1), sealer, error));
  ASSERT_EQ(0, other.prepend(make_raw_key(7), error));
  const NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);

  bufferlist plaintext;
  plaintext.append(std::string("payload"));
  std::string token;
  ASSERT_EQ(0, STS::seal_session_token(&dpp, g_ceph_context,
                                       sealer.sealing_key(), plaintext, token));

  bufferlist opened;
  EXPECT_EQ(-EPERM, STS::unseal_session_token(&dpp, other, token, opened));
}

TEST(sts_seal, rejects_a_truncated_envelope)
{
  StsKeyring keyring;
  std::string error;
  ASSERT_EQ(0, StsKeyring::parse(make_sts_keyring(1), keyring, error));
  const NoDoutPrefix dpp(g_ceph_context, ceph_subsys_rgw);

  bufferlist plaintext;
  plaintext.append(std::string("payload"));
  std::string token;
  ASSERT_EQ(0, STS::seal_session_token(&dpp, g_ceph_context,
                                       keyring.sealing_key(), plaintext, token));

  std::string envelope = rgw::from_base64(token.substr(3));
  envelope.resize(envelope.size() / 2);
  const std::string truncated = "v2." + rgw::to_base64(envelope);

  bufferlist opened;
  EXPECT_EQ(-EINVAL, STS::unseal_session_token(&dpp, keyring, truncated, opened));
}

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
