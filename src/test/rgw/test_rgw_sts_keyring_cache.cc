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

#include <cerrno>
#include <chrono>
#include <map>
#include <string>

#include "gtest/gtest.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "include/buffer.h"

#include "rgw_b64.h"
#include "rgw_sts_keyring.h"
#include "rgw_sts_keyring_cache.h"

namespace {

const std::string keyring_key{rgw::sts::STS_KEYRING_CONFIG_KEY};
const std::string legacy_key{rgw::sts::STS_LEGACY_KEY_CONFIG_KEY};

// one <hex-id>=<base64-key> keyring entry, distinct per index
std::string make_entry(size_t index)
{
  std::string id(40, '0');
  id.back() = static_cast<char>('0' + index);
  return id + '=' + rgw::to_base64(std::string(32, static_cast<char>(index)));
}

// serves canned config-key responses
class FakeKeyringCache : public STS::KeyringCache {
 public:
  explicit FakeKeyringCache(CephContext* cct)
      : KeyringCache(cct, nullptr, std::chrono::seconds(60)) {}
  // join before the override goes away; ~KeyringCache is too late
  ~FakeKeyringCache() override { stop(); }

  using KeyringCache::refresh;

  void set_response(const std::string& key, int ret, const std::string& value)
  {
    responses[key] = {ret, value};
  }

 private:
  int fetch(const std::string& key, ceph::bufferlist* bl) override
  {
    const auto& response = responses[key];
    if (response.ret == 0) {
      bl->append(response.value);
    }
    return response.ret;
  }

  struct Response {
    int ret = -ENOENT;
    std::string value;
  };
  std::map<std::string, Response> responses;
};

class sts_keyring_cache : public ::testing::Test {
 protected:
  FakeKeyringCache cache{g_ceph_context};
};

} // anonymous namespace

TEST_F(sts_keyring_cache, loads_and_serves)
{
  cache.set_response(keyring_key, 0, make_entry(1) + '\n' + make_entry(2));
  cache.set_response(legacy_key, 0, "0123456789abcdef\n");
  cache.refresh();

  const auto keyring = cache.get();
  ASSERT_TRUE(keyring);
  EXPECT_EQ(2u, keyring->size());

  // the stored value is served with its trailing whitespace trimmed
  const auto legacy = cache.get_legacy();
  ASSERT_TRUE(legacy);
  EXPECT_EQ("0123456789abcdef", *legacy);
}

TEST_F(sts_keyring_cache, removal_revokes)
{
  cache.set_response(keyring_key, 0, make_entry(1));
  cache.set_response(legacy_key, 0, "0123456789abcdef");
  cache.refresh();
  ASSERT_TRUE(cache.get());
  ASSERT_TRUE(cache.get_legacy());

  cache.set_response(keyring_key, -ENOENT, "");
  cache.set_response(legacy_key, -ENOENT, "");
  cache.refresh();
  EXPECT_FALSE(cache.get());
  EXPECT_FALSE(cache.get_legacy());
}

TEST_F(sts_keyring_cache, errors_keep_the_last_snapshot)
{
  cache.set_response(keyring_key, 0, make_entry(1));
  cache.set_response(legacy_key, 0, "0123456789abcdef");
  cache.refresh();

  // an unparseable keyring and a mon error both leave the snapshots alone
  cache.set_response(keyring_key, 0, "not a keyring");
  cache.set_response(legacy_key, -EIO, "");
  cache.refresh();

  const auto keyring = cache.get();
  ASSERT_TRUE(keyring);
  EXPECT_EQ(1u, keyring->size());
  EXPECT_TRUE(cache.get_legacy());
}

TEST_F(sts_keyring_cache, empty_legacy_value_revokes)
{
  cache.set_response(legacy_key, 0, "0123456789abcdef");
  cache.refresh();
  ASSERT_TRUE(cache.get_legacy());

  cache.set_response(legacy_key, 0, " \t\r\n");
  cache.refresh();
  EXPECT_FALSE(cache.get_legacy());
}

TEST_F(sts_keyring_cache, stop_keeps_the_snapshots)
{
  cache.set_response(keyring_key, 0, make_entry(1));
  cache.refresh();
  cache.start();
  cache.stop();
  EXPECT_TRUE(cache.get());
  EXPECT_FALSE(cache.get_legacy());
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
