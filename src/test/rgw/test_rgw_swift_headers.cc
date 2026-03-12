// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include "rgw_common.h"
#include "rgw_auth.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "global/global_context.h"

// Mock identity class for testing
class MockIdentity : public rgw::auth::Identity {
private:
  rgw_user owner_id;
  bool is_owner;
  bool is_system;

public:
  MockIdentity(const rgw_user& owner, bool owner_flag, bool system_flag = false)
    : owner_id(owner), is_owner(owner_flag), is_system(system_flag) {}

  ACLOwner get_aclowner() const override {
    return ACLOwner(owner_id);
  }

  uint32_t get_perms_from_aclspec(const DoutPrefixProvider* dpp, const aclspec_t& aclspec) const override {
    return 0;
  }

  bool is_admin() const override { return is_system; }

  bool is_owner_of(const rgw_owner& o) const override {
    return is_owner && owner_id == o.id;
  }

  bool is_root() const override { return is_system; }

  uint32_t get_perm_mask() const override { return 0; }

  void to_str(std::ostream& out) const override {
    out << "MockIdentity(owner=" << owner_id << ")";
  }

  bool is_identity(const rgw::IAM::Principal& p) const override { return false; }

  uint32_t get_identity_type() const override { return 0; }

  std::optional<rgw::ARN> get_caller_identity() const override { return std::nullopt; }

  std::string get_acct_name() const override { return owner_id.to_str(); }

  std::string get_subuser() const override { return ""; }

  const std::string& get_tenant() const override {
    static std::string empty;
    return empty;
  }

  const std::optional<RGWAccountInfo>& get_account() const override {
    static std::optional<RGWAccountInfo> empty;
    return empty;
  }
};

// Helper function to test header filtering logic
// This replicates the should_hide_header logic from rgw_rest_swift.cc
static bool test_should_hide_header(CephContext* cct,
                                     req_state* s,
                                     const std::string& header_name)
{
  // If filtering is disabled, never hide headers
  if (!cct->_conf->rgw_swift_hide_sensitive_headers) {
    return false;
  }

  // System users can see all headers
  if (s && s->system_request) {
    return false;
  }

  // Check if user is the bucket owner - owners can see all headers
  if (s && s->auth.identity && s->bucket) {
    if (s->auth.identity->is_owner_of(s->bucket->get_info().owner)) {
      return false;
    }
  }

  // Parse the sensitive headers list from config
  const std::string& sensitive_headers_str = cct->_conf->rgw_swift_sensitive_headers;

  // Check if header matches any sensitive prefix (comma-separated list)
  size_t pos = 0;
  while (pos < sensitive_headers_str.length()) {
    size_t comma_pos = sensitive_headers_str.find(',', pos);
    if (comma_pos == std::string::npos) {
      comma_pos = sensitive_headers_str.length();
    }

    // Extract and trim prefix
    size_t start = sensitive_headers_str.find_first_not_of(" \t", pos);
    size_t end = sensitive_headers_str.find_last_not_of(" \t", comma_pos - 1);

    if (start != std::string::npos && end != std::string::npos && start <= end) {
      std::string prefix = sensitive_headers_str.substr(start, end - start + 1);
      if (header_name.compare(0, prefix.length(), prefix) == 0) {
        return true;
      }
    }

    pos = comma_pos + 1;
  }

  return false;
}

class SwiftHeaderFilterTest : public ::testing::Test {
protected:
  CephContext* cct;

  void SetUp() override {
    cct = g_ceph_context;
  }

  void TearDown() override {
  }
};

TEST_F(SwiftHeaderFilterTest, FilteringDisabled)
{
  // When filtering is disabled, all headers should be visible
  cct->_conf.set_val("rgw_swift_hide_sensitive_headers", "false");

  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Read"));
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Write"));
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Temp-Url-Key"));
}

TEST_F(SwiftHeaderFilterTest, DefaultSensitiveHeaders)
{
  // When filtering is enabled, sensitive headers should be hidden from non-owners
  cct->_conf.set_val("rgw_swift_hide_sensitive_headers", "true");
  cct->_conf.set_val("rgw_swift_sensitive_headers",
                     "X-Container-Read,X-Container-Write,X-Container-Meta-");

  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Read"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Write"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Temp-Url-Key"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Custom"));

  // Non-sensitive headers should not be hidden
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Object-Count"));
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Bytes-Used"));
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Timestamp"));
}

TEST_F(SwiftHeaderFilterTest, CustomSensitiveHeaders)
{
  // Test with custom sensitive header list
  cct->_conf.set_val("rgw_swift_hide_sensitive_headers", "true");
  cct->_conf.set_val("rgw_swift_sensitive_headers", "X-Custom-Secret,X-Private-");

  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Custom-Secret"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Private-Data"));

  // Default sensitive headers should NOT be hidden with custom config
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Read"));
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Key"));
}

TEST_F(SwiftHeaderFilterTest, HeaderPrefixMatching)
{
  cct->_conf.set_val("rgw_swift_hide_sensitive_headers", "true");
  cct->_conf.set_val("rgw_swift_sensitive_headers", "X-Container-Meta-");

  // Should match prefix
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Temp-Url-Key"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Quota-Bytes"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Custom"));

  // Should not match if prefix doesn't match
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Read"));
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Write"));
}

TEST_F(SwiftHeaderFilterTest, WhitespaceHandling)
{
  cct->_conf.set_val("rgw_swift_hide_sensitive_headers", "true");
  // Test with whitespace around commas
  cct->_conf.set_val("rgw_swift_sensitive_headers",
                     " X-Container-Read , X-Container-Write , X-Container-Meta- ");

  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Read"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Write"));
  EXPECT_TRUE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Key"));
}

TEST_F(SwiftHeaderFilterTest, EmptyConfiguration)
{
  cct->_conf.set_val("rgw_swift_hide_sensitive_headers", "true");
  cct->_conf.set_val("rgw_swift_sensitive_headers", "");

  // With empty config, nothing should be hidden
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Read"));
  EXPECT_FALSE(test_should_hide_header(cct, nullptr, "X-Container-Meta-Key"));
}

// Note: Testing with req_state would require more complex mocking setup
// These tests focus on the configuration and string matching logic
// Integration tests should verify the full req_state behavior

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                        CODE_ENVIRONMENT_UTILITY,
                        CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
