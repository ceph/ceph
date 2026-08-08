// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

#include <gtest/gtest.h>
#include "rgw_main.h"

using namespace rgw;

// Mock DoutPrefixProvider for testing
class MockDPP : public DoutPrefixProvider {
public:
  CephContext* get_cct() const override { return nullptr; }
  unsigned get_subsys() const override { return 0; }
  std::ostream& gen_prefix(std::ostream& out) const override { return out; }
};

class FrontendTypeTest : public ::testing::Test {
protected:
  MockDPP dpp;
  
  void SetUp() override {
    // Setup code if needed
  }
  
  void TearDown() override {
    // Cleanup code if needed
  }
};

// Test FrontendType enum values
TEST_F(FrontendTypeTest, EnumValues) {
  // Verify enum values exist and are distinct
  FrontendType http = FrontendType::HTTP;
  FrontendType nfs = FrontendType::NFS;
  FrontendType smb = FrontendType::SMB;
  
  EXPECT_NE(http, nfs);
  EXPECT_NE(http, smb);
  EXPECT_NE(nfs, smb);
}

// Test is_non_http_frontend() helper
TEST_F(FrontendTypeTest, IsNonHttpFrontend) {
  AppMain app_http(&dpp);
  // Default should be HTTP
  EXPECT_FALSE(app_http.is_non_http_frontend());
  
  // Test with NFS
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(FrontendType::NFS);
  EXPECT_TRUE(app_nfs.is_non_http_frontend());
  
  // Test with SMB
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(FrontendType::SMB);
  EXPECT_TRUE(app_smb.is_non_http_frontend());
}

// Test get_config_prefix() helper
TEST_F(FrontendTypeTest, GetConfigPrefix) {
  // Test HTTP
  AppMain app_http(&dpp);
  app_http.init_frontends1(FrontendType::HTTP);
  EXPECT_EQ(app_http.get_config_prefix(), "rgw");
  
  // Test NFS
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(FrontendType::NFS);
  EXPECT_EQ(app_nfs.get_config_prefix(), "rgw_nfs");
  
  // Test SMB
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(FrontendType::SMB);
  EXPECT_EQ(app_smb.get_config_prefix(), "rgw_smb");
}

// Test get_daemon_type() helper
TEST_F(FrontendTypeTest, GetDaemonType) {
  // Test HTTP
  AppMain app_http(&dpp);
  app_http.init_frontends1(FrontendType::HTTP);
  EXPECT_EQ(app_http.get_daemon_type(), "rgw");
  
  // Test NFS
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(FrontendType::NFS);
  EXPECT_EQ(app_nfs.get_daemon_type(), "rgw-nfs");
  
  // Test SMB
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(FrontendType::SMB);
  EXPECT_EQ(app_smb.get_daemon_type(), "rgw-smb");
}

// Test default frontend type
TEST_F(FrontendTypeTest, DefaultFrontendType) {
  AppMain app(&dpp);
  // Default should be HTTP
  EXPECT_FALSE(app.is_non_http_frontend());
  EXPECT_EQ(app.get_config_prefix(), "rgw");
  EXPECT_EQ(app.get_daemon_type(), "rgw");
}

// Test frontend type switching
TEST_F(FrontendTypeTest, FrontendTypeSwitching) {
  AppMain app(&dpp);
  
  // Start with HTTP (default)
  EXPECT_EQ(app.get_daemon_type(), "rgw");
  
  // Switch to NFS
  app.init_frontends1(FrontendType::NFS);
  EXPECT_EQ(app.get_daemon_type(), "rgw-nfs");
  EXPECT_TRUE(app.is_non_http_frontend());
  
  // Switch to SMB
  app.init_frontends1(FrontendType::SMB);
  EXPECT_EQ(app.get_daemon_type(), "rgw-smb");
  EXPECT_TRUE(app.is_non_http_frontend());
  
  // Switch back to HTTP
  app.init_frontends1(FrontendType::HTTP);
  EXPECT_EQ(app.get_daemon_type(), "rgw");
  EXPECT_FALSE(app.is_non_http_frontend());
}

// Test config prefix format
TEST_F(FrontendTypeTest, ConfigPrefixFormat) {
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(FrontendType::NFS);
  std::string prefix = app_nfs.get_config_prefix();
  
  // Verify underscore format for config keys
  EXPECT_NE(prefix.find('_'), std::string::npos);
  EXPECT_EQ(prefix.find('-'), std::string::npos);
  
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(FrontendType::SMB);
  prefix = app_smb.get_config_prefix();
  
  // Verify underscore format for config keys
  EXPECT_NE(prefix.find('_'), std::string::npos);
  EXPECT_EQ(prefix.find('-'), std::string::npos);
}

// Test daemon type format
TEST_F(FrontendTypeTest, DaemonTypeFormat) {
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(FrontendType::NFS);
  std::string daemon = app_nfs.get_daemon_type();
  
  // Verify hyphen format for daemon names
  EXPECT_NE(daemon.find('-'), std::string::npos);
  
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(FrontendType::SMB);
  daemon = app_smb.get_daemon_type();
  
  // Verify hyphen format for daemon names
  EXPECT_NE(daemon.find('-'), std::string::npos);
}

// Test that HTTP doesn't have hyphen in daemon type
TEST_F(FrontendTypeTest, HttpDaemonTypeNoHyphen) {
  AppMain app(&dpp);
  app.init_frontends1(FrontendType::HTTP);
  std::string daemon = app.get_daemon_type();
  
  // HTTP daemon type should be just "rgw" without hyphen
  EXPECT_EQ(daemon.find('-'), std::string::npos);
  EXPECT_EQ(daemon, "rgw");
}
