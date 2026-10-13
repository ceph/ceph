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

class InstanceAndProtocolTypeTest : public ::testing::Test {
protected:
  MockDPP dpp;

  void SetUp() override {
    // Setup code if needed
  }

  void TearDown() override {
    // Cleanup code if needed
  }
};

// Test InstanceType enum values
TEST_F(InstanceAndProtocolTypeTest, InstanceTypeEnumValues) {
  // Verify enum values exist and are distinct
  InstanceType daemon = InstanceType::Daemon;
  InstanceType library = InstanceType::Library;

  EXPECT_NE(daemon, library);
}

// Test ProtocolType enum values
TEST_F(InstanceAndProtocolTypeTest, ProtocolTypeEnumValues) {
  // Verify enum values exist and are distinct
  ProtocolType http_s3 = ProtocolType::HTTP_S3;
  ProtocolType nfs = ProtocolType::NFS;
  ProtocolType smb = ProtocolType::SMB;
  
  EXPECT_NE(http_s3, nfs);
  EXPECT_NE(http_s3, smb);
  EXPECT_NE(nfs, smb);
}

// Test is_library_instance() helper
TEST_F(InstanceAndProtocolTypeTest, IsLibraryInstance) {
  // Test daemon instance (default)
  AppMain app_daemon(&dpp);
  EXPECT_FALSE(app_daemon.is_library_instance());

  // Test library instance with NFS
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(InstanceType::Library, ProtocolType::NFS);
  EXPECT_TRUE(app_nfs.is_library_instance());

  // Test library instance with SMB
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(InstanceType::Library, ProtocolType::SMB);
  EXPECT_TRUE(app_smb.is_library_instance());
}

// Test is_http_protocol() helper
TEST_F(InstanceAndProtocolTypeTest, IsHttpProtocol) {
  // Test HTTP/S3 protocol (default)
  AppMain app_http(&dpp);
  EXPECT_TRUE(app_http.is_http_protocol());

  // Test NFS protocol
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(InstanceType::Library, ProtocolType::NFS);
  EXPECT_FALSE(app_nfs.is_http_protocol());

  // Test SMB protocol
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(InstanceType::Library, ProtocolType::SMB);
  EXPECT_FALSE(app_smb.is_http_protocol());
}

// Test get_config_prefix() helper
TEST_F(InstanceAndProtocolTypeTest, GetConfigPrefix) {
  // Test HTTP/S3
  AppMain app_http(&dpp);
  app_http.init_frontends1(InstanceType::Daemon, ProtocolType::HTTP_S3);
  EXPECT_EQ(app_http.get_config_prefix(), "rgw");

  // Test NFS
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(InstanceType::Library, ProtocolType::NFS);
  EXPECT_EQ(app_nfs.get_config_prefix(), "rgw_nfs");

  // Test SMB
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(InstanceType::Library, ProtocolType::SMB);
  EXPECT_EQ(app_smb.get_config_prefix(), "rgw_smb");
}

// Test get_frontend_name() helper
TEST_F(InstanceAndProtocolTypeTest, GetFrontendName) {
  // Test HTTP/S3
  AppMain app_http(&dpp);
  app_http.init_frontends1(InstanceType::Daemon, ProtocolType::HTTP_S3);
  EXPECT_EQ(app_http.get_frontend_name(), "rgw");

  // Test NFS
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(InstanceType::Library, ProtocolType::NFS);
  EXPECT_EQ(app_nfs.get_frontend_name(), "rgw-nfs");

  // Test SMB
  AppMain app_smb(&dpp);
  app_smb.init_frontends1(InstanceType::Library, ProtocolType::SMB);
  EXPECT_EQ(app_smb.get_frontend_name(), "rgw-smb");
}

// Test default instance and protocol types
TEST_F(InstanceAndProtocolTypeTest, DefaultTypes) {
  AppMain app(&dpp);
  // Default should be Daemon with HTTP/S3
  EXPECT_FALSE(app.is_library_instance());
  EXPECT_TRUE(app.is_http_protocol());
  EXPECT_EQ(app.get_config_prefix(), "rgw");
  EXPECT_EQ(app.get_frontend_name(), "rgw");
}

// Test instance and protocol type combinations
TEST_F(InstanceAndProtocolTypeTest, TypeCombinations) {
  AppMain app(&dpp);

  // Start with Daemon + HTTP/S3 (default)
  EXPECT_EQ(app.get_frontend_name(), "rgw");
  EXPECT_FALSE(app.is_library_instance());
  EXPECT_TRUE(app.is_http_protocol());

  // Switch to Library + NFS
  app.init_frontends1(InstanceType::Library, ProtocolType::NFS);
  EXPECT_EQ(app.get_frontend_name(), "rgw-nfs");
  EXPECT_TRUE(app.is_library_instance());
  EXPECT_FALSE(app.is_http_protocol());

  // Switch to Library + SMB
  app.init_frontends1(InstanceType::Library, ProtocolType::SMB);
  EXPECT_EQ(app.get_frontend_name(), "rgw-smb");
  EXPECT_TRUE(app.is_library_instance());
  EXPECT_FALSE(app.is_http_protocol());

  // Switch back to Daemon + HTTP/S3
  app.init_frontends1(InstanceType::Daemon, ProtocolType::HTTP_S3);
  EXPECT_EQ(app.get_frontend_name(), "rgw");
  EXPECT_FALSE(app.is_library_instance());
  EXPECT_TRUE(app.is_http_protocol());
}

// Test config prefix format
TEST_F(InstanceAndProtocolTypeTest, ConfigPrefixFormat) {
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(InstanceType::Library, ProtocolType::NFS);
  std::string prefix = app_nfs.get_config_prefix();
  
  // Verify underscore format for config keys
  EXPECT_NE(prefix.find('_'), std::string::npos);
  EXPECT_EQ(prefix.find('-'), std::string::npos);

  AppMain app_smb(&dpp);
  app_smb.init_frontends1(InstanceType::Library, ProtocolType::SMB);
  prefix = app_smb.get_config_prefix();
  
  // Verify underscore format for config keys
  EXPECT_NE(prefix.find('_'), std::string::npos);
  EXPECT_EQ(prefix.find('-'), std::string::npos);
}

// Test frontend name format
TEST_F(InstanceAndProtocolTypeTest, FrontendNameFormat) {
  AppMain app_nfs(&dpp);
  app_nfs.init_frontends1(InstanceType::Library, ProtocolType::NFS);
  std::string name = app_nfs.get_frontend_name();

  // Verify hyphen format for frontend names
  EXPECT_NE(name.find('-'), std::string::npos);

  AppMain app_smb(&dpp);
  app_smb.init_frontends1(InstanceType::Library, ProtocolType::SMB);
  name = app_smb.get_frontend_name();

  // Verify hyphen format for frontend names
  EXPECT_NE(name.find('-'), std::string::npos);
}

// Test that HTTP/S3 doesn't have hyphen in frontend name
TEST_F(InstanceAndProtocolTypeTest, HttpFrontendNameNoHyphen) {
  AppMain app(&dpp);
  app.init_frontends1(InstanceType::Daemon, ProtocolType::HTTP_S3);
  std::string name = app.get_frontend_name();
  
  // HTTP/S3 frontend name should be just "rgw" without hyphen
  EXPECT_EQ(name.find('-'), std::string::npos);
  EXPECT_EQ(name, "rgw");
}

// Test separation of concerns: instance type vs protocol type
TEST_F(InstanceAndProtocolTypeTest, SeparationOfConcerns) {
  AppMain app_daemon_http(&dpp);
  app_daemon_http.init_frontends1(InstanceType::Daemon, ProtocolType::HTTP_S3);

  AppMain app_lib_nfs(&dpp);
  app_lib_nfs.init_frontends1(InstanceType::Library, ProtocolType::NFS);

  // Verify instance types are correctly identified
  EXPECT_FALSE(app_daemon_http.is_library_instance());
  EXPECT_TRUE(app_lib_nfs.is_library_instance());

  // Verify protocol types are correctly identified
  EXPECT_TRUE(app_daemon_http.is_http_protocol());
  EXPECT_FALSE(app_lib_nfs.is_http_protocol());

  // Verify they produce different config prefixes
  EXPECT_NE(app_daemon_http.get_config_prefix(), app_lib_nfs.get_config_prefix());

  // Verify they produce different frontend names
  EXPECT_NE(app_daemon_http.get_frontend_name(), app_lib_nfs.get_frontend_name());
}
