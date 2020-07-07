// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include <aws/core/Aws.h>
#include <rgw/rgw_aws.h>
#include <chrono>
#include <thread>
#include "common/ceph_context.h"
#include <aws/core/auth/AWSCredentialsProvider.h>

class TestAWS : public ::testing::Test {
protected:

  void SetUp() override {
    auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
    ASSERT_TRUE(rgw::aws::init(cct));
  }

  void TearDown() override {
    rgw::aws::shutdown();
  }

};


TEST_F(TestAWS, SNS) {
  Aws::Auth::ProfileConfigFileAWSCredentialsProvider credentialsProvider;
  Aws::Auth::AWSCredentials credentials = credentialsProvider.GetAWSCredentials();
  auto client = rgw::aws::connect(std::string(credentials.GetAWSAccessKeyId()),
                                  std::string(credentials.GetAWSSecretKey()),
                                  "arn:aws:sns:us-east-1:125341253834:gsoc20-ceph", "", true);
  EXPECT_EQ(rgw::aws::publish(client, "body", "arn:aws:sns:us-east-1:125341253834:gsoc20-ceph"), 1);

  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
}

TEST_F(TestAWS, Lambda) {
  Aws::Auth::ProfileConfigFileAWSCredentialsProvider credentialsProvider;
  Aws::Auth::AWSCredentials credentials = credentialsProvider.GetAWSCredentials();
  auto client = rgw::aws::connect(std::string(credentials.GetAWSAccessKeyId()),
                                  std::string(credentials.GetAWSSecretKey()),
                                  "arn:aws:lambda:us-east-1:125341253834:function:gsoc20", "", true);
  EXPECT_EQ(rgw::aws::publish(client, R"({ "name": "Bob" })", "arn:aws:lambda:us-east-1:125341253834:function:gsoc20"),
            1);

  std::this_thread::sleep_for(std::chrono::milliseconds(10000));
}
