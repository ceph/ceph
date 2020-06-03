// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include <aws/core/Aws.h>
#include <rgw/rgw_aws.h>
#include <chrono>
#include <thread>
#include "common/ceph_context.h"
#include <aws/core/auth/AWSCredentialsProvider.h>
/*
for running the unit tests, please install moto_server using
pip install moto
See http://docs.getmoto.org/en/latest/docs/server_mode.html for  more details.
Note: lambda mock server requires you to setup docker.
run the server using
> moto_server lambda -p5555
> moto_server sns -p4444

 Now create sns topic, and lambda function in moto_server using
  aws sns --region=us-east-1 --endpoint-url=http://localhost:4444 create-topic --name gsoc20-ceph

  Create the zip file from a python file index.py having handler() function, named file.zip
  aws lambda --region=us-east-1 --endpoint-url=http://localhost:5555 create-function --function-name gsoc20 --zip-file fileb://file.zip --handler index.handler --runtime python3.7 --role lambda-ex
 */

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
void callback(int status){
  if(status == 1){
    std::cout << "Successful" << std::endl;
  }else{
    std::cout << "Failed" << std::endl;
  }

}

TEST_F(TestAWS, SNS) {
  auto client = rgw::aws::connect("", "", "arn:aws:sns:us-east-1:123456789012:gsoc20-ceph", "aws://localhost:4444", false, false);
  EXPECT_EQ(rgw::aws::publish(client, "body", "arn:aws:sns:us-east-1:123456789012:gsoc20-ceph", nullptr), 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
}

TEST_F(TestAWS, Lambda) {
  auto client = rgw::aws::connect("", "", "arn:aws:lambda:us-east-1:123456789012:gsoc20", "aws://localhost:5555", false, false);
  EXPECT_EQ(rgw::aws::publish(client, R"({ "name": "Bob" })", "arn:aws:lambda:us-east-1:123456789012:function:gsoc20", callback), 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
}
