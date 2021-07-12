// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <gtest/gtest.h>
#include <aws/core/Aws.h>
#include <rgw/rgw_notify_aws.h>
#include <chrono>
#include <thread>
#include "common/ceph_context.h"
#include <aws/core/auth/AWSCredentialsProvider.h>
/*
  For running the unit tests, please install moto_server using
  > pip install moto
  
  See http://docs.getmoto.org/en/latest/docs/server_mode.html for more details
  Note: lambda mock server requires you to install and run docker on the machine
  
  Run the servers using:
  > moto_server -p5555

  Create SNS topic in moto_server using:
  > aws sns --region=us-east-1 --endpoint-url=http://localhost:5555 create-topic --name gsoc20-ceph

  Create an IAM role that could invoke the lambda:
  > aws iam --region=us-east-1 --endpoint-url=http://localhost:5555 create-role --role-name "lambda-ex" --assume-role-policy-document "some policy"

  Create the zip file from a python file index.py having handler() function, named file.zip:
  > printf 'import json\ndef lambda_handler(event, context):\n\tprint (event)\n\treturn {"statusCode": 200, "body": json.dumps("GSoC 20 Ceph Project!")}\n' > lambda_function.py

  > zip lambda_function.zip lambda_function.py

  Use the above file to create the lambda function in moto_server using:
  > aws lambda --region=us-east-1 --endpoint-url=http://localhost:5555 create-function --function-name gsoc20 --zip-file fileb://lambda_function.zip --handler lambda_function.lambda_handler \
    --runtime python --role arn:aws:iam::123456789012:role/lambda-ex 
*/

bool test_ok;

class TestAWS : public ::testing::Test {
protected:

  void SetUp() override {
    auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);
    ASSERT_TRUE(rgw::aws::init(cct));
    test_ok = false;
  }

  void TearDown() override {
    rgw::aws::shutdown();
  }
};


void callback(int status){
  if(status == 0){
    test_ok = true;
  } else {
    test_ok = false;
  }
}

const std::string endpoint = "aws://localhost:5555";

TEST_F(TestAWS, SNS) {
  const std::string arn = "arn:aws:sns:us-east-1:123456789012:gsoc20-ceph";
  auto client = rgw::aws::connect("", "", arn, endpoint, false, false);
  ASSERT_EQ(rgw::aws::publish(client, R"({ "name": "Bob" })", arn, callback), 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  ASSERT_TRUE(test_ok);
}

TEST_F(TestAWS, Lambda) {
  const std::string arn = "arn:aws:lambda:us-east-1:123456789012:function:gsoc20";
  const std::string access_key = "0555b35654ad1656d804";
  const std::string secret_key = "h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q==";
  auto client = rgw::aws::connect(access_key, secret_key, arn, endpoint, false, false);
  ASSERT_EQ(rgw::aws::publish(client, R"({ "name": "Bob" })", arn, callback), 0);

  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  ASSERT_TRUE(test_ok);
}

