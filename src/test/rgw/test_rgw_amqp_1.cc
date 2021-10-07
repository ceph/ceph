// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_amqp_1.h"
#include "common/ceph_context.h"
#include <chrono>
#include <gtest/gtest.h>

#include <iostream>
#include <string>
#include <thread>

/*

1. first build the amqp 1.0 broker, then run locally specifying a [address:port/topic]
  e.g.  # amqp_1_broker -a localhost:5672/amqp1_0
2. build this unit test and run it.

*/


auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

bool test_ok = false;

class TestAMQP_1 : public ::testing::Test {
  protected:
  // rgw::amqp_1::connection_ptr_t conn = nullptr;

  void SetUp() override {
    ASSERT_TRUE(rgw::amqp_1::init(cct));
  }

  void TearDown() override {
    // we have to make sure that we delete Manager after connections are closed.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    rgw::amqp_1::shutdown();
  }
};

void callback(int status) {
  if(status == 0) {
    test_ok = true;
  } else {
    test_ok = false;
  }
}

TEST_F(TestAMQP_1, ConnectionOK) {
  const std::string test_broker = "localhost:5672/amqp1_0";
  const auto connection_number = rgw::amqp_1::get_connection_count();
  auto conn = rgw::amqp_1::connect(test_broker);
  EXPECT_TRUE(conn);
  const auto connection_number_plus = rgw::amqp_1::get_connection_count();
  EXPECT_EQ(connection_number_plus, connection_number + 1);
}

TEST_F(TestAMQP_1, PublishOK) {
  const std::string test_broker = "localhost:5672/amqp1_0";
  auto conn = rgw::amqp_1::connect(test_broker);
  EXPECT_TRUE(conn);
  auto rc = rgw::amqp_1::publish(conn, "amqp1_0", "sample-message");
  EXPECT_EQ(rc, 0);
  rc = rgw::amqp_1::publish(conn, "amqp1_0", "sample-message-hah");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP_1, MultiplePublishOK) {
  const std::string test_broker = "localhost:5672/amqp1_0";
  auto conn = rgw::amqp_1::connect(test_broker);
  EXPECT_TRUE(conn);
  for(int i = 0; i < 100; ++i) {
    std::string num_tag;
    num_tag = std::string("multi-sample-msg") + std::to_string(i);
    std::string to("amqp1_0");
    auto rc = rgw::amqp_1::publish(conn, to, num_tag);
    EXPECT_EQ(rc, 0);
  }
}

TEST_F(TestAMQP_1, PublishWithCallback) {
  const std::string test_broker = "localhost:5672/amqp1_0";
  auto conn = rgw::amqp_1::connect(test_broker);
  EXPECT_TRUE(conn);
  auto rc = rgw::amqp_1::publish_with_confirm(conn, "amqp1_0", "callback-message", callback);
  EXPECT_EQ(rc, 0);
  std::this_thread::sleep_for(std::chrono::seconds(5));
  EXPECT_EQ(test_ok, true);
  test_ok = false;
}

