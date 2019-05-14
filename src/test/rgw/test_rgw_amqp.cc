// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_amqp.h"
#include "common/ceph_context.h"
#include "amqp_mock.h"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <atomic>

using namespace rgw;

const std::chrono::milliseconds wait_time(300);

class CctCleaner {
  CephContext* cct;
public:
  CctCleaner(CephContext* _cct) : cct(_cct) {}
  ~CctCleaner() { 
#ifdef WITH_SEASTAR
    delete cct; 
#else
    cct->put(); 
#endif
  }
};

auto cct = new CephContext(CEPH_ENTITY_TYPE_CLIENT);

CctCleaner cleaner(cct);

class TestAMQP : public ::testing::Test {
protected:
  void SetUp() override {
    ASSERT_TRUE(amqp::init(cct));
  }

  void TearDown() override {
    amqp::shutdown();
  }
};

TEST_F(TestAMQP, ConnectionOK)
{
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("amqp://localhost", "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP, ConnectionReuse)
{
  amqp::connection_ptr_t conn1 = amqp::connect("amqp://localhost", "ex1");
  EXPECT_TRUE(conn1);
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn2 = amqp::connect("amqp://localhost", "ex1");
  EXPECT_TRUE(conn2);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  auto rc = amqp::publish(conn1, "topic", "message");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP, NameResolutionFail)
{
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("amqp://kaboom", "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, InvalidPort)
{
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("amqp://localhost:1234", "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, InvalidHost)
{
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("amqp://0.0.0.1", "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, InvalidVhost)
{
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("amqp://localhost/kaboom", "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, UserPassword)
{
  amqp_mock::set_valid_host("127.0.0.1");
  {
    const auto connection_number = amqp::get_connection_count();
    amqp::connection_ptr_t conn = amqp::connect("amqp://foo:bar@127.0.0.1", "ex1");
    EXPECT_TRUE(conn);
    EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
    auto rc = amqp::publish(conn, "topic", "message");
    EXPECT_LT(rc, 0);
  }
  // now try the same connection with default user/password
  amqp_mock::set_valid_host("127.0.0.2");
  {
    const auto connection_number = amqp::get_connection_count();
    amqp::connection_ptr_t conn = amqp::connect("amqp://guest:guest@127.0.0.2", "ex1");
    EXPECT_TRUE(conn);
    EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
    auto rc = amqp::publish(conn, "topic", "message");
    EXPECT_EQ(rc, 0);
  }
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, URLParseError)
{
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("http://localhost", "ex1");
  EXPECT_FALSE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, ExchangeMismatch)
{
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("http://localhost", "ex2");
  EXPECT_FALSE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, MaxConnections)
{
  // fill up all connections
  std::vector<amqp::connection_ptr_t> connections;
  auto remaining_connections = amqp::get_max_connections() - amqp::get_connection_count();
  while (remaining_connections > 0) {
    const auto host = "127.10.0." + std::to_string(remaining_connections);
    amqp_mock::set_valid_host(host);
    amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1");
    EXPECT_TRUE(conn);
    auto rc = amqp::publish(conn, "topic", "message");
    EXPECT_EQ(rc, 0);
    --remaining_connections;
    connections.push_back(conn);
  }
  EXPECT_EQ(amqp::get_connection_count(), amqp::get_max_connections());
  // try to add another connection
  {
    const std::string host = "toomany";
    amqp_mock::set_valid_host(host);
    amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1");
    EXPECT_FALSE(conn);
    auto rc = amqp::publish(conn, "topic", "message");
    EXPECT_LT(rc, 0);
  }
  EXPECT_EQ(amqp::get_connection_count(), amqp::get_max_connections());
  amqp_mock::set_valid_host("localhost");
  // delete connections to make space for new ones
  for (auto conn : connections) {
    EXPECT_TRUE(amqp::disconnect(conn));
  }
  // wait for them to be deleted
  std::this_thread::sleep_for(wait_time);
  EXPECT_LT(amqp::get_connection_count(), amqp::get_max_connections());
}

std::atomic<bool> callback_invoked = false;

// note: because these callback are shared among different "publish" calls
// they should be used on different connections

void my_callback_expect_ack(int rc) {
  EXPECT_EQ(0, rc);
  callback_invoked = true;
}

void my_callback_expect_nack(int rc) {
  EXPECT_LT(rc, 0);
  callback_invoked = true;
}

TEST_F(TestAMQP, ReceiveAck)
{
  callback_invoked = false;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1");
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_ack);
  EXPECT_EQ(rc, 0);
  std::this_thread::sleep_for(wait_time);
  EXPECT_TRUE(callback_invoked);
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, ReceiveNack)
{
  callback_invoked = false;
  amqp_mock::REPLY_ACK = false;
  const std::string host("localhost2");
  amqp_mock::set_valid_host(host);
  amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1");
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  std::this_thread::sleep_for(wait_time);
  EXPECT_TRUE(callback_invoked);
  amqp_mock::REPLY_ACK = true;
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, FailWrite)
{
  callback_invoked = false;
  amqp_mock::FAIL_NEXT_WRITE = true;
  const std::string host("localhost2");
  amqp_mock::set_valid_host(host);
  amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1");
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  std::this_thread::sleep_for(wait_time);
  EXPECT_TRUE(callback_invoked);
  amqp_mock::FAIL_NEXT_WRITE = false;
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, ClosedConnection)
{
  callback_invoked = false;
  const auto current_connections = amqp::get_connection_count();
  const std::string host("localhost3");
  amqp_mock::set_valid_host(host);
  amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), current_connections + 1);
  EXPECT_TRUE(amqp::disconnect(conn));
  std::this_thread::sleep_for(wait_time);
  // make sure number of connections decreased back
  EXPECT_EQ(amqp::get_connection_count(), current_connections);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_LT(rc, 0);
  std::this_thread::sleep_for(wait_time);
  EXPECT_FALSE(callback_invoked);
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, RetryInvalidHost)
{
  const std::string host = "192.168.0.1";
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("amqp://"+host, "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
  // now next retry should be ok
  amqp_mock::set_valid_host(host);
  std::this_thread::sleep_for(wait_time);
  rc = amqp::publish(conn, "topic", "message");
  EXPECT_EQ(rc, 0);
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, RetryInvalidPort)
{
  const int port = 9999;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn = amqp::connect("amqp://localhost:" + std::to_string(port), "ex1");
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
  // now next retry should be ok
  amqp_mock::set_valid_port(port);
  std::this_thread::sleep_for(wait_time);
  rc = amqp::publish(conn, "topic", "message");
  EXPECT_EQ(rc, 0);
  amqp_mock::set_valid_port(5672);
}

TEST_F(TestAMQP, RetryFailWrite)
{
  callback_invoked = false;
  amqp_mock::FAIL_NEXT_WRITE = true;
  const std::string host("localhost4");
  amqp_mock::set_valid_host(host);
  amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1");
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  // set port to a different one, so that reconnect would fail
  amqp_mock::set_valid_port(9999);
  std::this_thread::sleep_for(wait_time);
  EXPECT_TRUE(callback_invoked);
  callback_invoked = false;
  rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_LT(rc, 0);
  // expect immediate failure, no callback called after sleep
  std::this_thread::sleep_for(wait_time);
  EXPECT_FALSE(callback_invoked);
  // set port to the right one so that reconnect would succeed
  amqp_mock::set_valid_port(5672);
  callback_invoked = false;
  amqp_mock::FAIL_NEXT_WRITE = false;
  // give time to reconnect
  std::this_thread::sleep_for(wait_time);
  // retry to publish should succeed now
  rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_ack);
  EXPECT_EQ(rc, 0);
  std::this_thread::sleep_for(wait_time);
  EXPECT_TRUE(callback_invoked);
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

