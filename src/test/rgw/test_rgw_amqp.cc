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

const std::chrono::milliseconds wait_time(10);
const std::chrono::milliseconds long_wait_time = wait_time*50;


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
  amqp::connection_ptr_t conn = nullptr;
  unsigned current_dequeued = 0U;

  void SetUp() override {
    ASSERT_TRUE(amqp::init(cct));
  }

  void TearDown() override {
    amqp::shutdown();
  }

  // wait for at least one new (since last drain) message to be dequeueud
  // and then wait for all pending answers to be received
  void wait_until_drained() {  
    while (amqp::get_dequeued() == current_dequeued) {
      std::this_thread::sleep_for(wait_time);
    }
    while (amqp::get_inflight() > 0) {
      std::this_thread::sleep_for(wait_time);
    }
    current_dequeued = amqp::get_dequeued();
  }
};

TEST_F(TestAMQP, ConnectionOK)
{
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("amqp://localhost", "ex1", false);
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP, ConnectionReuse)
{
  amqp::connection_ptr_t conn1 = amqp::connect("amqp://localhost", "ex1", false);
  EXPECT_TRUE(conn1);
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_ptr_t conn2 = amqp::connect("amqp://localhost", "ex1", false);
  EXPECT_TRUE(conn2);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  auto rc = amqp::publish(conn1, "topic", "message");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP, NameResolutionFail)
{
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("amqp://kaboom", "ex1", false);
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, InvalidPort)
{
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("amqp://localhost:1234", "ex1", false);
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, InvalidHost)
{
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("amqp://0.0.0.1", "ex1", false);
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, InvalidVhost)
{
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("amqp://localhost/kaboom", "ex1", false);
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
    conn = amqp::connect("amqp://foo:bar@127.0.0.1", "ex1", false);
    EXPECT_TRUE(conn);
    EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
    auto rc = amqp::publish(conn, "topic", "message");
    EXPECT_LT(rc, 0);
  }
  // now try the same connection with default user/password
  amqp_mock::set_valid_host("127.0.0.2");
  {
    const auto connection_number = amqp::get_connection_count();
    conn = amqp::connect("amqp://guest:guest@127.0.0.2", "ex1", false);
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
  conn = amqp::connect("http://localhost", "ex1", false);
  EXPECT_FALSE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
}

TEST_F(TestAMQP, ExchangeMismatch)
{
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("http://localhost", "ex2", false);
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
    amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1", false);
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
    amqp::connection_ptr_t conn = amqp::connect("amqp://" + host, "ex1", false);
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
  std::this_thread::sleep_for(long_wait_time);
  EXPECT_LT(amqp::get_connection_count(), amqp::get_max_connections());
}

std::atomic<bool> callback_invoked = false;

std::atomic<int> callbacks_invoked = 0;

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

void my_callback_expect_multiple_acks(int rc) {
  EXPECT_EQ(0, rc);
  ++callbacks_invoked;
}

class dynamic_callback_wrapper {
    dynamic_callback_wrapper() = default;
public:
    static dynamic_callback_wrapper* create() {
        return new dynamic_callback_wrapper;
    }
    void callback(int rc) {
      EXPECT_EQ(0, rc);
      ++callbacks_invoked;
      delete this;
    }
};

void my_callback_expect_close_or_ack(int rc) {
  // deleting the connection should trigger the callback with -4098
  // but due to race conditions, some my get an ack
  EXPECT_TRUE(-4098 == rc || 0 == rc);
}

TEST_F(TestAMQP, ReceiveAck)
{
  callback_invoked = false;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_ack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, ImplicitConnectionClose)
{
  callback_invoked = false;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  const auto NUMBER_OF_CALLS = 2000;
  for (auto i = 0; i < NUMBER_OF_CALLS; ++i) {
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_close_or_ack);
    EXPECT_EQ(rc, 0);
  }
  wait_until_drained();
  // deleting the connection object should close the connection
  conn.reset(nullptr);
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, ReceiveMultipleAck)
{
  callbacks_invoked = 0;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  const auto NUMBER_OF_CALLS = 100;
  for (auto i=0; i < NUMBER_OF_CALLS; ++i) {
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_multiple_acks);
    EXPECT_EQ(rc, 0);
  }
  wait_until_drained();
  EXPECT_EQ(callbacks_invoked, NUMBER_OF_CALLS);
  callbacks_invoked = 0;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, ReceiveAckForMultiple)
{
  callbacks_invoked = 0;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  amqp_mock::set_multiple(59);
  const auto NUMBER_OF_CALLS = 100;
  for (auto i=0; i < NUMBER_OF_CALLS; ++i) {
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_multiple_acks);
    EXPECT_EQ(rc, 0);
  }
  wait_until_drained();
  EXPECT_EQ(callbacks_invoked, NUMBER_OF_CALLS);
  callbacks_invoked = 0;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, DynamicCallback)
{
  callbacks_invoked = 0;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  amqp_mock::set_multiple(59);
  const auto NUMBER_OF_CALLS = 100;
  for (auto i=0; i < NUMBER_OF_CALLS; ++i) {
    auto rc = publish_with_confirm(conn, "topic", "message",
            std::bind(&dynamic_callback_wrapper::callback, dynamic_callback_wrapper::create(), std::placeholders::_1));
    EXPECT_EQ(rc, 0);
  }
  wait_until_drained();
  EXPECT_EQ(callbacks_invoked, NUMBER_OF_CALLS);
  callbacks_invoked = 0;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, ReceiveNack)
{
  callback_invoked = false;
  amqp_mock::REPLY_ACK = false;
  const std::string host("localhost2");
  amqp_mock::set_valid_host(host);
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
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
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
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
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), current_connections + 1);
  EXPECT_TRUE(amqp::disconnect(conn));
  std::this_thread::sleep_for(long_wait_time);
  // make sure number of connections decreased back
  EXPECT_EQ(amqp::get_connection_count(), current_connections);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_LT(rc, 0);
  std::this_thread::sleep_for(long_wait_time);
  EXPECT_FALSE(callback_invoked);
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, RetryInvalidHost)
{
  const std::string host = "192.168.0.1";
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("amqp://"+host, "ex1", false);
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
  // now next retry should be ok
  amqp_mock::set_valid_host(host);
  std::this_thread::sleep_for(long_wait_time);
  rc = amqp::publish(conn, "topic", "message");
  EXPECT_EQ(rc, 0);
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, RetryInvalidPort)
{
  const int port = 9999;
  const auto connection_number = amqp::get_connection_count();
  conn = amqp::connect("amqp://localhost:" + std::to_string(port), "ex1", false);
  EXPECT_TRUE(conn);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  auto rc = amqp::publish(conn, "topic", "message");
  EXPECT_LT(rc, 0);
  // now next retry should be ok
  amqp_mock::set_valid_port(port);
  std::this_thread::sleep_for(long_wait_time);
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
  conn = amqp::connect("amqp://" + host, "ex1", false);
  EXPECT_TRUE(conn);
  auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  // set port to a different one, so that reconnect would fail
  amqp_mock::set_valid_port(9999);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  callback_invoked = false;
  rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
  EXPECT_LT(rc, 0);
  // expect immediate failure, no callback called after sleep
  std::this_thread::sleep_for(long_wait_time);
  EXPECT_FALSE(callback_invoked);
  // set port to the right one so that reconnect would succeed
  amqp_mock::set_valid_port(5672);
  callback_invoked = false;
  amqp_mock::FAIL_NEXT_WRITE = false;
  // give time to reconnect
  std::this_thread::sleep_for(long_wait_time);
  // retry to publish should succeed now
  rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_ack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

