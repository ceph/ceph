// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw_amqp.h"
#include "common/ceph_context.h"
#include "amqp_mock.h"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <atomic>

using namespace rgw;

const std::chrono::milliseconds wait_time(10);
const std::chrono::milliseconds long_wait_time = wait_time*50;
const std::chrono::seconds idle_time(35);


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
  amqp::connection_id_t conn_id;
  unsigned current_dequeued = 0U;

  void SetUp() override {
    ASSERT_TRUE(amqp::init(cct));
  }

  void TearDown() override {
    amqp::shutdown();
  }

  // wait for at least one new (since last drain) message to be dequeued
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

TEST_F(TestAMQP, ConnectionOK)
{
  const auto connection_number = amqp::get_connection_count();
  auto rc = amqp::connect(conn_id, "amqp://localhost", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = amqp::publish(conn_id, "topic", "message");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP, SSLConnectionOK)
{
  const int port = 5671;
  const auto connection_number = amqp::get_connection_count();
  amqp_mock::set_valid_port(port);
  auto rc = amqp::connect(conn_id, "amqps://localhost", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = amqp::publish(conn_id, "topic", "message");
  EXPECT_EQ(rc, 0);
  amqp_mock::set_valid_port(5672);
}

TEST_F(TestAMQP, PlainAndSSLConnectionsOK)
{
  const int port = 5671;
  const auto connection_number = amqp::get_connection_count();
  amqp_mock::set_valid_port(port);
  amqp::connection_id_t conn_id1;
  auto rc = amqp::connect(conn_id1, "amqps://localhost", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = amqp::publish(conn_id1, "topic", "message");
  EXPECT_EQ(rc, 0);
  EXPECT_EQ(amqp::to_string(conn_id1), "amqps://localhost:5671/?exchange=ex1");
  amqp_mock::set_valid_port(5672);
  amqp::connection_id_t conn_id2;
  rc = amqp::connect(conn_id2, "amqp://localhost", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::to_string(conn_id2), "amqp://localhost:5672/?exchange=ex1");
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 2);
  rc = amqp::publish(conn_id2, "topic", "message");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP, ConnectionReuse)
{
  amqp::connection_id_t conn_id1;
  auto rc = amqp::connect(conn_id1, "amqp://localhost", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id2;
  rc = amqp::connect(conn_id2, "amqp://localhost", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  rc = amqp::publish(conn_id1, "topic", "message");
  EXPECT_EQ(rc, 0);
}

TEST_F(TestAMQP, NameResolutionFail)
{
  callback_invoked = false;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://kaboom", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
}

TEST_F(TestAMQP, InvalidPort)
{
  callback_invoked = false;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://localhost:1234", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
}

TEST_F(TestAMQP, InvalidHost)
{
  callback_invoked = false;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://0.0.0.1", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
}

TEST_F(TestAMQP, InvalidVhost)
{
  callback_invoked = false;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://localhost/kaboom", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
}

TEST_F(TestAMQP, UserPassword)
{
  amqp_mock::set_valid_host("127.0.0.1");
  {
    callback_invoked = false;
    const auto connection_number = amqp::get_connection_count();
    amqp::connection_id_t conn_id;
    auto rc = amqp::connect(conn_id, "amqp://foo:bar@127.0.0.1", "ex1", false, false, boost::none);
    EXPECT_TRUE(rc);
    EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
    rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
    EXPECT_EQ(rc, 0);
    wait_until_drained();
    EXPECT_TRUE(callback_invoked);
  }
  // now try the same connection with default user/password
  amqp_mock::set_valid_host("127.0.0.2");
  {
    callback_invoked = false;
    const auto connection_number = amqp::get_connection_count();
    amqp::connection_id_t conn_id;
    auto rc = amqp::connect(conn_id, "amqp://guest:guest@127.0.0.2", "ex1", false, false, boost::none);
    EXPECT_TRUE(rc);
    EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
    rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_ack);
    EXPECT_EQ(rc, 0);
    wait_until_drained();
    EXPECT_TRUE(callback_invoked);
  }
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, URLParseError)
{
  callback_invoked = false;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "http://localhost", "ex1", false, false, boost::none);
  EXPECT_FALSE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
}

TEST_F(TestAMQP, ExchangeMismatch)
{
  callback_invoked = false;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "http://localhost", "ex2", false, false, boost::none);
  EXPECT_FALSE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
}

TEST_F(TestAMQP, MaxConnections)
{
  // fill up all connections
  std::vector<amqp::connection_id_t> connections;
  auto remaining_connections = amqp::get_max_connections() - amqp::get_connection_count();
  while (remaining_connections > 0) {
    const auto host = "127.10.0." + std::to_string(remaining_connections);
    amqp_mock::set_valid_host(host);
    amqp::connection_id_t conn_id;
    auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
    EXPECT_TRUE(rc);
    rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_ack);
    EXPECT_EQ(rc, 0);
    --remaining_connections;
    connections.push_back(conn_id);
  }
  EXPECT_EQ(amqp::get_connection_count(), amqp::get_max_connections());
  wait_until_drained();
  // try to add another connection
  {
    const std::string host = "toomany";
    amqp_mock::set_valid_host(host);
    amqp::connection_id_t conn_id;
    auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
    EXPECT_FALSE(rc);
    rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
    EXPECT_EQ(rc, 0);
    wait_until_drained();
  }
  EXPECT_EQ(amqp::get_connection_count(), amqp::get_max_connections());
  amqp_mock::set_valid_host("localhost");
}


TEST_F(TestAMQP, ReceiveAck)
{
  callback_invoked = false;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_ack);
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
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  const auto NUMBER_OF_CALLS = 2000;
  for (auto i = 0; i < NUMBER_OF_CALLS; ++i) {
    auto rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_close_or_ack);
    EXPECT_EQ(rc, 0);
  }
  wait_until_drained();
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, ReceiveMultipleAck)
{
  callbacks_invoked = 0;
  const std::string host("localhost1");
  amqp_mock::set_valid_host(host);
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  const auto NUMBER_OF_CALLS = 100;
  for (auto i=0; i < NUMBER_OF_CALLS; ++i) {
    auto rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_multiple_acks);
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
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  amqp_mock::set_multiple(59);
  const auto NUMBER_OF_CALLS = 100;
  for (auto i=0; i < NUMBER_OF_CALLS; ++i) {
    rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_multiple_acks);
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
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  amqp_mock::set_multiple(59);
  const auto NUMBER_OF_CALLS = 100;
  for (auto i=0; i < NUMBER_OF_CALLS; ++i) {
    rc = publish_with_confirm(conn_id, "topic", "message",
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
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
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
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  amqp_mock::FAIL_NEXT_WRITE = false;
  callback_invoked = false;
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, RetryInvalidHost)
{
  callback_invoked = false;
  const std::string host = "192.168.0.1";
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://"+host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  // now next retry should be ok
  callback_invoked = false;
  amqp_mock::set_valid_host(host);
  std::this_thread::sleep_for(long_wait_time);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_ack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, RetryInvalidPort)
{
  callback_invoked = false;
  const int port = 9999;
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://localhost:" + std::to_string(port), "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  // now next retry should be ok
  callback_invoked = false;
  amqp_mock::set_valid_port(port);
  std::this_thread::sleep_for(long_wait_time);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_ack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  amqp_mock::set_valid_port(5672);
}

TEST_F(TestAMQP, RetryFailWrite)
{
  callback_invoked = false;
  amqp_mock::FAIL_NEXT_WRITE = true;
  const std::string host("localhost2");
  amqp_mock::set_valid_host(host);
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://" + host, "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  // now next retry should be ok
  amqp_mock::FAIL_NEXT_WRITE = false;
  callback_invoked = false;
  std::this_thread::sleep_for(long_wait_time);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_ack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
  amqp_mock::set_valid_host("localhost");
}

TEST_F(TestAMQP, IdleConnection)
{
  // this test is skipped since it takes 30seconds
  GTEST_SKIP();
  const auto connection_number = amqp::get_connection_count();
  amqp::connection_id_t conn_id;
  auto rc = amqp::connect(conn_id, "amqp://localhost", "ex1", false, false, boost::none);
  EXPECT_TRUE(rc);
  EXPECT_EQ(amqp::get_connection_count(), connection_number + 1);
  std::this_thread::sleep_for(idle_time);
  EXPECT_EQ(amqp::get_connection_count(), connection_number);
  rc = publish_with_confirm(conn_id, "topic", "message", my_callback_expect_nack);
  EXPECT_EQ(rc, 0);
  wait_until_drained();
  EXPECT_TRUE(callback_invoked);
}

