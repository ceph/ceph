// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_amqp.h"
#include "amqp_mock.h"
#include <gtest/gtest.h>
#include <chrono>
#include <thread>
#include <atomic>

using namespace rgw;

TEST(AMQP_Connection, ConnectionOK)
{
  try {
    // create connection for the first time
    EXPECT_EQ(amqp::get_connection_number(), 0U);
    amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
    EXPECT_EQ(amqp::get_connection_number(), 1U);
    auto rc = amqp::publish(conn, "topic", "message");
    EXPECT_EQ(rc, 0);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happen
    EXPECT_TRUE(false);
  }
}

TEST(AMQP_Connection, ConnectionReuse)
{
  try {
    // reuse the connection
    EXPECT_EQ(amqp::get_connection_number(), 1U);
    amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
    EXPECT_EQ(amqp::get_connection_number(), 1U);
    auto rc = amqp::publish(conn, "topic", "message");
    EXPECT_EQ(rc, 0);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happen
    EXPECT_TRUE(false);
  }
}

TEST(AMQP_Connection, NameResolutionFail)
{
  try {
    amqp::connect("amqp://kaboom", "ex1");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
}

TEST(AMQP_Connection, InvalidPort)
{
  try {
    amqp::connect("amqp://localhost:5677", "ex1");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
}

TEST(AMQP_Connection, InvalidHost)
{
  try {
    amqp::connect("amqp://0.0.0.1", "ex1");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
}

TEST(AMQP_Connection, InvalidVhost)
{
  try {
    amqp::connect("amqp://localhost/kaboom", "ex1");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
}

TEST(AMQP_Connection, UserPassword)
{
  amqp_mock::VALID_HOST = "127.0.0.1";
  try {
    amqp::connect("amqp://foo:bar@127.0.0.1", "ex1");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
  // now try the same connection with default user/password
  try {
    amqp::connect("amqp://guest:guest@127.0.0.1", "ex1");
    EXPECT_EQ(amqp::get_connection_number(), 2U);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happens
    EXPECT_TRUE(false);
  }
  amqp_mock::VALID_HOST = "localhost";
}

TEST(AMQP_Connection, URLParseError)
{
  try {
    amqp::connect("http://localhost", "ex1");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
}

TEST(AMQP_Connection, ExchangeNotThere)
{
  try {
    amqp::connect("amqp://127.0.0.1", "kaboom");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
}

TEST(AMQP_Connection, ExchangeMismatch)
{
  try {
    amqp::connect("amqp://localhost", "ex2");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
}

TEST(AMQP_Connection, MaxConnections)
{
  // fill up all connections
  auto remaining_connections = amqp::get_max_connections() - amqp::get_connection_number();
  while (remaining_connections > 0) {
    amqp_mock::VALID_HOST = "127.10.0." + std::to_string(remaining_connections);
    try {
      amqp::connect("amqp://" + amqp_mock::VALID_HOST, "ex1");
    } catch (const amqp::connection_error& e) {
      // make sure no exception happens
      EXPECT_TRUE(false);
    }
    --remaining_connections;
  }
  EXPECT_EQ(amqp::get_connection_number(), amqp::get_max_connections());
  // try to add another connection
  amqp_mock::VALID_HOST = "toomany";
  try {
    amqp::connect("amqp://" + amqp_mock::VALID_HOST, "ex1");
    // make sure exception happens
    EXPECT_TRUE(false);
  } catch (const amqp::connection_error& e) {
    EXPECT_STRNE("", e.what());
  }
  EXPECT_EQ(amqp::get_connection_number(), amqp::get_max_connections());
  amqp_mock::VALID_HOST = "localhost";
}

std::atomic<bool> callback_invoked = false;

void my_callback_expect_ack(int rc) {
  EXPECT_EQ(0, rc);
  callback_invoked = true;
}

void my_callback_expect_nack(int rc) {
  EXPECT_LT(rc, 0);
  callback_invoked = true;
}

std::chrono::milliseconds wait_time(500);

TEST(AMQP_PublishAndWait, ReceiveAck)
{
  callback_invoked = false;
  try {
    // create connection for the first time
    amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_ack);
    EXPECT_EQ(rc, 0);
    std::this_thread::sleep_for(wait_time);
    EXPECT_TRUE(callback_invoked);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happen
    EXPECT_TRUE(false);
  }
  callback_invoked = false;
}

TEST(AMQP_PublishAndWait, ReceiveNack)
{
  callback_invoked = false;
  amqp_mock::REPLY_ACK = false;
  try {
    // create connection for the first time
    amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
    EXPECT_EQ(rc, 0);
    std::this_thread::sleep_for(wait_time);
    EXPECT_TRUE(callback_invoked);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happen
    EXPECT_TRUE(false);
  }
  amqp_mock::REPLY_ACK = true;
  callback_invoked = false;
}

TEST(AMQP_PublishAndWait, FailWrite)
{
  callback_invoked = false;
  amqp_mock::FAIL_NEXT_WRITE = true;
  try {
    // create connection for the first time
    amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
    EXPECT_EQ(rc, 0);
    std::this_thread::sleep_for(wait_time);
    EXPECT_TRUE(callback_invoked);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happen
    EXPECT_TRUE(false);
  }
  amqp_mock::FAIL_NEXT_WRITE = false;
  callback_invoked = false;
}

TEST(AMQP_PublishAndWait, ClosedConnection)
{
  callback_invoked = false;
  try {
    const auto current_connections = amqp::get_connection_number();;
    const std::string url("amqp://localhost");
    // get one of the existing connections
    amqp::connection_t& conn = amqp::connect(url, "ex1");
    EXPECT_EQ(amqp::get_connection_number(), current_connections);
    amqp::disconnect(conn);
    std::this_thread::sleep_for(wait_time);
    // make sure number of connections decreased
    EXPECT_EQ(amqp::get_connection_number(), current_connections - 1);
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_nack);
    EXPECT_LT(rc, 0);
    std::this_thread::sleep_for(wait_time);
    EXPECT_FALSE(callback_invoked);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happen
    EXPECT_TRUE(false);
  }
  callback_invoked = false;
}

