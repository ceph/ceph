// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "rgw/rgw_amqp.h"
#include "amqp_mock.h"
#include <gtest/gtest.h>

using namespace rgw;

TEST(AMQP_Connection, ConnectionOK)
{
  try {
    // create connection for the first time
    EXPECT_EQ(amqp::get_connection_number(), 0U);
    const amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
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
    const amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
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

void my_callback_expect_ack(int rc) {
  EXPECT_EQ(0, rc);
}

TEST(AMQP_PublishAndWait, ReceiveAck)
{
  try {
    // create connection for the first time
    const amqp::connection_t& conn = amqp::connect("amqp://localhost", "ex1");
    auto rc = publish_with_confirm(conn, "topic", "message", my_callback_expect_ack);
    EXPECT_EQ(rc, 0);
  } catch (const amqp::connection_error& e) {
    // make sure exception dont happen
    EXPECT_TRUE(false);
  }
}

