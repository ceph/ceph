// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 UnitedStack <haomai@unitedstack.com>
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <unistd.h>
#include <time.h>
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "msg/Dispatcher.h"
#include "msg/msg_types.h"
#include "msg/Message.h"
#include "msg/Messenger.h"
#include "msg/Connection.h"
#include "messages/MPing.h"

#include <gtest/gtest.h>

#if GTEST_HAS_PARAM_TEST

#define CHECK_AND_WAIT_TRUE(expr) do {  \
  int n = 10;                           \
  while (--n) {                         \
    if (expr)                           \
      break;                            \
    usleep(100);                        \
  }                                     \
} while(0);

class MessengerTest : public ::testing::TestWithParam<const char*> {
 public:
  Messenger *server_msgr;
  Messenger *client_msgr;

  MessengerTest(): server_msgr(NULL), client_msgr(NULL) {}
  virtual void SetUp() {
    cerr << __func__ << " start set up " << GetParam() << std::endl;
    server_msgr = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::OSD(0), "server", getpid());
    client_msgr = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::CLIENT(-1), "client", getpid());
    server_msgr->set_default_policy(Messenger::Policy::stateless_server(0, 0));
    client_msgr->set_default_policy(Messenger::Policy::lossy_client(0, 0));
  }
  virtual void TearDown() {
    delete server_msgr;
    delete client_msgr;
  }

};


class FakeDispatcher : public Dispatcher {
 public:
  struct Session : public RefCountedObject {
    Mutex lock;
    uint64_t count;
    ConnectionRef con;

    Session(ConnectionRef c): RefCountedObject(g_ceph_context), lock("FakeDispatcher::Session::lock"), count(0), con(c) {
    }
    uint64_t get_count() {return count;}
  };

  Mutex lock;
  Cond cond;
  bool is_server;
  bool got_new;
  bool got_remote_reset;
  bool got_connect;

  FakeDispatcher(bool s): Dispatcher(g_ceph_context), lock("FakeDispatcher::lock"),
                          is_server(s), got_new(false), got_remote_reset(false),
                          got_connect(false) {}
  bool ms_can_fast_dispatch_any() const { return true; }
  bool ms_can_fast_dispatch(Message *m) const {
    switch (m->get_type()) {
    case CEPH_MSG_PING:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) {
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (!s) {
      s = new Session(con);
      con->set_priv(s->get());
      cerr << __func__ << " con: " << con << " count: " << s->count << std::endl;
    }
    s->put();
    lock.Lock();
    got_connect = true;
    cond.Signal();
    lock.Unlock();
  }
  void ms_handle_fast_accept(Connection *con) {
    Session *s = static_cast<Session*>(con->get_priv());
    if (!s) {
      s = new Session(con);
      con->set_priv(s->get());
    }
    s->put();
  }
  bool ms_dispatch(Message *m) {
    Session *s = static_cast<Session*>(m->get_connection()->get_priv());
    if (!s) {
      s = new Session(m->get_connection());
      m->get_connection()->set_priv(s->get());
    }
    s->put();
    Mutex::Locker l(s->lock);
    s->count++;
    cerr << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    if (is_server)
      reply_message(m);
    lock.Lock();
    got_new = true;
    cond.Signal();
    lock.Unlock();
    return true;
  }
  bool ms_handle_reset(Connection *con) {
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (s) {
      s->con.reset(NULL);  // break con <-> session ref cycle
      con->set_priv(NULL);   // break ref <-> session cycle, if any
      s->put();
    }
    return true;
  }
  void ms_handle_remote_reset(Connection *con) {
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (s) {
      Mutex::Locker l(s->lock);
      s->con.reset(NULL);  // break con <-> session ref cycle
      con->set_priv(NULL);   // break ref <-> session cycle, if any
      s->put();
    }
    got_remote_reset = true;
  }
  void ms_fast_dispatch(Message *m) {
    Session *s = static_cast<Session*>(m->get_connection()->get_priv());
    if (!s) {
      s = new Session(m->get_connection());
      m->get_connection()->set_priv(s->get());
    }
    s->put();
    Mutex::Locker (s->lock);
    s->count++;
    cerr << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    if (is_server)
      reply_message(m);
    lock.Lock();
    got_new = true;
    cond.Signal();
    lock.Unlock();
  }
  bool ms_verify_authorizer(Connection *con, int peer_type, int protocol,
                            bufferlist& authorizer, bufferlist& authorizer_reply,
                            bool& isvalid, CryptoKey& session_key) {
    isvalid = true;
    return true;
  }


  void reply_message(Message *m) {
    MPing *rm = new MPing();
    m->get_connection()->send_message(rm);
  }
};

typedef FakeDispatcher::Session Session;

TEST_P(MessengerTest, SimpleTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. simple round trip
  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_TRUE((static_cast<Session*>(conn->get_priv()))->get_count() == 1);
  ASSERT_TRUE(conn->peer_is_osd());

  // 2. test rebind port
  set<int> avoid_ports;
  for (int i = 0; i < 10 ; i++)
    avoid_ports.insert(server_msgr->get_myaddr().get_port() + i);
  server_msgr->rebind(avoid_ports);
  ASSERT_TRUE(avoid_ports.count(server_msgr->get_myaddr().get_port()) == 0);

  conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);

  // 3. test markdown connection
  conn->mark_down();
  ASSERT_FALSE(conn->is_connected());

  // 4. test failed connection
  server_msgr->shutdown();
  server_msgr->wait();

  m = new MPing();
  conn->send_message(m);
  CHECK_AND_WAIT_TRUE(!conn->is_connected());
  ASSERT_FALSE(conn->is_connected());

  // 5. loopback connection
  conn = client_msgr->get_loopback_connection();
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  client_msgr->shutdown();
  client_msgr->wait();
}

TEST_P(MessengerTest, NameAddrTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  ASSERT_TRUE(conn->get_peer_addr() == server_msgr->get_myaddr());
  ConnectionRef server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  // Make should server_conn is the one we already accepted from client,
  // so it means client_msgr has the same addr when server connection has
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, FeatureTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1");
  uint64_t all_feature_supported, feature_required, feature_supported = 0;
  for (int i = 0; i < 10; i++)
    feature_supported |= 1ULL << i;
  feature_required = feature_supported | 1ULL << 13;
  all_feature_supported = feature_required | 1ULL << 14;

  Messenger::Policy p = server_msgr->get_policy(entity_name_t::TYPE_CLIENT);
  p.features_required = feature_required;
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  // 1. Suppose if only support less than required
  p = client_msgr->get_policy(entity_name_t::TYPE_OSD);
  p.features_supported = feature_supported;
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  conn->send_message(m);
  CHECK_AND_WAIT_TRUE(!conn->is_connected());
  // should failed build a connection
  ASSERT_FALSE(conn->is_connected());

  client_msgr->shutdown();
  client_msgr->wait();

  // 2. supported met required
  p = client_msgr->get_policy(entity_name_t::TYPE_OSD);
  p.features_supported = all_feature_supported;
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);
  client_msgr->start();

  conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, StatefulTest) {
  Message *m;
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateful_server(0, 0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossless_client(0, 0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. test for server standby
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  conn->mark_down();
  ASSERT_FALSE(conn->is_connected());
  ConnectionRef server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  // don't lose state
  ASSERT_TRUE(static_cast<Session*>(server_conn->get_priv())->get_count() == 1);

  conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  ASSERT_TRUE(static_cast<Session*>(server_conn->get_priv())->get_count() == 1);

  // 2. test for client reconnect
  ASSERT_FALSE(cli_dispatcher.got_remote_reset);
  cli_dispatcher.got_connect = false;
  server_conn->mark_down();
  ASSERT_FALSE(server_conn->is_connected());
  // ensure client detect server socket closed
  {
    Mutex::Locker l(cli_dispatcher.lock);
    while (!cli_dispatcher.got_connect)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_connect = false;
  }
  CHECK_AND_WAIT_TRUE(conn->is_connected());
  ASSERT_TRUE(conn->is_connected());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_TRUE(conn->is_connected());
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  // resetcheck happen
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  ASSERT_TRUE(static_cast<Session*>(server_conn->get_priv())->get_count() == 1);
  ASSERT_TRUE(cli_dispatcher.got_remote_reset);
  cli_dispatcher.got_remote_reset = false;

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, StatelessTest) {
  Message *m;
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateless_server(0, 0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossy_client(0, 0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. test for server lose state
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  conn->mark_down();
  ASSERT_FALSE(conn->is_connected());

  conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  ConnectionRef server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  // server lose state
  ASSERT_TRUE(static_cast<Session*>(server_conn->get_priv())->get_count() == 1);

  // 2. test for client lossy
  server_conn->mark_down();
  ASSERT_FALSE(server_conn->is_connected());
  CHECK_AND_WAIT_TRUE(!conn->is_connected());
  ASSERT_FALSE(conn->is_connected());
  conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, ClientStandbyTest) {
  Message *m;
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateful_server(0, 0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossless_peer(0, 0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. test for client standby, resetcheck
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  ConnectionRef server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  ASSERT_FALSE(cli_dispatcher.got_remote_reset);
  server_conn->mark_down();
  ASSERT_FALSE(server_conn->is_connected());
  // client should be standby
  usleep(300*1000);
  // client should be standby, so we use original connection
  {
    m = new MPing();
    conn->send_keepalive();
    CHECK_AND_WAIT_TRUE(conn->is_connected());
    ASSERT_TRUE(conn->is_connected());
    Mutex::Locker l(cli_dispatcher.lock);
    ASSERT_EQ(conn->send_message(m), 0);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  // resetcheck for client, so it discard state previously
  ASSERT_TRUE(cli_dispatcher.got_remote_reset);
  cli_dispatcher.got_remote_reset = false;
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  ASSERT_TRUE(static_cast<Session*>(server_conn->get_priv())->get_count() == 1);

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

INSTANTIATE_TEST_CASE_P(
  Messenger,
  MessengerTest,
  ::testing::Values(
    "async",
    "simple"
  )
);

#else

// Google Test may not support value-parameterized tests with some
// compilers. If we use conditional compilation to compile out all
// code referring to the gtest_main library, MSVC linker will not link
// that library at all and consequently complain about missing entry
// point defined in that library (fatal error LNK1561: entry point
// must be defined). This dummy test keeps gtest_main linked in.
TEST(DummyTest, ValueParameterizedTestsAreNotSupportedOnThisPlatform) {}

#endif


int main(int argc, char **argv) {
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->set_val("auth_cluster_required", "none");
  g_ceph_context->_conf->set_val("auth_service_required", "none");
  g_ceph_context->_conf->set_val("auth_client_required", "none");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make ceph_test_msgr && ./ceph_test_msgr
 *
 * End:
 */
