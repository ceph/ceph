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
#include <stdlib.h>
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
#include "messages/MCommand.h"

#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/binomial_distribution.hpp>
#include <gtest/gtest.h>

typedef boost::mt11213b gen_type;

#if GTEST_HAS_PARAM_TEST

#define CHECK_AND_WAIT_TRUE(expr) do {  \
  int n = 1000;                         \
  while (--n) {                         \
    if (expr)                           \
      break;                            \
    usleep(1000);                       \
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
    uint64_t get_count() { return count; }
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
    lock.Lock();
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (!s) {
      s = new Session(con);
      con->set_priv(s->get());
      cerr << __func__ << " con: " << con << " count: " << s->count << std::endl;
    }
    s->put();
    got_connect = true;
    cond.Signal();
    lock.Unlock();
  }
  void ms_handle_fast_accept(Connection *con) {
    Mutex::Locker l(lock);
    Session *s = static_cast<Session*>(con->get_priv());
    if (!s) {
      s = new Session(con);
      con->set_priv(s->get());
    }
    s->put();
  }
  bool ms_dispatch(Message *m) {
    Mutex::Locker l(lock);
    Session *s = static_cast<Session*>(m->get_connection()->get_priv());
    if (!s) {
      s = new Session(m->get_connection());
      m->get_connection()->set_priv(s->get());
    }
    s->put();
    Mutex::Locker l1(s->lock);
    s->count++;
    cerr << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    if (is_server) {
      reply_message(m);
    }
    got_new = true;
    cond.Signal();
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) {
    Mutex::Locker l(lock);
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
    Mutex::Locker l(lock);
    cerr << __func__ << con << std::endl;
    Session *s = static_cast<Session*>(con->get_priv());
    if (s) {
      s->con.reset(NULL);  // break con <-> session ref cycle
      con->set_priv(NULL);   // break ref <-> session cycle, if any
      s->put();
    }
    got_remote_reset = true;
  }
  void ms_fast_dispatch(Message *m) {
    Mutex::Locker l(lock);
    Session *s = static_cast<Session*>(m->get_connection()->get_priv());
    if (!s) {
      s = new Session(m->get_connection());
      m->get_connection()->set_priv(s->get());
    }
    s->put();
    Mutex::Locker l1(s->lock);
    s->count++;
    cerr << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << std::endl;
    if (is_server) {
      reply_message(m);
    }
    got_new = true;
    cond.Signal();
    m->put();
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    ASSERT_TRUE(conn->is_connected());
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
  conn->send_keepalive();
  CHECK_AND_WAIT_TRUE(!conn->is_connected());
  ASSERT_FALSE(conn->is_connected());
  conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
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
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  ConnectionRef server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  ASSERT_FALSE(cli_dispatcher.got_remote_reset);
  cli_dispatcher.got_connect = false;
  server_conn->mark_down();
  ASSERT_FALSE(server_conn->is_connected());
  // client should be standby
  usleep(300*1000);
  // client should be standby, so we use original connection
  {
    conn->send_keepalive();
    {
      Mutex::Locker l(cli_dispatcher.lock);
      while (!cli_dispatcher.got_remote_reset)
        cli_dispatcher.cond.Wait(cli_dispatcher.lock);
      cli_dispatcher.got_remote_reset = false;
      while (!cli_dispatcher.got_connect)
        cli_dispatcher.cond.Wait(cli_dispatcher.lock);
      cli_dispatcher.got_connect = false;
    }
    CHECK_AND_WAIT_TRUE(conn->is_connected());
    ASSERT_TRUE(conn->is_connected());
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(static_cast<Session*>(conn->get_priv())->get_count() == 1);
  server_conn = server_msgr->get_connection(client_msgr->get_myinst());
  ASSERT_TRUE(static_cast<Session*>(server_conn->get_priv())->get_count() == 1);

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, AuthTest) {
  g_ceph_context->_conf->set_val("auth_cluster_required", "cephx");
  g_ceph_context->_conf->set_val("auth_service_required", "cephx");
  g_ceph_context->_conf->set_val("auth_client_required", "cephx");
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. simple auth round trip
  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_TRUE((static_cast<Session*>(conn->get_priv()))->get_count() == 1);

  // 2. mix auth
  g_ceph_context->_conf->set_val("auth_cluster_required", "none");
  g_ceph_context->_conf->set_val("auth_service_required", "none");
  g_ceph_context->_conf->set_val("auth_client_required", "none");
  conn->mark_down();
  ASSERT_FALSE(conn->is_connected());
  conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    MPing *m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    Mutex::Locker l(cli_dispatcher.lock);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.Wait(cli_dispatcher.lock);
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_TRUE((static_cast<Session*>(conn->get_priv()))->get_count() == 1);

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

class SyntheticDispatcher : public Dispatcher {
 public:
  Mutex lock;
  Cond cond;
  bool is_server;
  bool got_new;
  bool got_remote_reset;
  bool got_connect;
  map<ConnectionRef, list<uint64_t> > conn_sent;
  map<uint64_t, bufferlist> sent;
  atomic_t index;

  SyntheticDispatcher(bool s): Dispatcher(g_ceph_context), lock("SyntheticDispatcher::lock"),
                          is_server(s), got_new(false), got_remote_reset(false),
                          got_connect(false), index(0) {}
  bool ms_can_fast_dispatch_any() const { return true; }
  bool ms_can_fast_dispatch(Message *m) const {
    switch (m->get_type()) {
    case CEPH_MSG_PING:
    case MSG_COMMAND:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) {
    lock.Lock();
    got_connect = true;
    cond.Signal();
    lock.Unlock();
  }
  void ms_handle_fast_accept(Connection *con) { }
  bool ms_dispatch(Message *m) {
    assert(0);
  }
  bool ms_handle_reset(Connection *con) {
    return true;
  }
  void ms_handle_remote_reset(Connection *con) {
    Mutex::Locker l(lock);
    list<uint64_t> c = conn_sent[con];
    for (list<uint64_t>::iterator it = c.begin();
         it != c.end(); ++it)
      sent.erase(*it);
    conn_sent.erase(con);
    got_remote_reset = true;
  }
  void ms_fast_dispatch(Message *m) {
    Mutex::Locker l(lock);
    if (is_server) {
      reply_message(m);
    } else if (m->get_middle().length()) {
      bufferlist middle = m->get_middle();
      uint64_t i;
      ASSERT_EQ(sizeof(uint64_t), middle.length());
      memcpy(&i, middle.c_str(), middle.length());
      if (sent.count(i)) {
        ASSERT_EQ(conn_sent[m->get_connection()].front(), i);
        ASSERT_TRUE(m->get_data().contents_equal(sent[i]));
        conn_sent[m->get_connection()].pop_front();
        sent.erase(i);
      }
    }
    got_new = true;
    cond.Signal();
    m->put();
  }

  bool ms_verify_authorizer(Connection *con, int peer_type, int protocol,
                            bufferlist& authorizer, bufferlist& authorizer_reply,
                            bool& isvalid, CryptoKey& session_key) {
    isvalid = true;
    return true;
  }

  void reply_message(Message *m) {
    MPing *rm = new MPing();
    if (m->get_data_len())
      rm->set_data(m->get_data());
    if (m->get_middle().length())
      rm->set_middle(m->get_middle());
    m->get_connection()->send_message(rm);
  }

  void send_message_wrap(ConnectionRef con, Message *m) {
    {
      Mutex::Locker l(lock);
      bufferlist bl;
      uint64_t i = index.read();
      index.inc();
      bufferptr bp(sizeof(i));
      memcpy(bp.c_str(), (char*)&i, sizeof(i));
      bl.push_back(bp);
      m->set_middle(bl);
      sent[i] = m->get_data();
      conn_sent[con].push_back(i);
    }
    ASSERT_EQ(con->send_message(m), 0);
  }

  uint64_t get_pending() {
    Mutex::Locker l(lock);
    return sent.size();
  }

  void clear_pending(ConnectionRef con) {
    Mutex::Locker l(lock);

    for (list<uint64_t>::iterator it = conn_sent[con].begin();
         it != conn_sent[con].end(); ++it)
      sent.erase(*it);
    conn_sent.erase(con);
  }
};


TEST_P(MessengerTest, MessageTest) {
  SyntheticDispatcher cli_dispatcher(false), srv_dispatcher(true);
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


  // 1. A very large "front"(as well as "payload")
  // Because a external message need to invade Messenger::decode_message,
  // here we only use existing message class(MCommand)
  ConnectionRef conn = client_msgr->get_connection(server_msgr->get_myinst());
  {
    uuid_d uuid;
    uuid.generate_random();
    vector<string> cmds;
    string s("abcdefghijklmnopqrstuvwxyz");
    for (int i = 0; i < 1024*30; i++)
      cmds.push_back(s);
    MCommand *m = new MCommand(uuid);
    m->cmd = cmds;
    ASSERT_EQ(conn->send_message(m), 0);
    utime_t t;
    t += 1000*1000*500;
    Mutex::Locker l(cli_dispatcher.lock);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.WaitInterval(g_ceph_context, cli_dispatcher.lock, t);
    ASSERT_TRUE(cli_dispatcher.got_new);
    cli_dispatcher.got_new = false;
  }

  // 2. A very large "data"
  {
    bufferlist bl;
    string s("abcdefghijklmnopqrstuvwxyz");
    for (int i = 0; i < 1024*30; i++)
      bl.append(s);
    MPing *m = new MPing();
    m->set_data(bl);
    cli_dispatcher.send_message_wrap(conn, m);
    utime_t t;
    t += 1000*1000*500;
    Mutex::Locker l(cli_dispatcher.lock);
    while (!cli_dispatcher.got_new)
      cli_dispatcher.cond.WaitInterval(g_ceph_context, cli_dispatcher.lock, t);
    ASSERT_TRUE(cli_dispatcher.got_new);
    cli_dispatcher.got_new = false;
  }
  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}


class SyntheticWorkload {
  Mutex lock;
  Cond cond;
  set<Messenger*> available_servers;
  set<Messenger*> available_clients;
  map<pair<Messenger*, Messenger*>, ConnectionRef> available_connections;
  SyntheticDispatcher cli_dispatcher, srv_dispatcher;
  gen_type rng;
  vector<bufferlist> rand_data;

 public:
  static const unsigned max_in_flight = 64;
  static const unsigned max_connections = 128;
  static const unsigned max_message_len = 1024 * 1024 * 4;

  SyntheticWorkload(int servers, int clients, string type, int random_num,
                    Messenger::Policy srv_policy, Messenger::Policy cli_policy):
      lock("SyntheticWorkload::lock"), cli_dispatcher(false), srv_dispatcher(true),
      rng(time(NULL)) {
    Messenger *msgr;
    int base_port = 16800;
    entity_addr_t bind_addr;
    char addr[64];
    for (int i = 0; i < servers; ++i) {
      msgr = Messenger::create(g_ceph_context, type, entity_name_t::OSD(0),
                               "server", getpid()+i);
      snprintf(addr, sizeof(addr), "127.0.0.1:%d", base_port+i);
      bind_addr.parse(addr);
      msgr->bind(bind_addr);
      msgr->add_dispatcher_head(&srv_dispatcher);

      assert(msgr);
      msgr->set_default_policy(srv_policy);
      available_servers.insert(msgr);
      msgr->start();
    }

    for (int i = 0; i < clients; ++i) {
      msgr = Messenger::create(g_ceph_context, type, entity_name_t::CLIENT(-1),
                               "client", getpid()+i+servers);
      if (cli_policy.standby) {
        snprintf(addr, sizeof(addr), "127.0.0.1:%d", base_port+i+servers);
        bind_addr.parse(addr);
        msgr->bind(bind_addr);
      }
      msgr->add_dispatcher_head(&cli_dispatcher);

      assert(msgr);
      msgr->set_default_policy(cli_policy);
      available_clients.insert(msgr);
      msgr->start();
    }

    for (int i = 0; i < random_num; i++) {
      bufferlist bl;
      boost::uniform_int<> u(32, max_message_len);
      uint64_t value_len = u(rng);
      bufferptr bp(value_len);
      bp.zero();
      for (uint64_t j = 0; j < value_len-sizeof(i); ) {
        memcpy(bp.c_str()+j, &i, sizeof(i));
        j += 4096;
      }

      bl.append(bp);
      rand_data.push_back(bl);
    }
  }

  ConnectionRef _get_random_connection(pair<Messenger*, Messenger*> *p) {
    while (cli_dispatcher.get_pending() > max_in_flight)
      usleep(500);
    assert(lock.is_locked());
    boost::uniform_int<> choose(0, available_connections.size() - 1);
    int index = choose(rng);
    map<pair<Messenger*, Messenger*>, ConnectionRef>::iterator i = available_connections.begin();
    for (; index > 0; --index, ++i) ;
    if (p)
      *p = i->first;
    return i->second;
  }

  bool can_create_connection() {
    return available_connections.size() < max_connections;
  }

  void generate_connection() {
    Mutex::Locker l(lock);
    if (!can_create_connection())
      return ;

    Messenger *server, *client;
    {
      boost::uniform_int<> choose(0, available_servers.size() - 1);
      int index = choose(rng);
      set<Messenger*>::iterator i = available_servers.begin();
      for (; index > 0; --index, ++i) ;
      server = *i;
    }
    {
      boost::uniform_int<> choose(0, available_clients.size() - 1);
      int index = choose(rng);
      set<Messenger*>::iterator i = available_clients.begin();
      for (; index > 0; --index, ++i) ;
      client = *i;
    }

    if (!available_connections.count(make_pair(client, server))) {
      ConnectionRef conn = client->get_connection(server->get_myinst());
      available_connections[make_pair(client, server)] = conn;
    }
  }

  void send_message() {
    Message *m = new MPing();
    bufferlist bl;
    boost::uniform_int<> u(0, rand_data.size()-1);
    uint64_t index = u(rng);
    bl = rand_data[index];
    m->set_data(bl);
    Mutex::Locker l(lock);
    ConnectionRef conn = _get_random_connection(NULL);
    cli_dispatcher.send_message_wrap(conn, m);
  }

  void drop_connection() {
    pair<Messenger*, Messenger*> p;
    Mutex::Locker l(lock);
    if (available_connections.size() < 10)
      return;
    ConnectionRef conn = _get_random_connection(&p);
    cli_dispatcher.clear_pending(conn);
    conn->mark_down();
    ASSERT_EQ(available_connections.erase(p), 1U);
  }

  void print_internal_state() {
    Mutex::Locker l(lock);
    cerr << "available_connections: " << available_connections.size()
         << " inflight messages: " << cli_dispatcher.get_pending() << std::endl;
  }

  void wait_for_done() {
    uint64_t i = 0;
    while (cli_dispatcher.get_pending()) {
      usleep(1000*100);
      if (i++ % 50 == 0)
        print_internal_state();
    }
    for (set<Messenger*>::iterator it = available_servers.begin();
         it != available_servers.end(); ++it) {
      (*it)->shutdown();
      (*it)->wait();
      delete (*it);
    }
    available_servers.clear();

    for (set<Messenger*>::iterator it = available_clients.begin();
         it != available_clients.end(); ++it) {
      (*it)->shutdown();
      (*it)->wait();
      delete (*it);
    }
    available_clients.clear();
  }
};

TEST_P(MessengerTest, SyntheticStressTest) {
  SyntheticWorkload test_msg(32, 128, GetParam(), 100,
                             Messenger::Policy::stateful_server(0, 0),
                             Messenger::Policy::lossless_client(0, 0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) cerr << "seeding connection " << i << std::endl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      test_msg.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 90) {
      test_msg.generate_connection();
    } else if (val > 80) {
      test_msg.drop_connection();
    } else if (val > 10) {
      test_msg.send_message();
    } else {
      usleep(rand() % 1000 + 500);
    }
  }
  test_msg.wait_for_done();
}


TEST_P(MessengerTest, SyntheticInjectTest) {
  g_ceph_context->_conf->set_val("ms_inject_socket_failures", "30");
  g_ceph_context->_conf->set_val("ms_inject_internal_delays", "0.1");
  SyntheticWorkload test_msg(4, 16, GetParam(), 100,
                             Messenger::Policy::stateful_server(0, 0),
                             Messenger::Policy::lossless_client(0, 0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) cerr << "seeding connection " << i << std::endl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      test_msg.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 90) {
      test_msg.generate_connection();
    } else if (val > 80) {
      test_msg.drop_connection();
    } else if (val > 10) {
      test_msg.send_message();
    } else {
      usleep(rand() % 500 + 100);
    }
  }
  test_msg.wait_for_done();
  g_ceph_context->_conf->set_val("ms_inject_socket_failures", "0");
  g_ceph_context->_conf->set_val("ms_inject_internal_delays", "0");
}

TEST_P(MessengerTest, SyntheticInjectTest2) {
  g_ceph_context->_conf->set_val("ms_inject_socket_failures", "30");
  g_ceph_context->_conf->set_val("ms_inject_internal_delays", "0.1");
  SyntheticWorkload test_msg(4, 16, GetParam(), 100,
                             Messenger::Policy::lossless_peer_reuse(0, 0),
                             Messenger::Policy::lossless_peer_reuse(0, 0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) cerr << "seeding connection " << i << std::endl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) {
      cerr << "Op " << i << ": ";
      test_msg.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 90) {
      test_msg.generate_connection();
    } else if (val > 80) {
      test_msg.drop_connection();
    } else if (val > 10) {
      test_msg.send_message();
    } else {
      usleep(rand() % 500 + 100);
    }
  }
  test_msg.wait_for_done();
  g_ceph_context->_conf->set_val("ms_inject_socket_failures", "0");
  g_ceph_context->_conf->set_val("ms_inject_internal_delays", "0");
}


class MarkdownDispatcher : public Dispatcher {
  Mutex lock;
  set<ConnectionRef> conns;
  bool last_mark;
 public:
  atomic_t count;
  MarkdownDispatcher(bool s): Dispatcher(g_ceph_context), lock("MarkdownDispatcher::lock"),
                              last_mark(false), count(0) {}
  bool ms_can_fast_dispatch_any() const { return false; }
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
    Mutex::Locker l(lock);
    conns.insert(con);
  }
  void ms_handle_fast_accept(Connection *con) {
    Mutex::Locker l(lock);
    conns.insert(con);
  }
  bool ms_dispatch(Message *m) {
    cerr << __func__ << " conn: " << m->get_connection() << std::endl;
    Mutex::Locker l(lock);
    count.inc();
    conns.insert(m->get_connection());
    if (conns.size() < 2 && !last_mark)
      return true;

    last_mark = true;
    usleep(rand() % 500);
    for (set<ConnectionRef>::iterator it = conns.begin(); it != conns.end(); ++it) {
      if ((*it) != m->get_connection().get()) {
        (*it)->mark_down();
        conns.erase(it);
        break;
      }
    }
    if (conns.empty())
      last_mark = false;
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) {
    cerr << __func__ << con << std::endl;
    Mutex::Locker l(lock);
    conns.erase(con);
    usleep(rand() % 500);
    return true;
  }
  void ms_handle_remote_reset(Connection *con) {
    Mutex::Locker l(lock);
    conns.erase(con);
    cerr << __func__ << con << std::endl;
  }
  void ms_fast_dispatch(Message *m) {
    assert(0);
  }
  bool ms_verify_authorizer(Connection *con, int peer_type, int protocol,
                            bufferlist& authorizer, bufferlist& authorizer_reply,
                            bool& isvalid, CryptoKey& session_key) {
    isvalid = true;
    return true;
  }
};


// Markdown with external lock
TEST_P(MessengerTest, MarkdownTest) {
  Messenger *server_msgr2 = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::OSD(0), "server", getpid());
  MarkdownDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("127.0.0.1:16800");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  bind_addr.parse("127.0.0.1:16801");
  server_msgr2->bind(bind_addr);
  server_msgr2->add_dispatcher_head(&srv_dispatcher);
  server_msgr2->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  int i = 1000;
  uint64_t last = 0;
  bool equal = false;
  uint64_t equal_count = 0;
  while (i--) {
    ConnectionRef conn1 = client_msgr->get_connection(server_msgr->get_myinst());
    ConnectionRef conn2 = client_msgr->get_connection(server_msgr2->get_myinst());
    MPing *m = new MPing();
    ASSERT_EQ(conn1->send_message(m), 0);
    m = new MPing();
    ASSERT_EQ(conn2->send_message(m), 0);
    CHECK_AND_WAIT_TRUE(srv_dispatcher.count.read() > last + 1);
    if (srv_dispatcher.count.read() == last) {
      cerr << __func__ << " last is " << last << std::endl;
      equal = true;
      equal_count++;
    } else {
      equal = false;
      equal_count = 0;
    }
    last = srv_dispatcher.count.read();
    if (equal_count)
      usleep(1000*500);
    ASSERT_FALSE(equal && equal_count > 3);
  }
  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr2->shutdown();
  server_msgr->wait();
  client_msgr->wait();
  server_msgr2->wait();
  delete server_msgr2;
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
  g_ceph_context->_conf->set_val("auth_cluster_required", "none");
  g_ceph_context->_conf->set_val("auth_service_required", "none");
  g_ceph_context->_conf->set_val("auth_client_required", "none");
  g_ceph_context->_conf->set_val("enable_experimental_unrecoverable_data_corrupting_features", "ms-type-async");
  g_ceph_context->_conf->set_val("ms_die_on_bad_msg", "true");
  g_ceph_context->_conf->set_val("ms_die_on_old_message", "true");
  g_ceph_context->_conf->set_val("ms_max_backoff", "1");
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 unittest_msgr && valgrind --tool=memcheck ./unittest_msgr"
 * End:
 */
