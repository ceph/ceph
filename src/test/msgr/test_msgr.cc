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

#include <atomic>
#include <iostream>
#include <list>
#include <memory>
#include <set>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

#include <boost/random/binomial_distribution.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <gtest/gtest.h>

#define MSG_POLICY_UNIT_TESTING

#include "common/ceph_argparse.h"
#include "common/ceph_mutex.h"
#include "global/global_init.h"
#include "messages/MCommand.h"
#include "messages/MPing.h"
#include "msg/Connection.h"
#include "msg/Dispatcher.h"
#include "msg/Message.h"
#include "msg/Messenger.h"
#include "msg/msg_types.h"

typedef boost::mt11213b gen_type;

#include "common/dout.h"
#include "include/ceph_assert.h"

#include "auth/DummyAuth.h"

#define dout_subsys ceph_subsys_ms
#undef dout_prefix
#define dout_prefix *_dout << " ceph_test_msgr "


#define CHECK_AND_WAIT_TRUE(expr) do {  \
  int n = 1000;                         \
  while (--n) {                         \
    if (expr)                           \
      break;                            \
    usleep(1000);                       \
  }                                     \
} while(0);

using namespace std;

class MessengerTest : public ::testing::TestWithParam<const char*> {
 public:
  DummyAuthClientServer dummy_auth;
  Messenger *server_msgr;
  Messenger *client_msgr;

  MessengerTest() : dummy_auth(g_ceph_context),
		    server_msgr(NULL), client_msgr(NULL) {
    dummy_auth.auth_registry.refresh_config();
  }
  void SetUp() override {
    lderr(g_ceph_context) << __func__ << " start set up " << GetParam() << dendl;
    server_msgr = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::OSD(0), "server", getpid());
    client_msgr = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::CLIENT(-1), "client", getpid());
    server_msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    client_msgr->set_default_policy(Messenger::Policy::lossy_client(0));
    server_msgr->set_auth_client(&dummy_auth);
    server_msgr->set_auth_server(&dummy_auth);
    client_msgr->set_auth_client(&dummy_auth);
    client_msgr->set_auth_server(&dummy_auth);
    server_msgr->set_require_authorizer(false);
  }
  void TearDown() override {
    ASSERT_EQ(server_msgr->get_dispatch_queue_len(), 0);
    ASSERT_EQ(client_msgr->get_dispatch_queue_len(), 0);
    delete server_msgr;
    delete client_msgr;
  }

};


class FakeDispatcher : public Dispatcher {
 public:
  struct Session : public RefCountedObject {
    atomic<uint64_t> count;
    ConnectionRef con;

    explicit Session(ConnectionRef c): RefCountedObject(g_ceph_context), count(0), con(c) {
    }
    uint64_t get_count() { return count; }
  };

  ceph::mutex lock = ceph::make_mutex("FakeDispatcher::lock");
  ceph::condition_variable cond;
  bool is_server;
  bool got_new;
  bool got_remote_reset;
  bool got_connect;
  bool loopback;
  entity_addrvec_t last_accept;
  ConnectionRef *last_accept_con_ptr = nullptr;

  explicit FakeDispatcher(bool s): Dispatcher(g_ceph_context),
                          is_server(s), got_new(false), got_remote_reset(false),
                          got_connect(false), loopback(false) {
  }
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_PING:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) override {
    std::scoped_lock l{lock};
    lderr(g_ceph_context) << __func__ << " " << con << dendl;
    auto s = con->get_priv();
    if (!s) {
      auto session = new Session(con);
      con->set_priv(RefCountedPtr{session, false});
      lderr(g_ceph_context) << __func__ << " con: " << con
			    << " count: " << session->count << dendl;
    }
    got_connect = true;
    cond.notify_all();
  }
  void ms_handle_fast_accept(Connection *con) override {
    last_accept = con->get_peer_addrs();
    if (last_accept_con_ptr) {
      *last_accept_con_ptr = con;
    }
    if (!con->get_priv()) {
      con->set_priv(RefCountedPtr{new Session(con), false});
    }
  }
  bool ms_dispatch(Message *m) override {
    auto priv = m->get_connection()->get_priv();
    auto s = static_cast<Session*>(priv.get());
    if (!s) {
      s = new Session(m->get_connection());
      priv.reset(s, false);
      m->get_connection()->set_priv(priv);
    }
    s->count++;
    lderr(g_ceph_context) << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << dendl;
    if (is_server) {
      reply_message(m);
    }
    std::lock_guard l{lock};
    got_new = true;
    cond.notify_all();
    m->put();
    return true;
  }
  bool ms_handle_reset(Connection *con) override {
    std::lock_guard l{lock};
    lderr(g_ceph_context) << __func__ << " " << con << dendl;
    auto priv = con->get_priv();
    if (auto s = static_cast<Session*>(priv.get()); s) {
      s->con.reset();  // break con <-> session ref cycle
      con->set_priv(nullptr);   // break ref <-> session cycle, if any
    }
    return true;
  }
  void ms_handle_remote_reset(Connection *con) override {
    std::lock_guard l{lock};
    lderr(g_ceph_context) << __func__ << " " << con << dendl;
    auto priv = con->get_priv();
    if (auto s = static_cast<Session*>(priv.get()); s) {
      s->con.reset();  // break con <-> session ref cycle
      con->set_priv(nullptr);   // break ref <-> session cycle, if any
    }
    got_remote_reset = true;
    cond.notify_all();
  }
  bool ms_handle_refused(Connection *con) override {
    return false;
  }
  void ms_fast_dispatch(Message *m) override {
    auto priv = m->get_connection()->get_priv();
    auto s = static_cast<Session*>(priv.get());
    if (!s) {
      s = new Session(m->get_connection());
      priv.reset(s, false);
      m->get_connection()->set_priv(priv);
    }
    s->count++;
    lderr(g_ceph_context) << __func__ << " conn: " << m->get_connection() << " session " << s << " count: " << s->count << dendl;
    if (is_server) {
      if (loopback)
        ceph_assert(m->get_source().is_osd());
      else
        reply_message(m);
    } else if (loopback) {
      ceph_assert(m->get_source().is_client());
    }
    m->put();
    std::lock_guard l{lock};
    got_new = true;
    cond.notify_all();
  }

  int ms_handle_fast_authentication(Connection *con) override {
    return 1;
  }

  void reply_message(Message *m) {
    MPing *rm = new MPing();
    m->get_connection()->send_message(rm);
  }
};

typedef FakeDispatcher::Session Session;

struct TestInterceptor : public Interceptor {

  bool step_waiting = false;
  bool waiting = true;
  std::map<Connection *, uint32_t> current_step;
  std::map<Connection *, std::list<uint32_t>> step_history;
  std::map<uint32_t, std::optional<ACTION>> decisions;
  std::set<uint32_t> breakpoints;

  uint32_t count_step(Connection *conn, uint32_t step) {
    uint32_t count = 0;
    for (auto s : step_history[conn]) {
      if (s == step) {
        count++;
      }
    }
    return count;
  }

  void breakpoint(uint32_t step) {
    breakpoints.insert(step);
  }

  void remove_bp(uint32_t step) {
    breakpoints.erase(step);
  }

  Connection *wait(uint32_t step, Connection *conn=nullptr) {
    std::unique_lock<std::mutex> l(lock);
    while(true) {
      if (conn) {
        auto it = current_step.find(conn);
        if (it != current_step.end()) {
          if (it->second == step) {
            break;
          }
        }
      } else {
        for (auto it : current_step) {
          if (it.second == step) {
            conn = it.first;
            break; 
          }
        }
        if (conn) {
          break;
        }
      }
      step_waiting = true;
      cond_var.wait(l);
    }
    step_waiting = false;
    return conn;
  }

  ACTION wait_for_decision(uint32_t step, std::unique_lock<std::mutex> &l) {
    if (decisions[step]) {
      return *(decisions[step]);
    }
    waiting = true;
    cond_var.wait(l, [this] { return !waiting; });
    return *(decisions[step]);
  }

  void proceed(uint32_t step, ACTION decision) {
    std::unique_lock<std::mutex> l(lock);
    decisions[step] = decision;
    if (waiting) {
      waiting = false;
      cond_var.notify_one();
    }
  }

  ACTION intercept(Connection *conn, uint32_t step) override {
    lderr(g_ceph_context) << __func__ << " conn(" << conn 
                          << ") intercept called on step=" << step << dendl;

    {
      std::unique_lock<std::mutex> l(lock);
      step_history[conn].push_back(step);
      current_step[conn] = step;  
      if (step_waiting) {
        cond_var.notify_one();
      }
    }

    std::unique_lock<std::mutex> l(lock);
    ACTION decision = ACTION::CONTINUE;
    if (breakpoints.find(step) != breakpoints.end()) {
      lderr(g_ceph_context) << __func__ << " conn(" << conn 
                            << ") pausing on step=" << step << dendl;
      decision = wait_for_decision(step, l);
    } else {
      if (decisions[step]) {
        decision = *(decisions[step]);
      }
    }
    lderr(g_ceph_context) << __func__ << " conn(" << conn 
                          << ") resuming step=" << step << " with decision=" 
                          << decision << dendl;
    decisions[step].reset();
    return decision;
  }

};

/**
 * Scenario: A connects to B, and B connects to A at the same time.
 */ 
TEST_P(MessengerTest, ConnectionRaceTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(false);

  TestInterceptor *cli_interceptor = new TestInterceptor();
  TestInterceptor *srv_interceptor = new TestInterceptor();

  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, Messenger::Policy::lossless_peer_reuse(0));
  server_msgr->interceptor = srv_interceptor;

  client_msgr->set_policy(entity_name_t::TYPE_OSD, Messenger::Policy::lossless_peer_reuse(0));
  client_msgr->interceptor = cli_interceptor;

  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:3300");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  bind_addr.parse("v2:127.0.0.1:3301");
  client_msgr->bind(bind_addr);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // pause before sending client_ident message
  cli_interceptor->breakpoint(Interceptor::STEP::SEND_CLIENT_IDENTITY);
  // pause before sending client_ident message
  srv_interceptor->breakpoint(Interceptor::STEP::SEND_CLIENT_IDENTITY);

  ConnectionRef c2s = client_msgr->connect_to(server_msgr->get_mytype(),
					                                    server_msgr->get_myaddrs());
  MPing *m1 = new MPing();
  ASSERT_EQ(c2s->send_message(m1), 0);

  ConnectionRef s2c = server_msgr->connect_to(client_msgr->get_mytype(),
					                                    client_msgr->get_myaddrs());
  MPing *m2 = new MPing();
  ASSERT_EQ(s2c->send_message(m2), 0);

  cli_interceptor->wait(Interceptor::STEP::SEND_CLIENT_IDENTITY, c2s.get());
  srv_interceptor->wait(Interceptor::STEP::SEND_CLIENT_IDENTITY, s2c.get());

  // at this point both connections (A->B, B->A) are paused just before sending
  // the client_ident message.

  cli_interceptor->remove_bp(Interceptor::STEP::SEND_CLIENT_IDENTITY);
  srv_interceptor->remove_bp(Interceptor::STEP::SEND_CLIENT_IDENTITY);

  cli_interceptor->proceed(Interceptor::STEP::SEND_CLIENT_IDENTITY, Interceptor::ACTION::CONTINUE);
  srv_interceptor->proceed(Interceptor::STEP::SEND_CLIENT_IDENTITY, Interceptor::ACTION::CONTINUE);

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }

  {
    std::unique_lock l{srv_dispatcher.lock};
    srv_dispatcher.cond.wait(l, [&] { return srv_dispatcher.got_new; });
    srv_dispatcher.got_new = false;
  }
  
  ASSERT_TRUE(s2c->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(s2c->get_priv().get())->get_count());
  ASSERT_TRUE(s2c->peer_is_client());

  ASSERT_TRUE(c2s->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(c2s->get_priv().get())->get_count());
  ASSERT_TRUE(c2s->peer_is_osd());

  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();

  delete cli_interceptor;
  delete srv_interceptor;
}

/**
 * Scenario: A connects to B, and B connects to A at the same time.
 * The first (A -> B) connection gets to message flow handshake, the
 * second (B -> A) connection is stuck waiting for a banner from A.
 * After A sends client_ident to B, the first connection wins and B
 * calls reuse_connection() to replace the second connection's socket
 * while the second connection is still in BANNER_CONNECTING.
 */
TEST_P(MessengerTest, ConnectionRaceReuseBannerTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(false);

  auto cli_interceptor = std::make_unique<TestInterceptor>();
  auto srv_interceptor = std::make_unique<TestInterceptor>();

  server_msgr->set_policy(entity_name_t::TYPE_CLIENT,
                          Messenger::Policy::lossless_peer_reuse(0));
  server_msgr->interceptor = srv_interceptor.get();

  client_msgr->set_policy(entity_name_t::TYPE_OSD,
                          Messenger::Policy::lossless_peer_reuse(0));
  client_msgr->interceptor = cli_interceptor.get();

  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:3300");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  bind_addr.parse("v2:127.0.0.1:3301");
  client_msgr->bind(bind_addr);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // pause before sending client_ident message
  srv_interceptor->breakpoint(Interceptor::STEP::SEND_CLIENT_IDENTITY);

  ConnectionRef s2c = server_msgr->connect_to(client_msgr->get_mytype(),
                                              client_msgr->get_myaddrs());
  MPing *m1 = new MPing();
  ASSERT_EQ(s2c->send_message(m1), 0);

  srv_interceptor->wait(Interceptor::STEP::SEND_CLIENT_IDENTITY);
  srv_interceptor->remove_bp(Interceptor::STEP::SEND_CLIENT_IDENTITY);

  // pause before sending banner
  cli_interceptor->breakpoint(Interceptor::STEP::BANNER_EXCHANGE_BANNER_CONNECTING);

  ConnectionRef c2s = client_msgr->connect_to(server_msgr->get_mytype(),
                                              server_msgr->get_myaddrs());
  MPing *m2 = new MPing();
  ASSERT_EQ(c2s->send_message(m2), 0);

  cli_interceptor->wait(Interceptor::STEP::BANNER_EXCHANGE_BANNER_CONNECTING);
  cli_interceptor->remove_bp(Interceptor::STEP::BANNER_EXCHANGE_BANNER_CONNECTING);

  // second connection is in BANNER_CONNECTING, ensure it stays so
  // and send client_ident
  srv_interceptor->breakpoint(Interceptor::STEP::BANNER_EXCHANGE);
  srv_interceptor->proceed(Interceptor::STEP::SEND_CLIENT_IDENTITY, Interceptor::ACTION::CONTINUE);

  // handle client_ident -- triggers reuse_connection() with exproto
  // in BANNER_CONNECTING
  cli_interceptor->breakpoint(Interceptor::STEP::READY);
  cli_interceptor->proceed(Interceptor::STEP::BANNER_EXCHANGE_BANNER_CONNECTING, Interceptor::ACTION::CONTINUE);

  cli_interceptor->wait(Interceptor::STEP::READY);
  cli_interceptor->remove_bp(Interceptor::STEP::READY);

  // first connection is in READY
  Connection *s2c_accepter = srv_interceptor->wait(Interceptor::STEP::BANNER_EXCHANGE);
  srv_interceptor->remove_bp(Interceptor::STEP::BANNER_EXCHANGE);

  srv_interceptor->proceed(Interceptor::STEP::BANNER_EXCHANGE, Interceptor::ACTION::CONTINUE);
  cli_interceptor->proceed(Interceptor::STEP::READY, Interceptor::ACTION::CONTINUE);

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }

  {
    std::unique_lock l{srv_dispatcher.lock};
    srv_dispatcher.cond.wait(l, [&] { return srv_dispatcher.got_new; });
    srv_dispatcher.got_new = false;
  }

  EXPECT_TRUE(s2c->is_connected());
  EXPECT_EQ(1u, static_cast<Session*>(s2c->get_priv().get())->get_count());
  EXPECT_TRUE(s2c->peer_is_client());

  EXPECT_TRUE(c2s->is_connected());
  EXPECT_EQ(1u, static_cast<Session*>(c2s->get_priv().get())->get_count());
  EXPECT_TRUE(c2s->peer_is_osd());

  // closed in reuse_connection() -- EPIPE when writing banner/hello
  EXPECT_FALSE(s2c_accepter->is_connected());

  // established exactly once, never faulted and reconnected
  EXPECT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::START_CLIENT_BANNER_EXCHANGE), 1u);
  EXPECT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::SEND_RECONNECT), 0u);
  EXPECT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::READY), 1u);

  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();
}

/**
 * Scenario:
 *    - A connects to B
 *    - A sends client_ident to B
 *    - B fails before sending server_ident to A
 *    - A reconnects
 */ 
TEST_P(MessengerTest, MissingServerIdenTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(false);

  TestInterceptor *cli_interceptor = new TestInterceptor();
  TestInterceptor *srv_interceptor = new TestInterceptor();

  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, Messenger::Policy::stateful_server(0));
  server_msgr->interceptor = srv_interceptor;

  client_msgr->set_policy(entity_name_t::TYPE_OSD, Messenger::Policy::lossy_client(0));
  client_msgr->interceptor = cli_interceptor;

  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:3300");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  bind_addr.parse("v2:127.0.0.1:3301");
  client_msgr->bind(bind_addr);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // pause before sending server_ident message
  srv_interceptor->breakpoint(Interceptor::STEP::SEND_SERVER_IDENTITY);

  ConnectionRef c2s = client_msgr->connect_to(server_msgr->get_mytype(),
					                                    server_msgr->get_myaddrs());
  MPing *m1 = new MPing();
  ASSERT_EQ(c2s->send_message(m1), 0);

  Connection *c2s_accepter = srv_interceptor->wait(Interceptor::STEP::SEND_SERVER_IDENTITY);
  srv_interceptor->remove_bp(Interceptor::STEP::SEND_SERVER_IDENTITY);

  // We inject a message from this side of the connection to force it to be
  // in standby when we inject the failure below
  MPing *m2 = new MPing();
  ASSERT_EQ(c2s_accepter->send_message(m2), 0);

  srv_interceptor->proceed(Interceptor::STEP::SEND_SERVER_IDENTITY, Interceptor::ACTION::FAIL);

  {
    std::unique_lock l{srv_dispatcher.lock};
    srv_dispatcher.cond.wait(l, [&] { return srv_dispatcher.got_new; });
    srv_dispatcher.got_new = false;
  }

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  
  ASSERT_TRUE(c2s->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(c2s->get_priv().get())->get_count());
  ASSERT_TRUE(c2s->peer_is_osd());

  ASSERT_TRUE(c2s_accepter->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(c2s_accepter->get_priv().get())->get_count());
  ASSERT_TRUE(c2s_accepter->peer_is_client());

  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();

  delete cli_interceptor;
  delete srv_interceptor;
}

/**
 * Scenario:
 *    - A connects to B
 *    - A sends client_ident to B
 *    - B fails before sending server_ident to A
 *    - A goes to standby
 *    - B reconnects to A
 */ 
TEST_P(MessengerTest, MissingServerIdenTest2) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(false);

  TestInterceptor *cli_interceptor = new TestInterceptor();
  TestInterceptor *srv_interceptor = new TestInterceptor();

  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, Messenger::Policy::lossless_peer(0));
  server_msgr->interceptor = srv_interceptor;

  client_msgr->set_policy(entity_name_t::TYPE_OSD, Messenger::Policy::lossless_peer(0));
  client_msgr->interceptor = cli_interceptor;

  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:3300");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  bind_addr.parse("v2:127.0.0.1:3301");
  client_msgr->bind(bind_addr);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // pause before sending server_ident message
  srv_interceptor->breakpoint(Interceptor::STEP::SEND_SERVER_IDENTITY);

  ConnectionRef c2s = client_msgr->connect_to(server_msgr->get_mytype(),
					                                    server_msgr->get_myaddrs());

  Connection *c2s_accepter = srv_interceptor->wait(Interceptor::STEP::SEND_SERVER_IDENTITY);
  srv_interceptor->remove_bp(Interceptor::STEP::SEND_SERVER_IDENTITY);

  // We inject a message from this side of the connection to force it to be
  // in standby when we inject the failure below
  MPing *m2 = new MPing();
  ASSERT_EQ(c2s_accepter->send_message(m2), 0);

  srv_interceptor->proceed(Interceptor::STEP::SEND_SERVER_IDENTITY, Interceptor::ACTION::FAIL);

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  
  ASSERT_TRUE(c2s->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(c2s->get_priv().get())->get_count());
  ASSERT_TRUE(c2s->peer_is_osd());

  ASSERT_TRUE(c2s_accepter->is_connected());
  ASSERT_EQ(0u, static_cast<Session*>(c2s_accepter->get_priv().get())->get_count());
  ASSERT_TRUE(c2s_accepter->peer_is_client());

  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();

  delete cli_interceptor;
  delete srv_interceptor;
}

/**
 * Scenario:
 *    - A connects to B
 *    - A and B exchange messages
 *    - A fails
 *    - B goes into standby
 *    - A reconnects
 */ 
TEST_P(MessengerTest, ReconnectTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);

  TestInterceptor *cli_interceptor = new TestInterceptor();
  TestInterceptor *srv_interceptor = new TestInterceptor();

  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, Messenger::Policy::stateful_server(0));
  server_msgr->interceptor = srv_interceptor;

  client_msgr->set_policy(entity_name_t::TYPE_OSD, Messenger::Policy::lossless_peer(0));
  client_msgr->interceptor = cli_interceptor;

  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:3300");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  bind_addr.parse("v2:127.0.0.1:3301");
  client_msgr->bind(bind_addr);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  ConnectionRef c2s = client_msgr->connect_to(server_msgr->get_mytype(),
					                                    server_msgr->get_myaddrs());

  MPing *m1 = new MPing();
  ASSERT_EQ(c2s->send_message(m1), 0);

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }

  ASSERT_TRUE(c2s->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(c2s->get_priv().get())->get_count());
  ASSERT_TRUE(c2s->peer_is_osd());

  cli_interceptor->breakpoint(Interceptor::STEP::HANDLE_MESSAGE);
  
  MPing *m2 = new MPing();
  ASSERT_EQ(c2s->send_message(m2), 0);

  cli_interceptor->wait(Interceptor::STEP::HANDLE_MESSAGE, c2s.get());
  cli_interceptor->remove_bp(Interceptor::STEP::HANDLE_MESSAGE);

  // at this point client and server are connected together

  srv_interceptor->breakpoint(Interceptor::STEP::READY);

  // failing client
  cli_interceptor->proceed(Interceptor::STEP::HANDLE_MESSAGE, Interceptor::ACTION::FAIL);

  MPing *m3 = new MPing();
  ASSERT_EQ(c2s->send_message(m3), 0);

  Connection *c2s_accepter = srv_interceptor->wait(Interceptor::STEP::READY);
  // the srv end of theconnection is now paused at ready
  // this means that the reconnect was successful
  srv_interceptor->remove_bp(Interceptor::STEP::READY);

  ASSERT_TRUE(c2s_accepter->peer_is_client());
  // c2s_accepter sent 0 reconnect messages
  ASSERT_EQ(srv_interceptor->count_step(c2s_accepter, Interceptor::STEP::SEND_RECONNECT), 0u);
  // c2s_accepter sent 1 reconnect_ok messages
  ASSERT_EQ(srv_interceptor->count_step(c2s_accepter, Interceptor::STEP::SEND_RECONNECT_OK), 1u);
  // c2s sent 1 reconnect messages
  ASSERT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::SEND_RECONNECT), 1u);
  // c2s sent 0 reconnect_ok messages
  ASSERT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::SEND_RECONNECT_OK), 0u);

  srv_interceptor->proceed(15, Interceptor::ACTION::CONTINUE);

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }

  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();

  delete cli_interceptor;
  delete srv_interceptor;
}

/**
 * Scenario:
 *    - A connects to B
 *    - A and B exchange messages
 *    - A fails
 *    - A reconnects // B reconnects
 */ 
TEST_P(MessengerTest, ReconnectRaceTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);

  TestInterceptor *cli_interceptor = new TestInterceptor();
  TestInterceptor *srv_interceptor = new TestInterceptor();

  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, Messenger::Policy::lossless_peer(0));
  server_msgr->interceptor = srv_interceptor;

  client_msgr->set_policy(entity_name_t::TYPE_OSD, Messenger::Policy::lossless_peer(0));
  client_msgr->interceptor = cli_interceptor;

  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:3300");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  bind_addr.parse("v2:127.0.0.1:3301");
  client_msgr->bind(bind_addr);
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  ConnectionRef c2s = client_msgr->connect_to(server_msgr->get_mytype(),
					                                    server_msgr->get_myaddrs());

  MPing *m1 = new MPing();
  ASSERT_EQ(c2s->send_message(m1), 0);

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }

  ASSERT_TRUE(c2s->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(c2s->get_priv().get())->get_count());
  ASSERT_TRUE(c2s->peer_is_osd());

  cli_interceptor->breakpoint(Interceptor::STEP::HANDLE_MESSAGE);
  
  MPing *m2 = new MPing();
  ASSERT_EQ(c2s->send_message(m2), 0);

  cli_interceptor->wait(Interceptor::STEP::HANDLE_MESSAGE, c2s.get());
  cli_interceptor->remove_bp(Interceptor::STEP::HANDLE_MESSAGE);

  // at this point client and server are connected together

  // force both client and server to race on reconnect
  cli_interceptor->breakpoint(Interceptor::STEP::SEND_RECONNECT);
  srv_interceptor->breakpoint(Interceptor::STEP::SEND_RECONNECT);

  // failing client
  // this will cause both client and server to reconnect at the same time
  cli_interceptor->proceed(Interceptor::STEP::HANDLE_MESSAGE, Interceptor::ACTION::FAIL);

  MPing *m3 = new MPing();
  ASSERT_EQ(c2s->send_message(m3), 0);

  cli_interceptor->wait(Interceptor::STEP::SEND_RECONNECT, c2s.get());
  srv_interceptor->wait(Interceptor::STEP::SEND_RECONNECT);

  cli_interceptor->remove_bp(Interceptor::STEP::SEND_RECONNECT);
  srv_interceptor->remove_bp(Interceptor::STEP::SEND_RECONNECT);

  // pause on "ready"
  srv_interceptor->breakpoint(Interceptor::STEP::READY);

  cli_interceptor->proceed(Interceptor::STEP::SEND_RECONNECT, Interceptor::ACTION::CONTINUE);
  srv_interceptor->proceed(Interceptor::STEP::SEND_RECONNECT, Interceptor::ACTION::CONTINUE);

  Connection *c2s_accepter = srv_interceptor->wait(Interceptor::STEP::READY);

  // the server has reconnected and is "ready"
  srv_interceptor->remove_bp(Interceptor::STEP::READY);

  ASSERT_TRUE(c2s_accepter->peer_is_client());
  ASSERT_TRUE(c2s->peer_is_osd());
  
  // the server should win the reconnect race

  // c2s_accepter sent 1 or 2 reconnect messages
  ASSERT_LT(srv_interceptor->count_step(c2s_accepter, Interceptor::STEP::SEND_RECONNECT), 3u);
  ASSERT_GT(srv_interceptor->count_step(c2s_accepter, Interceptor::STEP::SEND_RECONNECT), 0u);
  // c2s_accepter sent 0 reconnect_ok messages
  ASSERT_EQ(srv_interceptor->count_step(c2s_accepter, Interceptor::STEP::SEND_RECONNECT_OK), 0u);
  // c2s sent 1 reconnect messages
  ASSERT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::SEND_RECONNECT), 1u);
  // c2s sent 1 reconnect_ok messages
  ASSERT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::SEND_RECONNECT_OK), 1u);

  if (srv_interceptor->count_step(c2s_accepter, Interceptor::STEP::SEND_RECONNECT) == 2) {
    // if the server send the reconnect message two times then
    // the client must have sent a session retry message to the server
    ASSERT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::SESSION_RETRY), 1u);
  } else {
    ASSERT_EQ(cli_interceptor->count_step(c2s.get(), Interceptor::STEP::SESSION_RETRY), 0u);
  }

  srv_interceptor->proceed(Interceptor::STEP::READY, Interceptor::ACTION::CONTINUE);

  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }

  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();

  delete cli_interceptor;
  delete srv_interceptor;
}

TEST_P(MessengerTest, SimpleTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. simple round trip
  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
					       server_msgr->get_myaddrs());
  {
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(conn->get_priv().get())->get_count());
  ASSERT_TRUE(conn->peer_is_osd());

  // 2. test rebind port
  set<int> avoid_ports;
  for (int i = 0; i < 10 ; i++) {
    for (auto a : server_msgr->get_myaddrs().v) {
      avoid_ports.insert(a.get_port() + i);
    }
  }
  server_msgr->rebind(avoid_ports);
  for (auto a : server_msgr->get_myaddrs().v) {
    ASSERT_TRUE(avoid_ports.count(a.get_port()) == 0);
  }

  conn = client_msgr->connect_to(server_msgr->get_mytype(),
				 server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());

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
  srv_dispatcher.loopback = true;
  conn = client_msgr->get_loopback_connection();
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  srv_dispatcher.loopback = false;
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();
}

TEST_P(MessengerTest, SimpleMsgr2Test) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t legacy_addr;
  legacy_addr.parse("v1:127.0.0.1");
  entity_addr_t msgr2_addr;
  msgr2_addr.parse("v2:127.0.0.1");
  entity_addrvec_t bind_addrs;
  bind_addrs.v.push_back(legacy_addr);
  bind_addrs.v.push_back(msgr2_addr);
  server_msgr->bindv(bind_addrs);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. simple round trip
  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->connect_to(
    server_msgr->get_mytype(),
    server_msgr->get_myaddrs());
  {
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_EQ(1u, static_cast<Session*>(conn->get_priv().get())->get_count());
  ASSERT_TRUE(conn->peer_is_osd());

  // 2. test rebind port
  set<int> avoid_ports;
  for (int i = 0; i < 10 ; i++) {
    for (auto a : server_msgr->get_myaddrs().v) {
      avoid_ports.insert(a.get_port() + i);
    }
  }
  server_msgr->rebind(avoid_ports);
  for (auto a : server_msgr->get_myaddrs().v) {
    ASSERT_TRUE(avoid_ports.count(a.get_port()) == 0);
  }

  conn = client_msgr->connect_to(
       server_msgr->get_mytype(),
       server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());

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
  srv_dispatcher.loopback = true;
  conn = client_msgr->get_loopback_connection();
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  srv_dispatcher.loopback = false;
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  client_msgr->shutdown();
  client_msgr->wait();
  server_msgr->shutdown();
  server_msgr->wait();
}

TEST_P(MessengerTest, FeatureTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  uint64_t all_feature_supported, feature_required, feature_supported = 0;
  for (int i = 0; i < 10; i++)
    feature_supported |= 1ULL << i;
  feature_supported |= CEPH_FEATUREMASK_MSG_ADDR2;
  feature_supported |= CEPH_FEATUREMASK_SERVER_NAUTILUS;
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
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
					       server_msgr->get_myaddrs());
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

  conn = client_msgr->connect_to(server_msgr->get_mytype(),
				 server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, TimeoutTest) {
  g_ceph_context->_conf.set_val("ms_connection_idle_timeout", "1");
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. build the connection
  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
						    server_msgr->get_myaddrs());
  {
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  ASSERT_TRUE(conn->peer_is_osd());

  // 2. wait for idle
  usleep(2500*1000);
  ASSERT_FALSE(conn->is_connected());

  server_msgr->shutdown();
  server_msgr->wait();

  client_msgr->shutdown();
  client_msgr->wait();
  g_ceph_context->_conf.set_val("ms_connection_idle_timeout", "900");
}

TEST_P(MessengerTest, StatefulTest) {
  Message *m;
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateful_server(0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossless_client(0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. test for server standby
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
					       server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  conn->mark_down();
  ASSERT_FALSE(conn->is_connected());
  ConnectionRef server_conn = server_msgr->connect_to(
    client_msgr->get_mytype(), srv_dispatcher.last_accept);
  // don't lose state
  ASSERT_EQ(1U, static_cast<Session*>(server_conn->get_priv().get())->get_count());

  srv_dispatcher.got_new = false;
  conn = client_msgr->connect_to(server_msgr->get_mytype(),
				 server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  server_conn = server_msgr->connect_to(client_msgr->get_mytype(),
					srv_dispatcher.last_accept);
  {
    std::unique_lock l{srv_dispatcher.lock};
    srv_dispatcher.cond.wait(l, [&] { return srv_dispatcher.got_remote_reset; });
  }

  // 2. test for client reconnect
  ASSERT_FALSE(cli_dispatcher.got_remote_reset);
  cli_dispatcher.got_connect = false;
  cli_dispatcher.got_new = false;
  cli_dispatcher.got_remote_reset = false;
  server_conn->mark_down();
  ASSERT_FALSE(server_conn->is_connected());
  // ensure client detect server socket closed
  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_remote_reset; });
    cli_dispatcher.got_remote_reset = false;
  }
  {
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_connect; });
    cli_dispatcher.got_connect = false;
  }
  CHECK_AND_WAIT_TRUE(conn->is_connected());
  ASSERT_TRUE(conn->is_connected());

  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    ASSERT_TRUE(conn->is_connected());
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  // resetcheck happen
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  server_conn = server_msgr->connect_to(client_msgr->get_mytype(),
					srv_dispatcher.last_accept);
  ASSERT_EQ(1U, static_cast<Session*>(server_conn->get_priv().get())->get_count());
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
  bind_addr.parse("v2:127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateless_server(0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossy_client(0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. test for server lose state
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
						    server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  conn->mark_down();
  ASSERT_FALSE(conn->is_connected());

  srv_dispatcher.got_new = false;
  ConnectionRef server_conn;
  srv_dispatcher.last_accept_con_ptr = &server_conn;
  conn = client_msgr->connect_to(server_msgr->get_mytype(),
				      server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  ASSERT_TRUE(server_conn);

  // server lose state
  {
    std::unique_lock l{srv_dispatcher.lock};
    srv_dispatcher.cond.wait(l, [&] { return srv_dispatcher.got_new; });
  }
  ASSERT_EQ(1U, static_cast<Session*>(server_conn->get_priv().get())->get_count());

  // 2. test for client lossy
  server_conn->mark_down();
  ASSERT_FALSE(server_conn->is_connected());
  conn->send_keepalive();
  CHECK_AND_WAIT_TRUE(!conn->is_connected());
  ASSERT_FALSE(conn->is_connected());
  conn = client_msgr->connect_to(server_msgr->get_mytype(),
				 server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, AnonTest) {
  Message *m;
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateless_server(0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossy_client(0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  ConnectionRef server_con_a, server_con_b;

  // a
  srv_dispatcher.last_accept_con_ptr = &server_con_a;
  ConnectionRef con_a = client_msgr->connect_to(server_msgr->get_mytype(),
					    server_msgr->get_myaddrs(),
					    true);
  {
    m = new MPing();
    ASSERT_EQ(con_a->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(con_a->get_priv().get())->get_count());

  // b
  srv_dispatcher.last_accept_con_ptr = &server_con_b;
  ConnectionRef con_b = client_msgr->connect_to(server_msgr->get_mytype(),
					    server_msgr->get_myaddrs(),
					    true);
  {
    m = new MPing();
    ASSERT_EQ(con_b->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(con_b->get_priv().get())->get_count());

  // these should be distinct
  ASSERT_NE(con_a, con_b);
  ASSERT_NE(server_con_a, server_con_b);

  // and both connected
  {
    m = new MPing();
    ASSERT_EQ(con_a->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  {
    m = new MPing();
    ASSERT_EQ(con_b->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }

  // clean up
  con_a->mark_down();
  ASSERT_FALSE(con_a->is_connected());
  con_b->mark_down();
  ASSERT_FALSE(con_b->is_connected());

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, ClientStandbyTest) {
  Message *m;
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateful_server(0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossless_peer(0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. test for client standby, resetcheck
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
						    server_msgr->get_myaddrs());
  {
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  ConnectionRef server_conn = server_msgr->connect_to(
    client_msgr->get_mytype(),
    srv_dispatcher.last_accept);
  ASSERT_FALSE(cli_dispatcher.got_remote_reset);
  cli_dispatcher.got_connect = false;
  server_conn->mark_down();
  ASSERT_FALSE(server_conn->is_connected());
  // client should be standby
  usleep(300*1000);
  // client should be standby, so we use original connection
  {
    // Try send message to verify got remote reset callback
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    {
      std::unique_lock l{cli_dispatcher.lock};
      cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_remote_reset; });
      cli_dispatcher.got_remote_reset = false;
      cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_connect; });
      cli_dispatcher.got_connect = false;
    }
    CHECK_AND_WAIT_TRUE(conn->is_connected());
    ASSERT_TRUE(conn->is_connected());
    m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  server_conn = server_msgr->connect_to(client_msgr->get_mytype(),
					srv_dispatcher.last_accept);
  ASSERT_EQ(1U, static_cast<Session*>(server_conn->get_priv().get())->get_count());

  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, AuthTest) {
  g_ceph_context->_conf.set_val("auth_cluster_required", "cephx");
  g_ceph_context->_conf.set_val("auth_service_required", "cephx");
  g_ceph_context->_conf.set_val("auth_client_required", "cephx");
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();

  // 1. simple auth round trip
  MPing *m = new MPing();
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
						    server_msgr->get_myaddrs());
  {
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());

  // 2. mix auth
  g_ceph_context->_conf.set_val("auth_cluster_required", "none");
  g_ceph_context->_conf.set_val("auth_service_required", "none");
  g_ceph_context->_conf.set_val("auth_client_required", "none");
  conn->mark_down();
  ASSERT_FALSE(conn->is_connected());
  conn = client_msgr->connect_to(server_msgr->get_mytype(),
						    server_msgr->get_myaddrs());
  {
    MPing *m = new MPing();
    ASSERT_EQ(conn->send_message(m), 0);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    cli_dispatcher.got_new = false;
  }
  ASSERT_TRUE(conn->is_connected());
  ASSERT_EQ(1U, static_cast<Session*>(conn->get_priv().get())->get_count());
  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}

TEST_P(MessengerTest, MessageTest) {
  FakeDispatcher cli_dispatcher(false), srv_dispatcher(true);
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1");
  Messenger::Policy p = Messenger::Policy::stateful_server(0);
  server_msgr->set_policy(entity_name_t::TYPE_CLIENT, p);
  p = Messenger::Policy::lossless_peer(0);
  client_msgr->set_policy(entity_name_t::TYPE_OSD, p);

  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->start();
  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->start();


  // 1. A very large "front"(as well as "payload")
  // Because a external message need to invade Messenger::decode_message,
  // here we only use existing message class(MCommand)
  ConnectionRef conn = client_msgr->connect_to(server_msgr->get_mytype(),
						    server_msgr->get_myaddrs());
  {
    uuid_d uuid;
    uuid.generate_random();
    vector<string> cmds;
    string s("abcdefghijklmnopqrstuvwxyz");
    for (int i = 0; i < 1024*30; i++)
      cmds.push_back(s);
    MCommand *m = new MCommand(uuid);
    m->cmd = cmds;
    conn->send_message(m);
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait_for(l, 500s, [&] { return cli_dispatcher.got_new; });
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
    conn->send_message(m);
    utime_t t;
    t += 1000*1000*500;
    std::unique_lock l{cli_dispatcher.lock};
    cli_dispatcher.cond.wait(l, [&] { return cli_dispatcher.got_new; });
    ASSERT_TRUE(cli_dispatcher.got_new);
    cli_dispatcher.got_new = false;
  }
  server_msgr->shutdown();
  client_msgr->shutdown();
  server_msgr->wait();
  client_msgr->wait();
}


class SyntheticWorkload;

struct Payload {
  enum Who : uint8_t {
    PING = 0,
    PONG = 1,
  };
  uint8_t who = 0;
  uint64_t seq = 0;
  bufferlist data;

  Payload(Who who, uint64_t seq, const bufferlist& data)
    : who(who), seq(seq), data(data)
  {}
  Payload() = default;
  DENC(Payload, v, p) {
    DENC_START(1, 1, p);
    denc(v.who, p);
    denc(v.seq, p);
    denc(v.data, p);
    DENC_FINISH(p);
  }
};
WRITE_CLASS_DENC(Payload)

ostream& operator<<(ostream& out, const Payload &pl)
{
  return out << "reply=" << pl.who << " i = " << pl.seq;
}

class SyntheticDispatcher : public Dispatcher {
 public:
  ceph::mutex lock = ceph::make_mutex("SyntheticDispatcher::lock");
  ceph::condition_variable cond;
  bool is_server;
  bool got_new;
  bool got_remote_reset;
  bool got_connect;
  map<ConnectionRef, list<uint64_t> > conn_sent;
  map<uint64_t, bufferlist> sent;
  atomic<uint64_t> index;
  SyntheticWorkload *workload;

  SyntheticDispatcher(bool s, SyntheticWorkload *wl):
      Dispatcher(g_ceph_context), is_server(s), got_new(false),
      got_remote_reset(false), got_connect(false), index(0), workload(wl) {
  }
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_PING:
    case MSG_COMMAND:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) override {
    std::lock_guard l{lock};
    list<uint64_t> c = conn_sent[con];
    for (list<uint64_t>::iterator it = c.begin();
         it != c.end(); ++it)
      sent.erase(*it);
    conn_sent.erase(con);
    got_connect = true;
    cond.notify_all();
  }
  void ms_handle_fast_accept(Connection *con) override {
    std::lock_guard l{lock};
    list<uint64_t> c = conn_sent[con];
    for (list<uint64_t>::iterator it = c.begin();
         it != c.end(); ++it)
      sent.erase(*it);
    conn_sent.erase(con);
    cond.notify_all();
  }
  bool ms_dispatch(Message *m) override {
    ceph_abort();
  }
  bool ms_handle_reset(Connection *con) override;
  void ms_handle_remote_reset(Connection *con) override {
    std::lock_guard l{lock};
    list<uint64_t> c = conn_sent[con];
    for (list<uint64_t>::iterator it = c.begin();
         it != c.end(); ++it)
      sent.erase(*it);
    conn_sent.erase(con);
    got_remote_reset = true;
  }
  bool ms_handle_refused(Connection *con) override {
    return false;
  }
  void ms_fast_dispatch(Message *m) override {
    // MSG_COMMAND is used to disorganize regular message flow
    if (m->get_type() == MSG_COMMAND) {
      m->put();
      return ;
    }

    Payload pl;
    auto p = m->get_data().cbegin();
    decode(pl, p);
    if (pl.who == Payload::PING) {
      lderr(g_ceph_context) << __func__ << " conn=" << m->get_connection() << pl << dendl;
      reply_message(m, pl);
      m->put();
      std::lock_guard l{lock};
      got_new = true;
      cond.notify_all();
    } else {
      std::lock_guard l{lock};
      if (sent.count(pl.seq)) {
	lderr(g_ceph_context) << __func__ << " conn=" << m->get_connection() << pl << dendl;
	ASSERT_EQ(conn_sent[m->get_connection()].front(), pl.seq);
	ASSERT_TRUE(pl.data.contents_equal(sent[pl.seq]));
	conn_sent[m->get_connection()].pop_front();
	sent.erase(pl.seq);
      }
      m->put();
      got_new = true;
      cond.notify_all();
    }
  }

  int ms_handle_fast_authentication(Connection *con) override {
    return 1;
  }

  void reply_message(const Message *m, Payload& pl) {
    pl.who = Payload::PONG;
    bufferlist bl;
    encode(pl, bl);
    MPing *rm = new MPing();
    rm->set_data(bl);
    m->get_connection()->send_message(rm);
    lderr(g_ceph_context) << __func__ << " conn=" << m->get_connection() << " reply m=" << m << " i=" << pl.seq << dendl;
  }

  void send_message_wrap(ConnectionRef con, const bufferlist& data) {
    Message *m = new MPing();
    Payload pl{Payload::PING, index++, data};
    bufferlist bl;
    encode(pl, bl);
    m->set_data(bl);
    if (!con->get_messenger()->get_default_policy().lossy) {
      std::lock_guard l{lock};
      sent[pl.seq] = pl.data;
      conn_sent[con].push_back(pl.seq);
    }
    lderr(g_ceph_context) << __func__ << " conn=" << con.get() << " send m=" << m << " i=" << pl.seq << dendl;
    ASSERT_EQ(0, con->send_message(m));
  }

  uint64_t get_pending() {
    std::lock_guard l{lock};
    return sent.size();
  }

  void clear_pending(ConnectionRef con) {
    std::lock_guard l{lock};

    for (list<uint64_t>::iterator it = conn_sent[con].begin();
         it != conn_sent[con].end(); ++it)
      sent.erase(*it);
    conn_sent.erase(con);
  }

  void print() {
    for (auto && p : conn_sent) {
      if (!p.second.empty()) {
        lderr(g_ceph_context) << __func__ << " " << p.first << " wait " << p.second.size() << dendl;
      }
    }
  }
};


class SyntheticWorkload {
  ceph::mutex lock = ceph::make_mutex("SyntheticWorkload::lock");
  ceph::condition_variable cond;
  set<Messenger*> available_servers;
  set<Messenger*> available_clients;
  Messenger::Policy client_policy;
  map<ConnectionRef, pair<Messenger*, Messenger*> > available_connections;
  SyntheticDispatcher dispatcher;
  gen_type rng;
  vector<bufferlist> rand_data;
  DummyAuthClientServer dummy_auth;

 public:
  const unsigned max_in_flight = 0;
  const unsigned max_connections = 0;
  static const unsigned max_message_len = 1024 * 1024 * 4;

  SyntheticWorkload(int servers, int clients, string type, int random_num,
                    Messenger::Policy srv_policy, Messenger::Policy cli_policy,
		    int _max_in_flight = 64, int _max_connections = 128)
    : client_policy(cli_policy),
      dispatcher(false, this),
      rng(time(NULL)),
      dummy_auth(g_ceph_context),
      max_in_flight(_max_in_flight),
      max_connections(_max_connections) {

    dummy_auth.auth_registry.refresh_config();
    Messenger *msgr;
    int base_port = 16800;
    entity_addr_t bind_addr;
    char addr[64];
    for (int i = 0; i < servers; ++i) {
      msgr = Messenger::create(g_ceph_context, type, entity_name_t::OSD(0),
                               "server", getpid()+i);
      snprintf(addr, sizeof(addr), "v2:127.0.0.1:%d",
	       base_port+i);
      bind_addr.parse(addr);
      msgr->bind(bind_addr);
      msgr->add_dispatcher_head(&dispatcher);
      msgr->set_auth_client(&dummy_auth);
      msgr->set_auth_server(&dummy_auth);

      ceph_assert(msgr);
      msgr->set_default_policy(srv_policy);
      available_servers.insert(msgr);
      msgr->start();
    }

    for (int i = 0; i < clients; ++i) {
      msgr = Messenger::create(g_ceph_context, type, entity_name_t::CLIENT(-1),
                               "client", getpid()+i+servers);
      if (cli_policy.standby) {
        snprintf(addr, sizeof(addr), "v2:127.0.0.1:%d",
		 base_port+i+servers);
        bind_addr.parse(addr);
        msgr->bind(bind_addr);
      }
      msgr->add_dispatcher_head(&dispatcher);
      msgr->set_auth_client(&dummy_auth);
      msgr->set_auth_server(&dummy_auth);

      ceph_assert(msgr);
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

  ConnectionRef _get_random_connection() {
    while (dispatcher.get_pending() > max_in_flight) {
      lock.unlock();
      usleep(500);
      lock.lock();
    }
    ceph_assert(ceph_mutex_is_locked(lock));
    boost::uniform_int<> choose(0, available_connections.size() - 1);
    int index = choose(rng);
    map<ConnectionRef, pair<Messenger*, Messenger*> >::iterator i = available_connections.begin();
    for (; index > 0; --index, ++i) ;
    return i->first;
  }

  bool can_create_connection() {
    return available_connections.size() < max_connections;
  }

  void generate_connection() {
    std::lock_guard l{lock};
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

    pair<Messenger*, Messenger*> p;
    {
      boost::uniform_int<> choose(0, available_servers.size() - 1);
      if (server->get_default_policy().server) {
        p = make_pair(client, server);
	ConnectionRef conn = client->connect_to(server->get_mytype(),
						server->get_myaddrs());
	available_connections[conn] = p;
      } else {
        ConnectionRef conn = client->connect_to(server->get_mytype(),
						server->get_myaddrs());
	p = make_pair(client, server);
	available_connections[conn] = p;
      }
    }
  }

  void send_message() {
    std::lock_guard l{lock};
    ConnectionRef conn = _get_random_connection();
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val >= 95) {
      uuid_d uuid;
      uuid.generate_random();
      MCommand *m = new MCommand(uuid);
      vector<string> cmds;
      cmds.push_back("command");
      m->cmd = cmds;
      m->set_priority(200);
      conn->send_message(m);
    } else {
      boost::uniform_int<> u(0, rand_data.size()-1);
      dispatcher.send_message_wrap(conn, rand_data[u(rng)]);
    }
  }

  void send_large_message(bool inject_network_congestion=false) {
    std::lock_guard l{lock};
    ConnectionRef conn = _get_random_connection();
    uuid_d uuid;
    uuid.generate_random();
    MCommand *m = new MCommand(uuid);
    vector<string> cmds;
    cmds.push_back("command");
    // set the random data to make the large message
    bufferlist bl;
    string s("abcdefghijklmnopqrstuvwxyz");
    for (int i = 0; i < 1024*256; i++)
      bl.append(s);
    // bl is around 6M
    m->set_data(bl);
    m->cmd = cmds;
    m->set_priority(200);
    // setup after connection is ready
    if (inject_network_congestion && conn->is_connected()) {
      g_ceph_context->_conf.set_val("ms_inject_network_congestion", "100");
    } else {
      g_ceph_context->_conf.set_val("ms_inject_network_congestion", "0");
    }
    conn->send_message(m);
  }

  void drop_connection() {
    std::lock_guard l{lock};
    if (available_connections.size() < 10)
      return;
    ConnectionRef conn = _get_random_connection();
    dispatcher.clear_pending(conn);
    conn->mark_down();
    if (!client_policy.server &&
	!client_policy.lossy &&
	client_policy.standby) {
      // it's a lossless policy, so we need to mark down each side
      pair<Messenger*, Messenger*> &p = available_connections[conn];
      if (!p.first->get_default_policy().server && !p.second->get_default_policy().server) {
	ASSERT_EQ(conn->get_messenger(), p.first);
	ConnectionRef peer = p.second->connect_to(p.first->get_mytype(),
						  p.first->get_myaddrs());
	peer->mark_down();
	dispatcher.clear_pending(peer);
	available_connections.erase(peer);
      }
    }
    ASSERT_EQ(available_connections.erase(conn), 1U);
  }

  void print_internal_state(bool detail=false) {
    std::lock_guard l{lock};
    lderr(g_ceph_context) << "available_connections: " << available_connections.size()
         << " inflight messages: " << dispatcher.get_pending() << dendl;
    if (detail && !available_connections.empty()) {
      dispatcher.print();
    }
  }

  void wait_for_done() {
    int64_t tick_us = 1000 * 100; // 100ms
    int64_t timeout_us = 5 * 60 * 1000 * 1000; // 5 mins
    int i = 0;
    while (dispatcher.get_pending()) {
      usleep(tick_us);
      timeout_us -= tick_us;
      if (i++ % 50 == 0)
        print_internal_state(true);
      if (timeout_us < 0)
        ceph_abort_msg(" loop time exceed 5 mins, it looks we stuck into some problems!");
    }
    for (set<Messenger*>::iterator it = available_servers.begin();
         it != available_servers.end(); ++it) {
      (*it)->shutdown();
      (*it)->wait();
      ASSERT_EQ((*it)->get_dispatch_queue_len(), 0);
      delete (*it);
    }
    available_servers.clear();

    for (set<Messenger*>::iterator it = available_clients.begin();
         it != available_clients.end(); ++it) {
      (*it)->shutdown();
      (*it)->wait();
      ASSERT_EQ((*it)->get_dispatch_queue_len(), 0);
      delete (*it);
    }
    available_clients.clear();
  }

  void handle_reset(Connection *con) {
    std::lock_guard l{lock};
    available_connections.erase(con);
    dispatcher.clear_pending(con);
  }
};

bool SyntheticDispatcher::ms_handle_reset(Connection *con) {
  workload->handle_reset(con);
  return true;
}

TEST_P(MessengerTest, SyntheticStressTest) {
  SyntheticWorkload test_msg(8, 32, GetParam(), 100,
                             Messenger::Policy::stateful_server(0),
                             Messenger::Policy::lossless_client(0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) lderr(g_ceph_context) << "seeding connection " << i << dendl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 5000; ++i) {
    if (!(i % 10)) {
      lderr(g_ceph_context) << "Op " << i << ": " << dendl;
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

TEST_P(MessengerTest, SyntheticStressTest1) {
  SyntheticWorkload test_msg(16, 32, GetParam(), 100,
                             Messenger::Policy::lossless_peer_reuse(0),
                             Messenger::Policy::lossless_peer_reuse(0));
  for (int i = 0; i < 10; ++i) {
    if (!(i % 10)) lderr(g_ceph_context) << "seeding connection " << i << dendl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 10000; ++i) {
    if (!(i % 10)) {
      lderr(g_ceph_context) << "Op " << i << ": " << dendl;
      test_msg.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 80) {
      test_msg.generate_connection();
    } else if (val > 60) {
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
  uint64_t dispatch_throttle_bytes = g_ceph_context->_conf->ms_dispatch_throttle_bytes;
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "30");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0.1");
  g_ceph_context->_conf.set_val("ms_dispatch_throttle_bytes", "16777216");
  SyntheticWorkload test_msg(8, 32, GetParam(), 100,
                             Messenger::Policy::stateful_server(0),
                             Messenger::Policy::lossless_client(0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) lderr(g_ceph_context) << "seeding connection " << i << dendl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) {
      lderr(g_ceph_context) << "Op " << i << ": " << dendl;
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
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "0");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0");
  g_ceph_context->_conf.set_val(
      "ms_dispatch_throttle_bytes", std::to_string(dispatch_throttle_bytes));
}

TEST_P(MessengerTest, SyntheticInjectTest2) {
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "30");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0.1");
  SyntheticWorkload test_msg(8, 16, GetParam(), 100,
                             Messenger::Policy::lossless_peer_reuse(0),
                             Messenger::Policy::lossless_peer_reuse(0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) lderr(g_ceph_context) << "seeding connection " << i << dendl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) {
      lderr(g_ceph_context) << "Op " << i << ": " << dendl;
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
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "0");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0");
}

TEST_P(MessengerTest, SyntheticInjectTest3) {
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "600");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0.1");
  SyntheticWorkload test_msg(8, 16, GetParam(), 100,
                             Messenger::Policy::stateless_server(0),
                             Messenger::Policy::lossy_client(0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) lderr(g_ceph_context) << "seeding connection " << i << dendl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) {
      lderr(g_ceph_context) << "Op " << i << ": " << dendl;
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
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "0");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0");
}


TEST_P(MessengerTest, SyntheticInjectTest4) {
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "30");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0.1");
  g_ceph_context->_conf.set_val("ms_inject_delay_probability", "1");
  g_ceph_context->_conf.set_val("ms_inject_delay_type", "client osd");
  g_ceph_context->_conf.set_val("ms_inject_delay_max", "5");
  SyntheticWorkload test_msg(16, 32, GetParam(), 100,
                             Messenger::Policy::lossless_peer(0),
                             Messenger::Policy::lossless_peer(0));
  for (int i = 0; i < 100; ++i) {
    if (!(i % 10)) lderr(g_ceph_context) << "seeding connection " << i << dendl;
    test_msg.generate_connection();
  }
  gen_type rng(time(NULL));
  for (int i = 0; i < 1000; ++i) {
    if (!(i % 10)) {
      lderr(g_ceph_context) << "Op " << i << ": " << dendl;
      test_msg.print_internal_state();
    }
    boost::uniform_int<> true_false(0, 99);
    int val = true_false(rng);
    if (val > 95) {
      test_msg.generate_connection();
    } else if (val > 80) {
      // test_msg.drop_connection();
    } else if (val > 10) {
      test_msg.send_message();
    } else {
      usleep(rand() % 500 + 100);
    }
  }
  test_msg.wait_for_done();
  g_ceph_context->_conf.set_val("ms_inject_socket_failures", "0");
  g_ceph_context->_conf.set_val("ms_inject_internal_delays", "0");
  g_ceph_context->_conf.set_val("ms_inject_delay_probability", "0");
  g_ceph_context->_conf.set_val("ms_inject_delay_type", "");
  g_ceph_context->_conf.set_val("ms_inject_delay_max", "0");
}

// This is test for network block, means ::send return EAGAIN
TEST_P(MessengerTest, SyntheticInjectTest5) {
  SyntheticWorkload test_msg(1, 8, GetParam(), 100,
                             Messenger::Policy::stateful_server(0),
                             Messenger::Policy::lossless_client(0),
			     64, 2);
  bool simulate_network_congestion = true;
  for (int i = 0; i < 2; ++i)
    test_msg.generate_connection();
  for (int i = 0; i < 5000; ++i) {
    if (!(i % 10)) {
      ldout(g_ceph_context, 0) << "Op " << i << ": " << dendl;
      test_msg.print_internal_state();
    }
    if (i < 1600) {
      // means that we would stuck 1600 * 6M (9.6G) around with 2 connections
      test_msg.send_large_message(simulate_network_congestion);
    } else {
      simulate_network_congestion = false;
      test_msg.send_large_message(simulate_network_congestion);
    }
  }
  test_msg.wait_for_done();
}


class MarkdownDispatcher : public Dispatcher {
  ceph::mutex lock = ceph::make_mutex("MarkdownDispatcher::lock");
  set<ConnectionRef> conns;
  bool last_mark;
 public:
  std::atomic<uint64_t> count = { 0 };
  explicit MarkdownDispatcher(bool s): Dispatcher(g_ceph_context),
                              last_mark(false) {
  }
  bool ms_can_fast_dispatch_any() const override { return false; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_PING:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) override {
    lderr(g_ceph_context) << __func__ << " " << con << dendl;
    std::lock_guard l{lock};
    conns.insert(con);
  }
  void ms_handle_fast_accept(Connection *con) override {
    std::lock_guard l{lock};
    conns.insert(con);
  }
  bool ms_dispatch(Message *m) override {
    lderr(g_ceph_context) << __func__ << " conn: " << m->get_connection() << dendl;
    std::lock_guard l{lock};
    count++;
    conns.insert(m->get_connection());
    if (conns.size() < 2 && !last_mark) {
      m->put();
      return true;
    }

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
  bool ms_handle_reset(Connection *con) override {
    lderr(g_ceph_context) << __func__ << " " << con << dendl;
    std::lock_guard l{lock};
    conns.erase(con);
    usleep(rand() % 500);
    return true;
  }
  void ms_handle_remote_reset(Connection *con) override {
    std::lock_guard l{lock};
    conns.erase(con);
    lderr(g_ceph_context) << __func__ << " " << con << dendl;
  }
  bool ms_handle_refused(Connection *con) override {
    return false;
  }
  void ms_fast_dispatch(Message *m) override {
    ceph_abort();
  }
  int ms_handle_fast_authentication(Connection *con) override {
    return 1;
  }
};


// Markdown with external lock
TEST_P(MessengerTest, MarkdownTest) {
  Messenger *server_msgr2 = Messenger::create(g_ceph_context, string(GetParam()), entity_name_t::OSD(0), "server", getpid());
  MarkdownDispatcher cli_dispatcher(false), srv_dispatcher(true);
  DummyAuthClientServer dummy_auth(g_ceph_context);
  dummy_auth.auth_registry.refresh_config();
  entity_addr_t bind_addr;
  bind_addr.parse("v2:127.0.0.1:16800");
  server_msgr->bind(bind_addr);
  server_msgr->add_dispatcher_head(&srv_dispatcher);
  server_msgr->set_auth_client(&dummy_auth);
  server_msgr->set_auth_server(&dummy_auth);
  server_msgr->start();
  bind_addr.parse("v2:127.0.0.1:16801");
  server_msgr2->bind(bind_addr);
  server_msgr2->add_dispatcher_head(&srv_dispatcher);
  server_msgr2->set_auth_client(&dummy_auth);
  server_msgr2->set_auth_server(&dummy_auth);
  server_msgr2->start();

  client_msgr->add_dispatcher_head(&cli_dispatcher);
  client_msgr->set_auth_client(&dummy_auth);
  client_msgr->set_auth_server(&dummy_auth);
  client_msgr->start();

  int i = 1000;
  uint64_t last = 0;
  bool equal = false;
  uint64_t equal_count = 0;
  while (i--) {
    ConnectionRef conn1 = client_msgr->connect_to(server_msgr->get_mytype(),
						       server_msgr->get_myaddrs());
    ConnectionRef conn2 = client_msgr->connect_to(server_msgr2->get_mytype(),
						       server_msgr2->get_myaddrs());
    MPing *m = new MPing();
    ASSERT_EQ(conn1->send_message(m), 0);
    m = new MPing();
    ASSERT_EQ(conn2->send_message(m), 0);
    CHECK_AND_WAIT_TRUE(srv_dispatcher.count > last + 1);
    if (srv_dispatcher.count == last) {
      lderr(g_ceph_context) << __func__ << " last is " << last << dendl;
      equal = true;
      equal_count++;
    } else {
      equal = false;
      equal_count = 0;
    }
    last = srv_dispatcher.count;
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

INSTANTIATE_TEST_SUITE_P(
  Messenger,
  MessengerTest,
  ::testing::Values(
    "async+posix"
  )
);

int main(int argc, char **argv) {
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  g_ceph_context->_conf.set_val("auth_cluster_required", "none");
  g_ceph_context->_conf.set_val("auth_service_required", "none");
  g_ceph_context->_conf.set_val("auth_client_required", "none");
  g_ceph_context->_conf.set_val("keyring", "/dev/null");
  g_ceph_context->_conf.set_val("enable_experimental_unrecoverable_data_corrupting_features", "ms-type-async");
  g_ceph_context->_conf.set_val("ms_die_on_bad_msg", "true");
  g_ceph_context->_conf.set_val("ms_die_on_old_message", "true");
  g_ceph_context->_conf.set_val("ms_max_backoff", "1");
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 ceph_test_msgr && valgrind --tool=memcheck ./ceph_test_msgr"
 * End:
 */
