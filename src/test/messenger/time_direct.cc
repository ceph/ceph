// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <condition_variable>
#include <mutex>

#include "msg/direct/DirectMessenger.h"
#include "msg/FastStrategy.h"
#include "messages/MDataPing.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

namespace {

  bool verbose = false;

  uint32_t max_replies = 1000000;
  uint32_t reply_count = 0;
  
  MDataPing* cmsg = new MDataPing();
  MDataPing* rmsg = new MDataPing();
  
  class C_NotifyCond : public Context {
    std::mutex *mutex;
    std::condition_variable *cond;
    bool *done;
  public:
    C_NotifyCond(std::mutex *mutex, std::condition_variable *cond, bool *done)
      : mutex(mutex), cond(cond), done(done) {}
    void finish(int r) {
      std::lock_guard<std::mutex> lock(*mutex);
      *done = true;
      cond->notify_one();
    }
  };

  class ClientDispatcher : public Dispatcher {
    Context *c;
  public:
    ClientDispatcher(CephContext *cct, Context *c)
      : Dispatcher(cct), c(c) {}

    bool ms_handle_reset(Connection *con) { return false; }
    void ms_handle_remote_reset(Connection *con) {}

    bool ms_dispatch(Message *m) {
      if (verbose)
	std::cout << "ClientDispatcher received " << *m << std::endl;
      ++reply_count; // XXX don't need atomics in this configuration
      if (reply_count == max_replies)
	c->complete(0);
      return true;
    }
  };

  class ServerDispatcher : public Dispatcher {
  public:
    ServerDispatcher(CephContext *cct) : Dispatcher(cct) {}

    bool ms_handle_reset(Connection *con) { return false; }
    void ms_handle_remote_reset(Connection *con) {}

    bool ms_dispatch(Message *m) {
      if (verbose)
	std::cout << "ServerDispatcher received " << *m
		  << ", sending reply" << std::endl;
      ConnectionRef c = m->get_connection();
      c->send_message(rmsg);
      return true;
    }
  };

  /* XXX Cohort GENERIC avoided faking a specific entity when desired */
  const entity_name_t entity1 = entity_name_t::MON(1);
  const entity_name_t entity2 = entity_name_t::MON(2);

  CephContext* cct = nullptr;
  DirectMessenger* m1 = nullptr;
  DirectMessenger* m2 = nullptr;
  ClientDispatcher* cdi = nullptr;
  ServerDispatcher* sdi = nullptr;
  C_NotifyCond* nc = nullptr;
  
  // condition variable to wait on ping reply
  std::mutex mtx;
  std::condition_variable cond;
  bool done;

} /* namespace */

TEST(DIRECT, INIT) {

  ASSERT_NE(cct, nullptr);

  m1 = new DirectMessenger(cct, entity1, "m1", 0, new FastStrategy());
  m2 = new DirectMessenger(cct, entity2, "m2", 0, new FastStrategy());

  m1->set_direct_peer(m2);
  m2->set_direct_peer(m1);

  nc = new C_NotifyCond(&mtx, &cond, &done);
  cdi = new ClientDispatcher(cct, nc);
  m1->add_dispatcher_head(cdi);

  sdi = new ServerDispatcher(cct);
  m2->add_dispatcher_head(sdi);
}

TEST(DIRECT, RUN) {
  for (uint32_t ix = 0; ix < max_replies; ++ix) {
    // send message to m2
    m1->send_message(cmsg, m2->get_myinst());
  }

  ASSERT_EQ(reply_count, max_replies);
}

TEST(DIRECT, SHUTDOWN) {
  delete m1;
  delete m2;
  delete sdi;
  delete cdi;
}

int main(int argc, char *argv[])
{
  string val;
  vector<const char*> args;

  argv_to_vec(argc, const_cast<const char**>(argv), args);
  env_to_vec(args);

  for (auto arg_iter = args.begin(); arg_iter != args.end();) {
    if (ceph_argparse_flag(args, arg_iter, "--verbose", (char*) nullptr)) {
      verbose = true;
    } else {
      ++arg_iter;
    }
  }

  global_init(nullptr, args, CEPH_ENTITY_TYPE_ANY, CODE_ENVIRONMENT_DAEMON,
	      0);
  cct = g_ceph_context;
  common_init_finish(cct);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
