// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang
 *
 * Author: Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdlib.h>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <iostream>

using namespace std;

#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/WorkQueue.h"
#include "global/global_init.h"
#include "msg/Messenger.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "auth/DummyAuth.h"

class ServerDispatcher : public Dispatcher {
  uint64_t think_time;
  ThreadPool op_tp;
  class OpWQ : public ThreadPool::WorkQueue<Message> {
    list<Message*> messages;

   public:
    OpWQ(ceph::timespan timeout, ceph::timespan suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<Message>("ServerDispatcher::OpWQ", timeout, suicide_timeout, tp) {}

    bool _enqueue(Message *m) override {
      messages.push_back(m);
      return true;
    }
    void _dequeue(Message *m) override {
      ceph_abort();
    }
    bool _empty() override {
      return messages.empty();
    }
    Message *_dequeue() override {
      if (messages.empty())
	return NULL;
      Message *m = messages.front();
      messages.pop_front();
      return m;
    }
    void _process(Message *m, ThreadPool::TPHandle &handle) override {
      MOSDOp *osd_op = static_cast<MOSDOp*>(m);
      MOSDOpReply *reply = new MOSDOpReply(osd_op, 0, 0, 0, false);
      m->get_connection()->send_message(reply);
      m->put();
    }
    void _process_finish(Message *m) override { }
    void _clear() override {
      ceph_assert(messages.empty());
    }
  } op_wq;

 public:
  ServerDispatcher(int threads, uint64_t delay): Dispatcher(g_ceph_context), think_time(delay),
    op_tp(g_ceph_context, "ServerDispatcher::op_tp", "tp_serv_disp", threads, "serverdispatcher_op_threads"),
    op_wq(ceph::make_timespan(30), ceph::make_timespan(30), &op_tp) {
    op_tp.start();
  }
  ~ServerDispatcher() override {
    op_tp.stop();
  }
  bool ms_can_fast_dispatch_any() const override { return true; }
  bool ms_can_fast_dispatch(const Message *m) const override {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OP:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) override {}
  void ms_handle_fast_accept(Connection *con) override {}
  bool ms_dispatch(Message *m) override { return true; }
  bool ms_handle_reset(Connection *con) override { return true; }
  void ms_handle_remote_reset(Connection *con) override {}
  bool ms_handle_refused(Connection *con) override { return false; }
  void ms_fast_dispatch(Message *m) override {
    usleep(think_time);
    //cerr << __func__ << " reply message=" << m << std::endl;
    op_wq.queue(m);
  }
  int ms_handle_fast_authentication(Connection *con) override {
    return 1;
  }
};

class MessengerServer {
  Messenger *msgr;
  string type;
  string bindaddr;
  ServerDispatcher dispatcher;
  DummyAuthClientServer dummy_auth;

 public:
  MessengerServer(const string &t, const string &addr, int threads, int delay):
      msgr(NULL), type(t), bindaddr(addr), dispatcher(threads, delay),
      dummy_auth(g_ceph_context) {
    msgr = Messenger::create(g_ceph_context, type, entity_name_t::OSD(0), "server", 0);
    msgr->set_default_policy(Messenger::Policy::stateless_server(0));
    dummy_auth.auth_registry.refresh_config();
      msgr->set_auth_server(&dummy_auth);
  }
  ~MessengerServer() {
    msgr->shutdown();
    msgr->wait();
  }
  void start() {
    entity_addr_t addr;
    addr.parse(bindaddr.c_str());
    msgr->bind(addr);
    msgr->add_dispatcher_head(&dispatcher);
    msgr->start();
    msgr->wait();
  }
};

void usage(const string &name) {
  cerr << "Usage: " << name << " [bind ip:port] [server worker threads] [thinktime us]" << std::endl;
  cerr << "       [bind ip:port]: The ip:port pair to bind, client need to specify this pair to connect" << std::endl;
  cerr << "       [server worker threads]: threads will process incoming messages and reply(matching pg threads)" << std::endl;
  cerr << "       [thinktime]: sleep time when do dispatching(match fast dispatch logic in OSD.cc)" << std::endl;
}

int main(int argc, char **argv)
{
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf.apply_changes(nullptr);

  if (args.size() < 3) {
    usage(argv[0]);
    return 1;
  }

  int worker_threads = atoi(args[1]);
  int think_time = atoi(args[2]);
  std::string public_msgr_type = g_ceph_context->_conf->ms_public_type.empty() ? g_ceph_context->_conf.get_val<std::string>("ms_type") : g_ceph_context->_conf->ms_public_type;

  cerr << " This tool won't handle connection error alike things, " << std::endl;
  cerr << "please ensure the proper network environment to test." << std::endl;
  cerr << " Or ctrl+c when meeting error and restart tests" << std::endl;
  cerr << " using ms-public-type " << public_msgr_type << std::endl;
  cerr << "       bind ip:port " << args[0] << std::endl;
  cerr << "       worker threads " << worker_threads << std::endl;
  cerr << "       thinktime(us) " << think_time << std::endl;

  MessengerServer server(public_msgr_type, args[0], worker_threads, think_time);
  server.start();

  return 0;
}
