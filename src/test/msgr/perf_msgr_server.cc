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

#include "include/atomic.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "common/Cycles.h"
#include "global/global_init.h"
#include "msg/Messenger.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"

class ServerDispatcher : public Dispatcher {
  uint64_t think_time;
  ThreadPool op_tp;
  class OpWQ : public ThreadPool::WorkQueue<Message> {
    list<Message*> messages;

   public:
    OpWQ(time_t timeout, time_t suicide_timeout, ThreadPool *tp)
      : ThreadPool::WorkQueue<Message>("ServerDispatcher::OpWQ", timeout, suicide_timeout, tp) {}

    bool _enqueue(Message *m) {
      messages.push_back(m);
      return true;
    }
    void _dequeue(Message *m) {
      assert(0);
    }
    bool _empty() {
      return messages.empty();
    }
    Message *_dequeue() {
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
    void _process_finish(Message *m) { }
    void _clear() {
      assert(messages.empty());
    }
  } op_wq;

 public:
  ServerDispatcher(int threads, uint64_t delay): Dispatcher(g_ceph_context), think_time(delay),
    op_tp(g_ceph_context, "ServerDispatcher::op_tp", "tp_serv_disp", threads, "serverdispatcher_op_threads"),
    op_wq(30, 30, &op_tp) {
    op_tp.start();
  }
  ~ServerDispatcher() {
    op_tp.stop();
  }
  bool ms_can_fast_dispatch_any() const { return true; }
  bool ms_can_fast_dispatch(Message *m) const {
    switch (m->get_type()) {
    case CEPH_MSG_OSD_OP:
      return true;
    default:
      return false;
    }
  }

  void ms_handle_fast_connect(Connection *con) {}
  void ms_handle_fast_accept(Connection *con) {}
  bool ms_dispatch(Message *m) { return true; }
  bool ms_handle_reset(Connection *con) { return true; }
  void ms_handle_remote_reset(Connection *con) {}
  void ms_fast_dispatch(Message *m) {
    usleep(think_time);
    //cerr << __func__ << " reply message=" << m << std::endl;
    op_wq.queue(m);
  }
  bool ms_verify_authorizer(Connection *con, int peer_type, int protocol,
                            bufferlist& authorizer, bufferlist& authorizer_reply,
                            bool& isvalid, CryptoKey& session_key) {
    isvalid = true;
    return true;
  }
};

class MessengerServer {
  Messenger *msgr;
  string type;
  string bindaddr;
  ServerDispatcher dispatcher;

 public:
  MessengerServer(string t, string addr, int threads, int delay):
      msgr(NULL), type(t), bindaddr(addr), dispatcher(threads, delay) {
    msgr = Messenger::create(g_ceph_context, type, entity_name_t::OSD(0), "server", 0);
    msgr->set_default_policy(Messenger::Policy::stateless_server(0, 0));
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
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (args.size() < 3) {
    usage(argv[0]);
    return 1;
  }

  int worker_threads = atoi(args[1]);
  int think_time = atoi(args[2]);
  cerr << " This tool won't handle connection error alike things, " << std::endl;
  cerr << "please ensure the proper network environment to test." << std::endl;
  cerr << " Or ctrl+c when meeting error and restart tests" << std::endl;
  cerr << " using ms-type " << g_ceph_context->_conf->ms_type << std::endl;
  cerr << "       bind ip:port " << args[0] << std::endl;
  cerr << "       worker threads " << worker_threads << std::endl;
  cerr << "       thinktime(us) " << think_time << std::endl;

  MessengerServer server(g_ceph_context->_conf->ms_type, args[0], worker_threads, think_time);
  server.start();

  return 0;
}
