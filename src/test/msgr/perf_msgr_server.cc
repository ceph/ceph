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
  class Worker : public Thread {
    ServerDispatcher *dispatcher;
    list<Message*> messages;
    bool is_stop;
    Mutex lock;
    Cond cond;

   public:
    Worker(ServerDispatcher *d): dispatcher(d), is_stop(false), lock("ServerDispatcher::Worker::lock") {}
    void queue(Message *m) {
      Mutex::Locker l(lock);
      messages.push_back(m);
      cond.Signal();
    }
    void *entry() {
      Mutex::Locker l(lock);
      while (!is_stop) {
        if (!messages.empty()) {
          Message *m = messages.back();
          messages.pop_back();
          lock.Unlock();
          MOSDOp *osd_op = static_cast<MOSDOp*>(m);
          MOSDOpReply *reply = new MOSDOpReply(osd_op, 0, 0, 0, false);
          m->get_connection()->send_message(reply);
          m->put();
          lock.Lock();
        } else {
          cond.Wait(lock);
        }
      }
      return 0;
    }
    void stop() {
      Mutex::Locker l(lock);
      is_stop = true;
      cond.Signal();
    }
  } worker;
  friend class Worker;

 public:
  ServerDispatcher(uint64_t delay): Dispatcher(g_ceph_context), think_time(delay), worker(this) {
    worker.create();
  }
  ~ServerDispatcher() {
    worker.stop();
    worker.join();
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
    worker.queue(m);
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
  MessengerServer(string t, string addr, int delay):
      msgr(NULL), type(t), bindaddr(addr), dispatcher(delay) {
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
  cerr << "Usage: " << name << " [bind ip:port] [thinktime us]" << std::endl;
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (args.size() < 2) {
    usage(argv[0]);
    return 1;
  }

  int think_time = atoi(args[1]);
  cerr << " This tool won't handle connection error alike things, " << std::endl;
  cerr << "please ensure the proper network environment to test." << std::endl;
  cerr << " Or ctrl+c when meeting error and restart tests" << std::endl;
  cerr << " using ms-type " << g_ceph_context->_conf->ms_type << std::endl;
  cerr << "       bind ip:port " << args[0] << std::endl;
  cerr << "       thinktime(us) " << think_time << std::endl;

  MessengerServer server(g_ceph_context->_conf->ms_type, args[0], think_time);
  server.start();

  return 0;
}
