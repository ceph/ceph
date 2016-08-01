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

class MessengerClient {
  class ClientThread;
  class ClientDispatcher : public Dispatcher {
    uint64_t think_time;
    ClientThread *thread;

   public:
    ClientDispatcher(uint64_t delay, ClientThread *t): Dispatcher(g_ceph_context), think_time(delay), thread(t) {}
    bool ms_can_fast_dispatch_any() const { return true; }
    bool ms_can_fast_dispatch(Message *m) const {
      switch (m->get_type()) {
      case CEPH_MSG_OSD_OPREPLY:
        return true;
      default:
        return false;
      }
    }

    void ms_handle_fast_connect(Connection *con) {}
    void ms_handle_fast_accept(Connection *con) {}
    bool ms_dispatch(Message *m) { return true; }
    void ms_fast_dispatch(Message *m);
    bool ms_handle_reset(Connection *con) { return true; }
    void ms_handle_remote_reset(Connection *con) {}
    bool ms_verify_authorizer(Connection *con, int peer_type, int protocol,
                              bufferlist& authorizer, bufferlist& authorizer_reply,
                              bool& isvalid, CryptoKey& session_key) {
      isvalid = true;
      return true;
    }
  };

  class ClientThread : public Thread {
    Messenger *msgr;
    int concurrent;
    ConnectionRef conn;
    atomic_t client_inc;
    object_t oid;
    object_locator_t oloc;
    pg_t pgid;
    int msg_len;
    bufferlist data;
    int ops;
    ClientDispatcher dispatcher;

   public:
    Mutex lock;
    Cond cond;
    uint64_t inflight;

    ClientThread(Messenger *m, int c, ConnectionRef con, int len, int ops, int think_time_us):
        msgr(m), concurrent(c), conn(con), client_inc(0), oid("object-name"), oloc(1, 1), msg_len(len), ops(ops),
        dispatcher(think_time_us, this), lock("MessengerBenchmark::ClientThread::lock") {
      m->add_dispatcher_head(&dispatcher);
      bufferptr ptr(msg_len);
      memset(ptr.c_str(), 0, msg_len);
      data.append(ptr);
    }
    void *entry() {
      lock.Lock();
      for (int i = 0; i < ops; ++i) {
        if (inflight > uint64_t(concurrent)) {
          cond.Wait(lock);
        }
        MOSDOp *m = new MOSDOp(client_inc.read(), 0, oid, oloc, pgid, 0, 0, 0);
        m->write(0, msg_len, data);
        inflight++;
        conn->send_message(m);
        //cerr << __func__ << " send m=" << m << std::endl;
      }
      lock.Unlock();
      msgr->shutdown();
      return 0;
    }
  };

  string type;
  string serveraddr;
  int think_time_us;
  vector<Messenger*> msgrs;
  vector<ClientThread*> clients;

 public:
  MessengerClient(string t, string addr, int delay):
      type(t), serveraddr(addr), think_time_us(delay) {
  }
  ~MessengerClient() {
    for (uint64_t i = 0; i < clients.size(); ++i)
      delete clients[i];
    for (uint64_t i = 0; i < msgrs.size(); ++i) {
      msgrs[i]->shutdown();
      msgrs[i]->wait();
    }
  }
  void ready(int c, int jobs, int ops, int msg_len) {
    entity_addr_t addr;
    addr.parse(serveraddr.c_str());
    addr.set_nonce(0);
    for (int i = 0; i < jobs; ++i) {
      Messenger *msgr = Messenger::create(g_ceph_context, type, entity_name_t::CLIENT(0), "client", getpid()+i);
      msgr->set_default_policy(Messenger::Policy::lossless_client(0, 0));
      entity_inst_t inst(entity_name_t::OSD(0), addr);
      ConnectionRef conn = msgr->get_connection(inst);
      ClientThread *t = new ClientThread(msgr, c, conn, msg_len, ops, think_time_us);
      msgrs.push_back(msgr);
      clients.push_back(t);
      msgr->start();
    }
    usleep(1000*1000);
  }
  void start() {
    for (uint64_t i = 0; i < clients.size(); ++i)
      clients[i]->create("client");
    for (uint64_t i = 0; i < msgrs.size(); ++i)
      msgrs[i]->wait();
  }
};

void MessengerClient::ClientDispatcher::ms_fast_dispatch(Message *m) {
  usleep(think_time);
  m->put();
  Mutex::Locker l(thread->lock);
  thread->inflight--;
  thread->cond.Signal();
}


void usage(const string &name) {
  cerr << "Usage: " << name << " [server ip:port] [numjobs] [concurrency] [ios] [thinktime us] [msg length]" << std::endl;
  cerr << "       [server ip:port]: connect to the ip:port pair" << std::endl;
  cerr << "       [numjobs]: how much client threads spawned and do benchmark" << std::endl;
  cerr << "       [concurrency]: the max inflight messages(like iodepth in fio)" << std::endl;
  cerr << "       [ios]: how much messages sent for each client" << std::endl;
  cerr << "       [thinktime]: sleep time when do fast dispatching(match client logic)" << std::endl;
  cerr << "       [msg length]: message data bytes" << std::endl;
}

int main(int argc, char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (args.size() < 6) {
    usage(argv[0]);
    return 1;
  }

  int numjobs = atoi(args[1]);
  int concurrent = atoi(args[2]);
  int ios = atoi(args[3]);
  int think_time = atoi(args[4]);
  int len = atoi(args[5]);

  cerr << " using ms-type " << g_ceph_context->_conf->ms_type << std::endl;
  cerr << "       server ip:port " << args[0] << std::endl;
  cerr << "       numjobs " << numjobs << std::endl;
  cerr << "       concurrency " << concurrent << std::endl;
  cerr << "       ios " << ios << std::endl;
  cerr << "       thinktime(us) " << think_time << std::endl;
  cerr << "       message data bytes " << len << std::endl;
  MessengerClient client(g_ceph_context->_conf->ms_type, args[0], think_time);
  client.ready(concurrent, numjobs, ios, len);
  Cycles::init();
  uint64_t start = Cycles::rdtsc();
  client.start();
  uint64_t stop = Cycles::rdtsc();
  cerr << " Total op " << ios << " run time " << Cycles::to_microseconds(stop - start) << "us." << std::endl;

  return 0;
}
