// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "messages/MPing.h"

#include "common/Timer.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"

#include <sys/types.h>
#include <fcntl.h>

#define dout_subsys ceph_subsys_ms

Messenger *messenger = 0;

Mutex test_lock("mylock");
Cond cond;

uint64_t received = 0;

class Admin : public Dispatcher {
public:
  Admin() 
    : Dispatcher(g_ceph_context)
  {
  }
private:
  bool ms_dispatch(Message *m) {

    //cerr << "got ping from " << m->get_source() << std::endl;
    dout(0) << "got ping from " << m->get_source() << dendl;
    test_lock.Lock();
    ++received;
    cond.Signal();
    test_lock.Unlock();

    m->put();
    return true;
  }

  bool ms_handle_reset(Connection *con) { return false; }
  void ms_handle_remote_reset(Connection *con) {}

} dispatcher;


int main(int argc, const char **argv, const char *envp[]) {

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  dout(0) << "i am mon " << args[0] << dendl;

  // get monmap
  MonClient mc(g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;
  
  // start up network
  int whoami = mc.monmap.get_rank(args[0]);
  assert(whoami >= 0);
  ostringstream ss;
  ss << mc.monmap.get_addr(whoami);
  std::string sss(ss.str());
  g_ceph_context->_conf->set_val("public_addr", sss.c_str());
  g_ceph_context->_conf->apply_changes(NULL);
  Messenger *rank = Messenger::create(g_ceph_context,
				      g_conf->ms_type,
				      entity_name_t::MON(whoami), "tester",
				      getpid());
  int err = rank->bind(g_ceph_context->_conf->public_addr);
  if (err < 0)
    return 1;

  // start monitor
  messenger = rank;
  messenger->set_default_send_priority(CEPH_MSG_PRIO_HIGH);
  messenger->add_dispatcher_head(&dispatcher);

  rank->start();
  
  int isend = 0;
  if (whoami == 0)
    isend = 100;

  test_lock.Lock();
  uint64_t sent = 0;
  while (1) {
    while (received + isend <= sent) {
      //cerr << "wait r " << received << " s " << sent << " is " << isend << std::endl;
      dout(0) << "wait r " << received << " s " << sent << " is " << isend << dendl;
      cond.Wait(test_lock);
    }

    int t = rand() % mc.get_num_mon();
    if (t == whoami)
      continue;
    
    if (rand() % 10 == 0) {
      //cerr << "mark_down " << t << std::endl;
      dout(0) << "mark_down " << t << dendl;
      messenger->mark_down(mc.get_mon_addr(t));
    } 
    //cerr << "pinging " << t << std::endl;
    dout(0) << "pinging " << t << dendl;
    messenger->send_message(new MPing, mc.get_mon_inst(t));
    cerr << isend << "\t" << ++sent << "\t" << received << "\r";
  }
  test_lock.Unlock();

  // wait for messenger to finish
  rank->wait();
  
  return 0;
}

