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

#include "config.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"
#include "msg/SimpleMessenger.h"
#include "messages/MPing.h"

#include "common/Timer.h"
#include "common/common_init.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


MonMap monmap;
Messenger *messenger = 0;

Mutex lock("mylock");
Cond cond;

__u64 received = 0;

class Admin : public Dispatcher {
  bool dispatch_impl(Message *m) {

    //cerr << "got ping from " << m->get_source() << std::endl;
    dout(0) << "got ping from " << m->get_source() << dendl;
    lock.Lock();
    ++received;
    cond.Signal();
    lock.Unlock();

    delete m;
    return true;
  }

  void ms_handle_failure(Message *m, const entity_inst_t& inst) { 
  }

} dispatcher;


int main(int argc, const char **argv, const char *envp[]) {

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, NULL, false);

  vec_to_argv(args, argc, argv);

  int whoami = atoi(args[0]);
  dout(0) << "i am mon" << whoami << dendl;

  // get monmap
  MonClient mc(&monmap, NULL);
  if (!mc.get_monmap())
    return -1;
  
  // start up network
  g_my_addr = monmap.get_inst(whoami).addr;
  SimpleMessenger rank;
  int err = rank.bind();
  if (err < 0)
    return 1;

  _dout_create_courtesy_output_symlink("mon", whoami);

  // start monitor
  messenger = rank.register_entity(entity_name_t::MON(whoami));
  messenger->set_default_send_priority(CEPH_MSG_PRIO_HIGH);
  messenger->set_dispatcher(&dispatcher);

  rank.start();
  
  int isend = 0;
  if (whoami == 0)
    isend = 100;

  lock.Lock();
  __u64 sent = 0;
  while (1) {
    while (received + isend <= sent) {
      //cerr << "wait r " << received << " s " << sent << " is " << isend << std::endl;
      dout(0) << "wait r " << received << " s " << sent << " is " << isend << dendl;
      cond.Wait(lock);
    }

    int t = rand() % monmap.size();
    if (t == whoami)
      continue;
    
    if (rand() % 10 == 0) {
      //cerr << "mark_down " << t << std::endl;
      dout(0) << "mark_down " << t << dendl;
      messenger->mark_down(monmap.get_inst(t).addr);
    } 
    //cerr << "pinging " << t << std::endl;
    dout(0) << "pinging " << t << dendl;
    messenger->send_message(new MPing, monmap.get_inst(t));
    cerr << isend << "\t" << ++sent << "\t" << received << "\r";
  }
  lock.Unlock();

  // wait for messenger to finish
  rank.wait();
  
  return 0;
}

