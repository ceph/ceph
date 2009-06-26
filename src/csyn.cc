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

#include "client/SyntheticClient.h"
#include "client/Client.h"

#include "msg/SimpleMessenger.h"

#include "mon/MonClient.h"

#include "common/Timer.h"
#include "common/common_init.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, const char **argv, char *envp[]) 
{
  //cerr << "csyn starting" << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  common_init(args, "csyn", false);
  parse_syn_options(args);   // for SyntheticClient

  vec_to_argv(args, argc, argv);

  if (g_conf.clock_tare) g_clock.tare();

  // get monmap
  MonClient mc;
  if (mc.build_initial_monmap() < 0)
    return -1;

  // start up network
  SimpleMessenger rank;
  rank.bind();
  cout << "starting csyn at " << rank.get_rank_addr() << std::endl;

  rank.set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::lossy_fast_fail());
  rank.set_policy(entity_name_t::TYPE_MDS, SimpleMessenger::Policy::lossless());
  rank.set_policy(entity_name_t::TYPE_OSD, SimpleMessenger::Policy::lossless());

  list<Client*> clients;
  list<SyntheticClient*> synclients;

  cout << "mounting and starting " << g_conf.num_client << " syn client(s)" << std::endl;
  for (int i=0; i<g_conf.num_client; i++) {
    Client *client = new Client(rank.register_entity(entity_name_t(entity_name_t::TYPE_CLIENT,-1)), &mc);
    SyntheticClient *syn = new SyntheticClient(client);
    clients.push_back(client);
    synclients.push_back(syn);
  }

  rank.start();

  for (list<SyntheticClient*>::iterator p = synclients.begin(); 
       p != synclients.end();
       p++)
    (*p)->start_thread();

  cout << "waiting for client(s) to finish" << std::endl;
  while (!clients.empty()) {
    Client *client = clients.front();
    SyntheticClient *syn = synclients.front();
    clients.pop_front();
    synclients.pop_front();    
    syn->join_thread();
    delete syn;
    delete client;
  }
    
  // wait for messenger to finish
  rank.wait();
  
  return 0;
}

