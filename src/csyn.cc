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

#include "common/Timer.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, const char **argv, char *envp[]) {

  //cerr << "csyn starting" << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);
  parse_syn_options(args);   // for SyntheticClient

  vec_to_argv(args, argc, argv);

  if (g_conf.clock_tare) g_clock.tare();

  // load monmap
  MonMap monmap;
  int r = monmap.read(".ceph_monmap");
  assert(r >= 0);

  // start up network
  rank.start_rank();

  Rank::Policy client_policy;
  client_policy.fail_interval = 0;
  client_policy.drop_msg_callback = false;
  rank.set_policy(entity_name_t::TYPE_CLIENT, client_policy);
  rank.set_policy(entity_name_t::TYPE_MON, client_policy);
  rank.set_policy(entity_name_t::TYPE_MDS, client_policy);
  rank.set_policy(entity_name_t::TYPE_OSD, client_policy);

  list<Client*> clients;
  list<SyntheticClient*> synclients;

  cout << "mounting and starting " << g_conf.num_client << " syn client(s)" << std::endl;
  for (int i=0; i<g_conf.num_client; i++) {
    Client *client = new Client(rank.register_entity(entity_name_t(entity_name_t::TYPE_CLIENT,-2-i)), &monmap);
    SyntheticClient *syn = new SyntheticClient(client);
    syn->start_thread();
    clients.push_back(client);
    synclients.push_back(syn);
  }

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

