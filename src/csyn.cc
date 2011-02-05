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

extern int syn_filer_flags;

int main(int argc, const char **argv, char *envp[]) 
{
  //cerr << "csyn starting" << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  common_set_defaults(false);
  common_init(args, "csyn", true);
  set_foreground_logging();

  parse_syn_options(args);   // for SyntheticClient

  vec_to_argv(args, argc, argv);

  // get monmap
  MonClient mc;
  if (mc.build_initial_monmap() < 0)
    return -1;

  list<Client*> clients;
  list<SyntheticClient*> synclients;
  SimpleMessenger* messengers[g_conf.num_client];
  MonClient* mclients[g_conf.num_client];

  cout << "csyn: starting " << g_conf.num_client << " syn client(s)" << std::endl;
  for (int i=0; i<g_conf.num_client; i++) {
    messengers[i] = new SimpleMessenger();
    messengers[i]->register_entity(entity_name_t(entity_name_t::TYPE_CLIENT,-1));
    messengers[i]->bind();
    mclients[i] = new MonClient();
    mclients[i]->build_initial_monmap();
    Client *client = new Client(messengers[i], mclients[i]);
    client->set_filer_flags(syn_filer_flags);
    SyntheticClient *syn = new SyntheticClient(client);
    clients.push_back(client);
    synclients.push_back(syn);
    messengers[i]->start();
  }

  for (list<SyntheticClient*>::iterator p = synclients.begin(); 
       p != synclients.end();
       p++)
    (*p)->start_thread();

  //cout << "waiting for client(s) to finish" << std::endl;
  while (!clients.empty()) {
    Client *client = clients.front();
    SyntheticClient *syn = synclients.front();
    clients.pop_front();
    synclients.pop_front();
    syn->join_thread();
    delete syn;
    delete client;
  }

  for (int i = 0; i < g_conf.num_client; ++i) {
    // wait for messenger to finish
    delete mclients[i];
    messengers[i]->wait();
    messengers[i]->destroy();
  }
  return 0;
}

