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

#include "common/config.h"

#include "client/SyntheticClient.h"
#include "client/Client.h"

#include "msg/Messenger.h"

#include "mon/MonClient.h"

#include "common/Timer.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/pick_address.h"

#include <sys/types.h>
#include <fcntl.h>

extern int syn_filer_flags;

int main(int argc, const char **argv, char *envp[]) 
{
  //cerr << "ceph-syn starting" << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  parse_syn_options(args);   // for SyntheticClient

  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);

  // get monmap
  MonClient mc(g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;

  list<Client*> clients;
  list<SyntheticClient*> synclients;
  vector<Messenger*> messengers{static_cast<unsigned>(num_client), nullptr};
  vector<MonClient*> mclients{static_cast<unsigned>(num_client), nullptr};

  cout << "ceph-syn: starting " << num_client << " syn client(s)" << std::endl;
  auto public_addr = g_conf->get_val<entity_addr_t>("public_addr");
  for (int i=0; i<num_client; i++) {
    messengers[i] = Messenger::create_client_messenger(g_ceph_context,
						       "synclient");
    messengers[i]->bind(public_addr);
    mclients[i] = new MonClient(g_ceph_context);
    mclients[i]->build_initial_monmap();
    auto client = new StandaloneClient(messengers[i], mclients[i]);
    client->set_filer_flags(syn_filer_flags);
    SyntheticClient *syn = new SyntheticClient(client);
    clients.push_back(client);
    synclients.push_back(syn);
    messengers[i]->start();
  }

  auto p = synclients.begin();
  auto end = synclients.end();
  for (; p != end; ++p)
    (*p)->start_thread();

  //cout << "waiting for client(s) to finish" << std::endl;
  while (!clients.empty()) {
    Client *client = clients.front();
    SyntheticClient *syn = synclients.front();
    clients.pop_front();
    synclients.pop_front();
    syn->join_thread();
    delete syn;
    syn = nullptr;
    delete client;
    client = nullptr;
  }

  for (int i = 0; i < num_client; ++i) {
    // wait for messenger to finish
    delete mclients[i];
    mclients[i] = nullptr;
    messengers[i]->shutdown();
    messengers[i]->wait();
    delete messengers[i];
    messengers[i] = nullptr;
  }
  return 0;
}
