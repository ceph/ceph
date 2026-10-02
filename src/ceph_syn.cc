// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*- 
// vim: ts=8 sw=2 sts=2 expandtab

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
#include <string>
#include <vector>
#include <iostream>

#include "common/config.h"

#include "common/async/context_pool.h"
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

using namespace std;

extern int syn_filer_flags;

int main(int argc, const char **argv, char *envp[]) 
{
  struct client_instance final {
    Client *client;
    SyntheticClient *workload;
  };

  //cerr << "ceph-syn starting" << std::endl;
  auto args = argv_to_vec(argc, argv);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  parse_syn_options(args);   // for SyntheticClient

  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);

  // get monmap
  ceph::async::io_context_pool  poolctx(1);
  MonClient mc(g_ceph_context, poolctx);
  if (mc.build_initial_monmap() < 0)
    return -1;

  vector<client_instance> clients;
  clients.reserve(static_cast<unsigned>(num_client));

  vector<Messenger*> messengers{static_cast<unsigned>(num_client), nullptr};
  vector<MonClient*> mclients{static_cast<unsigned>(num_client), nullptr};

  cout << "ceph-syn: starting " << num_client << " syn client(s)" << std::endl;
  for (int i=0; i<num_client; i++) {
    messengers[i] = Messenger::create_client_messenger(g_ceph_context,
						       "synclient");
    mclients[i] = new MonClient(g_ceph_context, poolctx);
    mclients[i]->build_initial_monmap();
    auto client = new StandaloneClient(messengers[i], mclients[i], poolctx);
    client->set_filer_flags(syn_filer_flags);
    SyntheticClient *syn = new SyntheticClient(client);
    clients.push_back({client, syn});
    messengers[i]->start();
  }

  for (const auto& client : clients) {
    client.workload->start_thread();
  }

  poolctx.stop();

  //cout << "waiting for client(s) to finish" << std::endl;
  for (const auto& client : clients) {
    client.workload->join_thread();
    delete client.workload;
    delete client.client;
  }

  for (int i = 0; i < num_client; ++i) {
    // wait for messenger to finish
    delete mclients[i];
    messengers[i]->shutdown();
    messengers[i]->wait();
    delete messengers[i];
  }
  return 0;
}
