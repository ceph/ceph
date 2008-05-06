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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "mon/MonMap.h"
#include "mds/MDS.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"

#include "mon/MonClient.h"

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  // mds specific args
  const char *monhost = 0;
  int whoami = -1;
  bool standby = false;  // by default, i'll start active.
  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--standby") == 0) 
      standby = true;
    else if (strcmp(args[i], "--mds") == 0) 
      whoami = atoi(args[++i]);
    else if (monhost == 0) 
      monhost = args[i];
    else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      return -1;
    }
  }

  if (g_conf.clock_tare) g_clock.tare();

  // get monmap
  MonMap monmap;
  MonClient mc;
  if (mc.get_monmap(&monmap) < 0)
    return -1;

  rank.bind();
  cout << "starting mds? at " << rank.get_rank_addr() << std::endl;
  rank.start();
  
  rank.set_policy(entity_name_t::TYPE_MON, Rank::Policy::fast_fail());
  rank.set_policy(entity_name_t::TYPE_CLIENT, Rank::Policy::retry_forever());  // mds does its own timeout/markdown
  //rank.set_policy(entity_name_t::TYPE_OSD, Rank::Policy::retry_forever());

  // start mds
  Messenger *m = rank.register_entity(entity_name_t::MDS(whoami));
  assert(m);
  MDS *mds = new MDS(whoami, m, &monmap);
  mds->init(standby);
  
  rank.wait();

  // yuck: grab the mds lock, so we can be sure that whoever in *mds 
  // called shutdown finishes what they were doing.
  mds->mds_lock.Lock();
  mds->mds_lock.Unlock();

  // done
  //delete mds;

  generic_dout(0) << "stopped." << dendl;
  return 0;
}

