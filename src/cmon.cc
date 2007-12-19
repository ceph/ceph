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
#include "mon/Monitor.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"


class C_Die : public Context {
public:
  void finish(int) {
    cerr << "die" << std::endl;
    exit(1);
  }
};

class C_Debug : public Context {
  public:
  void finish(int) {
    int size = &g_conf.debug_after - &g_conf.debug;
    memcpy((char*)&g_conf.debug, (char*)&g_debug_after_conf.debug, size);
    generic_dout(0) << "debug_after flipping debug settings" << dendl;
  }
};


int main(int argc, char **argv) 
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);
  if (g_conf.debug_after) 
    g_timer.add_event_after(g_conf.debug_after, new C_Debug);

  // args
  int whoami = -1;
  char *monmap_fn = ".ceph_monmap";
  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--mon") == 0) 
      whoami = atoi(args[++i]);
    else if (strcmp(args[i], "--monmap") == 0) 
      monmap_fn = args[++i];
    else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      return -1;
    }
  }

  if (g_conf.clock_tare) g_clock.tare();

  MonMap monmap;

  if (whoami < 0) {
    // let's assume a standalone monitor
    cout << "starting standalone mon0" << std::endl;
    whoami = 0;

    // start messenger
    rank.start_rank();
    cout << "bound to " << rank.get_rank_addr() << std::endl;

    // add single mon0
    entity_inst_t inst;
    inst.name = entity_name_t::MON(0);
    inst.addr = rank.rank_addr;
    monmap.add_mon(inst);
    
    // write monmap
    cout << "writing monmap to " << monmap_fn << std::endl;;
    int r = monmap.write(monmap_fn);
    assert(r >= 0);
  } else {
    // i am specific monitor.

    // read monmap
    cout << "reading monmap from .ceph_monmap" << std::endl;
    int r = monmap.read(monmap_fn);
    assert(r >= 0);

    // bind to a specific port
    cout << "starting mon" << whoami << " at " << monmap.get_inst(whoami) << std::endl;
    g_my_addr = monmap.get_inst(whoami).addr;
    rank.start_rank();
  }

  // start monitor
  Messenger *m = rank.register_entity(entity_name_t::MON(whoami));
  Monitor *mon = new Monitor(whoami, m, &monmap);
  mon->init();

  // wait
  cout << "waiting for shutdown ..." << std::endl;
  rank.wait();

  // done
  delete mon;

  return 0;
}

