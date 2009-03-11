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

#include "mon/Monitor.h"
#include "mon/MonitorStore.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"
#include "client/fuse.h"
#include "client/fuse_ll.h"

#include "common/Timer.h"
#include "common/common_init.h"

#include "msg/FakeMessenger.h"
#include "messages/MMonCommand.h"




#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define NUMCLIENT g_conf.num_client


class C_Test : public Context {
public:
  void finish(int r) {
    cout << "C_Test->finish(" << r << ")" << std::endl;
  }
};
class C_Test2 : public Context {
public:
  void finish(int r) {
    cout << "C_Test2->finish(" << r << ")" << std::endl;
    g_timer.add_event_after(2, new C_Test);
  }
};



int main(int argc, const char **argv) {
  cerr << "fakefuse starting" << std::endl;

  // stop on our own (by default)
  g_conf.mon_stop_on_last_unmount = true;
  g_conf.mon_stop_with_last_mds = true;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "fakefuse");

  // start messenger thread
  fakemessenger_startthread();

  //g_timer.add_event_after(5.0, new C_Test2);
  //g_timer.add_event_after(10.0, new C_Test);

  vector<const char*> nargs;
  for (unsigned i=0; i<args.size(); i++) {
    nargs.push_back(args[i]);
  }
  args = nargs;
  vec_to_argv(args, argc, argv);

  // FUSE will chdir("/"); be ready.
  g_conf.use_abspaths = true;

  if (g_conf.clock_tare) g_clock.tare();

  MonMap *monmap = new MonMap(g_conf.num_mon);
  entity_addr_t a;
  a.nonce = getpid();
  for (int i=0; i<g_conf.num_mon; i++) {
    a.erank = i;
    monmap->mon_inst[i] = entity_inst_t(entity_name_t::MON(i), a);  // hack ; see FakeMessenger.cc
  }

  Monitor *mon[g_conf.num_mon];
  for (int i=0; i<g_conf.num_mon; i++) {
    char fn[100];
    sprintf(fn, "mondata/mon%d", i);
    MonitorStore *store = new MonitorStore(fn);
    mon[i] = new Monitor(i, store, new FakeMessenger(entity_name_t::MON(i)), monmap);
    mon[i]->mkfs();
  }

  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
    osd[i] = new OSD(i, new FakeMessenger(entity_name_t::OSD(i)), monmap);
  }

  // create mds
  MDS *mds[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
    mds[i] = new MDS(i, new FakeMessenger(entity_name_t::MDS(i)), monmap);
  }
 
  // init
  for (int i=0; i<g_conf.num_mon; i++)
    mon[i]->init();

  // build initial osd map
  {
    OSDMap map;
    map.build_simple(0, monmap->fsid, g_conf.num_osd, 0, g_conf.osd_pg_bits, g_conf.osd_lpg_bits, 0);
    bufferlist bl;
    map.encode(bl);
    Messenger *messenger = new FakeMessenger(entity_name_t::ADMIN(-1));
    MMonCommand *m = new MMonCommand(monmap->fsid);
    m->set_data(bl);
    m->cmd.push_back("osd");
    m->cmd.push_back("setmap");
    messenger->send_message(m, monmap->get_inst(0));
    messenger->shutdown();
  }

  for (int i=0; i<NUMOSD; i++) 
    osd[i]->init();
  for (int i=0; i<NUMMDS; i++) 
    mds[i]->init();  

  // create client
  Client *client[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
    client[i] = new Client(new FakeMessenger(entity_name_t::CLIENT(0)), monmap);
    client[i]->init();


    // start up fuse
    // use my argc, argv (make sure you pass a mount point!)
    client[i]->mount();

    char oldcwd[200];
    getcwd(oldcwd, 200);
    cout << "starting fuse on pid " << getpid() << std::endl;
    if (g_conf.fuse_ll)
      ceph_fuse_ll_main(client[i], argc, argv);
    else
      ceph_fuse_main(client[i], argc, argv);
    cout << "fuse finished on pid " << getpid() << std::endl;
    ::chdir(oldcwd);                        // return to previous wd

    client[i]->unmount();
    client[i]->shutdown();
  }
  


  // wait for it to finish
  cout << "DONE -----" << std::endl;
  fakemessenger_wait();  // blocks until messenger stops
  

  // cleanup
  for (int i=0; i<NUMMDS; i++) {
    delete mds[i];
  }
  for (int i=0; i<NUMOSD; i++) {
    delete osd[i];
  }
  for (int i=0; i<NUMCLIENT; i++) {
    delete client[i];
  }
  
  return 0;
}

