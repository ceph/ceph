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

#include "mds/MDS.h"
#include "osd/OSD.h"
#include "mon/Monitor.h"
#include "mon/MonitorStore.h"
#include "client/Client.h"

#include "client/SyntheticClient.h"

#include "msg/FakeMessenger.h"
#include "messages/MMonCommand.h"

#include "common/Timer.h"
#include "common/common_init.h"


class C_Test : public Context {
public:
  void finish(int r) {
    cout << "C_Test->finish(" << r << ")" << std::endl;
  }
};

class C_Die : public Context {
public:
  void finish(int) {
    cerr << "die" << std::endl;
    exit(1);
  }
};


int main(int argc, const char **argv) 
{
  cerr << "fakesyn start" << std::endl;

  //cerr << "inode_t " << sizeof(inode_t) << std::endl;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  // stop on our own (by default)
  g_conf.mon_stop_on_last_unmount = true;
  g_conf.mon_stop_with_last_mds = true;

  common_init(args, "fakesyn", false);

  int start = 0;

  parse_syn_options(args);

  vector<const char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
    // unknown arg, pass it on.
    cerr << " stray arg " << args[i] << std::endl;
    nargs.push_back(args[i]);
  }
  assert(nargs.empty());


  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);

  if (g_conf.clock_tare) g_clock.tare();


  MonMap *monmap = new MonMap(g_conf.num_mon);
  entity_addr_t a;
  a.nonce = getpid();
  for (int i=0; i<g_conf.num_mon; i++) {
    a.erank = i;
    monmap->mon_inst[i] = entity_inst_t(entity_name_t::MON(i), a);  // hack ; see FakeMessenger.cc
  }
  
  char hostname[100];
  gethostname(hostname,100);
  //int pid = getpid();

  // create mon
  Monitor *mon[g_conf.num_mon];
  for (int i=0; i<g_conf.num_mon; i++) {
    char fn[100];
    snprintf(fn, sizeof(fn), "mondata/mon%d", i);
    MonitorStore *store = new MonitorStore(fn);
    mon[i] = new Monitor(i, store, new FakeMessenger(entity_name_t::MON(i)), monmap);
    mon[i]->mkfs();
  }

  // create mds
  MDS *mds[g_conf.num_mds];
  OSD *mdsosd[g_conf.num_mds];
  for (int i=0; i<g_conf.num_mds; i++) {
    //cerr << "mds" << i << " on rank " << myrank << " " << hostname << "." << pid << std::endl;
    mds[i] = new MDS(-1, new FakeMessenger(entity_name_t::MDS(i)), monmap);
    if (g_conf.mds_local_osd)
      mdsosd[i] = new OSD(i+g_conf.num_osd, new FakeMessenger(entity_name_t::OSD(i+g_conf.num_osd)), monmap);
    start++;
  }
  
  // build initial osd map
  {
    OSDMap map;
    map.build_simple(0, monmap->fsid, g_conf.num_osd, 0,
		     g_conf.osd_pg_bits, g_conf.osd_lpg_bits, 0);
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

  // create osd
  OSD *osd[g_conf.num_osd];
  for (int i=0; i<g_conf.num_osd; i++) {
    //cerr << "osd" << i << " on rank " << myrank << " " << hostname << "." << pid << std::endl;
    osd[i] = new OSD(i, new FakeMessenger(entity_name_t::OSD(i)), monmap);
    start++;
  }  


  // start message loop
  fakemessenger_startthread();
  
  // init
  for (int i=0; i<g_conf.num_mon; i++) {
    mon[i]->init();
  }
  for (int i=0; i<g_conf.num_osd; i++) {
    osd[i]->init();
  }
  for (int i=0; i<g_conf.num_mds; i++) {
    mds[i]->init();
    if (g_conf.mds_local_osd)
      mdsosd[i]->init();
  }
  
  
  // create client(s)
  Client *client[g_conf.num_client];
  SyntheticClient *syn[g_conf.num_client];
  for (int i=0; i<g_conf.num_client; i++) {
    //cout << "starting synthetic client  " << std::endl;
    client[i] = new Client(new FakeMessenger(entity_name_t::CLIENT(i)), monmap);
    syn[i] = new SyntheticClient(client[i]);
    syn[i]->start_thread();
    start++;
  }


  for (int i=0; i<g_conf.num_client; i++) {
    cout << "waiting for synthetic client " << i << " to finish" << std::endl;
    syn[i]->join_thread();
    delete syn[i];
  }
  
        
  // wait for it to finish
  fakemessenger_wait();
  
  // cleanup
  for (int i=0; i<g_conf.num_mon; i++) {
    delete mon[i];
  }
  for (int i=0; i<g_conf.num_mds; i++) {
    delete mds[i];
  }
  for (int i=0; i<g_conf.num_osd; i++) {
    delete osd[i];
  }
  for (int i=0; i<g_conf.num_client; i++) {
    delete client[i];
  }

  cout << "fakesyn done" << std::endl;
  return 0;
}

