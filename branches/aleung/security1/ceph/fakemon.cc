// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "mds/MDCluster.h"

#include "mds/MDS.h"
#include "osd/OSD.h"
#include "mon/Monitor.h"
#include "client/Client.h"

#include "client/SyntheticClient.h"

#include "msg/FakeMessenger.h"

#include "common/Timer.h"

#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define NUMCLIENT g_conf.num_client

class C_Test : public Context {
public:
  void finish(int r) {
    cout << "C_Test->finish(" << r << ")" << endl;
  }
};


int main(int argc, char **argv) 
{
  cerr << "fakesyn start" << endl;

  //cerr << "inode_t " << sizeof(inode_t) << endl;

  vector<char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  int start = 0;

  parse_syn_options(args);

  vector<char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
    // unknown arg, pass it on.
    cerr << " stray arg " << args[i] << endl;
    nargs.push_back(args[i]);
  }
  assert(nargs.empty());


  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);


  char hostname[100];
  gethostname(hostname,100);
  //int pid = getpid();

  // create mon
  Monitor *mon[g_conf.num_mon];
  for (int i=0; i<g_conf.num_mon; i++) {
    mon[i] = new Monitor(i, new FakeMessenger(MSG_ADDR_MON(i)));
  }

  // create mds
  MDS *mds[NUMMDS];
  OSD *mdsosd[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
    //cerr << "mds" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
    mds[i] = new MDS(mdc, i, new FakeMessenger(MSG_ADDR_MDS(i)));
    if (g_conf.mds_local_osd)
      mdsosd[i] = new OSD(i+10000, new FakeMessenger(MSG_ADDR_OSD(i+10000)));
    start++;
  }
  
  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
    //cerr << "osd" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
    osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)));
    start++;
  }
  
  // create client
  Client *client[NUMCLIENT];
  SyntheticClient *syn[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
    //cerr << "client" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
    client[i] = new Client(new FakeMessenger(MSG_ADDR_CLIENT(i)));
    start++;
  }


  // start message loop
  fakemessenger_startthread();
  
  // init
  for (int i=0; i<g_conf.num_mon; i++) {
    mon[i]->init();
  }
  for (int i=0; i<NUMMDS; i++) {
    mds[i]->init();
    if (g_conf.mds_local_osd)
      mdsosd[i]->init();
  }
  
  for (int i=0; i<NUMOSD; i++) {
    osd[i]->init();
  }

  
  // create client(s)
  for (int i=0; i<NUMCLIENT; i++) {
    client[i]->init();
    
    // use my argc, argv (make sure you pass a mount point!)
    //cout << "mounting" << endl;
    client[i]->mount();
    
    //cout << "starting synthetic client  " << endl;
    syn[i] = new SyntheticClient(client[i]);

    syn[i]->start_thread();
  }


  for (int i=0; i<NUMCLIENT; i++) {
    
    cout << "waiting for synthetic client " << i << " to finish" << endl;
    syn[i]->join_thread();
    delete syn[i];
    
    client[i]->unmount();
    //cout << "unmounted" << endl;
    client[i]->shutdown();
  }
  
        
  // wait for it to finish
  fakemessenger_wait();
  
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
  delete mdc;

  cout << "fakesyn done" << endl;
  return 0;
}

