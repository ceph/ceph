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

#include "mds/MDS.h"
#include "osd/OSD.h"
#include "mon/Monitor.h"
#include "client/Client.h"

#include "client/SyntheticClient.h"

#include "msg/FakeMessenger.h"

#include "common/Timer.h"

// crypto library
#include "crypto/CryptoLib.h"
using namespace CryptoLib;


class C_Test : public Context {
public:
  void finish(int r) {
    cout << "C_Test->finish(" << r << ")" << endl;
  }
};

class C_Die : public Context {
public:
  void finish(int) {
    cerr << "die" << endl;
    exit(1);
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


  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);


  g_clock.tare();
  
  if (g_conf.secure_io) {
    cout << "Testing crypto library" << endl;

    const byte* myMsg = (const byte*)"hash me";
    byte digestBuf[SHA1DIGESTSIZE];
    byte hexBuf[2*SHA1DIGESTSIZE];
    
    sha1(myMsg,digestBuf,strlen((const char*)myMsg));
    toHex(digestBuf, hexBuf, SHA1DIGESTSIZE,
		   2*SHA1DIGESTSIZE);
    
    cerr << "SHA1 of " << myMsg << " is " <<
      string((const char*)hexBuf,2*SHA1DIGESTSIZE) << endl;
  }

  // need to reload old monmap on !mkfs to make mon private key match monmap.  FIXME.
  assert(g_conf.mkfs); 

  MonMap *monmap = new MonMap(g_conf.num_mon);
  entity_addr_t a;
  monmap->mon_inst[0] = entity_inst_t(MSG_ADDR_MON(0), a);  // hack ; see FakeMessenger.cc

  //string mon_private_key;
  char mon_private_key[ESIGNPRIVSIZE];
  monmap->generate_key_pair(mon_private_key);

  char hostname[100];
  gethostname(hostname,100);
  //int pid = getpid();

  // create mon
  Monitor *mon[g_conf.num_mon];
  for (int i=0; i<g_conf.num_mon; i++) {
    mon[i] = new Monitor(i, new FakeMessenger(MSG_ADDR_MON(i)), monmap);
    mon[i]->set_new_private_key(mon_private_key);
  }

  // create mds
  MDS *mds[g_conf.num_mds];
  OSD *mdsosd[g_conf.num_mds];
  for (int i=0; i<g_conf.num_mds; i++) {
    //cerr << "mds" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
    mds[i] = new MDS(-1, new FakeMessenger(MSG_ADDR_MDS_NEW), monmap);
    if (g_conf.mds_local_osd)
      mdsosd[i] = new OSD(i+10000, new FakeMessenger(MSG_ADDR_OSD(i+10000)), monmap);
    start++;
  }
  
  // create osd
  OSD *osd[g_conf.num_osd];
  for (int i=0; i<g_conf.num_osd; i++) {
    //cerr << "osd" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
    osd[i] = new OSD(i, new FakeMessenger(MSG_ADDR_OSD(i)), monmap);
    start++;
  }
  
  // create client
  Client *client[g_conf.num_client];
  SyntheticClient *syn[g_conf.num_client];
  for (int i=0; i<g_conf.num_client; i++) {
    //cerr << "client" << i << " on rank " << myrank << " " << hostname << "." << pid << endl;
    client[i] = new Client(new FakeMessenger(MSG_ADDR_CLIENT(i)), monmap);
    start++;
  }


  // start message loop
  fakemessenger_startthread();
  
  // init
  for (int i=0; i<g_conf.num_mon; i++) {
    mon[i]->init();
  }
  for (int i=0; i<g_conf.num_mds; i++) {
    mds[i]->init();
    if (g_conf.mds_local_osd)
      mdsosd[i]->init();
  }
  
  for (int i=0; i<g_conf.num_osd; i++) {
    osd[i]->init();
  }

  
  // create client(s)
  for (int i=0; i<g_conf.num_client; i++) {
    client[i]->init();
    
    // use my argc, argv (make sure you pass a mount point!)
    //cout << "mounting" << endl;
    client[i]->mount();
    
    //cout << "starting synthetic client  " << endl;
    syn[i] = new SyntheticClient(client[i]);

    syn[i]->start_thread();
  }


  for (int i=0; i<g_conf.num_client; i++) {
    
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

  cout << "fakesyn done" << endl;
  return 0;
}

