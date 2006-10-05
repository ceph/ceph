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

#include "msg/TCPMessenger.h"

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


#include "msg/mpistarter.cc"

utime_t tick_start;
int tick_count = 0;

class C_Tick : public Context {
public:
  void finish(int) {
    utime_t now = g_clock.now() - tick_start;
    dout(0) << "tick +" << g_conf.tick << " -> " << now << "  (" << tick_count << ")" << endl;
    tick_count += g_conf.tick;
    utime_t next = tick_start;
    next.sec_ref() += tick_count;
    g_timer.add_event_at(next, new C_Tick);
  }
};

class C_Die : public Context {
public:
  void finish(int) {
    cerr << "die" << endl;
    exit(1);
  }
};

class C_Debug : public Context {
  public:
  void finish(int) {
    int size = &g_conf.debug_after - &g_conf.debug;
    memcpy((char*)&g_conf.debug, (char*)&g_debug_after_conf.debug, size);
    dout(0) << "debug_after flipping debug settings" << endl;
  }
};


int main(int argc, char **argv) 
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  parse_syn_options(args);

  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);
  if (g_conf.debug_after) 
    g_timer.add_event_after(g_conf.debug_after, new C_Debug);

  if (g_conf.tick) {
    tick_start = g_clock.now();
    g_timer.add_event_after(g_conf.tick, new C_Tick);
  }

  vector<char*> nargs;
  for (unsigned i=0; i<args.size(); i++) {
    //cout << "a " << args[i] << endl;
    // unknown arg, pass it on.
    nargs.push_back(args[i]);
  }

  args = nargs;
  if (!args.empty()) {
    for (unsigned i=0; i<args.size(); i++)
      cerr << "stray arg " << args[i] << endl;
  }
  assert(args.empty());


  // start up tcp messenger via MPI
  pair<int,int> mpiwho = mpi_bootstrap_tcp(argc, argv);
  int myrank = mpiwho.first;
  int world = mpiwho.second;

  int need = 0;
  if (g_conf.tcp_skip_rank0) need++;
  need += NUMMDS;
  need += NUMOSD;
  if (NUMCLIENT) {
    if (!g_conf.tcp_overlay_clients)
      need += 1;
  }
  assert(need <= world);

  if (myrank == 0)
    cerr << "nummds " << NUMMDS << "  numosd " << NUMOSD << "  numclient " << NUMCLIENT << " .. need " << need << ", have " << world << endl;
  
  MDCluster *mdc = new MDCluster(NUMMDS, NUMOSD);


  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  int started = 0;

  //if (myrank == 0) g_conf.debug = 20;
  
  // create mon
  if (myrank == 0) {
    Monitor *mon = new Monitor(0, new TCPMessenger(MSG_ADDR_MON(0)));
    mon->init();
  }

  // create mds
  MDS *mds[NUMMDS];
  OSD *mdsosd[NUMMDS];
  for (int i=0; i<NUMMDS; i++) {
    if (myrank != g_conf.tcp_skip_rank0+i) continue;
    TCPMessenger *m = new TCPMessenger(MSG_ADDR_MDS(i));
    cerr << "mds" << i << " on tcprank " << tcpmessenger_get_rank() << " " << hostname << "." << pid << endl;
    mds[i] = new MDS(mdc, i, m);
    mds[i]->init();
    started++;

    if (g_conf.mds_local_osd) {
      mdsosd[i] = new OSD(i+10000, new TCPMessenger(MSG_ADDR_OSD(i+10000)));
      mdsosd[i]->init();                                                    
    }
  }
  
  // create osd
  OSD *osd[NUMOSD];
  for (int i=0; i<NUMOSD; i++) {
    if (myrank != g_conf.tcp_skip_rank0+NUMMDS + i) continue;
    TCPMessenger *m = new TCPMessenger(MSG_ADDR_OSD(i));
    cerr << "osd" << i << " on tcprank " << tcpmessenger_get_rank() <<  " " << hostname << "." << pid << endl;
    osd[i] = new OSD(i, m);
    osd[i]->init();
    started++;
  }
  
  if (g_conf.tcp_overlay_clients) sleep(5);

  // create client
  int skip_osd = NUMOSD;
  if (g_conf.tcp_overlay_clients) 
    skip_osd = 0;        // put clients with osds too!
  int client_nodes = world - NUMMDS - skip_osd - g_conf.tcp_skip_rank0;
  int clients_per_node = 1;
  if (NUMCLIENT) clients_per_node = (NUMCLIENT-1) / client_nodes + 1;
  set<int> clientlist;
  Client *client[NUMCLIENT];
  SyntheticClient *syn[NUMCLIENT];
  for (int i=0; i<NUMCLIENT; i++) {
    //if (myrank != NUMMDS + NUMOSD + i % client_nodes) continue;
    if (myrank != g_conf.tcp_skip_rank0+NUMMDS + skip_osd + i / clients_per_node) continue;
    clientlist.insert(i);
    client[i] = new Client(new TCPMessenger(MSG_ADDR_CLIENT_NEW));//(i)) );

    // logger?
    if (client_logger == 0) {
      char s[80];
      sprintf(s,"clnode.%d", myrank);
      client_logger = new Logger(s, &client_logtype);

      client_logtype.add_inc("lsum");
      client_logtype.add_inc("lnum");
      client_logtype.add_inc("lwsum");
      client_logtype.add_inc("lwnum");
      client_logtype.add_inc("lrsum");
      client_logtype.add_inc("lrnum");
      client_logtype.add_inc("trsum");
      client_logtype.add_inc("trnum");
      client_logtype.add_inc("wrlsum");
      client_logtype.add_inc("wrlnum");
      client_logtype.add_inc("lstatsum");
      client_logtype.add_inc("lstatnum");
      client_logtype.add_inc("ldirsum");
      client_logtype.add_inc("ldirnum");
      client_logtype.add_inc("readdir");
      client_logtype.add_inc("stat");
    }

    client[i]->init();
    started++;

    syn[i] = new SyntheticClient(client[i]);
  }

  if (!clientlist.empty()) dout(2) << "i have " << clientlist << endl;

  int nclients = 0;
  for (set<int>::iterator it = clientlist.begin();
       it != clientlist.end();
       it++) {
    int i = *it;

    //cerr << "starting synthetic client" << i << " on rank " << myrank << endl;
    client[i]->mount();
    syn[i]->start_thread();
    
    nclients++;
  }
  if (nclients) {
    cerr << nclients << " clients on tcprank " << tcpmessenger_get_rank() << " " << hostname << "." << pid << endl;
  }

  for (set<int>::iterator it = clientlist.begin();
       it != clientlist.end();
       it++) {
    int i = *it;

    //      cout << "waiting for synthetic client" << i << " to finish" << endl;
    syn[i]->join_thread();
    delete syn[i];
    
    client[i]->unmount();
    //cout << "client" << i << " unmounted" << endl;
    client[i]->shutdown();
  }
  

  if (myrank && !started) {
    //dout(1) << "IDLE" << endl;
    cerr << "idle on tcprank " << tcpmessenger_get_rank() << " " << hostname << "." << pid << endl; 
    tcpmessenger_stop_rankserver();
  }

  // wait for everything to finish
  tcpmessenger_wait();

  if (started) cerr << "tcpsyn finishing" << endl;
  
  tcpmessenger_shutdown(); 
  

  /*
  // cleanup
  for (int i=0; i<NUMMDS; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
    delete mds[i];
  }
  for (int i=0; i<NUMOSD; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
    delete osd[i];
  }
  for (int i=0; i<NUMCLIENT; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
    delete client[i];
  }
  */
  delete mdc;

  
  return 0;
}

