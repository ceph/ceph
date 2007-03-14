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

#include <fcntl.h>

#include "config.h"

#include "mds/MDS.h"
#include "osd/OSD.h"
#include "mon/Monitor.h"
#include "client/Client.h"
#include "client/SyntheticClient.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

#define NUMMDS g_conf.num_mds
#define NUMOSD g_conf.num_osd
#define NUMCLIENT g_conf.num_client

class C_Test : public Context {
public:
  void finish(int r) {
    cout << "C_Test->finish(" << r << ")" << endl;
  }
};


/*
 * start up NewMessenger via MPI.
 */ 
#include <mpi.h>

pair<int,int> mpi_bootstrap_new(int& argc, char**& argv, MonMap *monmap)
{
  MPI_Init(&argc, &argv);
  
  int mpi_world;
  int mpi_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_world);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  // first, synchronize clocks.
  MPI_Barrier(MPI_COMM_WORLD);
  //dout(-10) << "tare" << endl;
  g_clock.tare();
  
  // start up all monitors at known addresses.
  entity_inst_t moninst[mpi_world];  // only care about first g_conf.num_mon of these.

  rank.start_rank();   // bind and listen

  if (mpi_rank < g_conf.num_mon) {
    moninst[mpi_rank].addr = rank.my_addr;
    moninst[mpi_rank].name = MSG_ADDR_MON(mpi_rank);

    //cerr << mpi_rank << " at " << rank.get_listen_addr() << endl;
  } 

  MPI_Gather( &moninst[mpi_rank], sizeof(entity_inst_t), MPI_CHAR,
              moninst, sizeof(entity_inst_t), MPI_CHAR,
              0, MPI_COMM_WORLD);
  
  if (mpi_rank == 0) {
    for (int i=0; i<g_conf.num_mon; i++) {
      cerr << "mon" << i << " is at " << moninst[i] << endl;
      monmap->mon_inst[i] = moninst[i];
    }
  }


  // distribute monmap
  bufferlist bl;
  if (mpi_rank == 0) {
    monmap->encode(bl);
    monmap->write(".ceph_monmap");
  } else {
    int l = g_conf.num_mon * 1000;   // nice'n big.
    bufferptr bp(l); 
    bl.append(bp);
  }
  
  MPI_Bcast(bl.c_str(), bl.length(), MPI_CHAR,
            0, MPI_COMM_WORLD);

  if (mpi_rank > 0) {
    monmap->decode(bl);
  }
  
  // wait for everyone!
  MPI_Barrier(MPI_COMM_WORLD);

  return pair<int,int>(mpi_rank, mpi_world);
}

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

  map<int,int> kill_osd_after;
  if (1) {
    vector<char*> nargs;
    for (unsigned i=0; i<args.size(); i++) {
      if (strcmp(args[i],"--kill_osd_after") == 0) {
        int o = atoi(args[++i]);
        int w = atoi(args[++i]);
        kill_osd_after[o] = w;
      }
      else {
        nargs.push_back( args[i] );
      }
    }
    args.swap(nargs);
  }

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


  // start up messenger via MPI
  MonMap *monmap = new MonMap(g_conf.num_mon);
  
  // need a key pair
  //string mon_private_key;
  //monmap->generate_key_pair(mon_private_key);
  char mon_private_key[ESIGNPRIVSIZE];
  monmap->generate_key_pair(mon_private_key);

  pair<int,int> mpiwho = mpi_bootstrap_new(argc, argv, monmap);
  int myrank = mpiwho.first;
  int world = mpiwho.second;

  int need = 0;
  if (g_conf.ms_skip_rank0) need++;
  need += NUMMDS;
  if (g_conf.ms_stripe_osds)
    need++;
  else
    need += NUMOSD;
  if (NUMCLIENT) {
    if (!g_conf.ms_overlay_clients)
      need += 1;
  }
  assert(need <= world);

  if (myrank == 0)
    cerr << "nummds " << NUMMDS << "  numosd " << NUMOSD << "  numclient " << NUMCLIENT << " .. need " << need << ", have " << world << endl;
  

  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  int started = 0;

  //if (myrank == 0) g_conf.debug = 20;
  
  // create mon
  if (myrank < g_conf.num_mon) {
    Monitor *mon = new Monitor(myrank, rank.register_entity(MSG_ADDR_MON(myrank)), monmap);
    mon->set_new_private_key(mon_private_key);
    mon->init();
  }

  
  // wait for monitors to start.
  MPI_Barrier(MPI_COMM_WORLD);

  // okay, home free!
  MPI_Finalize();


  // create mds
  map<int,MDS*> mds;
  map<int,OSD*> mdsosd;
  for (int i=0; i<NUMMDS; i++) {
    if (myrank != g_conf.ms_skip_rank0+i) continue;
    Messenger *m = rank.register_entity(MSG_ADDR_MDS(i));
    cerr << "mds" << i << " at " << rank.my_addr << " " << hostname << "." << pid << endl;
    mds[i] = new MDS(i, m, monmap);
    mds[i]->init();
    started++;

    if (g_conf.mds_local_osd) {
      mdsosd[i] = new OSD(i+10000, rank.register_entity(MSG_ADDR_OSD(i+10000)), monmap);
      mdsosd[i]->init();                                                    
    }
  }
  
  // create osd
  map<int,OSD*> osd;
  int max_osd_nodes = world - NUMMDS - g_conf.ms_skip_rank0;  // assumes 0 clients, if we stripe.
  int osds_per_node = (NUMOSD-1)/max_osd_nodes + 1;
  for (int i=0; i<NUMOSD; i++) {
    if (g_conf.ms_stripe_osds) {
      if (myrank != g_conf.ms_skip_rank0+NUMMDS + i / osds_per_node) continue;
    } else {
      if (myrank != g_conf.ms_skip_rank0+NUMMDS + i) continue;
    }

    if (kill_osd_after.count(i))
      g_timer.add_event_after(kill_osd_after[i], new C_Die);

    Messenger *m = rank.register_entity(MSG_ADDR_OSD(i));
    cerr << "osd" << i << " at " << rank.my_addr <<  " " << hostname << "." << pid << endl;
    osd[i] = new OSD(i, m, monmap);
    osd[i]->init();
    started++;
  }
  
  if (g_conf.ms_overlay_clients) sleep(5);

  // create client
  int skip_osd = NUMOSD;
  if (g_conf.ms_overlay_clients) 
    skip_osd = 0;        // put clients with osds too!
  int client_nodes = world - NUMMDS - skip_osd - g_conf.ms_skip_rank0;
  int clients_per_node = 1;
  if (NUMCLIENT && client_nodes > 0) clients_per_node = (NUMCLIENT-1) / client_nodes + 1;
  set<int> clientlist;
  map<int,Client *> client;//[NUMCLIENT];
  map<int,SyntheticClient *> syn;//[NUMCLIENT];
  int nclients = 0;
  for (int i=0; i<NUMCLIENT; i++) {
    //if (myrank != NUMMDS + NUMOSD + i % client_nodes) continue;
    if (myrank != g_conf.ms_skip_rank0+NUMMDS + skip_osd + i / clients_per_node) continue;
    clientlist.insert(i);
    client[i] = new Client(rank.register_entity(MSG_ADDR_CLIENT_NEW), monmap);

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

    client[i]->mount();
    nclients++;
  }

  if (!clientlist.empty()) dout(2) << "i have " << clientlist << endl;

  for (set<int>::iterator it = clientlist.begin();
       it != clientlist.end();
       it++) {
    int i = *it;

    //cerr << "starting synthetic client" << i << " on rank " << myrank << endl;
    syn[i]->start_thread();
    
  }
  if (nclients) {
    cerr << nclients << " clients  at " << rank.my_addr << " " << hostname << "." << pid << endl;
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

    delete client[i];
  }
  

  if (myrank && !started) {
    //dout(1) << "IDLE" << endl;
    cerr << "idle at " << rank.my_addr << " " << hostname << "." << pid << endl; 
    //rank.stop_rank();
  } 

  // wait for everything to finish
  rank.wait();

  if (started) cerr << "newsyn finishing" << endl;

  return 0;  // whatever, cleanup hangs sometimes (stopping ebofs threads?).


  // cleanup
  for (map<int,MDS*>::iterator i = mds.begin(); i != mds.end(); i++)
    delete i->second;
  for (map<int,OSD*>::iterator i = mdsosd.begin(); i != mdsosd.end(); i++)
    delete i->second;
  for (map<int,OSD*>::iterator i = osd.begin(); i != osd.end(); i++)
    delete i->second;
  /*
  for (map<int,Client*>::iterator i = client.begin(); i != client.end(); i++)
    delete i->second;
  for (map<int,SyntheticClient*>::iterator i = syn.begin(); i != syn.end(); i++)
    delete i->second;
  */
  /*
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

  
  return 0;
}

