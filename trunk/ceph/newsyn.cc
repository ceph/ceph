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

#define intabs(x) ((x) >= 0 ? (x):(-(x)))

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

class C_Test : public Context {
public:
  void finish(int r) {
    cout << "C_Test->finish(" << r << ")" << std::endl;
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
  if (g_conf.clock_tare) {
    if (1) {
      // use an MPI barrier.  probably not terribly precise.
      MPI_Barrier(MPI_COMM_WORLD);
      g_clock.tare();
    } else {
      // use wall clock; assume NTP has all nodes synchronized already.
      // FIXME someday: this hangs for some reason.  whatever.
      utime_t z = g_clock.now();
      MPI_Bcast( &z, sizeof(z), MPI_CHAR,
		 0, MPI_COMM_WORLD);
      cout << "z is " << z << std::endl;
      g_clock.tare(z);
    }
  }
  
  // start up all monitors at known addresses.
  entity_inst_t moninst[mpi_world];  // only care about first g_conf.num_mon of these.

  rank.start_rank();   // bind and listen

  if (mpi_rank < g_conf.num_mon) {
    moninst[mpi_rank].addr = rank.my_addr;
    moninst[mpi_rank].name = MSG_ADDR_MON(mpi_rank);

    //cerr << mpi_rank << " at " << rank.get_listen_addr() << std::endl;
  } 

  MPI_Gather( &moninst[mpi_rank], sizeof(entity_inst_t), MPI_CHAR,
              moninst, sizeof(entity_inst_t), MPI_CHAR,
              0, MPI_COMM_WORLD);
  
  if (mpi_rank == 0) {
    for (int i=0; i<g_conf.num_mon; i++) {
      cerr << "mon" << i << " is at " << moninst[i] << std::endl;
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
    cout << "tick +" << g_conf.tick << " -> " << now << "  (" << tick_count << ")" << std::endl;
    tick_count += g_conf.tick;
    utime_t next = tick_start;
    next.sec_ref() += tick_count;
    g_timer.add_event_at(next, new C_Tick);
  }
};

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
    cout << "debug_after flipping debug settings" << std::endl;
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

  // stop on our own (by default)
  g_conf.mon_stop_on_last_unmount = true;
  g_conf.mon_stop_with_last_mds = true;

  parse_config_options(args);
  parse_syn_options(args);


  //int start_mon = g_conf.num_mon > 0 ? g_conf.num_mon:0;
  int start_mds = g_conf.num_mds > 0 ? g_conf.num_mds:0;
  int start_osd = g_conf.num_osd > 0 ? g_conf.num_osd:0;
  int start_client = g_conf.num_client > 0 ? g_conf.num_client:0;

  //g_conf.num_mon = intabs(g_conf.num_mon);
  g_conf.num_mds = intabs(g_conf.num_mds);
  g_conf.num_client = intabs(g_conf.num_client);
  g_conf.num_osd = intabs(g_conf.num_osd);


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
    //cout << "a " << args[i] << std::endl;
    // unknown arg, pass it on.
    nargs.push_back(args[i]);
  }

  args = nargs;
  if (!args.empty()) {
    for (unsigned i=0; i<args.size(); i++)
      cerr << "stray arg " << args[i] << std::endl;
  }
  assert(args.empty());


  // start up messenger via MPI
  MonMap *monmap = new MonMap(g_conf.num_mon);
  pair<int,int> mpiwho = mpi_bootstrap_new(argc, argv, monmap);
  int myrank = mpiwho.first;
  int world = mpiwho.second;

  int need = 0;
  if (g_conf.ms_skip_rank0) need++;
  need += start_mds;
  if (g_conf.ms_stripe_osds)
    need++;
  else
    need += start_osd;
  if (start_client) {
    if (!g_conf.ms_overlay_clients)
      need += 1;
  }
  assert(need <= world);

  if (myrank == 0)
    cerr << "nummds " << start_mds << "  numosd " << start_osd << "  numclient " << start_client << " .. need " << need << ", have " << world << std::endl;
  

  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  int started = 0;

  //if (myrank == 0) g_conf.debug = 20;
  
  // create mon
  if (myrank < g_conf.num_mon) {
    Monitor *mon = new Monitor(myrank, rank.register_entity(MSG_ADDR_MON(myrank)), monmap);
    mon->init();
  }

  
  // wait for monitors to start.
  MPI_Barrier(MPI_COMM_WORLD);

  // okay, home free!
  MPI_Finalize();


  // create mds
  map<int,MDS*> mds;
  map<int,OSD*> mdsosd;
  for (int i=0; i<start_mds; i++) {
    if (myrank != g_conf.ms_skip_rank0+i) continue;
    Messenger *m = rank.register_entity(MSG_ADDR_MDS(i));
    cerr << "mds" << i << " at " << rank.my_addr << " " << hostname << "." << pid << std::endl;
    mds[i] = new MDS(i, m, monmap);
    mds[i]->init();
    started++;

    if (g_conf.mds_local_osd) {
      int n = i+g_conf.mds_local_osd_offset;
      mdsosd[i] = new OSD(n, rank.register_entity(MSG_ADDR_OSD(n)), monmap);
      mdsosd[i]->init();                                                    
    }
  }
  
  // create osd
  map<int,OSD*> osd;
  int max_osd_nodes = world - start_mds - g_conf.ms_skip_rank0;  // assumes 0 clients, if we stripe.
  int osds_per_node = (start_osd-1)/max_osd_nodes + 1;
  for (int i=0; i<start_osd; i++) {
    if (g_conf.ms_stripe_osds) {
      if (myrank != g_conf.ms_skip_rank0+start_mds + i / osds_per_node) continue;
    } else {
      if (myrank != g_conf.ms_skip_rank0+start_mds + i) continue;
    }

    if (kill_osd_after.count(i))
      g_timer.add_event_after(kill_osd_after[i], new C_Die);

    Messenger *m = rank.register_entity(MSG_ADDR_OSD(i));
    cerr << "osd" << i << " at " << rank.my_addr <<  " " << hostname << "." << pid << std::endl;
    osd[i] = new OSD(i, m, monmap);
    osd[i]->init();
    started++;
  }
  
  if (g_conf.ms_overlay_clients) sleep(5);

  // create client
  int skip_osd = start_osd;
  if (g_conf.ms_overlay_clients) 
    skip_osd = 0;        // put clients with osds too!
  int client_nodes = world - start_mds - skip_osd - g_conf.ms_skip_rank0;
  int clients_per_node = 1;
  if (start_client && client_nodes > 0) clients_per_node = (start_client-1) / client_nodes + 1;
  set<int> clientlist;
  map<int,Client *> client;//[start_client];
  map<int,SyntheticClient *> syn;//[start_client];
  int nclients = 0;
  for (int i=0; i<start_client; i++) {
    //if (myrank != start_mds + start_osd + i % client_nodes) continue;
    if (myrank != g_conf.ms_skip_rank0+start_mds + skip_osd + i / clients_per_node) continue;
    clientlist.insert(i);
    client[i] = new Client(rank.register_entity(entity_name_t(entity_name_t::TYPE_CLIENT, -1-i)), //MSG_ADDR_CLIENT_NEW), 
			   monmap);

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

    //client[i]->mount();
    nclients++;
  }

  if (!clientlist.empty()) generic_dout(2) << "i have " << clientlist << dendl;

  for (set<int>::iterator it = clientlist.begin();
       it != clientlist.end();
       it++) {
    int i = *it;

    //cerr << "starting synthetic client" << i << " on rank " << myrank << std::endl;
    syn[i]->start_thread();
  }
  if (nclients) {
    cerr << nclients << " clients  at " << rank.my_addr << " " << hostname << "." << pid << std::endl;
  }

  for (set<int>::iterator it = clientlist.begin();
       it != clientlist.end();
       it++) {
    int i = *it;

    //      cout << "waiting for synthetic client" << i << " to finish" << std::endl;
    syn[i]->join_thread();
    delete syn[i];
    
    //client[i]->unmount();
    //cout << "client" << i << " unmounted" << std::endl;
    client[i]->shutdown();

    delete client[i];
  }
  

  if (myrank && !started) {
    //dout(1) << "IDLE" << dendl;
    cerr << "idle at " << rank.my_addr << " " << hostname << "." << pid << std::endl; 
    //rank.stop_rank();
  } 

  // wait for everything to finish
  rank.wait();

  if (started) cerr << "newsyn finishing" << std::endl;

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  sprintf(s, "gmon/%d", myrank);
  mkdir(s, 0755);
  chdir(s);



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
  for (int i=0; i<start_mds; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
    delete mds[i];
  }
  for (int i=0; i<start_osd; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
    delete osd[i];
  }
  for (int i=0; i<start_client; i++) {
    if (myrank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
    delete client[i];
  }
  */



  
  return 0;
}

