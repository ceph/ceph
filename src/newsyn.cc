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

#include <mpi.h>

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
#include "common/common_init.h"


class C_Test : public Context {
public:
  void finish(int r) {
    cout << "C_Test->finish(" << r << ")" << std::endl;
  }
};

extern std::map<entity_name_t,float> g_fake_kill_after;

bool use_existing_monmap = false;
const char *monmap_fn = ".ceph_monmap";
/*
 * start up NewMessenger via MPI.
 */ 

pair<int,int> mpi_bootstrap_new(int& argc, const char**& argv, MonMap *monmap)
{
  MPI_Init(&argc, (char***)&argv);
  
  int mpi_world;
  int mpi_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_world);
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

  if (use_existing_monmap && mpi_rank < g_conf.num_mon) {
    int r = monmap->read(monmap_fn);
    assert(r >= 0);
    g_my_addr = monmap->get_inst(mpi_rank).addr;
    cout << "i am monitor, will bind to " << g_my_addr 
	 << " from existing " << monmap_fn << std::endl;
  }

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

  rank.bind();   // bind and listen
  rank.start();

  if (mpi_rank < g_conf.num_mon) {
    moninst[mpi_rank].addr = rank.rank_addr;
    moninst[mpi_rank].name = entity_name_t(entity_name_t::TYPE_MON, mpi_rank);

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
    monmap->write(monmap_fn);
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
    _exit(1);
  }
};

class C_Debug : public Context {
  public:
  void finish(int) {
    int size = (long)&g_conf.debug_after - (long)&g_conf.debug;
    memcpy((char*)&g_conf.debug, (char*)&g_debug_after_conf.debug, size);
    cout << "debug_after flipping debug settings" << std::endl;
    //g_conf.debug_ms = 1;
  }
};


int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  map<int,int> kill_osd_after;
  int share_single_client = 0;
  if (1) {
    vector<const char*> nargs;
    for (unsigned i=0; i<args.size(); i++) {
      if (strcmp(args[i],"--kill_osd_after") == 0) {
        int o = atoi(args[++i]);
        int w = atoi(args[++i]);
        kill_osd_after[o] = w;
      }
      else if (strcmp(args[i], "--use_existing_monmap") == 0) {
	use_existing_monmap = true;
      }
      else if (strcmp(args[i], "--share_single_client") == 0) {
	share_single_client = 1;
      } else {
        nargs.push_back( args[i] );
      }
    }
    args.swap(nargs);
  }

  // stop on our own (by default)
  g_conf.mon_stop_on_last_unmount = true;
  g_conf.mon_stop_with_last_mds = true;

  env_to_vec(args);

  common_init(args, "newsyn");
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

  vector<const char*> nargs;
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
  int mpirank = mpiwho.first;
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

  if (mpirank == 0)
    cerr << "nummds " << start_mds << "  numosd " << start_osd << "  numclient " << start_client << " .. need " << need << ", have " << world << std::endl;
  

  char hostname[100];
  gethostname(hostname,100);
  int pid = getpid();

  int started = 0;

  //if (mpirank == 0) g_conf.debug = 20;
  
  // courtesy symlinks
  char ffrom[100];
  char fto[100];
  sprintf(fto, "%s.%d", hostname, pid);


  // create mon
  if (mpirank < g_conf.num_mon) {
    Monitor *mon = new Monitor(mpirank, rank.register_entity(entity_name_t(entity_name_t::TYPE_MON, mpirank)), monmap);
    mon->init();
    if (g_conf.dout_dir) {
      sprintf(ffrom, "%s/mon%d", g_conf.dout_dir, mpirank);
      ::unlink(ffrom);
      ::symlink(fto, ffrom);
    }
  }

  // wait for monitors to start.
  MPI_Barrier(MPI_COMM_WORLD);

  // okay, home free!
  MPI_Finalize();


  // create mds
  map<int,MDS*> mds;
  map<int,OSD*> mdsosd;
  for (int i=0; i<start_mds; i++) {
    if (mpirank != g_conf.ms_skip_rank0+i) continue;
    Messenger *m = rank.register_entity(entity_name_t(entity_name_t::TYPE_MDS, i));
    cerr << "mds" << i << " at " << m->get_myaddr() << " " << hostname << "." << pid << std::endl;
    if (g_conf.dout_dir) {
      sprintf(ffrom, "%s/mds%d", g_conf.dout_dir, i);
      ::unlink(ffrom);
      ::symlink(fto, ffrom);
    }
    mds[i] = new MDS(i, m, monmap);
    mds[i]->init();
    started++;

    if (g_conf.mds_local_osd) {
      int n = i+g_conf.num_osd;
      mdsosd[i] = new OSD(n, rank.register_entity(entity_name_t(entity_name_t::TYPE_OSD, n)), monmap);
      mdsosd[i]->init();                                                    
    }

    if (g_fake_kill_after.count(entity_name_t::MDS(i))) {
      cerr << "mds" << i << " will die after " << g_fake_kill_after[entity_name_t::MDS(i)] << std::endl;
      g_timer.add_event_after(g_fake_kill_after[entity_name_t::MDS(i)], new C_Die);
    }
  }
  
  // create osd
  map<int,OSD*> osd;
  int max_osd_nodes = world - start_mds - g_conf.ms_skip_rank0;  // assumes 0 clients, if we stripe.
  int osds_per_node = (start_osd-1)/max_osd_nodes + 1;
  for (int i=0; i<start_osd; i++) {
    if (g_conf.ms_stripe_osds) {
      if (mpirank != g_conf.ms_skip_rank0+start_mds + i / osds_per_node) continue;
    } else {
      if (mpirank != g_conf.ms_skip_rank0+start_mds + i) continue;
    }

    if (kill_osd_after.count(i))
      g_timer.add_event_after(kill_osd_after[i], new C_Die);

    Messenger *m = rank.register_entity(entity_name_t(entity_name_t::TYPE_OSD, i));
    cerr << "osd" << i << " at " << m->get_myaddr() <<  " " << hostname << "." << pid << std::endl;
    if (g_conf.dout_dir) {
      sprintf(ffrom, "%s/osd%d", g_conf.dout_dir, i);
      ::unlink(ffrom);
      ::symlink(fto, ffrom);
    }

    osd[i] = new OSD(i, m, monmap);
    if (osd[i]->init() < 0)
      return 1;
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
  map<int,Client*> client;
  map<int,SyntheticClient*> syn;
  int nclients = 0;
  
  // create the synthetic clients, and one Ceph client per synthetic client
  Client* single_client = 0;   // unless share_single_client...
  for (int i=0; i<start_client; i++) {
    int node = g_conf.ms_skip_rank0+start_mds + skip_osd + i % client_nodes;
    if (mpirank != node) continue;

    clientlist.insert(i);
    if (share_single_client) {
      if (!single_client) {
	single_client = new Client(rank.register_entity(entity_name_t(entity_name_t::TYPE_CLIENT, -1)), monmap);
	cout << "creating single shared client" << std::endl;
      }
      syn[i] = new SyntheticClient(single_client, i);
      //cout << "creating synthetic" << i << std::endl;
    } else {
      clientlist.insert(i);
      client[i] = new Client(rank.register_entity(entity_name_t(entity_name_t::TYPE_CLIENT, -1)), monmap);
      syn[i] = new SyntheticClient(client[i]);
    }
    
    started++;
    nclients++;
  }

  if (!clientlist.empty()) {
    generic_dout(2) << "i have " << clientlist << dendl;
  }

  // start all the synthetic clients
  for (set<int>::iterator it = clientlist.begin();
       it != clientlist.end();
       it++) {
    int i = *it;

    //cerr << "starting synthetic" << i << " on rank " << mpirank << std::endl;
    syn[i]->start_thread();
  }

  // client status message
  if (nclients) {
    if (share_single_client) 
      cerr << "In one-client-per-synclient mode:";
    cerr << nclients << " clients at " << rank.rank_addr << " " << hostname << "." << pid << std::endl;
  }

  // wait for the synthetic clients to finish
  for (set<int>::iterator it = clientlist.begin();
       it != clientlist.end();
       it++) {
    int i = *it;
    //      cout << "waiting for synthetic client" << i << " to finish" << std::endl;
    syn[i]->join_thread();

    // fix simplemessenger race before deleting synclients and clients
    // delete syn[i];

    // if (!ALL_SYNCLIENTS_THROUGH_ONE_CLIENT)
    // delete client[i];
  }
  // if (ALL_SYNCLIENTS_THROUGH_ONE_CLIENT)
  // delete client[0];

  if (mpirank && !started) {
    //dout(1) << "IDLE" << dendl;
    cerr << "idle at " << rank.rank_addr << " mpirank " << mpirank << " " << hostname << "." << pid << std::endl; 
  } 

  // wait for everything to finish
  rank.wait();

  cerr << "newsyn done on " << hostname << "." << pid << std::endl;

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  sprintf(s, "gmon/%d", mpirank);
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
    if (mpirank != MPI_DEST_TO_RANK(MSG_ADDR_MDS(i),world)) continue;
    delete mds[i];
  }
  for (int i=0; i<start_osd; i++) {
    if (mpirank != MPI_DEST_TO_RANK(MSG_ADDR_OSD(i),world)) continue;
    delete osd[i];
  }
  for (int i=0; i<start_client; i++) {
    if (mpirank != MPI_DEST_TO_RANK(MSG_ADDR_CLIENT(i),world)) continue;
    delete client[i];
  }
  */

  return 0;
}

