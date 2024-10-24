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

#include "include/compat.h"

#include <iostream>
#include <sstream>


#include "common/config.h"
#include "SyntheticClient.h"
#include "osdc/Objecter.h"
#include "osdc/Filer.h"


#include "include/filepath.h"
#include "common/perf_counters.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <utime.h>
#include <math.h>
#include <sys/statvfs.h>

#include "common/errno.h"
#include "include/ceph_assert.h"
#include "include/cephfs/ceph_ll_client.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_client
#undef dout_prefix
#define dout_prefix *_dout << "client." << (whoami >= 0 ? whoami:client->get_nodeid()) << " "

using namespace std;
// traces
//void trace_include(SyntheticClient *syn, Client *cl, string& prefix);
//void trace_openssh(SyntheticClient *syn, Client *cl, string& prefix);

int num_client = 1;
list<int> syn_modes;
list<int> syn_iargs;
list<string> syn_sargs;
int syn_filer_flags = 0;

void parse_syn_options(vector<const char*>& args)
{
  vector<const char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"--num-client") == 0) {
      num_client = atoi(args[++i]);
      continue;
    }
    if (strcmp(args[i],"--syn") == 0) {
      ++i;

      if (strcmp(args[i], "mksnap") == 0) {
	syn_modes.push_back(SYNCLIENT_MODE_MKSNAP);
	syn_sargs.push_back(args[++i]); // path
	syn_sargs.push_back(args[++i]); // name
      }
      else if (strcmp(args[i], "rmsnap") == 0) {
	syn_modes.push_back(SYNCLIENT_MODE_RMSNAP);
	syn_sargs.push_back(args[++i]); // path
	syn_sargs.push_back(args[++i]); // name
      } else if (strcmp(args[i], "mksnapfile") == 0) {
	syn_modes.push_back(SYNCLIENT_MODE_MKSNAPFILE);
	syn_sargs.push_back(args[++i]); // path
      } else if (strcmp(args[i],"rmfile") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_RMFILE );
      } else if (strcmp(args[i],"writefile") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_WRITEFILE );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"wrshared") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_WRSHARED );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"writebatch") == 0) {
          syn_modes.push_back( SYNCLIENT_MODE_WRITEBATCH );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"readfile") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_READFILE );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"readwriterandom") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_RDWRRANDOM );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"readwriterandom_ex") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_RDWRRANDOM_EX );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"overloadosd0") == 0) {
	syn_modes.push_back( SYNCLIENT_MODE_OVERLOAD_OSD_0 );
	syn_iargs.push_back( atoi(args[++i]) );
	syn_iargs.push_back( atoi(args[++i]) );
	syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"readshared") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_READSHARED );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"rw") == 0) {
        int a = atoi(args[++i]);
        int b = atoi(args[++i]);
        syn_modes.push_back( SYNCLIENT_MODE_WRITEFILE );
        syn_iargs.push_back( a );
        syn_iargs.push_back( b );
        syn_modes.push_back( SYNCLIENT_MODE_READFILE );
        syn_iargs.push_back( a );
        syn_iargs.push_back( b );
      } else if (strcmp(args[i],"dumpplacement") == 0) {
	syn_modes.push_back( SYNCLIENT_MODE_DUMP );
	syn_sargs.push_back( args[++i] );   
      } else if (strcmp(args[i],"dropcache") == 0) {
	syn_modes.push_back( SYNCLIENT_MODE_DROPCACHE );
      } else if (strcmp(args[i],"makedirs") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MAKEDIRS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"makedirmess") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MAKEDIRMESS );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"statdirs") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_STATDIRS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"readdirs") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_READDIRS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"makefiles") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MAKEFILES );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"makefiles2") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_MAKEFILES2 );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"linktest") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_LINKTEST );
      } else if (strcmp(args[i],"createshared") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_CREATESHARED );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"openshared") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_OPENSHARED );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"createobjects") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_CREATEOBJECTS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"objectrw") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_OBJECTRW );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"walk") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_FULLWALK );
        //syn_sargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"randomwalk") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_RANDOMWALK );
        syn_iargs.push_back( atoi(args[++i]) );       
      } else if (strcmp(args[i],"trace") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_TRACE );
        syn_sargs.push_back( args[++i] );
        syn_iargs.push_back( atoi(args[++i]) );
	syn_iargs.push_back(1);// data
      } else if (strcmp(args[i],"mtrace") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_TRACE );
        syn_sargs.push_back( args[++i] );
        syn_iargs.push_back( atoi(args[++i]) );
	syn_iargs.push_back(0);// no data
      } else if (strcmp(args[i],"thrashlinks") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_THRASHLINKS );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"foo") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_FOO );
      } else if (strcmp(args[i],"until") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_UNTIL );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"sleepuntil") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_SLEEPUNTIL );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"only") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_ONLY );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"onlyrange") == 0) {
        syn_modes.push_back( SYNCLIENT_MODE_ONLYRANGE );
        syn_iargs.push_back( atoi(args[++i]) );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"sleep") == 0) { 
        syn_modes.push_back( SYNCLIENT_MODE_SLEEP );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"randomsleep") == 0) { 
        syn_modes.push_back( SYNCLIENT_MODE_RANDOMSLEEP );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"opentest") == 0) { 
        syn_modes.push_back( SYNCLIENT_MODE_OPENTEST );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"optest") == 0) {
	syn_modes.push_back( SYNCLIENT_MODE_OPTEST );
        syn_iargs.push_back( atoi(args[++i]) );
      } else if (strcmp(args[i],"truncate") == 0) { 
        syn_modes.push_back( SYNCLIENT_MODE_TRUNCATE );
	syn_sargs.push_back(args[++i]);
        syn_iargs.push_back(atoi(args[++i]));
      } else if (strcmp(args[i],"importfind") == 0) {
	syn_modes.push_back(SYNCLIENT_MODE_IMPORTFIND);
	syn_sargs.push_back(args[++i]);
	syn_sargs.push_back(args[++i]);
	syn_iargs.push_back(atoi(args[++i]));
      } else if (strcmp(args[i], "lookuphash") == 0) {
	syn_modes.push_back(SYNCLIENT_MODE_LOOKUPHASH);
	syn_sargs.push_back(args[++i]);
	syn_sargs.push_back(args[++i]);
	syn_sargs.push_back(args[++i]);
      } else if (strcmp(args[i], "lookupino") == 0) {
	syn_modes.push_back(SYNCLIENT_MODE_LOOKUPINO);
	syn_sargs.push_back(args[++i]);
      } else if (strcmp(args[i], "chunkfile") == 0) {
	syn_modes.push_back(SYNCLIENT_MODE_CHUNK);
	syn_sargs.push_back(args[++i]);
      } else {
        cerr << "unknown syn arg " << args[i] << std::endl;
        ceph_abort();
      }
    }
    else if (strcmp(args[i], "localize_reads") == 0) {
      cerr << "set CEPH_OSD_FLAG_LOCALIZE_READS" << std::endl;
      syn_filer_flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
    }
    else {
      nargs.push_back(args[i]);
    }
  }

  args = nargs;
}


SyntheticClient::SyntheticClient(StandaloneClient *client, int w)
{
  this->client = client;
  whoami = w;
  thread_id = 0;
  
  did_readdir = false;

  run_only = -1;
  exclude = -1;

  this->modes = syn_modes;
  this->iargs = syn_iargs;
  this->sargs = syn_sargs;

  run_start = ceph_clock_now();
}




#define DBL 2

void *synthetic_client_thread_entry(void *ptr)
{
  ceph_pthread_setname("client");
  SyntheticClient *sc = static_cast<SyntheticClient*>(ptr);
  //int r = 
  sc->run();
  return 0;//(void*)r;
}

string SyntheticClient::get_sarg(int seq) 
{
  string a;
  if (!sargs.empty()) {
    a = sargs.front(); 
    sargs.pop_front();
  }
  if (a.length() == 0 || a == "~") {
    char s[30];
    snprintf(s, sizeof(s), "syn.%lld.%d", (long long)client->whoami.v, seq);
    a = s;
  } 
  return a;
}

int SyntheticClient::run()
{
  UserPerm perms = client->pick_my_perms();
  dout(15) << "initing" << dendl;
  int err = client->init();
  if (err < 0) {
    dout(0) << "failed to initialize: " << cpp_strerror(err) << dendl;
    return -1;
  }

  dout(15) << "mounting" << dendl;
  err = client->mount("", perms);
  if (err < 0) {
    dout(0) << "failed to mount: " << cpp_strerror(err) << dendl;
    client->shutdown();
    return -1;
  }

  //run_start = ceph_clock_now(client->cct);
  run_until = utime_t(0,0);
  dout(5) << "run" << dendl;

  int seq = 0;

  for (list<int>::iterator it = modes.begin();
       it != modes.end();
       ++it) {
    int mode = *it;
    dout(3) << "mode " << mode << dendl;

    switch (mode) {


      // WHO?

    case SYNCLIENT_MODE_ONLY:
      {
        run_only = iargs.front();
        iargs.pop_front();
        if (run_only == client->get_nodeid())
          dout(2) << "only " << run_only << dendl;
      }
      break;
    case SYNCLIENT_MODE_ONLYRANGE:
      {
        int first = iargs.front();
        iargs.pop_front();
        int last = iargs.front();
        iargs.pop_front();
        if (first <= client->get_nodeid() &&
	    last > client->get_nodeid()) {
	  run_only = client->get_nodeid();
          dout(2) << "onlyrange [" << first << ", " << last << ") includes me" << dendl;
	} else
	  run_only = client->get_nodeid().v+1;  // not me
      }
      break;
    case SYNCLIENT_MODE_EXCLUDE:
      {
        exclude = iargs.front();
        iargs.pop_front();
        if (exclude == client->get_nodeid()) {
	  run_only = client->get_nodeid().v + 1;
          dout(2) << "not running " << exclude << dendl;
	} else
	  run_only = -1;
      }
      break;

      // HOW LONG?

    case SYNCLIENT_MODE_UNTIL:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
	if (run_me()) {
	  if (iarg1) {
	    dout(2) << "until " << iarg1 << dendl;
	    utime_t dur(iarg1,0);
	    run_until = run_start + dur;
	  } else {
	    dout(2) << "until " << iarg1 << " (no limit)" << dendl;
	    run_until = utime_t(0,0);
	  }
	}
      }
      break;


      // ...

    case SYNCLIENT_MODE_FOO:
      if (run_me()) {
	foo();
      }
      did_run_me();
      break;

    case SYNCLIENT_MODE_RANDOMSLEEP:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (run_me()) {
          srand(time(0) + getpid() + client->whoami.v);
          sleep(rand() % iarg1);
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_SLEEP:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (run_me()) {
          dout(2) << "sleep " << iarg1 << dendl;
          sleep(iarg1);
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_SLEEPUNTIL:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (iarg1 && run_me()) {
          dout(2) << "sleepuntil " << iarg1 << dendl;
          utime_t at = ceph_clock_now() - run_start;
          if (at.sec() < iarg1)
            sleep(iarg1 - at.sec());
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_RANDOMWALK:
      {
        int iarg1 = iargs.front();
        iargs.pop_front();
        if (run_me()) {
          dout(2) << "randomwalk " << iarg1 << dendl;
          random_walk(iarg1);
        }
	did_run_me();
      }
      break;


    case SYNCLIENT_MODE_DROPCACHE:
      {
	client->sync_fs();
	client->drop_caches();
      }
      break;

    case SYNCLIENT_MODE_DUMP:
      {
	string sarg1 = get_sarg(0);
	if (run_me()) {
	  dout(2) << "placement dump " << sarg1 << dendl;
	  dump_placement(sarg1);
	}
	did_run_me();
      }
      break;


    case SYNCLIENT_MODE_MAKEDIRMESS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makedirmess " << sarg1 << " " << iarg1 << dendl;
          make_dir_mess(sarg1.c_str(), iarg1);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_MAKEDIRS:
      {
        string sarg1 = get_sarg(seq++);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        int iarg3 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makedirs " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << dendl;
          make_dirs(sarg1.c_str(), iarg1, iarg2, iarg3);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_STATDIRS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        int iarg3 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "statdirs " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << dendl;
          stat_dirs(sarg1.c_str(), iarg1, iarg2, iarg3);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_READDIRS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        int iarg3 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "readdirs " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << dendl;
          read_dirs(sarg1.c_str(), iarg1, iarg2, iarg3);
        }
	did_run_me();
      }
      break;


    case SYNCLIENT_MODE_THRASHLINKS:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        int iarg3 = iargs.front();  iargs.pop_front();
        int iarg4 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "thrashlinks " << sarg1 << " " << iarg1 << " " << iarg2 << " " << iarg3 << dendl;
          thrash_links(sarg1.c_str(), iarg1, iarg2, iarg3, iarg4);
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_LINKTEST:
      {
	if (run_me()) {
	  link_test();
	}
	did_run_me();
      }
      break;


    case SYNCLIENT_MODE_MAKEFILES:
      {
        int num = iargs.front();  iargs.pop_front();
        int count = iargs.front();  iargs.pop_front();
        int priv = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makefiles " << num << " " << count << " " << priv << dendl;
          make_files(num, count, priv, false);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_MAKEFILES2:
      {
        int num = iargs.front();  iargs.pop_front();
        int count = iargs.front();  iargs.pop_front();
        int priv = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "makefiles2 " << num << " " << count << " " << priv << dendl;
          make_files(num, count, priv, true);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_CREATESHARED:
      {
        string sarg1 = get_sarg(0);
        int num = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "createshared " << num << dendl;
          create_shared(num);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_OPENSHARED:
      {
        string sarg1 = get_sarg(0);
        int num = iargs.front();  iargs.pop_front();
        int count = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "openshared " << num << dendl;
          open_shared(num, count);
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_CREATEOBJECTS:
      {
        int count = iargs.front();  iargs.pop_front();
        int size = iargs.front();  iargs.pop_front();
        int inflight = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "createobjects " << count << " of " << size << " bytes"
		  << ", " << inflight << " in flight" << dendl;
          create_objects(count, size, inflight);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_OBJECTRW:
      {
        int count = iargs.front();  iargs.pop_front();
        int size = iargs.front();  iargs.pop_front();
        int wrpc = iargs.front();  iargs.pop_front();
        int overlap = iargs.front();  iargs.pop_front();
        int rskew = iargs.front();  iargs.pop_front();
        int wskew = iargs.front();  iargs.pop_front();
        if (run_me()) {
          dout(2) << "objectrw " << count << " " << size << " " << wrpc 
		  << " " << overlap << " " << rskew << " " << wskew << dendl;
          object_rw(count, size, wrpc, overlap, rskew, wskew);
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_FULLWALK:
      {
        string sarg1;// = get_sarg(0);
        if (run_me()) {
          dout(2) << "fullwalk" << sarg1 << dendl;
          full_walk(sarg1);
        }
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_REPEATWALK:
      {
        string sarg1 = get_sarg(0);
        if (run_me()) {
          dout(2) << "repeatwalk " << sarg1 << dendl;
          while (full_walk(sarg1) == 0) ;
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_RMFILE:
      {
        string sarg1 = get_sarg(0);
        if (run_me()) {
          rm_file(sarg1);
	}
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_WRITEFILE:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        dout(1) << "WRITING SYN CLIENT" << dendl;
        if (run_me()) {
          write_file(sarg1, iarg1, iarg2);
	}
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_CHUNK:
      if (run_me()) {
        string sarg1 = get_sarg(0);
	chunk_file(sarg1);
      }
      did_run_me();
      break;


    case SYNCLIENT_MODE_OVERLOAD_OSD_0:
      {
	dout(1) << "OVERLOADING OSD 0" << dendl;
	int iarg1 = iargs.front(); iargs.pop_front();
	int iarg2 = iargs.front(); iargs.pop_front();
	int iarg3 = iargs.front(); iargs.pop_front();
	if (run_me()) {
	  overload_osd_0(iarg1, iarg2, iarg3);
	}
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_WRSHARED:
      {
        string sarg1 = "shared";
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          write_file(sarg1, iarg1, iarg2);
	}
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_READSHARED:
      {
        string sarg1 = "shared";
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();
        if (run_me()) {
          read_file(sarg1, iarg1, iarg2, true);
	}
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_WRITEBATCH:
      {
	int iarg1 = iargs.front(); iargs.pop_front();
        int iarg2 = iargs.front(); iargs.pop_front();
        int iarg3 = iargs.front(); iargs.pop_front();

        if (run_me()) {
          write_batch(iarg1, iarg2, iarg3);
	}
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_READFILE:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();

        dout(1) << "READING SYN CLIENT" << dendl;
        if (run_me()) {
          read_file(sarg1, iarg1, iarg2);
	}
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_RDWRRANDOM:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();

        dout(1) << "RANDOM READ WRITE SYN CLIENT" << dendl;
        if (run_me()) {
          read_random(sarg1, iarg1, iarg2);
	}
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_RDWRRANDOM_EX:
      {
        string sarg1 = get_sarg(0);
        int iarg1 = iargs.front();  iargs.pop_front();
        int iarg2 = iargs.front();  iargs.pop_front();

        dout(1) << "RANDOM READ WRITE SYN CLIENT" << dendl;
        if (run_me()) {
          read_random_ex(sarg1, iarg1, iarg2);
	}
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_TRACE:
      {
        string tfile = get_sarg(0);
        sargs.push_front(string("~"));
        int iarg1 = iargs.front();  iargs.pop_front();
	int playdata = iargs.front(); iargs.pop_front();
        string prefix = get_sarg(0);
	char realtfile[100];
	snprintf(realtfile, sizeof(realtfile), tfile.c_str(), (int)client->get_nodeid().v);

        if (run_me()) {
          dout(0) << "trace " << tfile << " prefix=" << prefix << " count=" << iarg1 << " data=" << playdata << dendl;
          
          Trace t(realtfile);
          
	  if (iarg1 == 0) iarg1 = 1; // play trace at least once!

          for (int i=0; i<iarg1; i++) {
            utime_t start = ceph_clock_now();
            
            if (time_to_stop()) break;
            play_trace(t, prefix, !playdata);
            if (time_to_stop()) break;
            if (iarg1 > 1) clean_dir(prefix);  // clean only if repeat
            
            utime_t lat = ceph_clock_now();
            lat -= start;
            
            dout(0) << " trace " << tfile << " loop " << (i+1) << "/" << iarg1 << " done in " << (double)lat << " seconds" << dendl;
            if (client->logger
                && i > 0
                && i < iarg1-1
                ) {
              //client->logger->finc("trsum", (double)lat);
              //client->logger->inc("trnum");
            }
          }
	  dout(1) << "done " << dendl;
        }
	did_run_me();
      }
      break;


    case SYNCLIENT_MODE_OPENTEST:
      {
        int count = iargs.front();  iargs.pop_front();
        if (run_me()) {
          for (int i=0; i<count; i++) {
            int fd = client->open("test", (rand()%2) ?
				  (O_WRONLY|O_CREAT) : O_RDONLY,
				  perms);
            if (fd > 0) client->close(fd);
          }
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_OPTEST:
      {
        int count = iargs.front();  iargs.pop_front();
        if (run_me()) {
	  client->mknod("test", 0777, perms);
	  struct stat st;
	  for (int i=0; i<count; i++) {
	    client->lstat("test", &st, perms);
	    client->chmod("test", 0777, perms);
          }
        }
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_TRUNCATE:
      {
        string file = get_sarg(0);
        sargs.push_front(file);
        int iarg1 = iargs.front();  iargs.pop_front();
	if (run_me()) {
	  client->truncate(file.c_str(), iarg1, perms);
	}
	did_run_me();
      }
      break;


    case SYNCLIENT_MODE_IMPORTFIND:
      {
	string base = get_sarg(0);
	string find = get_sarg(0);
	int data = get_iarg();
	if (run_me()) {
	  import_find(base.c_str(), find.c_str(), data);
	}
	did_run_me();
      }
      break;

    case SYNCLIENT_MODE_LOOKUPHASH:
      {
	inodeno_t ino;
	string iname = get_sarg(0);
	sscanf(iname.c_str(), "%llx", (long long unsigned*)&ino.val);
	inodeno_t dirino;
	string diname = get_sarg(0);
	sscanf(diname.c_str(), "%llx", (long long unsigned*)&dirino.val);
	string name = get_sarg(0);
	if (run_me()) {
	  lookup_hash(ino, dirino, name.c_str(), perms);
	}
      }
      break;
    case SYNCLIENT_MODE_LOOKUPINO:
      {
	inodeno_t ino;
	string iname = get_sarg(0);
	sscanf(iname.c_str(), "%llx", (long long unsigned*)&ino.val);
	if (run_me()) {
	  lookup_ino(ino, perms);
	}
      }
      break;
      
    case SYNCLIENT_MODE_MKSNAP:
      {
	string base = get_sarg(0);
	string name = get_sarg(0);
	if (run_me())
	  mksnap(base.c_str(), name.c_str(), perms);
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_RMSNAP:
      {
	string base = get_sarg(0);
	string name = get_sarg(0);
	if (run_me())
	  rmsnap(base.c_str(), name.c_str(), perms);
	did_run_me();
      }
      break;
    case SYNCLIENT_MODE_MKSNAPFILE:
      {
	string base = get_sarg(0);
	if (run_me())
	  mksnapfile(base.c_str());
	did_run_me();
      }
      break;

    default:
      ceph_abort();
    }
  }
  dout(1) << "syn done, unmounting " << dendl;

  client->unmount();
  client->shutdown();
  return 0;
}


int SyntheticClient::start_thread()
{
  ceph_assert(!thread_id);

  pthread_create(&thread_id, NULL, synthetic_client_thread_entry, this);
  ceph_assert(thread_id);
  return 0;
}

int SyntheticClient::join_thread()
{
  ceph_assert(thread_id);
  void *rv;
  pthread_join(thread_id, &rv);
  return 0;
}


bool roll_die(float p) 
{
  float r = (float)(rand() % 100000) / 100000.0;
  if (r < p) 
    return true;
  else 
    return false;
}

void SyntheticClient::init_op_dist()
{
  op_dist.clear();
#if 0
  op_dist.add( CEPH_MDS_OP_STAT, 610 );
  op_dist.add( CEPH_MDS_OP_UTIME, 0 );
  op_dist.add( CEPH_MDS_OP_CHMOD, 1 );
  op_dist.add( CEPH_MDS_OP_CHOWN, 1 );
#endif

  op_dist.add( CEPH_MDS_OP_READDIR, 2 );
  op_dist.add( CEPH_MDS_OP_MKNOD, 30 );
  op_dist.add( CEPH_MDS_OP_LINK, 0 );
  op_dist.add( CEPH_MDS_OP_UNLINK, 20 );
  op_dist.add( CEPH_MDS_OP_RENAME, 40 );

  op_dist.add( CEPH_MDS_OP_MKDIR, 10 );
  op_dist.add( CEPH_MDS_OP_RMDIR, 20 );
  op_dist.add( CEPH_MDS_OP_SYMLINK, 20 );

  op_dist.add( CEPH_MDS_OP_OPEN, 200 );
  //op_dist.add( CEPH_MDS_OP_READ, 0 );
  //op_dist.add( CEPH_MDS_OP_WRITE, 0 );
  //op_dist.add( CEPH_MDS_OP_TRUNCATE, 0 );
  //op_dist.add( CEPH_MDS_OP_FSYNC, 0 );
  //op_dist.add( CEPH_MDS_OP_RELEASE, 200 );
  op_dist.normalize();
}

void SyntheticClient::up()
{
  cwd = cwd.prefixpath(cwd.depth()-1);
  dout(DBL) << "cd .. -> " << cwd << dendl;
  clear_dir();
}

int SyntheticClient::play_trace(Trace& t, string& prefix, bool metadata_only)
{
  dout(4) << "play trace prefix '" << prefix << "'" << dendl;
  UserPerm perms = client->pick_my_perms();
  t.start();

  string buf;
  string buf2;

  utime_t start = ceph_clock_now();

  ceph::unordered_map<int64_t, int64_t> open_files;
  ceph::unordered_map<int64_t, dir_result_t*> open_dirs;

  ceph::unordered_map<int64_t, Fh*> ll_files;
  ceph::unordered_map<int64_t, dir_result_t*> ll_dirs;
  ceph::unordered_map<uint64_t, int64_t> ll_inos;

  Inode *i1, *i2;

  ll_inos[1] = 1; // root inode is known.

  // prefix?
  const char *p = prefix.c_str();
  if (prefix.length()) {
    client->mkdir(prefix.c_str(), 0755, perms);
    struct ceph_statx stx;
    i1 = client->ll_get_inode(vinodeno_t(1, CEPH_NOSNAP));
    if (client->ll_lookupx(i1, prefix.c_str(), &i2, &stx, CEPH_STATX_INO, 0, perms) == 0) {
      ll_inos[1] = stx.stx_ino;
      dout(5) << "'root' ino is " << inodeno_t(stx.stx_ino) << dendl;
      client->ll_put(i1);
    } else {
      dout(0) << "warning: play_trace couldn't lookup up my per-client directory" << dendl;
    }
  } else
    (void) client->ll_get_inode(vinodeno_t(1, CEPH_NOSNAP));

  utime_t last_status = start;

  int n = 0;

  // for object traces
  ceph::mutex lock = ceph::make_mutex("synclient foo");
  ceph::condition_variable cond;
  bool ack;

  while (!t.end()) {

    if (++n == 100) {
      n = 00;
      utime_t now = last_status;
      if (now - last_status > 1.0) {
	last_status = now;
	dout(1) << "play_trace at line " << t.get_line() << dendl;
      }
    }
    
    if (time_to_stop()) break;
    
    // op
    const char *op = t.get_string(buf, 0);
    dout(4) << (t.get_line()-1) << ": trace op " << op << dendl;
    
    if (op[0] == '@') {
      // timestamp... ignore it!
      t.get_int(); // sec
      t.get_int(); // usec
      op = t.get_string(buf, 0);
    }

    // high level ops ---------------------
    UserPerm perms = client->pick_my_perms();
    if (strcmp(op, "link") == 0) {
      const char *a = t.get_string(buf, p);
      const char *b = t.get_string(buf2, p);
      client->link(a, b, perms);
    } else if (strcmp(op, "unlink") == 0) {
      const char *a = t.get_string(buf, p);
      client->unlink(a, perms);
    } else if (strcmp(op, "rename") == 0) {
      const char *a = t.get_string(buf, p);
      const char *b = t.get_string(buf2, p);
      client->rename(a,b, perms);
    } else if (strcmp(op, "mkdir") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      client->mkdir(a, b, perms);
    } else if (strcmp(op, "rmdir") == 0) {
      const char *a = t.get_string(buf, p);
      client->rmdir(a, perms);
    } else if (strcmp(op, "symlink") == 0) {
      const char *a = t.get_string(buf, p);
      const char *b = t.get_string(buf2, p);
      client->symlink(a, b, perms);
    } else if (strcmp(op, "readlink") == 0) {
      const char *a = t.get_string(buf, p);
      char buf[100];
      client->readlink(a, buf, 100, perms);
    } else if (strcmp(op, "lstat") == 0) {
      struct stat st;
      const char *a = t.get_string(buf, p);
      if (strcmp(a, p) != 0 &&
	  strcmp(a, "/") != 0 &&
	  strcmp(a, "/lib") != 0 && // or /lib.. that would be a lookup. hack.
	  a[0] != 0)  // stop stating the root directory already
	client->lstat(a, &st, perms);
    } else if (strcmp(op, "chmod") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      client->chmod(a, b, perms);
    } else if (strcmp(op, "chown") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      int64_t c = t.get_int();
      client->chown(a, b, c, perms);
    } else if (strcmp(op, "utime") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      int64_t c = t.get_int();
      struct utimbuf u;
      u.actime = b;
      u.modtime = c;
      client->utime(a, &u, perms);
    } else if (strcmp(op, "mknod") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      int64_t c = t.get_int();
      client->mknod(a, b, perms, c);
    } else if (strcmp(op, "oldmknod") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      client->mknod(a, b, perms, 0);
    } else if (strcmp(op, "getdir") == 0) {
      const char *a = t.get_string(buf, p);
      list<string> contents;
      int r = client->getdir(a, contents, perms);
      if (r < 0) {
        dout(1) << "getdir on " << a << " returns " << r << dendl;
      }
    } else if (strcmp(op, "opendir") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      dir_result_t *dirp;
      client->opendir(a, &dirp, perms);
      if (dirp) open_dirs[b] = dirp;
    } else if (strcmp(op, "closedir") == 0) {
      int64_t a = t.get_int();
      client->closedir(open_dirs[a]);
      open_dirs.erase(a);
    } else if (strcmp(op, "open") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int(); 
      int64_t c = t.get_int(); 
      int64_t d = t.get_int();
      int64_t fd = client->open(a, b, perms, c);
      if (fd > 0) open_files[d] = fd;
    } else if (strcmp(op, "oldopen") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int(); 
      int64_t d = t.get_int();
      int64_t fd = client->open(a, b, perms, 0755);
      if (fd > 0) open_files[d] = fd;
    } else if (strcmp(op, "close") == 0) {
      int64_t id = t.get_int();
      int64_t fh = open_files[id];
      if (fh > 0) client->close(fh);
      open_files.erase(id);
    } else if (strcmp(op, "lseek") == 0) {
      int64_t f = t.get_int();
      int fd = open_files[f];
      int64_t off = t.get_int();
      int64_t whence = t.get_int();
      client->lseek(fd, off, whence);
    } else if (strcmp(op, "read") == 0) {
      int64_t f = t.get_int();
      int64_t size = t.get_int();
      int64_t off = t.get_int();
      int64_t fd = open_files[f];
      if (!metadata_only) {
	char *b = new char[size];
	client->read(fd, b, size, off);
	delete[] b;
      }
    } else if (strcmp(op, "write") == 0) {
      int64_t f = t.get_int();
      int64_t fd = open_files[f];
      int64_t size = t.get_int();
      int64_t off = t.get_int();
      if (!metadata_only) {
	char *b = new char[size];
	memset(b, 1, size);            // let's write 1's!
	client->write(fd, b, size, off);
	delete[] b;
      } else {
	client->write(fd, NULL, 0, size+off);
      }
    } else if (strcmp(op, "truncate") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t l = t.get_int();
      client->truncate(a, l, perms);
    } else if (strcmp(op, "ftruncate") == 0) {
      int64_t f = t.get_int();
      int fd = open_files[f];
      int64_t l = t.get_int();
      client->ftruncate(fd, l, perms);
    } else if (strcmp(op, "fsync") == 0) {
      int64_t f = t.get_int();
      int64_t b = t.get_int();
      int fd = open_files[f];
      client->fsync(fd, b);
    } else if (strcmp(op, "chdir") == 0) {
      const char *a = t.get_string(buf, p);
      // Client users should remember their path, but since this
      // is just a synthetic client we ignore it.
      std::string ignore;
      client->chdir(a, ignore, perms);
    } else if (strcmp(op, "statfs") == 0) {
      struct statvfs stbuf;
      client->statfs("/", &stbuf, perms);
    }

    // low level ops ---------------------
    else if (strcmp(op, "ll_lookup") == 0) {
      int64_t i = t.get_int();
      const char *name = t.get_string(buf, p);
      int64_t r = t.get_int();
      struct ceph_statx stx;
      if (ll_inos.count(i)) {
	  i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	  if (client->ll_lookupx(i1, name, &i2, &stx, CEPH_STATX_INO, 0, perms) == 0)
	    ll_inos[r] = stx.stx_ino;
	  client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_forget") == 0) {
      int64_t i = t.get_int();
      int64_t n = t.get_int();
      if (ll_inos.count(i) && 
	  client->ll_forget(
	    client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP)), n))
	ll_inos.erase(i);
    } else if (strcmp(op, "ll_getattr") == 0) {
      int64_t i = t.get_int();
      struct stat attr;
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	client->ll_getattr(i1, &attr, perms);
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_setattr") == 0) {
      int64_t i = t.get_int();
      struct stat attr;
      memset(&attr, 0, sizeof(attr));
      attr.st_mode = t.get_int();
      attr.st_uid = t.get_int();
      attr.st_gid = t.get_int();
      attr.st_size = t.get_int();
      attr.st_mtime = t.get_int();
      attr.st_atime = t.get_int();
      int mask = t.get_int();
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	client->ll_setattr(i1, &attr, mask, perms);
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_readlink") == 0) {
      int64_t i = t.get_int();
      if (ll_inos.count(i)) {
        char buf[PATH_MAX];
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	client->ll_readlink(i1, buf, sizeof(buf), perms);
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_mknod") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int m = t.get_int();
      int r = t.get_int();
      int64_t ri = t.get_int();
      struct stat attr;
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	if (client->ll_mknod(i1, n, m, r, &attr, &i2, perms) == 0)
	  ll_inos[ri] = attr.st_ino;
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_mkdir") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int m = t.get_int();
      int64_t ri = t.get_int();
      struct stat attr;
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	if (client->ll_mkdir(i1, n, m, &attr, &i2, perms) == 0)
	  ll_inos[ri] = attr.st_ino;
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_symlink") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      const char *v = t.get_string(buf2, p);
      int64_t ri = t.get_int();
      struct stat attr;
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	if (client->ll_symlink(i1, n, v, &attr, &i2, perms) == 0)
	  ll_inos[ri] = attr.st_ino;
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_unlink") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	client->ll_unlink(i1, n, perms);
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_rmdir") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	client->ll_rmdir(i1, n, perms);
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_rename") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int64_t ni = t.get_int();
      const char *nn = t.get_string(buf2, p);
      if (ll_inos.count(i) &&
	  ll_inos.count(ni)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	i2 = client->ll_get_inode(vinodeno_t(ll_inos[ni],CEPH_NOSNAP));
	client->ll_rename(i1, n, i2, nn, perms);
	client->ll_put(i1);
	client->ll_put(i2);
      }
    } else if (strcmp(op, "ll_link") == 0) {
      int64_t i = t.get_int();
      int64_t ni = t.get_int();
      const char *nn = t.get_string(buf, p);
      if (ll_inos.count(i) &&
	  ll_inos.count(ni)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	i2 = client->ll_get_inode(vinodeno_t(ll_inos[ni],CEPH_NOSNAP));
	client->ll_link(i1, i2, nn, perms);
	client->ll_put(i1);
	client->ll_put(i2);
      }
    } else if (strcmp(op, "ll_opendir") == 0) {
      int64_t i = t.get_int();
      int64_t r = t.get_int();
      dir_result_t *dirp;
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	if (client->ll_opendir(i1, O_RDONLY, &dirp, perms) == 0)
	  ll_dirs[r] = dirp;
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_releasedir") == 0) {
      int64_t f = t.get_int();
      if (ll_dirs.count(f)) {
	client->ll_releasedir(ll_dirs[f]);
	ll_dirs.erase(f);
      }
    } else if (strcmp(op, "ll_open") == 0) {
      int64_t i = t.get_int();
      int64_t f = t.get_int();
      int64_t r = t.get_int();
      Fh *fhp;
      if (ll_inos.count(i)) {
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	if (client->ll_open(i1, f, &fhp, perms) == 0)
	  ll_files[r] = fhp;
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_create") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int64_t m = t.get_int();
      int64_t f = t.get_int();
      int64_t r = t.get_int();
      int64_t ri = t.get_int();
      struct stat attr;
      if (ll_inos.count(i)) {
	Fh *fhp;
	i1 = client->ll_get_inode(vinodeno_t(ll_inos[i],CEPH_NOSNAP));
	if (client->ll_create(i1, n, m, f, &attr, NULL, &fhp, perms) == 0) {
	  ll_inos[ri] = attr.st_ino;
	  ll_files[r] = fhp;
	}
	client->ll_put(i1);
      }
    } else if (strcmp(op, "ll_read") == 0) {
      int64_t f = t.get_int();
      int64_t off = t.get_int();
      int64_t size = t.get_int();
      if (ll_files.count(f) &&
	  !metadata_only) {
	bufferlist bl;
	client->ll_read(ll_files[f], off, size, &bl);
      }
    } else if (strcmp(op, "ll_write") == 0) {
      int64_t f = t.get_int();
      int64_t off = t.get_int();
      int64_t size = t.get_int();
      if (ll_files.count(f)) {
	if (!metadata_only) {
	  bufferlist bl;
	  bufferptr bp(size);
	  bl.push_back(bp);
	  bp.zero();
	  client->ll_write(ll_files[f], off, size, bl.c_str());
	} else {
	  client->ll_write(ll_files[f], off+size, 0, NULL);
	}
      }
    } else if (strcmp(op, "ll_flush") == 0) {
      int64_t f = t.get_int();
      if (!metadata_only &&
	  ll_files.count(f)) 
	client->ll_flush(ll_files[f]);
    } else if (strcmp(op, "ll_fsync") == 0) {
      int64_t f = t.get_int();
      if (!metadata_only &&
	  ll_files.count(f)) 
	client->ll_fsync(ll_files[f], false); // FIXME dataonly param
    } else if (strcmp(op, "ll_release") == 0) {
      int64_t f = t.get_int();
      if (ll_files.count(f)) {
	client->ll_release(ll_files[f]);
	ll_files.erase(f);
      }
    } else if (strcmp(op, "ll_statfs") == 0) {
      int64_t i = t.get_int();
      if (ll_inos.count(i))
	{} //client->ll_statfs(vinodeno_t(ll_inos[i],CEPH_NOSNAP), perms);
    }


    // object-level traces

    else if (strcmp(op, "o_stat") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      object_t oid = file_object_t(oh, ol);
      std::unique_lock locker{lock};
      object_locator_t oloc(SYNCLIENT_FIRST_POOL);
      uint64_t size;
      ceph::real_time mtime;
      client->objecter->stat(oid, oloc, CEPH_NOSNAP, &size, &mtime, 0,
			     new C_SafeCond(lock, cond, &ack));
      cond.wait(locker, [&ack] { return ack; });
    }
    else if (strcmp(op, "o_read") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      int64_t off = t.get_int();
      int64_t len = t.get_int();
      object_t oid = file_object_t(oh, ol);
      object_locator_t oloc(SYNCLIENT_FIRST_POOL);
      std::unique_lock locker{lock};
      bufferlist bl;
      client->objecter->read(oid, oloc, off, len, CEPH_NOSNAP, &bl, 0,
			     new C_SafeCond(lock, cond, &ack));
      cond.wait(locker, [&ack] { return ack; });
    }
    else if (strcmp(op, "o_write") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      int64_t off = t.get_int();
      int64_t len = t.get_int();
      object_t oid = file_object_t(oh, ol);
      object_locator_t oloc(SYNCLIENT_FIRST_POOL);
      std::unique_lock locker{lock};
      bufferptr bp(len);
      bufferlist bl;
      bl.push_back(bp);
      SnapContext snapc;
      client->objecter->write(oid, oloc, off, len, snapc, bl,
			      ceph::real_clock::now(), 0,
			      new C_SafeCond(lock, cond, &ack));
      cond.wait(locker, [&ack] { return ack; });
    }
    else if (strcmp(op, "o_zero") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      int64_t off = t.get_int();
      int64_t len = t.get_int();
      object_t oid = file_object_t(oh, ol);
      object_locator_t oloc(SYNCLIENT_FIRST_POOL);
      std::unique_lock locker{lock};
      SnapContext snapc;
      client->objecter->zero(oid, oloc, off, len, snapc,
			     ceph::real_clock::now(), 0,
			     new C_SafeCond(lock, cond, &ack));
      cond.wait(locker, [&ack] { return ack; });
    }


    else {
      dout(0) << (t.get_line()-1) << ": *** trace hit unrecognized symbol '" << op << "' " << dendl;
      ceph_abort();
    }
  }

  dout(10) << "trace finished on line " << t.get_line() << dendl;

  // close open files
  for (ceph::unordered_map<int64_t, int64_t>::iterator fi = open_files.begin();
       fi != open_files.end();
       ++fi) {
    dout(1) << "leftover close " << fi->second << dendl;
    if (fi->second > 0) client->close(fi->second);
  }
  for (ceph::unordered_map<int64_t, dir_result_t*>::iterator fi = open_dirs.begin();
       fi != open_dirs.end();
       ++fi) {
    dout(1) << "leftover closedir " << fi->second << dendl;
    if (fi->second != 0) client->closedir(fi->second);
  }
  for (ceph::unordered_map<int64_t,Fh*>::iterator fi = ll_files.begin();
       fi != ll_files.end();
       ++fi) {
    dout(1) << "leftover ll_release " << fi->second << dendl;
    if (fi->second) client->ll_release(fi->second);
  }
  for (ceph::unordered_map<int64_t,dir_result_t*>::iterator fi = ll_dirs.begin();
       fi != ll_dirs.end();
       ++fi) {
    dout(1) << "leftover ll_releasedir " << fi->second << dendl;
    if (fi->second) client->ll_releasedir(fi->second);
  }
  
  return 0;
}



int SyntheticClient::clean_dir(string& basedir)
{
  // read dir
  list<string> contents;
  UserPerm perms = client->pick_my_perms();
  int r = client->getdir(basedir.c_str(), contents, perms);
  if (r < 0) {
    dout(1) << "getdir on " << basedir << " returns " << r << dendl;
    return r;
  }

  for (list<string>::iterator it = contents.begin();
       it != contents.end();
       ++it) {
    if (*it == ".") continue;
    if (*it == "..") continue;
    string file = basedir + "/" + *it;

    if (time_to_stop()) break;

    struct stat st;
    int r = client->lstat(file.c_str(), &st, perms);
    if (r < 0) {
      dout(1) << "stat error on " << file << " r=" << r << dendl;
      continue;
    }

    if ((st.st_mode & S_IFMT) == S_IFDIR) {
      clean_dir(file);
      client->rmdir(file.c_str(), perms);
    } else {
      client->unlink(file.c_str(), perms);
    }
  }

  return 0;

}


int SyntheticClient::full_walk(string& basedir) 
{
  if (time_to_stop()) return -1;

  list<string> dirq;
  list<frag_info_t> statq;
  dirq.push_back(basedir);
  frag_info_t empty;
  statq.push_back(empty);

  ceph::unordered_map<inodeno_t, int> nlink;
  ceph::unordered_map<inodeno_t, int> nlink_seen;

  UserPerm perms = client->pick_my_perms();
  while (!dirq.empty()) {
    string dir = dirq.front();
    frag_info_t expect = statq.front();
    dirq.pop_front();
    statq.pop_front();

    frag_info_t actual = empty;

    // read dir
    list<string> contents;
    int r = client->getdir(dir.c_str(), contents, perms);
    if (r < 0) {
      dout(1) << "getdir on " << dir << " returns " << r << dendl;
      continue;
    }
    
    for (list<string>::iterator it = contents.begin();
	 it != contents.end();
	 ++it) {
      if (*it == "." ||
	  *it == "..") 
	continue;
      string file = dir + "/" + *it;
      
      struct stat st;
      frag_info_t dirstat;
      int r = client->lstat(file.c_str(), &st, perms, &dirstat);
      if (r < 0) {
	dout(1) << "stat error on " << file << " r=" << r << dendl;
	continue;
      }

      nlink_seen[st.st_ino]++;
      nlink[st.st_ino] = st.st_nlink;

      if (S_ISDIR(st.st_mode))
	actual.nsubdirs++;
      else
	actual.nfiles++;

      // print
      char *tm = ctime(&st.st_mtime);
      tm[strlen(tm)-1] = 0;
      printf("%llx %c%c%c%c%c%c%c%c%c%c %2d %5d %5d %8llu %12s %s\n",
	     (long long)st.st_ino,
	     S_ISDIR(st.st_mode) ? 'd':'-',
	     (st.st_mode & 0400) ? 'r':'-',
	     (st.st_mode & 0200) ? 'w':'-',
	     (st.st_mode & 0100) ? 'x':'-',
	     (st.st_mode & 040) ? 'r':'-',
	     (st.st_mode & 020) ? 'w':'-',
	     (st.st_mode & 010) ? 'x':'-',
	     (st.st_mode & 04) ? 'r':'-',
	     (st.st_mode & 02) ? 'w':'-',
	     (st.st_mode & 01) ? 'x':'-',
	     (int)st.st_nlink,
	     (int)st.st_uid, (int)st.st_gid,
	     (long long unsigned)st.st_size,
	     tm,
	     file.c_str());

      
      if ((st.st_mode & S_IFMT) == S_IFDIR) {
	dirq.push_back(file);
	statq.push_back(dirstat);
      }
    }

    if (dir != "" &&
	(actual.nsubdirs != expect.nsubdirs ||
	 actual.nfiles != expect.nfiles)) {
      dout(0) << dir << ": expected " << expect << dendl;
      dout(0) << dir << ":      got " << actual << dendl;
    }
  }

  for (ceph::unordered_map<inodeno_t,int>::iterator p = nlink.begin(); p != nlink.end(); ++p) {
    if (nlink_seen[p->first] != p->second)
      dout(0) << p->first << " nlink " << p->second << " != " << nlink_seen[p->first] << "seen" << dendl;
  }

  return 0;
}



int SyntheticClient::dump_placement(string& fn) {
  
  UserPerm perms = client->pick_my_perms();

  // open file
  int fd = client->open(fn.c_str(), O_RDONLY, perms);
  dout(5) << "reading from " << fn << " fd " << fd << dendl;
  if (fd < 0) return fd;


  // How big is it?
  struct stat stbuf;
  int lstat_result = client->lstat(fn.c_str(), &stbuf, perms);
  if (lstat_result < 0) {
    dout(0) << "lstat error for file " << fn << dendl;
    client->close(fd);
    return lstat_result;
  }
    
  off_t filesize = stbuf.st_size;
 
  // grab the placement info
  vector<ObjectExtent> extents;
  off_t offset = 0;
  client->enumerate_layout(fd, extents, filesize, offset);
  client->close(fd);


  // run through all the object extents
  dout(0) << "file size is " << filesize << dendl;
  dout(0) << "(osd, start, length) tuples for file " << fn << dendl;
  for (const auto& x : extents) {
    int osd = client->objecter->with_osdmap([&](const OSDMap& o) {
	return o.get_pg_acting_primary(o.object_locator_to_pg(x.oid, x.oloc));
      });

    // run through all the buffer extents
    for (const auto& be : x.buffer_extents)
      dout(0) << "OSD " << osd << ", offset " << be.first
	      << ", length " << be.second << dendl;
  }
  return 0;
}



int SyntheticClient::make_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  UserPerm perms = client->pick_my_perms();
  // make sure base dir exists
  int r = client->mkdir(basedir, 0755, perms);
  if (r != 0) {
    dout(1) << "can't make base dir? " << basedir << dendl;
    //return -1;
  }

  // children
  char d[500];
  dout(3) << "make_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << dendl;
  for (int i=0; i<files; i++) {
    snprintf(d, sizeof(d), "%s/file.%d", basedir, i);
    client->mknod(d, 0644, perms);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
    snprintf(d, sizeof(d), "%s/dir.%d", basedir, i);
    make_dirs(d, dirs, files, depth-1);
  }
  
  return 0;
}

int SyntheticClient::stat_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  UserPerm perms = client->pick_my_perms();

  // make sure base dir exists
  struct stat st;
  int r = client->lstat(basedir, &st, perms);
  if (r != 0) {
    dout(1) << "can't make base dir? " << basedir << dendl;
    return -1;
  }

  // children
  char d[500];
  dout(3) << "stat_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << dendl;
  for (int i=0; i<files; i++) {
    snprintf(d, sizeof(d), "%s/file.%d", basedir, i);
    client->lstat(d, &st, perms);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
    snprintf(d, sizeof(d), "%s/dir.%d", basedir, i);
    stat_dirs(d, dirs, files, depth-1);
  }
  
  return 0;
}
int SyntheticClient::read_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  struct stat st;

  // children
  char d[500];
  dout(3) << "read_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << dendl;

  list<string> contents;
  UserPerm perms = client->pick_my_perms();
  utime_t s = ceph_clock_now();
  int r = client->getdir(basedir, contents, perms);
  utime_t e = ceph_clock_now();
  e -= s;
  if (r < 0) {
    dout(0) << "getdir couldn't readdir " << basedir << ", stopping" << dendl;
    return -1;
  }

  for (int i=0; i<files; i++) {
    snprintf(d, sizeof(d), "%s/file.%d", basedir, i);
    utime_t s = ceph_clock_now();
    if (client->lstat(d, &st, perms) < 0) {
      dout(2) << "read_dirs failed stat on " << d << ", stopping" << dendl;
      return -1;
    }
    utime_t e = ceph_clock_now();
    e -= s;
  }

  if (depth > 0) 
    for (int i=0; i<dirs; i++) {
      snprintf(d, sizeof(d), "%s/dir.%d", basedir, i);
      if (read_dirs(d, dirs, files, depth-1) < 0) return -1;
    }

  return 0;
}


int SyntheticClient::make_files(int num, int count, int priv, bool more)
{
  int whoami = client->get_nodeid().v;
  char d[255];
  UserPerm perms = client->pick_my_perms();

  if (priv) {
    for (int c=0; c<count; c++) {
      snprintf(d, sizeof(d), "dir.%d.run%d", whoami, c);
      client->mkdir(d, 0755, perms);
    }
  } else {
    // shared
    if (true || whoami == 0) {
      for (int c=0; c<count; c++) {
        snprintf(d, sizeof(d), "dir.%d.run%d", 0, c);
        client->mkdir(d, 0755, perms);
      }
    } else {
      sleep(2);
    }
  }
  
  // files
  struct stat st;
  utime_t start = ceph_clock_now();
  for (int c=0; c<count; c++) {
    for (int n=0; n<num; n++) {
      snprintf(d, sizeof(d), "dir.%d.run%d/file.client%d.%d", priv ? whoami:0, c, whoami, n);

      client->mknod(d, 0644, perms);

      if (more) {
        client->lstat(d, &st, perms);
        int fd = client->open(d, O_RDONLY, perms);
        client->unlink(d, perms);
        client->close(fd);
      }

      if (time_to_stop()) return 0;
    }
  }
  utime_t end = ceph_clock_now();
  end -= start;
  dout(0) << "makefiles time is " << end << " or " << ((double)end / (double)num) <<" per file" << dendl;

  return 0;
}

int SyntheticClient::link_test()
{
  char d[255];
  char e[255];

  UserPerm perms = client->pick_my_perms();

 // create files
  int num = 200;

  client->mkdir("orig", 0755, perms);
  client->mkdir("copy", 0755, perms);

  utime_t start = ceph_clock_now();
  for (int i=0; i<num; i++) {
    snprintf(d, sizeof(d), "orig/file.%d", i);
    client->mknod(d, 0755, perms);
  }
  utime_t end = ceph_clock_now();
  end -= start;

  dout(0) << "orig " << end << dendl;

  // link
  start = ceph_clock_now();
  for (int i=0; i<num; i++) {
    snprintf(d, sizeof(d), "orig/file.%d", i);
    snprintf(e, sizeof(e), "copy/file.%d", i);
    client->link(d, e, perms);
  }
  end = ceph_clock_now();
  end -= start;
  dout(0) << "copy " << end << dendl;

  return 0;
}


int SyntheticClient::create_shared(int num)
{
  // files
  UserPerm perms = client->pick_my_perms();
  char d[255];
  client->mkdir("test", 0755, perms);
  for (int n=0; n<num; n++) {
    snprintf(d, sizeof(d), "test/file.%d", n);
    client->mknod(d, 0644, perms);
  }
  
  return 0;
}

int SyntheticClient::open_shared(int num, int count)
{
  // files
  char d[255];
  UserPerm perms = client->pick_my_perms();
  for (int c=0; c<count; c++) {
    // open
    list<int> fds;
    for (int n=0; n<num; n++) {
      snprintf(d, sizeof(d), "test/file.%d", n);
      int fd = client->open(d, O_RDONLY, perms);
      if (fd > 0) fds.push_back(fd);
    }

    if (false && client->get_nodeid() == 0)
      for (int n=0; n<num; n++) {
	snprintf(d, sizeof(d), "test/file.%d", n);
	client->unlink(d, perms);
      }

    while (!fds.empty()) {
      int fd = fds.front();
      fds.pop_front();
      client->close(fd);
    }
  }
  
  return 0;
}


// Hits OSD 0 with writes to various files with OSD 0 as the primary.
int SyntheticClient::overload_osd_0(int n, int size, int wrsize) {
  UserPerm perms = client->pick_my_perms();
  // collect a bunch of files starting on OSD 0
  int left = n;
  int tried = 0;
  while (left < 0) {


    // pull open a file
    dout(0) << "in OSD overload" << dendl;
    string filename = get_sarg(tried);
    dout(1) << "OSD Overload workload: trying file " << filename << dendl;
    int fd = client->open(filename.c_str(), O_RDWR|O_CREAT, perms);
    ++tried;

    // only use the file if its first primary is OSD 0
    int primary_osd = check_first_primary(fd);
    if (primary_osd != 0) {
      client->close(fd);
      dout(1) << "OSD Overload workload: SKIPPING file " << filename <<
	" with OSD " << primary_osd << " as first primary. " << dendl;
      continue;
    }
      dout(1) << "OSD Overload workload: USING file " << filename <<
	" with OSD 0 as first primary. " << dendl;


    --left;
    // do whatever operation we want to do on the file. How about a write?
    write_fd(fd, size, wrsize);
  }
  return 0;
}


// See what the primary is for the first object in this file.
int SyntheticClient::check_first_primary(int fh)
{
  vector<ObjectExtent> extents;
  client->enumerate_layout(fh, extents, 1, 0);
  return client->objecter->with_osdmap([&](const OSDMap& o) {
      return o.get_pg_acting_primary(
	o.object_locator_to_pg(extents.begin()->oid, extents.begin()->oloc));
    });
}

int SyntheticClient::rm_file(string& fn)
{
  UserPerm perms = client->pick_my_perms();
  return client->unlink(fn.c_str(), perms);
}

int SyntheticClient::write_file(string& fn, int size, loff_t wrsize)   // size is in MB, wrsize in bytes
{
  //uint64_t wrsize = 1024*256;
  char *buf = new char[wrsize+100];   // 1 MB
  memset(buf, 7, wrsize);
  int64_t chunks = (uint64_t)size * (uint64_t)(1024*1024) / (uint64_t)wrsize;
  UserPerm perms = client->pick_my_perms();

  int fd = client->open(fn.c_str(), O_RDWR|O_CREAT, perms);
  dout(5) << "writing to " << fn << " fd " << fd << dendl;
  if (fd < 0) {
    delete[] buf;
    return fd;
  }

  utime_t from = ceph_clock_now();
  utime_t start = from;
  uint64_t bytes = 0, total = 0;


  for (loff_t i=0; i<chunks; i++) {
    if (time_to_stop()) {
      dout(0) << "stopping" << dendl;
      break;
    }
    dout(2) << "writing block " << i << "/" << chunks << dendl;
    
    // fill buf with a 16 byte fingerprint
    // 64 bits : file offset
    // 64 bits : client id
    // = 128 bits (16 bytes)
    uint64_t *p = (uint64_t*)buf;
    while ((char*)p < buf + wrsize) {
      *p = (uint64_t)i*(uint64_t)wrsize + (uint64_t)((char*)p - buf);      
      p++;
      *p = client->get_nodeid().v;
      p++;
    }

    client->write(fd, buf, wrsize, i*wrsize);
    bytes += wrsize;
    total += wrsize;

    utime_t now = ceph_clock_now();
    if (now - from >= 1.0) {
      double el = now - from;
      dout(0) << "write " << (bytes / el / 1048576.0) << " MB/sec" << dendl;
      from = now;
      bytes = 0;
    }
  }

  client->fsync(fd, true);

  utime_t stop = ceph_clock_now();
  double el = stop - start;
  dout(0) << "write total " << (total / el / 1048576.0) << " MB/sec ("
	  << total << " bytes in " << el << " seconds)" << dendl;

  client->close(fd);
  delete[] buf;

  return 0;
}

int SyntheticClient::write_fd(int fd, int size, int wrsize)   // size is in MB, wrsize in bytes
{
  //uint64_t wrsize = 1024*256;
  char *buf = new char[wrsize+100];   // 1 MB
  memset(buf, 7, wrsize);
  uint64_t chunks = (uint64_t)size * (uint64_t)(1024*1024) / (uint64_t)wrsize;

  //dout(5) << "SyntheticClient::write_fd: writing to fd " << fd << dendl;
  if (fd < 0) {
    delete[] buf;
    return fd;
  }

  for (unsigned i=0; i<chunks; i++) {
    if (time_to_stop()) {
      dout(0) << "stopping" << dendl;
      break;
    }
    dout(2) << "writing block " << i << "/" << chunks << dendl;
    
    // fill buf with a 16 byte fingerprint
    // 64 bits : file offset
    // 64 bits : client id
    // = 128 bits (16 bytes)
    uint64_t *p = (uint64_t*)buf;
    while ((char*)p < buf + wrsize) {
      *p = (uint64_t)i*(uint64_t)wrsize + (uint64_t)((char*)p - buf);      
      p++;
      *p = client->get_nodeid().v;
      p++;
    }

    client->write(fd, buf, wrsize, i*wrsize);
  }
  client->close(fd);
  delete[] buf;

  return 0;
}


int SyntheticClient::write_batch(int nfile, int size, int wrsize)
{
  for (int i=0; i<nfile; i++) {
      string sarg1 = get_sarg(i);
    dout(0) << "Write file " << sarg1 << dendl;
    write_file(sarg1, size, wrsize);
  }
  return 0;
}

// size is in MB, wrsize in bytes
int SyntheticClient::read_file(const std::string& fn, int size,
			       int rdsize, bool ignoreprint)
{
  char *buf = new char[rdsize]; 
  memset(buf, 1, rdsize);
  uint64_t chunks = (uint64_t)size * (uint64_t)(1024*1024) / (uint64_t)rdsize;
  UserPerm perms = client->pick_my_perms();

  int fd = client->open(fn.c_str(), O_RDONLY, perms);
  dout(5) << "reading from " << fn << " fd " << fd << dendl;
  if (fd < 0) {
    delete[] buf;
    return fd;
  }

  utime_t from = ceph_clock_now();
  utime_t start = from;
  uint64_t bytes = 0, total = 0;

  for (unsigned i=0; i<chunks; i++) {
    if (time_to_stop()) break;
    dout(2) << "reading block " << i << "/" << chunks << dendl;
    int r = client->read(fd, buf, rdsize, i*rdsize);
    if (r < rdsize) {
      dout(1) << "read_file got r = " << r << ", probably end of file" << dendl;
      break;
    }
 
    bytes += rdsize;
    total += rdsize;

    utime_t now = ceph_clock_now();
    if (now - from >= 1.0) {
      double el = now - from;
      dout(0) << "read " << (bytes / el / 1048576.0) << " MB/sec" << dendl;
      from = now;
      bytes = 0;
    }

    // verify fingerprint
    int bad = 0;
    uint64_t *p = (uint64_t*)buf;
    while ((char*)p + 32 < buf + rdsize) {
      uint64_t readoff = *p;
      uint64_t wantoff = (uint64_t)i*(uint64_t)rdsize + (uint64_t)((char*)p - buf);
      p++;
      int64_t readclient = *p;
      p++;
      if (readoff != wantoff ||
	  readclient != client->get_nodeid()) {
        if (!bad && !ignoreprint)
          dout(0) << "WARNING: wrong data from OSD, block says fileoffset=" << readoff << " client=" << readclient
		  << ", should be offset " << wantoff << " client " << client->get_nodeid()
		  << dendl;
        bad++;
      }
    }
    if (bad && !ignoreprint) 
      dout(0) << " + " << (bad-1) << " other bad 16-byte bits in this block" << dendl;
  }

  utime_t stop = ceph_clock_now();
  double el = stop - start;
  dout(0) << "read total " << (total / el / 1048576.0) << " MB/sec ("
	  << total << " bytes in " << el << " seconds)" << dendl;

  client->close(fd);
  delete[] buf;

  return 0;
}




class C_Ref : public Context {
  ceph::mutex& lock;
  ceph::condition_variable& cond;
  int *ref;
public:
  C_Ref(ceph::mutex &l, ceph::condition_variable &c, int *r)
    : lock(l), cond(c), ref(r) {
    lock_guard locker{lock};
    (*ref)++;
  }
  void finish(int) override {
    lock_guard locker{lock};
    (*ref)--;
    cond.notify_all();
  }
};

int SyntheticClient::create_objects(int nobj, int osize, int inflight)
{
  // divy up
  int numc = num_client ? num_client : 1;

  int start, inc, end;

  if (1) {
    // strided
    start = client->get_nodeid().v; //nobjs % numc;
    inc = numc;
    end = start + nobj;
  } else {
    // segments
    start = nobj * client->get_nodeid().v / numc;
    inc = 1;
    end = nobj * (client->get_nodeid().v+1) / numc;
  }

  dout(5) << "create_objects " << nobj << " size=" << osize 
	  << " .. doing [" << start << "," << end << ") inc " << inc
	  << dendl;
  
  bufferptr bp(osize);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);

  ceph::mutex lock = ceph::make_mutex("create_objects lock");
  ceph::condition_variable cond;
  
  int unsafe = 0;
  
  list<utime_t> starts;

  for (int i=start; i<end; i += inc) {
    if (time_to_stop()) break;

    object_t oid = file_object_t(999, i);
    object_locator_t oloc(SYNCLIENT_FIRST_POOL);
    SnapContext snapc;
    
    if (i % inflight == 0) {
      dout(6) << "create_objects " << i << "/" << (nobj+1) << dendl;
    }
    dout(10) << "writing " << oid << dendl;

    starts.push_back(ceph_clock_now());
    {
      std::lock_guard locker{client->client_lock};
      client->objecter->write(oid, oloc, 0, osize, snapc, bl,
			      ceph::real_clock::now(), 0,
			      new C_Ref(lock, cond, &unsafe));
    }
    {
      std::unique_lock locker{lock};
      cond.wait(locker, [&unsafe, inflight, this] {
        if (unsafe > inflight) {
	  dout(20) << "waiting for " << unsafe << " unsafe" << dendl;
	}
	return unsafe <= inflight;
      });
    }
    utime_t lat = ceph_clock_now();
    lat -= starts.front();
    starts.pop_front();
  }
  {
    std::unique_lock locker{lock};
    cond.wait(locker, [&unsafe, this] {
      if (unsafe > 0) {
	dout(10) << "waiting for " << unsafe << " unsafe" << dendl;
      }
      return unsafe <= 0;
    });
  }
  dout(5) << "create_objects done" << dendl;
  return 0;
}

int SyntheticClient::object_rw(int nobj, int osize, int wrpc, 
			       int overlappc,
			       double rskew, double wskew)
{
  dout(5) << "object_rw " << nobj << " size=" << osize << " with "
	  << wrpc << "% writes" 
	  << ", " << overlappc << "% overlap"
	  << ", rskew = " << rskew
	  << ", wskew = " << wskew
	  << dendl;

  bufferptr bp(osize);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);

  // start with odd number > nobj
  rjhash<uint32_t> h;
  unsigned prime = nobj + 1;             // this is the minimum!
  prime += h(nobj) % (3*nobj);  // bump it up some
  prime |= 1;                               // make it odd

  while (true) {
    unsigned j;
    for (j=2; j*j<=prime; j++)
      if (prime % j == 0) break;
    if (j*j > prime) {
      break;
      //cout << "prime " << prime << endl;
    }
    prime += 2;
  }

  ceph::mutex lock = ceph::make_mutex("lock");
  ceph::condition_variable cond;

  int unack = 0;

  while (1) {
    if (time_to_stop()) break;
    
    // read or write?
    bool write = (rand() % 100) < wrpc;

    // choose object
    double r = drand48(); // [0..1)
    long o;
    if (write) {
      o = (long)trunc(pow(r, wskew) * (double)nobj);  // exponentially skew towards 0
      int pnoremap = (long)(r * 100.0);
      if (pnoremap >= overlappc) 
	o = (o*prime) % nobj;    // remap
    } else {
      o = (long)trunc(pow(r, rskew) * (double)nobj);  // exponentially skew towards 0
    }
    object_t oid = file_object_t(999, o);
    object_locator_t oloc(SYNCLIENT_FIRST_POOL);
    SnapContext snapc;
    
    client->client_lock.lock();
    utime_t start = ceph_clock_now();
    if (write) {
      dout(10) << "write to " << oid << dendl;

      ObjectOperation m;
      OSDOp op;
      op.op.op = CEPH_OSD_OP_WRITE;
      op.op.extent.offset = 0;
      op.op.extent.length = osize;
      op.indata = bl;
      m.ops.push_back(op);
      client->objecter->mutate(oid, oloc, m, snapc,
			       ceph::real_clock::now(), 0,
			       new C_Ref(lock, cond, &unack));
    } else {
      dout(10) << "read from " << oid << dendl;
      bufferlist inbl;
      client->objecter->read(oid, oloc, 0, osize, CEPH_NOSNAP, &inbl, 0,
			     new C_Ref(lock, cond, &unack));
    }
    client->client_lock.unlock();

    {
      std::unique_lock locker{lock};
      cond.wait(locker, [&unack, this] {
	if (unack > 0) {
	  dout(20) << "waiting for " << unack << " unack" << dendl;
	}
	return unack <= 0;
      });
    }

    utime_t lat = ceph_clock_now();
    lat -= start;
  }

  return 0;
}





int SyntheticClient::read_random(string& fn, int size, int rdsize)   // size is in MB, wrsize in bytes
{
  UserPerm perms = client->pick_my_perms();
  uint64_t chunks = (uint64_t)size * (uint64_t)(1024*1024) / (uint64_t)rdsize;
  int fd = client->open(fn.c_str(), O_RDWR, perms);
  dout(5) << "reading from " << fn << " fd " << fd << dendl;

  if (fd < 0) return fd;
  int offset = 0;
  char * buf = NULL;

  for (unsigned i=0; i<2000; i++) {
    if (time_to_stop()) break;

    bool read=false;

    time_t seconds;
    time( &seconds);
    srand(seconds);

    // use rand instead ??
    double x = drand48();

    // cleanup before call 'new'
    if (buf != NULL) {
	delete[] buf;
	buf = NULL;
    }
    if (x < 0.5) {
        buf = new char[rdsize]; 
        memset(buf, 1, rdsize);
        read=true;
    } else {
        buf = new char[rdsize+100];   // 1 MB
        memset(buf, 7, rdsize);
    }
    
    if (read) {
        offset=(rand())%(chunks+1);
        dout(2) << "reading block " << offset << "/" << chunks << dendl;

        int r = client->read(fd, buf, rdsize, offset*rdsize);
        if (r < rdsize) {
	  dout(1) << "read_file got r = " << r << ", probably end of file" << dendl;
	}
    } else {
      dout(2) << "writing block " << offset << "/" << chunks << dendl;

      // fill buf with a 16 byte fingerprint
      // 64 bits : file offset
      // 64 bits : client id
      // = 128 bits (16 bytes)

      offset=(rand())%(chunks+1);
      uint64_t *p = (uint64_t*)buf;
      while ((char*)p < buf + rdsize) {
	*p = offset*rdsize + (char*)p - buf;      
	p++;
	*p = client->get_nodeid().v;
	p++;
      }

      client->write(fd, buf, rdsize,
                        offset*rdsize);
    }

    // verify fingerprint
    if (read) {
      int bad = 0;
      int64_t *p = (int64_t*)buf;
      while ((char*)p + 32 < buf + rdsize) {
	int64_t readoff = *p;
	int64_t wantoff = offset*rdsize + (int64_t)((char*)p - buf);
	p++;
	int64_t readclient = *p;
	p++;
	if (readoff != wantoff || readclient != client->get_nodeid()) {
	  if (!bad)
	    dout(0) << "WARNING: wrong data from OSD, block says fileoffset=" << readoff << " client=" << readclient
		    << ", should be offset " << wantoff << " client " << client->get_nodeid()
		    << dendl;
	  bad++;
	}
      }
      if (bad) 
	dout(0) << " + " << (bad-1) << " other bad 16-byte bits in this block" << dendl;
    }
  }
  
  client->close(fd);
  delete[] buf;

  return 0;
}

int normdist(int min, int max, int stdev) /* specifies input values */
{
  /* min: Minimum value; max: Maximum value; stdev: degree of deviation */
  
  //int min, max, stdev; {
  time_t seconds;
  time( &seconds);
  srand(seconds);
  
  int range, iterate, result;
  /* declare range, iterate and result as integers, to avoid the need for
     floating point math*/
  
  result = 0;
  /* ensure result is initialized to 0 */
  
  range = max -min;
  /* calculate range of possible values between the max and min values */
  
  iterate = range / stdev;
  /* this number of iterations ensures the proper shape of the resulting
     curve */
  
  stdev += 1; /* compensation for integer vs. floating point math */
  for (int c = iterate; c != 0; c--) /* loop through iterations */
    {
      //  result += (uniform (1, 100) * stdev) / 100; /* calculate and
      result += ( (rand()%100 + 1)  * stdev) / 100;
      // printf("result=%d\n", result );
    }
  printf("\n final result=%d\n", result );
  return result + min; /* send final result back */
}

int SyntheticClient::read_random_ex(string& fn, int size, int rdsize)   // size is in MB, wrsize in bytes
{
  uint64_t chunks = (uint64_t)size * (uint64_t)(1024*1024) / (uint64_t)rdsize;
  UserPerm perms = client->pick_my_perms();
  int fd = client->open(fn.c_str(), O_RDWR, perms);
  dout(5) << "reading from " << fn << " fd " << fd << dendl;
  
  if (fd < 0) return fd;
  int offset = 0;
  char * buf = NULL;
  
  for (unsigned i=0; i<2000; i++) {
    if (time_to_stop()) break;
    
    bool read=false;
    
    time_t seconds;
    time( &seconds);
    srand(seconds);
    
    // use rand instead ??
    double x = drand48();
    
    // cleanup before call 'new'
    if (buf != NULL) {
      delete[] buf;
      buf = NULL;
    }
    if (x < 0.5) {
      buf = new char[rdsize]; 
      memset(buf, 1, rdsize);
      read=true;
    } else {
      buf = new char[rdsize+100];   // 1 MB
      memset(buf, 7, rdsize);
    }
    
    if (read) {
      dout(2) << "reading block " << offset << "/" << chunks << dendl;
	
      int r = client->read(fd, buf, rdsize,
			     offset*rdsize);
      if (r < rdsize) {
	dout(1) << "read_file got r = " << r << ", probably end of file" << dendl;
      }
    } else {
        dout(2) << "writing block " << offset << "/" << chunks << dendl;
	
	// fill buf with a 16 byte fingerprint
	// 64 bits : file offset
	// 64 bits : client id
	// = 128 bits (16 bytes)
	
	int count = rand()%10;
	
	for ( int j=0;j<count; j++ ) {
	  offset=(rand())%(chunks+1);
	  uint64_t *p = (uint64_t*)buf;
	  while ((char*)p < buf + rdsize) {
	    *p = offset*rdsize + (char*)p - buf;      
	    p++;
	    *p = client->get_nodeid().v;
	    p++;
	  }
	    
	  client->write(fd, buf, rdsize, offset*rdsize);
	}
    }
    
    // verify fingerprint
    if (read) {
      int bad = 0;
      int64_t *p = (int64_t*)buf;
      while ((char*)p + 32 < buf + rdsize) {
	int64_t readoff = *p;
	int64_t wantoff = offset*rdsize + (int64_t)((char*)p - buf);
	p++;
	int64_t readclient = *p;
	p++;
	if (readoff != wantoff || readclient != client->get_nodeid()) { 
	  if (!bad)
	    dout(0) << "WARNING: wrong data from OSD, block says fileoffset=" << readoff << " client=" << readclient
		    << ", should be offset " << wantoff << " client " << client->get_nodeid()
		    << dendl;
	  bad++;
	}
      }
      if (bad) 
	dout(0) << " + " << (bad-1) << " other bad 16-byte bits in this block" << dendl;
    }
  }
  
  client->close(fd);
  delete[] buf;

  return 0;
}


int SyntheticClient::random_walk(int num_req)
{
  int left = num_req;

  //dout(1) << "random_walk() will do " << left << " ops" << dendl;

  init_op_dist();  // set up metadata op distribution
 
  UserPerm perms = client->pick_my_perms();
  while (left > 0) {
    left--;

    if (time_to_stop()) break;

    // ascend?
    if (cwd.depth() && !roll_die(::pow((double).9, (double)cwd.depth()))) {
      dout(DBL) << "die says up" << dendl;
      up();
      continue;
    }

    // descend?
    if (roll_die(::pow((double).9,(double)cwd.depth())) && !subdirs.empty()) {
      string s = get_random_subdir();
      cwd.push_dentry( s );
      dout(DBL) << "cd " << s << " -> " << cwd << dendl;
      clear_dir();
      continue;
    }

    int op = 0;
    filepath path;

    if (contents.empty() && roll_die(.3)) {
      if (did_readdir) {
        dout(DBL) << "empty dir, up" << dendl;
        up();
      } else
        op = CEPH_MDS_OP_READDIR;
    } else {
      op = op_dist.sample();
    }
    //dout(DBL) << "op is " << op << dendl;

    int r = 0;

    // do op
    if (op == CEPH_MDS_OP_UNLINK) {
      if (contents.empty())
        op = CEPH_MDS_OP_READDIR;
      else 
        r = client->unlink(get_random_sub(), perms);   // will fail on dirs
    }
     
    if (op == CEPH_MDS_OP_RENAME) {
      if (contents.empty())
        op = CEPH_MDS_OP_READDIR;
      else {
        r = client->rename(get_random_sub(), make_sub("ren"), perms);
      }
    }
    
    if (op == CEPH_MDS_OP_MKDIR) {
      r = client->mkdir(make_sub("mkdir"), 0755, perms);
    }
    
    if (op == CEPH_MDS_OP_RMDIR) {
      if (!subdirs.empty())
        r = client->rmdir(get_random_subdir(), perms);
      else
        r = client->rmdir(cwd.c_str(), perms);     // will pbly fail
    }
    
    if (op == CEPH_MDS_OP_SYMLINK) {
    }
    /*
    if (op == CEPH_MDS_OP_CHMOD) {
      if (contents.empty())
        op = CEPH_MDS_OP_READDIR;
      else
        r = client->chmod(get_random_sub(), rand() & 0755, perms);
    }
    
    if (op == CEPH_MDS_OP_CHOWN) {
      if (contents.empty())         r = client->chown(cwd.c_str(), rand(), rand(), perms);
      else
        r = client->chown(get_random_sub(), rand(), rand(), perms);
    }
     
    if (op == CEPH_MDS_OP_UTIME) {
      struct utimbuf b;
      memset(&b, 1, sizeof(b));
      if (contents.empty()) 
        r = client->utime(cwd.c_str(), &b, perms);
      else
        r = client->utime(get_random_sub(), &b, perms);
    }
    */
    if (op == CEPH_MDS_OP_LINK) {
    }
    
    if (op == CEPH_MDS_OP_MKNOD) {
      r = client->mknod(make_sub("mknod"), 0644, perms);
    }
     
    if (op == CEPH_MDS_OP_OPEN) {
      if (contents.empty())
        op = CEPH_MDS_OP_READDIR;
      else {
        r = client->open(get_random_sub(), O_RDONLY, perms);
        if (r > 0) {
          ceph_assert(open_files.count(r) == 0);
          open_files.insert(r);
        }
      }
    }

    /*if (op == CEPH_MDS_OP_RELEASE) {   // actually, close
      if (open_files.empty())
        op = CEPH_MDS_OP_STAT;
      else {
        int fh = get_random_fh();
        r = client->close( fh );
        if (r == 0) open_files.erase(fh);
      }
    }
    */
    
    if (op == CEPH_MDS_OP_GETATTR) {
      struct stat st;
      if (contents.empty()) {
        if (did_readdir) {
          if (roll_die(.1)) {
            dout(DBL) << "stat in empty dir, up" << dendl;
            up();
          } else {
            op = CEPH_MDS_OP_MKNOD;
          }
        } else
          op = CEPH_MDS_OP_READDIR;
      } else
        r = client->lstat(get_random_sub(), &st, perms);
    }

    if (op == CEPH_MDS_OP_READDIR) {
      clear_dir();
      
      list<string> c;
      r = client->getdir(cwd.c_str(), c, perms);
      
      for (list<string>::iterator it = c.begin();
           it != c.end();
           ++it) {
        //dout(DBL) << " got " << *it << dendl;
	ceph_abort();
	/*contents[*it] = it->second;
        if (it->second &&
	    S_ISDIR(it->second->st_mode)) 
          subdirs.insert(*it);
	*/
      }
      
      did_readdir = true;
    }
      
    // errors?
    if (r < 0) {
      // reevaluate cwd.
      //while (cwd.depth()) {
      //if (client->lookup(cwd)) break;   // it's in the cache
        
      //dout(DBL) << "r = " << r << ", client doesn't have " << cwd << ", cd .." << dendl;
      dout(DBL) << "r = " << r << ", client may not have " << cwd << ", cd .." << dendl;
      up();
      //}      
    }
  }

  // close files
  dout(DBL) << "closing files" << dendl;
  while (!open_files.empty()) {
    int fh = get_random_fh();
    int r = client->close( fh );
    if (r == 0) open_files.erase(fh);
  }

  dout(DBL) << "done" << dendl;
  return 0;
}




void SyntheticClient::make_dir_mess(const char *basedir, int n)
{
  UserPerm perms = client->pick_my_perms();
  vector<string> dirs;
  
  dirs.push_back(basedir);
  dirs.push_back(basedir);
  
  client->mkdir(basedir, 0755, perms);

  // motivation:
  //  P(dir) ~ subdirs_of(dir) + 2
  // from 5-year metadata workload paper in fast'07

  // create dirs
  for (int i=0; i<n; i++) {
    // pick a dir
    int k = rand() % dirs.size();
    string parent = dirs[k];
    
    // pick a name
    std::stringstream ss;
    ss << parent << "/" << i;
    string dir = ss.str();

    // update dirs
    dirs.push_back(parent);
    dirs.push_back(dir);
    dirs.push_back(dir);

    // do it
    client->mkdir(dir.c_str(), 0755, perms);
  }
    
  
}



void SyntheticClient::foo()
{
  UserPerm perms = client->pick_my_perms();

  if (1) {
    // make 2 parallel dirs, link/unlink between them.
    char a[100], b[100];
    client->mkdir("/a", 0755, perms);
    client->mkdir("/b", 0755, perms);
    for (int i=0; i<10; i++) {
      snprintf(a, sizeof(a), "/a/%d", i);
      client->mknod(a, 0644, perms);
    }
    while (1) {
      for (int i=0; i<10; i++) {
	snprintf(a, sizeof(a), "/a/%d", i);
	snprintf(b, sizeof(b), "/b/%d", i);
	client->link(a, b, perms);
      }
      for (int i=0; i<10; i++) {
	snprintf(b, sizeof(b), "/b/%d", i);
	client->unlink(b, perms);
      }
    }
    return;
  }
  if (1) {
    // bug1.cpp
    const char *fn = "blah";
    char buffer[8192]; 
    client->unlink(fn, perms);
    int handle = client->open(fn, O_CREAT|O_RDWR, perms, S_IRWXU);
    ceph_assert(handle>=0);
    int r=client->write(handle,buffer,8192);
    ceph_assert(r>=0);
    r=client->close(handle);
    ceph_assert(r>=0);
         
    handle = client->open(fn, O_RDWR, perms); // open the same  file, it must have some data already
    ceph_assert(handle>=0);      
    r=client->read(handle,buffer,8192);
    ceph_assert(r==8192); //  THIS ASSERTION FAILS with disabled cache
    r=client->close(handle);
    ceph_assert(r>=0);

    return;
  }
  if (1) {
    dout(0) << "first" << dendl;
    int fd = client->open("tester", O_WRONLY|O_CREAT, perms);
    client->write(fd, "hi there", 0, 8);
    client->close(fd);
    dout(0) << "sleep" << dendl;
    sleep(10);
    dout(0) << "again" << dendl;
    fd = client->open("tester", O_WRONLY|O_CREAT, perms);
    client->write(fd, "hi there", 0, 8);
    client->close(fd);
    return;    
  }
  if (1) {
    // open some files
    srand(0);
    for (int i=0; i<20; i++) {
      int s = 5;
      int a = rand() % s;
      int b = rand() % s;
      int c = rand() % s;
      char src[80];
      snprintf(src, sizeof(src), "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      //int fd = 
      client->open(src, O_RDONLY, perms);
    }

    return;
  }

  if (0) {
    // rename fun
    for (int i=0; i<100; i++) {
      int s = 5;
      int a = rand() % s;
      int b = rand() % s;
      int c = rand() % s;
      int d = rand() % s;
      int e = rand() % s;
      int f = rand() % s;
      char src[80];
      char dst[80];
      snprintf(src, sizeof(src), "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      snprintf(dst, sizeof(dst), "syn.0.0/dir.%d/dir.%d/file.%d", d, e, f);
      client->rename(src, dst, perms);
    }
    return;
  }

  if (1) {
    // link fun
    srand(0);
    for (int i=0; i<100; i++) {
      int s = 5;
      int a = rand() % s;
      int b = rand() % s;
      int c = rand() % s;
      int d = rand() % s;
      int e = rand() % s;
      int f = rand() % s;
      char src[80];
      char dst[80];
      snprintf(src, sizeof(src), "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      snprintf(dst, sizeof(dst), "syn.0.0/dir.%d/dir.%d/newlink.%d", d, e, f);
      client->link(src, dst, perms);
    }
    srand(0);
    for (int i=0; i<100; i++) {
      int s = 5;
      int a = rand() % s;
      int b = rand() % s;
      int c = rand() % s;
      int d = rand() % s;
      int e = rand() % s;
      int f = rand() % s;
      char src[80];
      char dst[80];
      snprintf(src, sizeof(src), "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      snprintf(dst, sizeof(dst), "syn.0.0/dir.%d/dir.%d/newlink.%d", d, e, f);
      client->unlink(dst, perms);
    }

    
    return;
  }

  // link fun
  client->mknod("one", 0755, perms);
  client->mknod("two", 0755, perms);
  client->link("one", "three", perms);
  client->mkdir("dir", 0755, perms);
  client->link("two", "/dir/twolink", perms);
  client->link("dir/twolink", "four", perms);
  
  // unlink fun
  client->mknod("a", 0644, perms);
  client->unlink("a", perms);
  client->mknod("b", 0644, perms);
  client->link("b", "c", perms);
  client->unlink("c", perms);
  client->mkdir("d", 0755, perms);
  client->unlink("d", perms);
  client->rmdir("d", perms);

  // rename fun
  client->mknod("p1", 0644, perms);
  client->mknod("p2", 0644, perms);
  client->rename("p1","p2", perms);
  client->mknod("p3", 0644, perms);
  client->rename("p3","p4", perms);

  // check dest dir ambiguity thing
  client->mkdir("dir1", 0755, perms);
  client->mkdir("dir2", 0755, perms);
  client->rename("p2", "dir1/p2", perms);
  client->rename("dir1/p2", "dir2/p2", perms);
  client->rename("dir2/p2", "/p2", perms);
  
  // check primary+remote link merging
  client->link("p2","p2.l", perms);
  client->link("p4","p4.l", perms);
  client->rename("p2.l", "p2", perms);
  client->rename("p4", "p4.l", perms);

  // check anchor updates
  client->mknod("dir1/a", 0644, perms);
  client->link("dir1/a", "da1", perms);
  client->link("dir1/a", "da2", perms);
  client->link("da2","da3", perms);
  client->rename("dir1/a", "dir2/a", perms);
  client->rename("dir2/a", "da2", perms);
  client->rename("da1", "da2", perms);
  client->rename("da2", "da3", perms);

  // check directory renames
  client->mkdir("dir3", 0755, perms);
  client->mknod("dir3/asdf", 0644, perms);
  client->mkdir("dir4", 0755, perms);
  client->mkdir("dir5", 0755, perms);
  client->mknod("dir5/asdf", 0644, perms);
  client->rename("dir3", "dir4", perms); // ok
  client->rename("dir4", "dir5", perms); // fail
}

int SyntheticClient::thrash_links(const char *basedir, int dirs, int files, int depth, int n)
{
  dout(1) << "thrash_links " << basedir << " " << dirs << " " << files << " " << depth
	  << " links " << n
	  << dendl;

  if (time_to_stop()) return 0;

  UserPerm perms = client->pick_my_perms();

  srand(0);
  if (1) {
    bool renames = true; // thrash renames too?
    for (int k=0; k<n; k++) {
      
      if (renames && rand() % 10 == 0) {
	// rename some directories.  whee!
	int dep = (rand() % depth) + 1;
	string src = basedir;
	{
	  char t[80];
	  for (int d=0; d<dep; d++) {
	    int a = rand() % dirs;
	    snprintf(t, sizeof(t), "/dir.%d", a);
	    src += t;
	  }
	}
	string dst = basedir;
	{
	  char t[80];
	  for (int d=0; d<dep; d++) {
	    int a = rand() % dirs;
	    snprintf(t, sizeof(t), "/dir.%d", a);
	    dst += t;
	  }
	}
	
	if (client->rename(dst.c_str(), "/tmp", perms) == 0) {
	  client->rename(src.c_str(), dst.c_str(), perms);
	  client->rename("/tmp", src.c_str(), perms);
	}
	continue;
      } 
      
      // pick a dest dir
      string src = basedir;
      {
	char t[80];
	for (int d=0; d<depth; d++) {
	  int a = rand() % dirs;
	  snprintf(t, sizeof(t), "/dir.%d", a);
	  src += t;
	}
	int a = rand() % files;
	snprintf(t, sizeof(t), "/file.%d", a);
	src += t;
      }
      string dst = basedir;
      {
	char t[80];
	for (int d=0; d<depth; d++) {
	  int a = rand() % dirs;
	  snprintf(t, sizeof(t), "/dir.%d", a);
	  dst += t;
	}
	int a = rand() % files;
	snprintf(t, sizeof(t), "/file.%d", a);
	dst += t;
      }
      
      int o = rand() % 4;
      switch (o) {
      case 0: 
	client->mknod(src.c_str(), 0755, perms);
	if (renames) client->rename(src.c_str(), dst.c_str(), perms);
	break;
      case 1: 
	client->mknod(src.c_str(), 0755, perms);
	client->unlink(dst.c_str(), perms);
	client->link(src.c_str(), dst.c_str(), perms); 
	break;
      case 2: client->unlink(src.c_str(), perms); break;
      case 3: client->unlink(dst.c_str(), perms); break;
	//case 4: client->mknod(src.c_str(), 0755, perms); break;
	//case 5: client->mknod(dst.c_str(), 0755, perms); break;
      }
    }
    return 0;
  }

  if (1) {
    // now link shit up
    for (int i=0; i<n; i++) {
      if (time_to_stop()) return 0;
      
      char f[20];
      
      // pick a file
      string file = basedir;
      
      if (depth) {
	int d = rand() % (depth+1);
	for (int k=0; k<d; k++) {
	  snprintf(f, sizeof(f), "/dir.%d", rand() % dirs);
	  file += f;
	}
      }
      snprintf(f, sizeof(f), "/file.%d", rand() % files);
      file += f;
      
      // pick a dir for our link
      string ln = basedir;
      if (depth) {
	int d = rand() % (depth+1);
	for (int k=0; k<d; k++) {
	  snprintf(f, sizeof(f), "/dir.%d", rand() % dirs);
	  ln += f;
	}
      }
      snprintf(f, sizeof(f), "/ln.%d", i);
      ln += f;
      
      client->link(file.c_str(), ln.c_str(), perms);
    }
  }
  return 0;
}




void SyntheticClient::import_find(const char *base, const char *find, bool data)
{
  dout(1) << "import_find " << base << " from " << find << " data=" << data << dendl;

  /* use this to gather the static trace:
   *
   *  find . -exec ls -dilsn --time-style=+%s \{\} \;
   * or if it's wafl,
   *  find . -path ./.snapshot -prune -o -exec ls -dilsn --time-style=+%s \{\} \;
   *
   */

  UserPerm process_perms = client->pick_my_perms();

  if (base[0] != '-') 
    client->mkdir(base, 0755, process_perms);

  ifstream f(find);
  ceph_assert(f.is_open());
  
  int dirnum = 0;

  while (!f.eof()) {
    uint64_t ino;
    int dunno, nlink;
    string modestring;
    int uid, gid;
    off_t size;
    time_t mtime;
    string filename;
    f >> ino;
    if (f.eof()) break;
    f >> dunno;
    f >> modestring;
    f >> nlink;
    f >> uid;
    f >> gid;
    f >> size;
    f >> mtime;
    f.seekg(1, ios::cur);
    getline(f, filename);
    UserPerm perms(uid, gid);

    // ignore "."
    if (filename == ".") continue;

    // remove leading ./
    ceph_assert(filename[0] == '.' && filename[1] == '/');
    filename = filename.substr(2);

    // new leading dir?
    int sp = filename.find("/");
    if (sp < 0) dirnum++;

    //dout(0) << "leading dir " << filename << " " << dirnum << dendl;
    if (dirnum % num_client != client->get_nodeid()) {
      dout(20) << "skipping leading dir " << dirnum << " " << filename << dendl;
      continue;
    }

    // parse the mode
    ceph_assert(modestring.length() == 10);
    mode_t mode = 0;
    switch (modestring[0]) {
    case 'd': mode |= S_IFDIR; break;
    case 'l': mode |= S_IFLNK; break;
    default:
    case '-': mode |= S_IFREG; break;
    }
    if (modestring[1] == 'r') mode |= 0400;
    if (modestring[2] == 'w') mode |= 0200;
    if (modestring[3] == 'x') mode |= 0100;
    if (modestring[4] == 'r') mode |= 040;
    if (modestring[5] == 'w') mode |= 020;
    if (modestring[6] == 'x') mode |= 010;
    if (modestring[7] == 'r') mode |= 04;
    if (modestring[8] == 'w') mode |= 02;
    if (modestring[9] == 'x') mode |= 01;

    dout(20) << " mode " << modestring << " to " << oct << mode << dec << dendl;

    if (S_ISLNK(mode)) {
      // target vs destination
      int pos = filename.find(" -> ");
      ceph_assert(pos > 0);
      string link;
      if (base[0] != '-') {
	link = base;
	link += "/";
      }
      link += filename.substr(0, pos);
      string target;
      if (filename[pos+4] == '/') {
	if (base[0] != '-') 
	  target = base;
	target += filename.substr(pos + 4);
      } else {
	target = filename.substr(pos + 4);
      }
      dout(10) << "symlink from '" << link << "' -> '" << target << "'" << dendl;
      client->symlink(target.c_str(), link.c_str(), perms);
    } else {
      string f;
      if (base[0] != '-') {
	f = base;
	f += "/";
      }
      f += filename;
      if (S_ISDIR(mode)) {
	client->mkdir(f.c_str(), mode, perms);
      } else {
	int fd = client->open(f.c_str(), O_WRONLY|O_CREAT, perms, mode & 0777);
	ceph_assert(fd > 0);	
	if (data) {
	  client->write(fd, "", 0, size);
	} else {
	  client->truncate(f.c_str(), size, perms);
	}
	client->close(fd);

	//client->chmod(f.c_str(), mode & 0777, perms, process_perms);
	client->chown(f.c_str(), uid, gid, process_perms);

	struct utimbuf ut;
	ut.modtime = mtime;
	ut.actime = mtime;
	client->utime(f.c_str(), &ut, perms);
      }
    }
  }
  

}


int SyntheticClient::lookup_hash(inodeno_t ino, inodeno_t dirino,
				 const char *name, const UserPerm& perms)
{
  int r = client->lookup_hash(ino, dirino, name, perms);
  dout(0) << "lookup_hash(" << ino << ", #" << dirino << "/" << name << ") = " << r << dendl;
  return r;
}

int SyntheticClient::lookup_ino(inodeno_t ino, const UserPerm& perms)
{
  int r = client->lookup_ino(ino, perms);
  dout(0) << "lookup_ino(" << ino << ") = " << r << dendl;
  return r;
}

int SyntheticClient::chunk_file(string &filename)
{
  UserPerm perms = client->pick_my_perms();
  int fd = client->open(filename.c_str(), O_RDONLY, perms);
  if (fd < 0)
    return fd;

  struct stat st;
  int ret = client->fstat(fd, &st, perms);
  if (ret < 0) {
    client->close(fd);
    return ret;
  }
  uint64_t size = st.st_size;
  dout(0) << "file " << filename << " size is " << size << dendl;

  inode_t inode{};
  inode.ino = st.st_ino;
  ret = client->fdescribe_layout(fd, &inode.layout);
  ceph_assert(ret == 0); // otherwise fstat did a bad thing

  uint64_t pos = 0;
  bufferlist from_before;
  while (pos < size) {
    int get = std::min<int>(size - pos, 1048576);

    ceph::mutex flock = ceph::make_mutex("synclient chunk_file lock");
    ceph::condition_variable cond;
    bool done;
    bufferlist bl;
    {
      std::unique_lock locker{flock};
      Context *onfinish = new C_SafeCond(flock, cond, &done);
      client->filer->read(inode.ino, &inode.layout, CEPH_NOSNAP, pos, get, &bl, 0,
			  onfinish);
      cond.wait(locker, [&done] { return done; });
    }
    dout(0) << "got " << bl.length() << " bytes at " << pos << dendl;
    
    if (from_before.length()) {
      dout(0) << " including bit from previous block" << dendl;
      pos -= from_before.length();
      from_before.claim_append(bl);
      bl.swap(from_before);
    }      

    // ....

    // keep last 32 bytes around
    from_before.clear();
    from_before.substr_of(bl, bl.length()-32, 32);

    pos += bl.length();
  }

  client->close(fd);
  return 0;
}



void SyntheticClient::mksnap(const char *base, const char *name, const UserPerm& perms)
{
  client->mksnap(base, name, perms);
}

void SyntheticClient::rmsnap(const char *base, const char *name, const UserPerm& perms)
{
  client->rmsnap(base, name, perms);
}

void SyntheticClient::mksnapfile(const char *dir)
{
  UserPerm perms = client->pick_my_perms();
  client->mkdir(dir, 0755, perms);

  string f = dir;
  f += "/foo";
  int fd = client->open(f.c_str(), O_WRONLY|O_CREAT|O_TRUNC, perms);

  char buf[1048576*4];
  client->write(fd, buf, sizeof(buf), 0);
  client->fsync(fd, true);
  client->close(fd);
  
  string s = dir;
  s += "/.snap/1";
  client->mkdir(s.c_str(), 0755, perms);

  fd = client->open(f.c_str(), O_WRONLY, perms);
  client->write(fd, buf, 1048576*2, 1048576);
  client->fsync(fd, true);
  client->close(fd);
}
