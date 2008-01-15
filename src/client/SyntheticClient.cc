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

#include <iostream>
#include <sstream>
using namespace std;



#include "SyntheticClient.h"
#include "osdc/Objecter.h"

#include "include/filepath.h"
#include "mds/mdstypes.h"
#include "common/Logger.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/types.h>
#include <utime.h>
#include <math.h>
#include <sys/statvfs.h>

#include "config.h"

#define  dout(l)    if (l<=g_conf.debug || l<=g_conf.debug_client) *_dout << dbeginl << g_clock.now() << " synthetic" << (this->whoami >= 0 ? this->whoami:client->get_nodeid()) << " "
#define  derr(l)    if (l<=g_conf.debug || l<=g_conf.debug_client) *_derr << dbeginl << g_clock.now() << " synthetic" << (this->whoami >= 0 ? this->whoami:client->get_nodeid()) << " "

// traces
//void trace_include(SyntheticClient *syn, Client *cl, string& prefix);
//void trace_openssh(SyntheticClient *syn, Client *cl, string& prefix);


list<int> syn_modes;
list<int> syn_iargs;
list<string> syn_sargs;

void parse_syn_options(vector<const char*>& args)
{
  vector<const char*> nargs;

  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"--syn") == 0) {
      ++i;

      if (strcmp(args[i],"writefile") == 0) {
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
      } else {
        cerr << "unknown syn arg " << args[i] << std::endl;
        assert(0);
      }
    }
    else {
      nargs.push_back(args[i]);
    }
  }

  args = nargs;
}


SyntheticClient::SyntheticClient(Client *client, int w) 
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

  run_start = g_clock.now();
}




#define DBL 2

void *synthetic_client_thread_entry(void *ptr)
{
  SyntheticClient *sc = (SyntheticClient*)ptr;
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
    char s[20];
    sprintf(s,"/syn.%d.%d", client->whoami, seq);
    a = s;
  } 
  return a;
}

int SyntheticClient::run()
{ 
  dout(15) << "initing" << dendl;
  client->init();
  dout(15) << "mounting" << dendl;
  client->mount();

  //run_start = g_clock.now();
  run_until = utime_t(0,0);
  dout(5) << "run" << dendl;

  int seq = 0;

  for (list<int>::iterator it = modes.begin();
       it != modes.end();
       it++) {
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
	  run_only = client->get_nodeid()+1;  // not me
      }
      break;
    case SYNCLIENT_MODE_EXCLUDE:
      {
        exclude = iargs.front();
        iargs.pop_front();
        if (exclude == client->get_nodeid()) {
	  run_only = client->get_nodeid() + 1;
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
          srand(time(0) + getpid() + client->whoami);
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
          utime_t at = g_clock.now() - run_start;
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
          dout(2) << "createobjects " << cout << " of " << size << " bytes"
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
          dout(2) << "objectrw " << cout << " " << size << " " << wrpc 
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
	sprintf(realtfile, tfile.c_str(), client->get_nodeid());

        if (run_me()) {
          dout(-2) << "trace " << tfile << " prefix=" << prefix << " count=" << iarg1 << " data=" << playdata << dendl;
          
          Trace t(realtfile);
          
	  if (iarg1 == 0) iarg1 = 1; // play trace at least once!

          for (int i=0; i<iarg1; i++) {
            utime_t start = g_clock.now();
            
            if (time_to_stop()) break;
            play_trace(t, prefix, !playdata);
            if (time_to_stop()) break;
            if (iarg1 > 1) clean_dir(prefix);  // clean only if repeat
            
            utime_t lat = g_clock.now();
            lat -= start;
            
            dout(0) << " trace " << tfile << " loop " << (i+1) << "/" << iarg1 << " done in " << (double)lat << " seconds" << dendl;
            if (client_logger 
                && i > 0
                && i < iarg1-1
                ) {
              client_logger->finc("trsum", (double)lat);
              client_logger->inc("trnum");
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
            int fd = client->open("test", rand()%2 ? (O_WRONLY|O_CREAT):O_RDONLY);
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
	  client->mknod("test", 0777);
	  struct stat st;
	  for (int i=0; i<count; i++) {
	    client->lstat("test", &st);
	    client->chmod("test", 0777);
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
	  client->truncate(file.c_str(), iarg1);
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
      
    default:
      assert(0);
    }
  }
  dout(1) << "syn done, unmounting " << dendl;

  if (client->unmount() == 0)
    client->shutdown();
  return 0;
}


int SyntheticClient::start_thread()
{
  assert(!thread_id);

  pthread_create(&thread_id, NULL, synthetic_client_thread_entry, this);
  assert(thread_id);
  return 0;
}

int SyntheticClient::join_thread()
{
  assert(thread_id);
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
  op_dist.add( CEPH_MDS_OP_STAT, 610 );
  op_dist.add( CEPH_MDS_OP_UTIME, 0 );
  op_dist.add( CEPH_MDS_OP_CHMOD, 1 );
  op_dist.add( CEPH_MDS_OP_CHOWN, 1 );

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
  op_dist.add( CEPH_MDS_OP_TRUNCATE, 0 );
  op_dist.add( CEPH_MDS_OP_FSYNC, 0 );
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
  t.start();

  char buf[1024];
  char buf2[1024];

  utime_t start = g_clock.now();

  hash_map<int64_t, int64_t> open_files;
  hash_map<int64_t, DIR*>    open_dirs;

  hash_map<int64_t, Fh*> ll_files;
  hash_map<int64_t, void*> ll_dirs;
  hash_map<uint64_t, int64_t> ll_inos;

  ll_inos[1] = 1; // root inode is known.

  // prefix?
  const char *p = prefix.c_str();
  if (prefix.length()) {
    client->mkdir(prefix.c_str(), 0755);
    struct stat attr;
    if (client->ll_lookup(1, prefix.c_str(), &attr) == 0) {
      ll_inos[1] = attr.st_ino;
      dout(5) << "'root' ino is " << inodeno_t(attr.st_ino) << dendl;
    } else {
      dout(0) << "warning: play_trace coudln't lookup up my per-client directory" << dendl;
    }
  }


  utime_t last_status = start;
  
  int n = 0;

  // for object traces
  Mutex &lock = client->client_lock;
  Cond cond;
  bool ack;
  bool safe;
  C_Gather *safeg = new C_Gather(new C_SafeCond(&lock, &cond, &safe));
  Context *safegref = safeg->new_sub();  // take a ref

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
    if (strcmp(op, "link") == 0) {
      const char *a = t.get_string(buf, p);
      const char *b = t.get_string(buf2, p);
      client->link(a,b);      
    } else if (strcmp(op, "unlink") == 0) {
      const char *a = t.get_string(buf, p);
      client->unlink(a);
    } else if (strcmp(op, "rename") == 0) {
      const char *a = t.get_string(buf, p);
      const char *b = t.get_string(buf2, p);
      client->rename(a,b);      
    } else if (strcmp(op, "mkdir") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      client->mkdir(a, b);
    } else if (strcmp(op, "rmdir") == 0) {
      const char *a = t.get_string(buf, p);
      client->rmdir(a);
    } else if (strcmp(op, "symlink") == 0) {
      const char *a = t.get_string(buf, p);
      const char *b = t.get_string(buf2, p);
      client->symlink(a,b);      
    } else if (strcmp(op, "readlink") == 0) {
      const char *a = t.get_string(buf, p);
      char buf[100];
      client->readlink(a, buf, 100);
    } else if (strcmp(op, "lstat") == 0) {
      struct stat st;
      const char *a = t.get_string(buf, p);
      if (strcmp(a, p) != 0 &&
	  strcmp(a, "/") != 0 &&
	  strcmp(a, "/lib") != 0 && // or /lib.. that would be a lookup. hack.
	  a[0] != 0)  // stop stating the root directory already
	client->lstat(a, &st);
    } else if (strcmp(op, "chmod") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      client->chmod(a, b);
    } else if (strcmp(op, "chown") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      int64_t c = t.get_int();
      client->chown(a, b, c);
    } else if (strcmp(op, "utime") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      int64_t c = t.get_int();
      struct utimbuf u;
      u.actime = b;
      u.modtime = c;
      client->utime(a, &u);
    } else if (strcmp(op, "mknod") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      int64_t c = t.get_int();
      client->mknod(a, b, c);
    } else if (strcmp(op, "oldmknod") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      client->mknod(a, b, 0);
    } else if (strcmp(op, "getdir") == 0) {
      const char *a = t.get_string(buf, p);
      list<string> contents;
      client->getdir(a, contents);
    } else if (strcmp(op, "getdir") == 0) {
      const char *a = t.get_string(buf, p);
      list<string> contents;
      client->getdir(a, contents);
    } else if (strcmp(op, "opendir") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int();
      DIR *dirp;
      client->opendir(a, &dirp);
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
      int64_t fd = client->open(a, b, c);
      if (fd > 0) open_files[d] = fd;
    } else if (strcmp(op, "oldopen") == 0) {
      const char *a = t.get_string(buf, p);
      int64_t b = t.get_int(); 
      int64_t d = t.get_int();
      int64_t fd = client->open(a, b, 0755);
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
      client->truncate(a, l);
    } else if (strcmp(op, "ftruncate") == 0) {
      int64_t f = t.get_int();
      int fd = open_files[f];
      int64_t l = t.get_int();
      client->ftruncate(fd, l);
    } else if (strcmp(op, "fsync") == 0) {
      int64_t f = t.get_int();
      int64_t b = t.get_int();
      int fd = open_files[f];
      client->fsync(fd, b);
    } else if (strcmp(op, "chdir") == 0) {
      const char *a = t.get_string(buf, p);
      client->chdir(a);
    } else if (strcmp(op, "statfs") == 0) {
      struct statvfs stbuf;
      client->statfs("/", &stbuf);
    }

    // low level ops ---------------------
    else if (strcmp(op, "ll_lookup") == 0) {
      int64_t i = t.get_int();
      const char *name = t.get_string(buf, p);
      int64_t r = t.get_int();
      struct stat attr;
      if (ll_inos.count(i) &&
	  client->ll_lookup(ll_inos[i], name, &attr) == 0)
	ll_inos[r] = attr.st_ino;
    } else if (strcmp(op, "ll_forget") == 0) {
      int64_t i = t.get_int();
      int64_t n = t.get_int();
      if (ll_inos.count(i) && 
	  client->ll_forget(ll_inos[i], n))
	ll_inos.erase(i);
    } else if (strcmp(op, "ll_getattr") == 0) {
      int64_t i = t.get_int();
      struct stat attr;
      if (ll_inos.count(i))
	client->ll_getattr(ll_inos[i], &attr);
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
      if (ll_inos.count(i))
	client->ll_setattr(ll_inos[i], &attr, mask);
    } else if (strcmp(op, "ll_readlink") == 0) {
      int64_t i = t.get_int();
      const char *value;
      if (ll_inos.count(i))
	client->ll_readlink(ll_inos[i], &value);
    } else if (strcmp(op, "ll_mknod") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int m = t.get_int();
      int r = t.get_int();
      int64_t ri = t.get_int();
      struct stat attr;
      if (ll_inos.count(i) &&
	  client->ll_mknod(ll_inos[i], n, m, r, &attr) == 0)
	ll_inos[ri] = attr.st_ino;
    } else if (strcmp(op, "ll_mkdir") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int m = t.get_int();
      int64_t ri = t.get_int();
      struct stat attr;
      if (ll_inos.count(i) &&
	  client->ll_mkdir(ll_inos[i], n, m, &attr) == 0)
	ll_inos[ri] = attr.st_ino;
    } else if (strcmp(op, "ll_symlink") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      const char *v = t.get_string(buf2, p);
      int64_t ri = t.get_int();
      struct stat attr;
      if (ll_inos.count(i) &&
	  client->ll_symlink(ll_inos[i], n, v, &attr) == 0)
	ll_inos[ri] = attr.st_ino;
    } else if (strcmp(op, "ll_unlink") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      if (ll_inos.count(i))
	client->ll_unlink(ll_inos[i], n);
    } else if (strcmp(op, "ll_rmdir") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      if (ll_inos.count(i))
	client->ll_rmdir(ll_inos[i], n);
    } else if (strcmp(op, "ll_rename") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int64_t ni = t.get_int();
      const char *nn = t.get_string(buf2, p);
      if (ll_inos.count(i) &&
	  ll_inos.count(ni))
	client->ll_rename(ll_inos[i], n, ll_inos[ni], nn);
    } else if (strcmp(op, "ll_link") == 0) {
      int64_t i = t.get_int();
      int64_t ni = t.get_int();
      const char *nn = t.get_string(buf, p);
      struct stat attr;
      if (ll_inos.count(i) &&
	  ll_inos.count(ni))
      client->ll_link(ll_inos[i], ll_inos[ni], nn, &attr);
    } else if (strcmp(op, "ll_opendir") == 0) {
      int64_t i = t.get_int();
      int64_t r = t.get_int();
      void *dirp;
      if (ll_inos.count(i) &&
	  client->ll_opendir(ll_inos[i], &dirp) == 0)
	ll_dirs[r] = dirp;
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
      if (ll_inos.count(i) &&
	  client->ll_open(ll_inos[i], f, &fhp) == 0)
	ll_files[r] = fhp;
    } else if (strcmp(op, "ll_create") == 0) {
      int64_t i = t.get_int();
      const char *n = t.get_string(buf, p);
      int64_t m = t.get_int();
      int64_t f = t.get_int();
      int64_t r = t.get_int();
      int64_t ri = t.get_int();
      Fh *fhp;
      struct stat attr;
      if (ll_inos.count(i) &&
	  client->ll_create(ll_inos[i], n, m, f, &attr, &fhp) == 0) {
	ll_inos[ri] = attr.st_ino;
	ll_files[r] = fhp;
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
	{} //client->ll_statfs(ll_inos[i]);
    } 


    // object-level traces

    else if (strcmp(op, "o_stat") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      object_t oid(oh, ol);
      lock.Lock();
      ceph_object_layout layout = client->osdmap->make_object_layout(oid, pg_t::TYPE_REP, 2);
      off_t size;
      client->objecter->stat(oid, &size, layout, new C_SafeCond(&lock, &cond, &ack));
      while (!ack) cond.Wait(lock);
      lock.Unlock();
    }
    else if (strcmp(op, "o_read") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      int64_t off = t.get_int();
      int64_t len = t.get_int();
      object_t oid(oh, ol);
      lock.Lock();
      ceph_object_layout layout = client->osdmap->make_object_layout(oid, pg_t::TYPE_REP, 2);
      bufferlist bl;
      client->objecter->read(oid, off, len, layout, &bl, new C_SafeCond(&lock, &cond, &ack));
      while (!ack) cond.Wait(lock);
      lock.Unlock();
    }
    else if (strcmp(op, "o_write") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      int64_t off = t.get_int();
      int64_t len = t.get_int();
      object_t oid(oh, ol);
      lock.Lock();
      ceph_object_layout layout = client->osdmap->make_object_layout(oid, pg_t::TYPE_REP, 2);
      bufferptr bp(len);
      bufferlist bl;
      bl.push_back(bp);
      client->objecter->write(oid, off, len, layout, bl, 
			      new C_SafeCond(&lock, &cond, &ack),
			      safeg->new_sub());
      while (!ack) cond.Wait(lock);
      lock.Unlock();
    }
    else if (strcmp(op, "o_zero") == 0) {
      int64_t oh = t.get_int();
      int64_t ol = t.get_int();
      int64_t off = t.get_int();
      int64_t len = t.get_int();
      object_t oid(oh, ol);
      lock.Lock();
      ceph_object_layout layout = client->osdmap->make_object_layout(oid, pg_t::TYPE_REP, 2);
      client->objecter->zero(oid, off, len, layout, 
			     new C_SafeCond(&lock, &cond, &ack),
			     safeg->new_sub());
      while (!ack) cond.Wait(lock);
      lock.Unlock();
    }


    else {
      dout(0) << (t.get_line()-1) << ": *** trace hit unrecognized symbol '" << op << "' " << dendl;
      assert(0);
    }
  }

  dout(10) << "trace finished on line " << t.get_line() << dendl;

  // wait for safe after an object trace
  safegref->finish(0);
  delete safegref;
  lock.Lock();
  while (!safe) {
    dout(10) << "waiting for safe" << dendl;
    cond.Wait(lock);
  }
  lock.Unlock();

  // close open files
  for (hash_map<int64_t, int64_t>::iterator fi = open_files.begin();
       fi != open_files.end();
       fi++) {
    dout(1) << "leftover close " << fi->second << dendl;
    if (fi->second > 0) client->close(fi->second);
  }
  for (hash_map<int64_t, DIR*>::iterator fi = open_dirs.begin();
       fi != open_dirs.end();
       fi++) {
    dout(1) << "leftover closedir " << fi->second << dendl;
    if (fi->second != 0) client->closedir(fi->second);
  }
  for (hash_map<int64_t,Fh*>::iterator fi = ll_files.begin();
       fi != ll_files.end();
       fi++) {
    dout(1) << "leftover ll_release " << fi->second << dendl;
    if (fi->second > 0) client->ll_release(fi->second);
  }
  for (hash_map<int64_t,void*>::iterator fi = ll_dirs.begin();
       fi != ll_dirs.end();
       fi++) {
    dout(1) << "leftover ll_releasedir " << fi->second << dendl;
    if (fi->second > 0) client->ll_releasedir(fi->second);
  }
  
  return 0;
}



int SyntheticClient::clean_dir(string& basedir)
{
  // read dir
  list<string> contents;
  int r = client->getdir(basedir.c_str(), contents);
  if (r < 0) {
    dout(1) << "readdir on " << basedir << " returns " << r << dendl;
    return r;
  }

  for (list<string>::iterator it = contents.begin();
       it != contents.end();
       it++) {
    if (*it == ".") continue;
    if (*it == "..") continue;
    string file = basedir + "/" + *it;

    if (time_to_stop()) break;

    struct stat st;
    int r = client->lstat(file.c_str(), &st);
    if (r < 0) {
      dout(1) << "stat error on " << file << " r=" << r << dendl;
      continue;
    }

    if ((st.st_mode & S_IFMT) == S_IFDIR) {
      clean_dir(file);
      client->rmdir(file.c_str());
    } else {
      client->unlink(file.c_str());
    }
  }

  return 0;

}


int SyntheticClient::full_walk(string& basedir) 
{
  if (time_to_stop()) return -1;

  list<string> dirq;
  dirq.push_back(basedir);

  while (!dirq.empty()) {
    string dir = dirq.front();
    dirq.pop_front();

    // read dir
    list<string> contents;
    int r = client->getdir(dir.c_str(), contents);
    if (r < 0) {
      dout(1) << "readdir on " << dir << " returns " << r << dendl;
      continue;
    }
    
    for (list<string>::iterator it = contents.begin();
	 it != contents.end();
	 it++) {
      if (*it == "." ||
	  *it == "..") 
	continue;
      string file = dir + "/" + *it;
      
      struct stat st;
      int r = client->lstat(file.c_str(), &st);
      if (r < 0) {
	dout(1) << "stat error on " << file << " r=" << r << dendl;
	continue;
      }
      
      // print
      char *tm = ctime(&st.st_mtime);
      tm[strlen(tm)-1] = 0;
      printf("%llx %c%c%c%c%c%c%c%c%c%c %2d %5d %5d %8d %12s %s\n",
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
	     (int)st.st_size,
	     tm,
	     file.c_str());

      
      if ((st.st_mode & S_IFMT) == S_IFDIR) {
	dirq.push_back(file);
      }
    }
  }

  return 0;
}

int SyntheticClient::make_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  // make sure base dir exists
  int r = client->mkdir(basedir, 0755);
  if (r != 0) {
    dout(1) << "can't make base dir? " << basedir << dendl;
    //return -1;
  }

  // children
  char d[500];
  dout(3) << "make_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << dendl;
  for (int i=0; i<files; i++) {
    sprintf(d,"%s/file.%d", basedir, i);
    client->mknod(d, 0644);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
    sprintf(d, "%s/dir.%d", basedir, i);
    make_dirs(d, dirs, files, depth-1);
  }
  
  return 0;
}

int SyntheticClient::stat_dirs(const char *basedir, int dirs, int files, int depth)
{
  if (time_to_stop()) return 0;

  // make sure base dir exists
  struct stat st;
  int r = client->lstat(basedir, &st);
  if (r != 0) {
    dout(1) << "can't make base dir? " << basedir << dendl;
    return -1;
  }

  // children
  char d[500];
  dout(3) << "stat_dirs " << basedir << " dirs " << dirs << " files " << files << " depth " << depth << dendl;
  for (int i=0; i<files; i++) {
    sprintf(d,"%s/file.%d", basedir, i);
    client->lstat(d, &st);
  }

  if (depth == 0) return 0;

  for (int i=0; i<dirs; i++) {
    sprintf(d, "%s/dir.%d", basedir, i);
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
  utime_t s = g_clock.now();
  int r = client->getdir(basedir, contents);
  utime_t e = g_clock.now();
  e -= s;
  if (client_logger) client_logger->finc("readdir", e);
  if (r < 0) {
    dout(0) << "read_dirs couldn't readdir " << basedir << ", stopping" << dendl;
    return -1;
  }

  for (int i=0; i<files; i++) {
    sprintf(d,"%s/file.%d", basedir, i);
    utime_t s = g_clock.now();
    if (client->lstat(d, &st) < 0) {
      dout(2) << "read_dirs failed stat on " << d << ", stopping" << dendl;
      return -1;
    }
    utime_t e = g_clock.now();
    e -= s;
    if (client_logger) client_logger->finc("stat", e);
  }

  if (depth > 0) 
    for (int i=0; i<dirs; i++) {
      sprintf(d, "%s/dir.%d", basedir, i);
      if (read_dirs(d, dirs, files, depth-1) < 0) return -1;
    }

  return 0;
}


int SyntheticClient::make_files(int num, int count, int priv, bool more)
{
  int whoami = client->get_nodeid();
  char d[255];

  if (priv) {
    for (int c=0; c<count; c++) {
      sprintf(d,"dir.%d.run%d", whoami, c);
      client->mkdir(d, 0755);
    }
  } else {
    // shared
    if (true || whoami == 0) {
      for (int c=0; c<count; c++) {
        sprintf(d,"dir.%d.run%d", 0, c);
        client->mkdir(d, 0755);
      }
    } else {
      sleep(2);
    }
  }
  
  // files
  struct stat st;
  utime_t start = g_clock.now();
  for (int c=0; c<count; c++) {
    for (int n=0; n<num; n++) {
      sprintf(d,"dir.%d.run%d/file.client%d.%d", priv ? whoami:0, c, whoami, n);

      client->mknod(d, 0644);

      if (more) {
        client->lstat(d, &st);
        int fd = client->open(d, O_RDONLY);
        client->unlink(d);
        client->close(fd);
      }

      if (time_to_stop()) return 0;
    }
  }
  utime_t end = g_clock.now();
  end -= start;
  dout(0) << "makefiles time is " << end << " or " << ((double)end / (double)num) <<" per file" << dendl;
  
  return 0;
}

int SyntheticClient::link_test()
{
  char d[255];
  char e[255];

 // create files
  int num = 200;

  client->mkdir("orig", 0755);
  client->mkdir("copy", 0755);

  utime_t start = g_clock.now();
  for (int i=0; i<num; i++) {
    sprintf(d,"orig/file.%d", i);
    client->mknod(d, 0755);
  }
  utime_t end = g_clock.now();
  end -= start;

  dout(0) << "orig " << end << dendl;

  // link
  start = g_clock.now();
  for (int i=0; i<num; i++) {
    sprintf(d,"orig/file.%d", i);
    sprintf(e,"copy/file.%d", i);
    client->link(d, e);
  }
  end = g_clock.now();
  end -= start;
  dout(0) << "copy " << end << dendl;

  return 0;
}


int SyntheticClient::create_shared(int num)
{
  // files
  char d[255];
  for (int n=0; n<num; n++) {
    sprintf(d,"file.%d", n);
    client->mknod(d, 0644);
  }
  
  return 0;
}

int SyntheticClient::open_shared(int num, int count)
{
  // files
  char d[255];
  for (int c=0; c<count; c++) {
    // open
    list<int> fds;
    for (int n=0; n<num; n++) {
      sprintf(d,"file.%d", n);
      int fd = client->open(d,O_RDONLY);
      if (fd > 0) fds.push_back(fd);
    }

    if (false && client->get_nodeid() == 0)
      for (int n=0; n<num; n++) {
	sprintf(d,"file.%d", n);
	client->unlink(d);
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

  // collect a bunch of files starting on OSD 0
  int left = n;
  int tried = 0;
  while (left < 0) {


    // pull open a file
    dout(-1) << "in OSD overload" << dendl;
    string filename = get_sarg(tried);
    dout(1) << "OSD Overload workload: trying file " << filename << dendl;
    int fd = client->open(filename.c_str(), O_RDWR|O_CREAT);
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
int SyntheticClient::check_first_primary(int fh) {
  list<ObjectExtent> extents;
  client->enumerate_layout(fh, extents, 1, 0);  
  return client->osdmap->get_pg_primary((extents.begin())->layout.ol_pgid);
}

int SyntheticClient::write_file(string& fn, int size, int wrsize)   // size is in MB, wrsize in bytes
{
  //uint64_t wrsize = 1024*256;
  char *buf = new char[wrsize+100];   // 1 MB
  memset(buf, 7, wrsize);
  uint64_t chunks = (uint64_t)size * (uint64_t)(1024*1024) / (uint64_t)wrsize;

  int fd = client->open(fn.c_str(), O_RDWR|O_CREAT);
  dout(5) << "writing to " << fn << " fd " << fd << dendl;
  if (fd < 0) return fd;

#define CHEAP_HACK 0
#if CHEAP_HACK
  // START temporary hack piece 1 --Esteban
  for (int foo = 0; foo < 10; ++foo) {
  // END temporary hack piece 1 --Esteban
#endif

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
      *p = client->get_nodeid();
      p++;
    }

    client->write(fd, buf, wrsize, i*wrsize);
  }
#if CHEAP_HACK
  // START temporary hack piece 2--Esteban
  }
  sleep(5);
  // END temporary hack piece 2 --Esteban
#endif

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
  if (fd < 0) return fd;

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
      *p = client->get_nodeid();
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

int SyntheticClient::read_file(string& fn, int size, int rdsize, bool ignoreprint)   // size is in MB, wrsize in bytes
{
  char *buf = new char[rdsize]; 
  memset(buf, 1, rdsize);
  uint64_t chunks = (uint64_t)size * (uint64_t)(1024*1024) / (uint64_t)rdsize;

  int fd = client->open(fn.c_str(), O_RDONLY);
  dout(5) << "reading from " << fn << " fd " << fd << dendl;
  if (fd < 0) return fd;

  for (unsigned i=0; i<chunks; i++) {
    if (time_to_stop()) break;
    dout(2) << "reading block " << i << "/" << chunks << dendl;
    int r = client->read(fd, buf, rdsize, i*rdsize);
    if (r < rdsize) {
      dout(1) << "read_file got r = " << r << ", probably end of file" << dendl;
      break;
    }
 
    // verify fingerprint
    int bad = 0;
    uint64_t *p = (uint64_t*)buf;
    uint64_t readoff;
    int64_t readclient;
    while ((char*)p + 32 < buf + rdsize) {
      readoff = *p;
      uint64_t wantoff = (uint64_t)i*(uint64_t)rdsize + (uint64_t)((char*)p - buf);
      p++;
      readclient = *p;
      p++;
      if (readoff != wantoff ||
	  readclient != client->get_nodeid()) {
        if (!bad && !ignoreprint)
          dout(0) << "WARNING: wrong data from OSD, block says fileoffset=" << readoff << " client=" << readclient
		  << ", should be offset " << wantoff << " clietn " << client->get_nodeid()
		  << dendl;
        bad++;
      }
    }
    if (bad && !ignoreprint) 
      dout(0) << " + " << (bad-1) << " other bad 16-byte bits in this block" << dendl;
  }
  
  client->close(fd);
  delete[] buf;

  return 0;
}




class C_Ref : public Context {
  Mutex& lock;
  Cond& cond;
  int *ref;
public:
  C_Ref(Mutex &l, Cond &c, int *r) : lock(l), cond(c), ref(r) {
    lock.Lock();
    (*ref)++;
    lock.Unlock();
  }
  void finish(int) {
    lock.Lock();
    (*ref)--;
    cond.Signal();
    lock.Unlock();
  }
};

int SyntheticClient::create_objects(int nobj, int osize, int inflight)
{
  // divy up
  int numc = g_conf.num_client ? g_conf.num_client : 1;

  int start, inc, end;

  if (1) {
    // strided
    start = client->get_nodeid(); //nobjs % numc;
    inc = numc;
    end = start + nobj;
  } else {
    // segments
    start = nobj * client->get_nodeid() / numc;
    inc = 1;
    end = nobj * (client->get_nodeid()+1) / numc;
  }

  dout(5) << "create_objects " << nobj << " size=" << osize 
	  << " .. doing [" << start << "," << end << ") inc " << inc
	  << dendl;
  
  bufferptr bp(osize);
  bp.zero();
  bufferlist bl;
  bl.push_back(bp);

  Mutex lock;
  Cond cond;
  
  int unack = 0;
  int unsafe = 0;
  
  list<utime_t> starts;

  for (int i=start; i<end; i += inc) {
    if (time_to_stop()) break;

    object_t oid(0x1000, i);
    ceph_object_layout layout = client->osdmap->make_object_layout(oid, pg_t::TYPE_REP, g_OSD_FileLayout.fl_pg_size);
    
    if (i % inflight == 0) {
      dout(6) << "create_objects " << i << "/" << (nobj+1) << dendl;
    }
    dout(10) << "writing " << oid << dendl;
    
    starts.push_back(g_clock.now());
    client->client_lock.Lock();
    client->objecter->write(oid, 0, osize, layout, bl, 
			    new C_Ref(lock, cond, &unack),
			    new C_Ref(lock, cond, &unsafe));
    client->client_lock.Unlock();

    lock.Lock();
    while (unack > inflight) {
      dout(20) << "waiting for " << unack << " unack" << dendl;
      cond.Wait(lock);
    }
    lock.Unlock();
    
    utime_t lat = g_clock.now();
    lat -= starts.front();
    starts.pop_front();
    if (client_logger) 
      client_logger->favg("owrlat", lat);
  }

  lock.Lock();
  while (unack > 0) {
    dout(20) << "waiting for " << unack << " unack" << dendl;
    cond.Wait(lock);
  }
  while (unsafe > 0) {
    dout(10) << "waiting for " << unsafe << " unsafe" << dendl;
    cond.Wait(lock);
  }
  lock.Unlock();

  dout(5) << "create_objects done" << dendl;
  derr(0) << "create_objects done" << dendl;
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

  Mutex lock;
  Cond cond;

  int unack = 0;
  int unsafe = 0;

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
    object_t oid(0x1000, o);

    ceph_object_layout layout = client->osdmap->make_object_layout(oid, pg_t::TYPE_REP, g_OSD_FileLayout.fl_pg_size);
    
    client->client_lock.Lock();
    utime_t start = g_clock.now();
    if (write) {
      dout(10) << "write to " << oid << dendl;
      client->objecter->write(oid, 0, osize, layout, bl, 
			      new C_Ref(lock, cond, &unack),
			      new C_Ref(lock, cond, &unsafe));
    } else {
      dout(10) << "read from " << oid << dendl;
      bufferlist inbl;
      client->objecter->read(oid, 0, osize, layout, &inbl, 
			     new C_Ref(lock, cond, &unack));
    }
    client->client_lock.Unlock();

    lock.Lock();
    while (unack > 0) {
      dout(20) << "waiting for " << unack << " unack" << dendl;
      cond.Wait(lock);
    }
    lock.Unlock();

    utime_t lat = g_clock.now();
    lat -= start;
    if (client_logger) {
      if (write) 
	client_logger->favg("owrlat", lat);
      else 
	client_logger->favg("ordlat", lat);
    }
  }


  lock.Lock();
  while (unsafe > 0) {
    dout(10) << "waiting for " << unsafe << " unsafe" << dendl;
    cond.Wait(lock);
  }
  lock.Unlock();
  return 0;
}





int SyntheticClient::read_random(string& fn, int size, int rdsize)   // size is in MB, wrsize in bytes
{
  __uint64_t chunks = (__uint64_t)size * (__uint64_t)(1024*1024) / (__uint64_t)rdsize;

  int fd = client->open(fn.c_str(), O_RDWR);
  dout(5) << "reading from " << fn << " fd " << fd << dendl;
   
 // dout(0) << "READING FROM  " << fn << " fd " << fd << dendl;

 // dout(0) << "filename " << fn << " size:" << size  << " read size|" << rdsize << "|" <<  "\ chunks: |" << chunks <<"|" <<  dendl;

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

    //dout(0) << "RANDOM NUMBER RETURN |" << x << "|" << dendl;

    if ( x < 0.5) 
    {
        //dout(0) << "DECIDED TO READ " << x << dendl;
        buf = new char[rdsize]; 
        memset(buf, 1, rdsize);
        read=true;
    }
    else
    {
       // dout(0) << "DECIDED TO WRITE " << x << dendl;
        buf = new char[rdsize+100];   // 1 MB
        memset(buf, 7, rdsize);
    }

    //double  y  = drand48() ;

    //dout(0) << "OFFSET is |" << offset << "| chunks |" << chunks<<  dendl;
    
    if ( read)
    {
        offset=(rand())%(chunks+1);
        dout(2) << "reading block " << offset << "/" << chunks << dendl;

        int r = client->read(fd, buf, rdsize,
                        offset*rdsize);
        if (r < rdsize) {
                  dout(1) << "read_file got r = " << r << ", probably end of file" << dendl;
    }
    }
    else
    {
        dout(2) << "writing block " << offset << "/" << chunks << dendl;

    // fill buf with a 16 byte fingerprint
    // 64 bits : file offset
    // 64 bits : client id
    // = 128 bits (16 bytes)

      //if (true )
      //{
      //int count = rand()%10;

      //for ( int j=0;j<count; j++ )
      //{

      offset=(rand())%(chunks+1);
    __uint64_t *p = (__uint64_t*)buf;
    while ((char*)p < buf + rdsize) {
      *p = offset*rdsize + (char*)p - buf;      
      p++;
      *p = client->get_nodeid();
      p++;
    }

      client->write(fd, buf, rdsize,
                        offset*rdsize);
      //}
      //}
    }

    // verify fingerprint
    if ( read )
    {
    int bad = 0;
    __int64_t *p = (__int64_t*)buf;
    __int64_t readoff, readclient;
    while ((char*)p + 32 < buf + rdsize) {
      readoff = *p;
      __int64_t wantoff = offset*rdsize + (__int64_t)((char*)p - buf);
      p++;
      readclient = *p;
      p++;
      if (readoff != wantoff ||
	  readclient != client->get_nodeid()) {
        if (!bad)
          dout(0) << "WARNING: wrong data from OSD, block says fileoffset=" << readoff << " client=" << readclient
		  << ", should be offset " << wantoff << " clietn " << client->get_nodeid()
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


//#include<stdio.h>
//#include<stdlib.h>

int normdist(int min, int max, int stdev) /* specifies input values */;
//main()
//{
 // for ( int i=0; i < 10; i++ )
 //  normdist ( 0 , 10, 1 );
   
//}


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
  __uint64_t chunks = (__uint64_t)size * (__uint64_t)(1024*1024) / (__uint64_t)rdsize;
  
  int fd = client->open(fn.c_str(), O_RDWR);
  dout(5) << "reading from " << fn << " fd " << fd << dendl;
  
  // dout(0) << "READING FROM  " << fn << " fd " << fd << dendl;
  
  // dout(0) << "filename " << fn << " size:" << size  << " read size|" << rdsize << "|" <<  "\ chunks: |" << chunks <<"|" <<  dendl;
  
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
    
    //dout(0) << "RANDOM NUMBER RETURN |" << x << "|" << dendl;
    
    if ( x < 0.5) 
      {
        //dout(0) << "DECIDED TO READ " << x << dendl;
        buf = new char[rdsize]; 
        memset(buf, 1, rdsize);
        read=true;
      }
    else
      {
	// dout(0) << "DECIDED TO WRITE " << x << dendl;
        buf = new char[rdsize+100];   // 1 MB
        memset(buf, 7, rdsize);
      }
    
    //double  y  = drand48() ;
    
    //dout(0) << "OFFSET is |" << offset << "| chunks |" << chunks<<  dendl;
    
    if ( read)
      {
        //offset=(rand())%(chunks+1);
	
	/*    if ( chunks > 10000 ) 
	      offset= normdist( 0 , chunks/1000 , 5  )*1000;
	      else if ( chunks > 1000 )
	      offset= normdist( 0 , chunks/100 , 5  )*100;
	      else if ( chunks > 100 )
	      offset= normdist( 0 , chunks/20 , 5  )*20;*/
	
	
        dout(2) << "reading block " << offset << "/" << chunks << dendl;
	
        int r = client->read(fd, buf, rdsize,
			     offset*rdsize);
        if (r < rdsize) {
	  dout(1) << "read_file got r = " << r << ", probably end of file" << dendl;
	}
      }
    else
      {
        dout(2) << "writing block " << offset << "/" << chunks << dendl;
	
	// fill buf with a 16 byte fingerprint
	// 64 bits : file offset
	// 64 bits : client id
	// = 128 bits (16 bytes)
	
	//if (true )
	//{
	int count = rand()%10;
	
	for ( int j=0;j<count; j++ )
	  {
	    
	    offset=(rand())%(chunks+1);
	    __uint64_t *p = (__uint64_t*)buf;
	    while ((char*)p < buf + rdsize) {
	      *p = offset*rdsize + (char*)p - buf;      
	      p++;
	      *p = client->get_nodeid();
	      p++;
	    }
	    
	    client->write(fd, buf, rdsize,
			  offset*rdsize);
	  }
	//}
      }
    
    // verify fingerprint
    if ( read )
      {
	int bad = 0;
	__int64_t *p = (__int64_t*)buf;
	__int64_t readoff, readclient;
	while ((char*)p + 32 < buf + rdsize) {
	  readoff = *p;
	  __int64_t wantoff = offset*rdsize + (__int64_t)((char*)p - buf);
	  p++;
	  readclient = *p;
	  p++;
	  if (readoff != wantoff ||
	      readclient != client->get_nodeid()) {
	    if (!bad)
	      dout(0) << "WARNING: wrong data from OSD, block says fileoffset=" << readoff << " client=" << readclient
		      << ", should be offset " << wantoff << " clietn " << client->get_nodeid()
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
    if (.9*roll_die(::pow((double).9,(double)cwd.depth())) && subdirs.size()) {
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
        r = client->unlink( get_random_sub() );   // will fail on dirs
    }
     
    if (op == CEPH_MDS_OP_RENAME) {
      if (contents.empty())
        op = CEPH_MDS_OP_READDIR;
      else {
        r = client->rename( get_random_sub(), make_sub("ren") );
      }
    }
    
    if (op == CEPH_MDS_OP_MKDIR) {
      r = client->mkdir( make_sub("mkdir"), 0755);
    }
    
    if (op == CEPH_MDS_OP_RMDIR) {
      if (!subdirs.empty())
        r = client->rmdir( get_random_subdir() );
      else
        r = client->rmdir( cwd.c_str() );     // will pbly fail
    }
    
    if (op == CEPH_MDS_OP_SYMLINK) {
    }
    
    if (op == CEPH_MDS_OP_CHMOD) {
      if (contents.empty())
        op = CEPH_MDS_OP_READDIR;
      else
        r = client->chmod( get_random_sub(), rand() & 0755 );
    }
    
    if (op == CEPH_MDS_OP_CHOWN) {
      if (contents.empty())         r = client->chown( cwd.c_str(), rand(), rand() );
      else
        r = client->chown( get_random_sub(), rand(), rand() );
    }
     
    if (op == CEPH_MDS_OP_LINK) {
    }
     
    if (op == CEPH_MDS_OP_UTIME) {
      struct utimbuf b;
      memset(&b, 1, sizeof(b));
      if (contents.empty()) 
        r = client->utime( cwd.c_str(), &b );
      else
        r = client->utime( get_random_sub(), &b );
    }
    
    if (op == CEPH_MDS_OP_MKNOD) {
      r = client->mknod( make_sub("mknod"), 0644);
    }
     
    if (op == CEPH_MDS_OP_OPEN) {
      if (contents.empty())
        op = CEPH_MDS_OP_READDIR;
      else {
        r = client->open( get_random_sub(), O_RDONLY );
        if (r > 0) {
          assert(open_files.count(r) == 0);
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
    
    if (op == CEPH_MDS_OP_STAT) {
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
        r = client->lstat(get_random_sub(), &st);
    }

    if (op == CEPH_MDS_OP_READDIR) {
      clear_dir();
      
      list<string> c;
      r = client->getdir( cwd.c_str(), c );
      
      for (list<string>::iterator it = c.begin();
           it != c.end();
           it++) {
        //dout(DBL) << " got " << *it << dendl;
	assert(0);
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
  vector<string> dirs;
  
  dirs.push_back(basedir);
  dirs.push_back(basedir);
  
  client->mkdir(basedir, 0755);

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
    client->mkdir(dir.c_str(), 0755);
  }
    
  
}



void SyntheticClient::foo()
{
  if (1) {
    // open some files
    srand(0);
    for (int i=0; i<20; i++) {
      int s = 5;
      int a = rand() % s;
      int b = rand() % s;
      int c = rand() % s;
      char src[80];
      sprintf(src, "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      //int fd = 
      client->open(src, O_RDONLY);
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
      sprintf(src, "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      sprintf(dst, "syn.0.0/dir.%d/dir.%d/file.%d", d, e, f);
      client->rename(src, dst);
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
      sprintf(src, "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      sprintf(dst, "syn.0.0/dir.%d/dir.%d/newlink.%d", d, e, f);
      client->link(src, dst);
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
      sprintf(src, "syn.0.0/dir.%d/dir.%d/file.%d", a, b, c);
      sprintf(dst, "syn.0.0/dir.%d/dir.%d/newlink.%d", d, e, f);
      client->unlink(dst);
    }

    
    return;
  }

  // link fun
  client->mknod("one", 0755);
  client->mknod("two", 0755);
  client->link("one", "three");
  client->mkdir("dir", 0755);
  client->link("two", "/dir/twolink");
  client->link("dir/twolink", "four");
  
  // unlink fun
  client->mknod("a", 0644);
  client->unlink("a");
  client->mknod("b", 0644);
  client->link("b", "c");
  client->unlink("c");
  client->mkdir("d", 0755);
  client->unlink("d");
  client->rmdir("d");

  // rename fun
  client->mknod("p1", 0644);
  client->mknod("p2", 0644);
  client->rename("p1","p2");
  client->mknod("p3", 0644);
  client->rename("p3","p4");

  // check dest dir ambiguity thing
  client->mkdir("dir1", 0755);
  client->mkdir("dir2", 0755);
  client->rename("p2","dir1/p2");
  client->rename("dir1/p2","dir2/p2");
  client->rename("dir2/p2","/p2");
  
  // check primary+remote link merging
  client->link("p2","p2.l");
  client->link("p4","p4.l");
  client->rename("p2.l","p2");
  client->rename("p4","p4.l");

  // check anchor updates
  client->mknod("dir1/a", 0644);
  client->link("dir1/a", "da1");
  client->link("dir1/a", "da2");
  client->link("da2","da3");
  client->rename("dir1/a","dir2/a");
  client->rename("dir2/a","da2");
  client->rename("da1","da2");
  client->rename("da2","da3");

  // check directory renames
  client->mkdir("dir3", 0755);
  client->mknod("dir3/asdf", 0644);
  client->mkdir("dir4", 0755);
  client->mkdir("dir5", 0755);
  client->mknod("dir5/asdf", 0644);
  client->rename("dir3","dir4"); // ok
  client->rename("dir4","dir5"); // fail
}

int SyntheticClient::thrash_links(const char *basedir, int dirs, int files, int depth, int n)
{
  dout(1) << "thrash_links " << basedir << " " << dirs << " " << files << " " << depth
	  << " links " << n
	  << dendl;

  if (time_to_stop()) return 0;

  for (int k=0; k<n; k++) {

    if (rand() % 10 == 0) {
      // rename some directories.  whee!
      int dep = (rand() % depth) + 1;
      string src = basedir;
      {
	char t[80];
	for (int d=0; d<dep; d++) {
	  int a = rand() % dirs;
	  sprintf(t, "/dir.%d", a);
	  src += t;
	}
      }
      string dst = basedir;
      {
	char t[80];
	for (int d=0; d<dep; d++) {
	  int a = rand() % dirs;
	  sprintf(t, "/dir.%d", a);
	  dst += t;
	}
      }
      
      if (client->rename(dst.c_str(), "/tmp") == 0) {
	client->rename(src.c_str(), dst.c_str());
	client->rename("/tmp", src.c_str());
      }
      continue;
    } 

    // pick a dest dir
    string src = basedir;
    {
      char t[80];
      for (int d=0; d<depth; d++) {
	int a = rand() % dirs;
	sprintf(t, "/dir.%d", a);
	src += t;
      }
      int a = rand() % files;
      sprintf(t, "/file.%d", a);
      src += t;
    }
    string dst = basedir;
    {
      char t[80];
      for (int d=0; d<depth; d++) {
	int a = rand() % dirs;
	sprintf(t, "/dir.%d", a);
	dst += t;
      }
      int a = rand() % files;
      sprintf(t, "/file.%d", a);
      dst += t;
    }

    int o = rand() % 4;
    switch (o) {
    case 0: 
      client->mknod(src.c_str(), 0755); 
      client->rename(src.c_str(), dst.c_str()); 
      break;
    case 1: 
      client->mknod(src.c_str(), 0755); 
      client->unlink(dst.c_str());
      client->link(src.c_str(), dst.c_str()); 
      break;
    case 2: client->unlink(src.c_str()); break;
    case 3: client->unlink(dst.c_str()); break;
      //case 4: client->mknod(src.c_str(), 0755); break;
      //case 5: client->mknod(dst.c_str(), 0755); break;
    }
  }
  return 0;
 
  // now link shit up
  for (int i=0; i<n; i++) {
    if (time_to_stop()) return 0;

    char f[20];

    // pick a file
    string file = basedir;

    if (depth) {
      int d = rand() % (depth+1);
      for (int k=0; k<d; k++) {
	sprintf(f, "/dir.%d", rand() % dirs);
	file += f;
      }
    }
    sprintf(f, "/file.%d", rand() % files);
    file += f;

    // pick a dir for our link
    string ln = basedir;
    if (depth) {
      int d = rand() % (depth+1);
      for (int k=0; k<d; k++) {
	sprintf(f, "/dir.%d", rand() % dirs);
	ln += f;
      }
    }
    sprintf(f, "/ln.%d", i);
    ln += f;

    client->link(file.c_str(), ln.c_str());  
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

  if (base[0] != '-') 
    client->mkdir(base, 0755);

  ifstream f(find);
  assert(f.is_open());
  
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

    // ignore "."
    if (filename == ".") continue;

    // remove leading ./
    assert(filename[0] == '.' && filename[1] == '/');
    filename = filename.substr(2);

    // new leading dir?
    int sp = filename.find("/");
    if (sp < 0) dirnum++;

    //dout(0) << "leading dir " << filename << " " << dirnum << dendl;
    if (dirnum % g_conf.num_client != client->get_nodeid()) {
      dout(20) << "skipping leading dir " << dirnum << " " << filename << dendl;
      continue;
    }

    // parse the mode
    assert(modestring.length() == 10);
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
      assert(pos > 0);
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
      client->symlink(target.c_str(), link.c_str());
    } else {
      string f;
      if (base[0] != '-') {
	f = base;
	f += "/";
      }
      f += filename;
      if (S_ISDIR(mode)) {
	client->mkdir(f.c_str(), mode);
      } else {
	int fd = client->open(f.c_str(), O_WRONLY|O_CREAT, mode & 0777);
	assert(fd > 0);	
	client->write(fd, "", 0, size);
	client->close(fd);

	//client->chmod(f.c_str(), mode & 0777);
	client->chown(f.c_str(), uid, gid);

	struct utimbuf ut;
	ut.modtime = mtime;
	ut.actime = mtime;
	client->utime(f.c_str(), &ut);
      }
    }
  }
  

}

