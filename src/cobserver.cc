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

#include "mon/MonMap.h"
#include "mon/MonClient.h"
#include "mon/mon_types.h"
#include "mon/ClientMonitor.h"
#include "mon/PGMap.h"
#include "mon/ClientMap.h"
#include "osd/OSDMap.h"
#include "mds/MDSMap.h"
#include "msg/SimpleMessenger.h"
#include "messages/MMonCommand.h"
#include "messages/MMonObserve.h"
#include "messages/MMonObserveNotify.h"
#include "include/LogEntry.h"

#include "common/Timer.h"

#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sstream>

Mutex lock("cobserver.cc lock");
Messenger *messenger = 0;

const char *outfile = 0;

static PGMap pgmap;
static MonMap monmap;
static MDSMap mdsmap;
static OSDMap osdmap;
static ClientMap clientmap;
static bufferlist log_bl;

version_t map_ver[PAXOS_NUM];

enum { OSD, MON, MDS, CLIENT, LAST };
int which = 0;
int same = 0;
const char *prefix[4] = { "mds", "osd", "pg", "client" };
map<string,string> status;

int lines = 0;

void handle_notify(MMonObserveNotify *notify)
{
    generic_dout(0) << notify->get_source() << " -> " << get_paxos_name(notify->machine_id) << " v" << notify->ver
		    << (notify->is_incremental ? " (i)" : "") << dendl;
    if (notify->is_incremental) {
        if (map_ver[notify->machine_id] >= notify->ver)
		return;
	switch (notify->machine_id) {
	case PAXOS_PGMAP:
		{
                  PGMap::Incremental inc;
                  bufferlist::iterator p = notify->bl.begin();
		  inc.decode(p);
		  pgmap.apply_incremental(inc);
		  break;
		}
	case PAXOS_MDSMAP:
		mdsmap.decode(notify->bl);
		break;
	case PAXOS_OSDMAP:
		{
                  OSDMap::Incremental inc(notify->bl);
		  osdmap.apply_incremental(inc);
		}
		break;
	case PAXOS_CLIENTMAP:
		{
                  ClientMap::Incremental inc;
		  bufferlist::iterator p = notify->bl.begin();
		  inc.decode(p);
		  clientmap.apply_incremental(inc);
		}
		break;
	case PAXOS_LOG:
		{
		  LogEntry le;
		  bufferlist::iterator p = notify->bl.begin();
		  le.decode(p);
/*
		  std::stringstream ss;
		  ss << le;
		  string s;
		  getline(ss, s);
		  log_bl.append(s);
		  log_bl.append("\n"); */
		  break;
		}
	}
    } else {
	switch (notify->machine_id) {
	case PAXOS_PGMAP:
		{
		  bufferlist::iterator p = notify->bl.begin();
	          pgmap.decode(p);
		}
		break;
	case PAXOS_MDSMAP:
		mdsmap.decode(notify->bl);
		break;
	case PAXOS_OSDMAP:
	        osdmap.decode(notify->bl);
		break;
	case PAXOS_CLIENTMAP:
		{
		  bufferlist::iterator p = notify->bl.begin();
		  clientmap.decode(p);
		  break;
		}
	case PAXOS_LOG:
		{
		  LogEntry le;
		  bufferlist::iterator p = notify->bl.begin();
		  le.decode(p);
		  break;
		}
	}
    }
    map_ver[notify->machine_id] = notify->ver;
}

class Admin : public Dispatcher {
  bool dispatch_impl(Message *m) {
    switch (m->get_type()) {
    case MSG_MON_OBSERVE_NOTIFY:
      handle_notify((MMonObserveNotify *)m);
      break;
    default:
      return false;
    }
    return true;
  }
} dispatcher;


void usage()
{
  cerr << "usage: covserver [options] monhost] command" << std::endl;
  cerr << "Options:" << std::endl;
  cerr << "   -m monhost        -- specify monitor hostname or ip" << std::endl;
  exit(1);
}

static void send_requests();

class C_ObserverRefresh : public Context {
 public:
    C_ObserverRefresh() {}
    void finish(int r) {
       send_requests();
    }
};

static void send_requests()
{
   bufferlist indata;
   for (int i=0; i<PAXOS_NUM; i++) {
        MMonObserve *m = new MMonObserve(monmap.fsid, i, map_ver[i]);
        m->set_data(indata);
        int mon = monmap.pick_mon();
        generic_dout(0) << "mon" << mon << " <- observe " << get_paxos_name(i) << dendl;
        messenger->send_message(m, monmap.get_inst(mon));
  }

  C_ObserverRefresh *observe_refresh_event = new C_ObserverRefresh();
  timer.add_event_after(g_conf.paxos_observer_timeout/2, observe_refresh_event);

}

int main(int argc, const char **argv, const char *envp[]) {

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  parse_config_options(args);

  vec_to_argv(args, argc, argv);

  srand(getpid());

  vector<const char*> nargs;

  // get monmap
  MonClient mc;
  if (mc.get_monmap(&monmap) < 0)
    return -1;
   memset(map_ver, 0, sizeof(map_ver));

  // start up network
  rank.bind();
  g_conf.daemonize = false; // not us!
  messenger = rank.register_entity(entity_name_t::ADMIN());
  messenger->set_dispatcher(&dispatcher);

  rank.start();
  rank.set_policy(entity_name_t::TYPE_MON, Rank::Policy::lossy_fail_after(1.0));

  lock.Lock();
  send_requests();
  lock.Unlock();

  // wait for messenger to finish
  rank.wait();
  messenger->destroy();
  return 0;
}

