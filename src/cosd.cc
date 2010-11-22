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

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"

#include "osd/OSD.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"
#include "common/common_init.h"

#include "include/color.h"

void usage() 
{
  cerr << "usage: cosd -i osdid [--osd-data=path] [--osd-journal=path] [--mkfs] [--mkjournal]" << std::endl;
  cerr << "   --debug_osd N   set debug level (e.g. 10)" << std::endl;
  generic_server_usage();
}


int main(int argc, const char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  bool should_authenticate = true;
  vector<const char *>::iterator args_iter;

  for (args_iter = args.begin(); args_iter != args.end(); ++args_iter) {
    if (strcmp(*args_iter, "--mkfs") == 0) {
      should_authenticate = false;
      break;
    } 
  }

  common_set_defaults(true);
#ifdef HAVE_LIBTCMALLOC
  g_conf.profiler_start = HeapProfilerStart;
  g_conf.profiler_running = IsHeapProfilerRunning;
  g_conf.profiler_stop = HeapProfilerStop;
  g_conf.profiler_dump = HeapProfilerDump;
  g_conf.tcmalloc_have = true;
#endif //HAVE_LIBTCMALLOC
  common_init(args, "osd", should_authenticate);

  if (g_conf.clock_tare) g_clock.tare();

  // osd specific args
  bool mkfs = false;
  bool mkjournal = false;
  bool flushjournal = false;
  char *dump_pg_log = 0;
  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("mkfs", '\0')) {
      mkfs = true;
    } else if (CONF_ARG_EQ("mkjournal", '\0')) {
      mkjournal = true;
    } else if (CONF_ARG_EQ("flush-journal", '\0')) {
      flushjournal = true;
    } else if (CONF_ARG_EQ("dump-pg-log", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&dump_pg_log, OPT_STR);
    } else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      ARGS_USAGE();
    }
  }

  if (dump_pg_log) {
    bufferlist bl;
    int r = bl.read_file(dump_pg_log);
    if (r >= 0) {
      PG::Log::Entry e;
      bufferlist::iterator p = bl.begin();
      while (!p.end()) {
	uint64_t pos = p.get_off();
	try {
	  ::decode(e, p);
	}
	catch (const buffer::error &e) {
	  cerr << "failed to decode LogEntry at offset " << pos << std::endl;
	  return 1;
	}
	cout << pos << ":\t" << e << std::endl;
      }
    } else
      cerr << "unable to open " << dump_pg_log << ": " << strerror(r) << std::endl;
    return 0;
  }

  // whoami
  char *end;
  int whoami = strtol(g_conf.id, &end, 10);
  if (*end || end == g_conf.id || whoami < 0) {
    cerr << "must specify '-i #' where # is the osd number" << std::endl;
    usage();
  }

  if (!g_conf.osd_data) {
    cerr << "must specify '--osd-data=foo' data path" << std::endl;
    usage();
  }

  // get monmap
  RotatingKeyRing rkeys(CEPH_ENTITY_TYPE_OSD, &g_keyring);
  MonClient mc(&rkeys);
  if (mc.build_initial_monmap() < 0)
    return -1;

  char buf[80];
  if (mkfs) {
    if (mc.get_monmap_privately() < 0)
      return -1;

    int err = OSD::mkfs(g_conf.osd_data, g_conf.osd_journal, mc.monmap.fsid, whoami);
    if (err < 0) {
      cerr << TEXT_RED << " ** ERROR: error creating empty object store in " << g_conf.osd_data
	   << ": " << strerror_r(-err, buf, sizeof(buf)) << TEXT_NORMAL << std::endl;
      exit(1);
    }
    cout << "created object store " << g_conf.osd_data;
    if (g_conf.osd_journal)
      cout << " journal " << g_conf.osd_journal;
    cout << " for osd" << whoami << " fsid " << mc.monmap.fsid << std::endl;
    exit(0);
  }
  if (mkjournal) {
    int err = OSD::mkjournal(g_conf.osd_data, g_conf.osd_journal);
    if (err < 0) {
      cerr << TEXT_RED << " ** ERROR: error creating fresh journal " << g_conf.osd_journal
	   << " for object store " << g_conf.osd_data
	   << ": " << strerror_r(-err, buf, sizeof(buf)) << std::endl;
      exit(1);
    }
    cout << "created new journal " << g_conf.osd_journal
	 << " for object store " << g_conf.osd_data
	 << std::endl;
    exit(0);
  }
  if (flushjournal) {
    int err = OSD::flushjournal(g_conf.osd_data, g_conf.osd_journal);
    if (err < 0) {
      cerr << TEXT_RED << " ** ERROR: error flushing journal " << g_conf.osd_journal
	   << " for object store " << g_conf.osd_data
	   << ": " << strerror_r(-err, buf, sizeof(buf)) << std::endl;
      exit(1);
    }
    cout << "flushed journal " << g_conf.osd_journal
	 << " for object store " << g_conf.osd_data
	 << std::endl;
    exit(0);
  }
  
  string magic;
  ceph_fsid_t fsid;
  int w;
  int r = OSD::peek_meta(g_conf.osd_data, magic, fsid, w);
  if (r < 0) {
    cerr << TEXT_RED << " ** ERROR: unable to open OSD superblock on " << g_conf.osd_data << ": " << strerror_r(-r, buf, sizeof(buf)) << TEXT_NORMAL << std::endl;
    if (r == -ENOTSUP)
      cerr << TEXT_RED << " **        please verify that underlying storage supports xattrs" << TEXT_NORMAL << std::endl;
    derr(0) << "unable to open OSD superblock on " << g_conf.osd_data << ": " << strerror_r(-r, buf, sizeof(buf)) << dendl;
    exit(1);
  }
  if (w != whoami) {
    cerr << "OSD id " << w << " != my id " << whoami << std::endl;
    exit(1);
  }
  if (strcmp(magic.c_str(), CEPH_OSD_ONDISK_MAGIC)) {
    cerr << "OSD magic " << magic << " != my " << CEPH_OSD_ONDISK_MAGIC << std::endl;
    exit(1);
  }

  bool client_addr_set = !g_conf.public_addr.is_blank_addr();
  bool cluster_addr_set = !g_conf.cluster_addr.is_blank_addr();

  if (cluster_addr_set && !client_addr_set) {
    cerr << TEXT_RED << " ** "
         << "WARNING: set cluster address but not client address!" << " **\n"
         << "using cluster address for clients" << TEXT_NORMAL << std::endl;
    g_conf.public_addr = g_conf.cluster_addr;
    client_addr_set = true;
    cluster_addr_set = false;
  }

  SimpleMessenger *client_messenger = new SimpleMessenger();
  SimpleMessenger *cluster_messenger = new SimpleMessenger();
  SimpleMessenger *messenger_hb = new SimpleMessenger();

  entity_addr_t hb_addr;

  if (client_addr_set) {
    client_messenger->bind(g_conf.public_addr);
    hb_addr = g_conf.public_addr;
    hb_addr.set_port(0);
  } else {
    client_messenger->bind();
  }

  if (cluster_addr_set) {
    cluster_messenger->bind(g_conf.cluster_addr);
    hb_addr = g_conf.cluster_addr;
    hb_addr.set_port(0);
  } else {
    cluster_messenger->bind();
  }

  messenger_hb->bind(hb_addr);

  cout << "starting osd" << whoami
       << " at " << client_messenger->get_ms_addr() 
       << " osd_data " << g_conf.osd_data
       << " " << ((g_conf.osd_journal && g_conf.osd_journal[0]) ? g_conf.osd_journal:"(no journal)")
       << std::endl;

  client_messenger->register_entity(entity_name_t::OSD(whoami));
  cluster_messenger->register_entity(entity_name_t::OSD(whoami));
  messenger_hb->register_entity(entity_name_t::OSD(whoami));

  Throttle client_throttler(g_conf.osd_client_message_size_cap);

  uint64_t supported =
    CEPH_FEATURE_UID | 
    CEPH_FEATURE_NOSRCADDR;

  client_messenger->set_default_policy(SimpleMessenger::Policy::stateless_server(supported, 0));
  client_messenger->set_policy(entity_name_t::TYPE_CLIENT,
			       SimpleMessenger::Policy::stateless_server(supported, 0));
  client_messenger->set_policy_throttler(entity_name_t::TYPE_CLIENT, &client_throttler);
  client_messenger->set_policy(entity_name_t::TYPE_MON,
                               SimpleMessenger::Policy::client(supported,
                                                               CEPH_FEATURE_UID));
  //try to poison pill any OSD connections on the wrong address
  client_messenger->set_policy(entity_name_t::TYPE_OSD,
			       SimpleMessenger::Policy::stateless_server(0,0));

  cluster_messenger->set_default_policy(SimpleMessenger::Policy::stateless_server(0, 0));
  cluster_messenger->set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::client(0,0));
  cluster_messenger->set_policy(entity_name_t::TYPE_OSD,
				SimpleMessenger::Policy::lossless_peer(supported, CEPH_FEATURE_UID));
  cluster_messenger->set_policy(entity_name_t::TYPE_CLIENT,
				SimpleMessenger::Policy::stateless_server(0, 0));



  OSD *osd = new OSD(whoami, cluster_messenger, client_messenger, messenger_hb, &mc, g_conf.osd_data, g_conf.osd_journal);

  int err = osd->pre_init();
  if (err < 0) {
    char buf[80];
    cerr << TEXT_RED << " ** ERROR: initializing osd failed: " << strerror_r(-err, buf, sizeof(buf)) << TEXT_NORMAL << std::endl;
    return 1;
  }

  client_messenger->start();
  messenger_hb->start(true);  // only need to daemon() once
  cluster_messenger->start(true);

  // start osd
  if (osd->init() < 0) {
    cerr << TEXT_RED << " ** ERROR: initializing osd failed: " << strerror_r(-err, buf, sizeof(buf)) << TEXT_NORMAL << std::endl;
    return 1;
  }

  client_messenger->wait();
  messenger_hb->wait();
  cluster_messenger->wait();

  // done
  delete osd;
  client_messenger->destroy();
  messenger_hb->destroy();
  cluster_messenger->destroy();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if (mkdir(s, 0755) == 0)
    chdir(s);

  return 0;
}

