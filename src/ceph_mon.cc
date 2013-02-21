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

#include <iostream>
#include <string>
using namespace std;

#include "common/config.h"
#include "include/ceph_features.h"

#include "mon/MonMap.h"
#include "mon/Monitor.h"
#include "mon/MonitorDBStore.h"
#include "mon/MonClient.h"

#include "msg/Messenger.h"

#include "include/CompatSet.h"

#include "common/ceph_argparse.h"
#include "common/pick_address.h"
#include "common/Timer.h"
#include "common/errno.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_mon

Monitor *mon = NULL;

void handle_mon_signal(int signum)
{
  if (mon)
    mon->handle_signal(signum);
}

void usage()
{
  cerr << "usage: ceph-mon -i monid [--mon-data=pathtodata] [flags]" << std::endl;
  cerr << "  --debug_mon n\n";
  cerr << "        debug monitor level (e.g. 10)\n";
  cerr << "  --mkfs\n";
  cerr << "        build fresh monitor fs\n";
  generic_server_usage();
}

int main(int argc, const char **argv) 
{
  int err;

  bool mkfs = false;
  std::string osdmapfn, inject_monmap;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_MON, CODE_ENVIRONMENT_DAEMON, 0);

  uuid_d fsid;
  std::string val;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      exit(0);
    } else if (ceph_argparse_flag(args, i, "--mkfs", (char*)NULL)) {
      mkfs = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--osdmap", (char*)NULL)) {
      osdmapfn = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--inject_monmap", (char*)NULL)) {
      inject_monmap = val;
    } else {
      ++i;
    }
  }
  if (!args.empty()) {
    cerr << "too many arguments: " << args << std::endl;
    usage();
  }

  if (g_conf->mon_data.empty()) {
    cerr << "must specify '--mon-data=foo' data path" << std::endl;
    usage();
  }

  if (g_conf->name.get_id().empty()) {
    cerr << "must specify id (--id <id> or --name mon.<id>)" << std::endl;
    usage();
  }

  // -- mkfs --
  if (mkfs) {
    // resolve public_network -> public_addr
    pick_addresses(g_ceph_context);

    common_init_finish(g_ceph_context);

    bufferlist monmapbl, osdmapbl;
    std::string error;
    MonMap monmap;

    // load or generate monmap
    if (g_conf->monmap.length()) {
      int err = monmapbl.read_file(g_conf->monmap.c_str(), &error);
      if (err < 0) {
	cerr << argv[0] << ": error reading " << g_conf->monmap << ": " << error << std::endl;
	exit(1);
      }
      try {
	monmap.decode(monmapbl);

	// always mark seed/mkfs monmap as epoch 0
	monmap.set_epoch(0);
      }
      catch (const buffer::error& e) {
	cerr << argv[0] << ": error decoding monmap " << g_conf->monmap << ": " << e.what() << std::endl;
	exit(1);
      }      
    } else {
      int err = monmap.build_initial(g_ceph_context, cerr);
      if (err < 0) {
	cerr << argv[0] << ": warning: no initial monitors; must use admin socket to feed hints" << std::endl;
      }

      // am i part of the initial quorum?
      if (monmap.contains(g_conf->name.get_id())) {
	// hmm, make sure the ip listed exists on the current host?
	// maybe later.
      } else if (!g_conf->public_addr.is_blank_ip()) {
	entity_addr_t a = g_conf->public_addr;
	if (a.get_port() == 0)
	  a.set_port(CEPH_MON_PORT);
	if (monmap.contains(a)) {
	  string name;
	  monmap.get_addr_name(a, name);
	  monmap.rename(name, g_conf->name.get_id());
	  cout << argv[0] << ": renaming mon." << name << " " << a
	       << " to mon." << g_conf->name.get_id() << std::endl;
	}
      } else {
	// is a local address listed without a name?  if so, name myself.
	list<entity_addr_t> ls;
	monmap.list_addrs(ls);
	entity_addr_t local;

	if (have_local_addr(g_ceph_context, ls, &local)) {
	  string name;
	  monmap.get_addr_name(local, name);

	  if (name.find("noname-") == 0) {
	    cout << argv[0] << ": mon." << name << " " << local
		 << " is local, renaming to mon." << g_conf->name.get_id() << std::endl;
	    monmap.rename(name, g_conf->name.get_id());
	  } else {
	    cout << argv[0] << ": mon." << name << " " << local
		 << " is local, but not 'noname-' + something; not assuming it's me" << std::endl;
	  }
	}
      }
    }

    if (!g_conf->fsid.is_zero()) {
      monmap.fsid = g_conf->fsid;
      cout << argv[0] << ": set fsid to " << g_conf->fsid << std::endl;
    }
    
    if (monmap.fsid.is_zero()) {
      cerr << argv[0] << ": generated monmap has no fsid; use '--fsid <uuid>'" << std::endl;
      exit(10);
    }

    //monmap.print(cout);

    // osdmap
    if (osdmapfn.length()) {
      err = osdmapbl.read_file(osdmapfn.c_str(), &error);
      if (err < 0) {
	cerr << argv[0] << ": error reading " << osdmapfn << ": "
	     << error << std::endl;
	exit(1);
      }
    }

    // go
    MonitorDBStore store(g_conf->mon_data);
    assert(!store.create_and_open(cerr));

    Monitor mon(g_ceph_context, g_conf->name.get_id(), &store, 0, &monmap);
    int r = mon.mkfs(osdmapbl);
    if (r < 0) {
      cerr << argv[0] << ": error creating monfs: " << cpp_strerror(r) << std::endl;
      exit(1);
    }
    cout << argv[0] << ": created monfs at " << g_conf->mon_data 
	 << " for " << g_conf->name << std::endl;
    return 0;
  }

  {
    Monitor::StoreConverter converter(g_conf->mon_data);
    if (converter.needs_conversion())
      assert(!converter.convert());
  }

  MonitorDBStore store(g_conf->mon_data);
  assert(!store.open(std::cerr));

  bufferlist magicbl;
  err = store.get(Monitor::MONITOR_NAME, "magic", magicbl);
  if (!magicbl.length()) {
    cerr << "unable to read magic from mon data.. did you run mkcephfs?" << std::endl;
    exit(1);
  }
  string magic(magicbl.c_str(), magicbl.length()-1);  // ignore trailing \n
  if (strcmp(magic.c_str(), CEPH_MON_ONDISK_MAGIC)) {
    cerr << "mon fs magic '" << magic << "' != current '" << CEPH_MON_ONDISK_MAGIC << "'" << std::endl;
    exit(1);
  }

  err = Monitor::check_features(&store);
  if (err < 0) {
    cerr << "error checking features: " << cpp_strerror(err) << std::endl;
    exit(1);
  }

  // inject new monmap?
  if (!inject_monmap.empty()) {
    bufferlist bl;
    std::string error;
    int r = bl.read_file(inject_monmap.c_str(), &error);
    if (r) {
      cerr << "unable to read monmap from " << inject_monmap << ": "
	   << error << std::endl;
      exit(1);
    }

    // get next version
    version_t v = store.get("monmap", "last_committed");
    cout << "last committed monmap epoch is " << v << ", injected map will be " << (v+1) << std::endl;
    v++;

    // set the version
    MonMap tmp;
    tmp.decode(bl);
    if (tmp.get_epoch() != v) {
      cout << "changing monmap epoch from " << tmp.get_epoch() << " to " << v << std::endl;
      tmp.set_epoch(v);
    }
    bufferlist mapbl;
    tmp.encode(mapbl, CEPH_FEATURES_ALL);
    bufferlist final;
    ::encode(v, final);
    ::encode(mapbl, final);

    MonitorDBStore::Transaction t;
    // save it
    t.put("monmap", v, mapbl);
    t.put("monmap", "latest", final);
    t.put("monmap", "last_committed", v);
    store.apply_transaction(t);

    cout << "done." << std::endl;
    exit(0);
  }


  // monmap?
  MonMap monmap;
  {
    bufferlist mapbl;
    bufferlist latest;
    store.get("monmap", "latest", latest);
    if (latest.length() > 0) {
      bufferlist::iterator p = latest.begin();
      version_t v;
      ::decode(v, p);
      ::decode(mapbl, p);
    } else {
      store.get("mkfs", "monmap", mapbl);
      if (mapbl.length() == 0) {
	cerr << "mon fs missing 'monmap/latest' and 'mkfs/monmap'" << std::endl;
	exit(1);
      }
    }
    try {
      monmap.decode(mapbl);
    }
    catch (const buffer::error& e) {
      cerr << "can't decode monmap: " << e.what() << std::endl;
    }
  }

  // this is what i will bind to
  entity_addr_t ipaddr;

  if (monmap.contains(g_conf->name.get_id())) {
    ipaddr = monmap.get_addr(g_conf->name.get_id());

    // print helpful warning if the conf file doesn't match
    entity_addr_t conf_addr;
    std::vector <std::string> my_sections;
    g_conf->get_my_sections(my_sections);
    std::string mon_addr_str;
    if (g_conf->get_val_from_conf_file(my_sections, "mon addr",
				       mon_addr_str, true) == 0) {
      if (conf_addr.parse(mon_addr_str.c_str()) && (ipaddr != conf_addr)) {
	cerr << "WARNING: 'mon addr' config option " << conf_addr
	     << " does not match monmap file" << std::endl
	     << "         continuing with monmap configuration" << std::endl;
      }
    }
  } else {
    dout(0) << g_conf->name << " does not exist in monmap, will attempt to join an existing cluster" << dendl;

    pick_addresses(g_ceph_context);
    if (!g_conf->public_addr.is_blank_ip()) {
      ipaddr = g_conf->public_addr;
      if (ipaddr.get_port() == 0)
	ipaddr.set_port(CEPH_MON_PORT);
    } else {
      MonMap tmpmap;
      int err = tmpmap.build_initial(g_ceph_context, cerr);
      if (err < 0) {
	cerr << argv[0] << ": error generating initial monmap: " << cpp_strerror(err) << std::endl;
	usage();
	exit(1);
      }
      if (tmpmap.contains(g_conf->name.get_id())) {
	ipaddr = tmpmap.get_addr(g_conf->name.get_id());
      } else {
	derr << "no public_addr or public_network specified, and " << g_conf->name
	     << " not present in monmap or ceph.conf" << dendl;
	exit(1);
      }
    }
  }

  // bind
  int rank = monmap.get_rank(g_conf->name.get_id());
  Messenger *messenger = Messenger::create(g_ceph_context,
					   entity_name_t::MON(rank),
					   "mon",
					   0);
  messenger->set_cluster_protocol(CEPH_MON_PROTOCOL);
  messenger->set_default_send_priority(CEPH_MSG_PRIO_HIGH);

  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_MONCLOCKCHECK |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_MSG_AUTH;
  messenger->set_default_policy(Messenger::Policy::stateless_server(supported, 0));
  messenger->set_policy(entity_name_t::TYPE_MON,
                        Messenger::Policy::lossless_peer_reuse(supported,
							       CEPH_FEATURE_UID |
							       CEPH_FEATURE_PGID64 |
							       CEPH_FEATURE_MON_SINGLE_PAXOS));
  messenger->set_policy(entity_name_t::TYPE_OSD,
                        Messenger::Policy::stateless_server(supported,
                                                            CEPH_FEATURE_PGID64 |
                                                            CEPH_FEATURE_OSDENC));
  messenger->set_policy(entity_name_t::TYPE_CLIENT,
			Messenger::Policy::stateless_server(supported, 0));
  messenger->set_policy(entity_name_t::TYPE_MDS,
			Messenger::Policy::stateless_server(supported, 0));


  // throttle client traffic
  Throttle *client_throttler = new Throttle(g_ceph_context, "mon_client_bytes",
					    g_conf->mon_client_bytes);
  messenger->set_policy_throttler(entity_name_t::TYPE_CLIENT, client_throttler);

  // throttle daemon traffic
  // NOTE: actual usage on the leader may multiply by the number of
  // monitors if they forward large update messages from daemons.
  Throttle *daemon_throttler = new Throttle(g_ceph_context, "mon_daemon_bytes",
					    g_conf->mon_daemon_bytes);
  messenger->set_policy_throttler(entity_name_t::TYPE_OSD, daemon_throttler);
  messenger->set_policy_throttler(entity_name_t::TYPE_MDS, daemon_throttler);

  cout << "starting " << g_conf->name << " rank " << rank
       << " at " << ipaddr
       << " mon_data " << g_conf->mon_data
       << " fsid " << monmap.get_fsid()
       << std::endl;

  err = messenger->bind(ipaddr);
  if (err < 0)
    return 1;

  // start monitor
  mon = new Monitor(g_ceph_context, g_conf->name.get_id(), &store, 
		    messenger, &monmap);

  err = mon->preinit();
  if (err < 0)
    return 1;

  global_init_daemonize(g_ceph_context, 0);
  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  // set up signal handlers, now that we've daemonized/forked.
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_mon_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mon_signal);

  messenger->start();

  mon->init();

  messenger->wait();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_mon_signal);
  unregister_async_signal_handler(SIGTERM, handle_mon_signal);

  shutdown_async_signal_handler();

  delete mon;
  delete messenger;
  delete client_throttler;
  delete daemon_throttler;
  g_ceph_context->put();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "ceph-mon: gmon.out should be in " << s << dendl;
  }

  return 0;
}

