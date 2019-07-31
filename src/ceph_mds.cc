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
#include <pthread.h>

#include <iostream>
#include <string>

#include "include/ceph_features.h"
#include "include/compat.h"
#include "include/random.h"

#include "common/config.h"
#include "common/strtol.h"

#include "mon/MonMap.h"
#include "mds/MDSDaemon.h"

#include "msg/Messenger.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/pick_address.h"
#include "common/Preforker.h"

#include "global/global_init.h"
#include "global/signal_handler.h"
#include "global/pidfile.h"

#include "mon/MonClient.h"

#include "auth/KeyRing.h"

#include "perfglue/heap_profiler.h"

#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds

static void usage()
{
  cout << "usage: ceph-mds -i <ID> [flags]\n"
       << "  -m monitorip:port\n"
       << "        connect to monitor at given address\n"
       << "  --debug_mds n\n"
       << "        debug MDS level (e.g. 10)\n"
       << std::endl;
  generic_server_usage();
}


MDSDaemon *mds = NULL;


static void handle_mds_signal(int signum)
{
  if (mds)
    mds->handle_signal(signum);
}

int main(int argc, const char **argv)
{
  ceph_pthread_setname(pthread_self(), "ceph-mds");

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args,
			 CEPH_ENTITY_TYPE_MDS, CODE_ENVIRONMENT_DAEMON,
			 0, "mds_data");
  ceph_heap_profiler_init();

  std::string val, action;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--hot-standby", (char*)NULL)) {
      dout(0) << "--hot-standby is obsolete and has no effect" << dendl;
    }
    else {
      derr << "Error: can't understand argument: " << *i << "\n" << dendl;
      exit(1);
    }
  }

  Preforker forker;

  entity_addrvec_t addrs;
  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC, &addrs);

  // Normal startup
  if (g_conf()->name.has_default_id()) {
    derr << "must specify '-i name' with the ceph-mds instance name" << dendl;
    exit(1);
  }

  if (g_conf()->name.get_id().empty() ||
      (g_conf()->name.get_id()[0] >= '0' && g_conf()->name.get_id()[0] <= '9')) {
    derr << "MDS id '" << g_conf()->name << "' is invalid. "
      "MDS names may not start with a numeric digit." << dendl;
    exit(1);
  }

  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    int r = forker.prefork(err);
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }
    if (forker.is_parent()) {
      if (forker.parent_wait(err) != 0) {
        return -ENXIO;
      }
      return 0;
    }
    global_init_postfork_start(g_ceph_context);
  }
  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  auto nonce = ceph::util::generate_random_number<uint64_t>();

  std::string public_msgr_type = g_conf()->ms_public_type.empty() ? g_conf().get_val<std::string>("ms_type") : g_conf()->ms_public_type;
  Messenger *msgr = Messenger::create(g_ceph_context, public_msgr_type,
				      entity_name_t::MDS(-1), "mds",
				      nonce, Messenger::HAS_MANY_CONNECTIONS);
  if (!msgr)
    forker.exit(1);
  msgr->set_cluster_protocol(CEPH_MDS_PROTOCOL);

  cout << "starting " << g_conf()->name << " at " << msgr->get_myaddrs()
       << std::endl;
  uint64_t required =
    CEPH_FEATURE_OSDREPLYMUX;

  msgr->set_default_policy(Messenger::Policy::lossy_client(required));
  msgr->set_policy(entity_name_t::TYPE_MON,
                   Messenger::Policy::lossy_client(CEPH_FEATURE_UID |
                                                   CEPH_FEATURE_PGID64));
  msgr->set_policy(entity_name_t::TYPE_MDS,
                   Messenger::Policy::lossless_peer(CEPH_FEATURE_UID));
  msgr->set_policy(entity_name_t::TYPE_CLIENT,
                   Messenger::Policy::stateful_server(0));

  int r = msgr->bindv(addrs);
  if (r < 0)
    forker.exit(1);

  // set up signal handlers, now that we've daemonized/forked.
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  
  // get monmap
  MonClient mc(g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    forker.exit(1);
  global_init_chdir(g_ceph_context);

  msgr->start();

  // start mds
  mds = new MDSDaemon(g_conf()->name.get_id().c_str(), msgr, &mc);

  // in case we have to respawn...
  mds->orig_argc = argc;
  mds->orig_argv = argv;

  if (g_conf()->daemonize) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }

  r = mds->init();
  if (r < 0) {
    msgr->wait();
    goto shutdown;
  }

  register_async_signal_handler_oneshot(SIGINT, handle_mds_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mds_signal);

  if (g_conf()->inject_early_sigterm)
    kill(getpid(), SIGTERM);

  msgr->wait();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_mds_signal);
  unregister_async_signal_handler(SIGTERM, handle_mds_signal);
  shutdown_async_signal_handler();

 shutdown:
  // yuck: grab the mds lock, so we can be sure that whoever in *mds
  // called shutdown finishes what they were doing.
  mds->mds_lock.Lock();
  mds->mds_lock.Unlock();

  pidfile_remove();

  // only delete if it was a clean shutdown (to aid memory leak
  // detection, etc.).  don't bother if it was a suicide.
  if (mds->is_clean_shutdown()) {
    delete mds;
    delete msgr;
  }

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    cerr << "ceph-mds: gmon.out should be in " << s << std::endl;
  }

  return 0;
}

