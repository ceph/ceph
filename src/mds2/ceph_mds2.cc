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

#include "include/ceph_features.h"

#include "common/config.h"
#include "common/strtol.h"

#include "mon/MonMap.h"
#include "MDSDaemon.h"

#include "msg/Messenger.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/pick_address.h"

#include "global/global_init.h"
#include "global/signal_handler.h"
#include "global/pidfile.h"

#include "mon/MonClient.h"

#include "auth/KeyRing.h"

#include "perfglue/heap_profiler.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_mds

void usage()
{
  cout << "usage: ceph-mds -i name [flags] [[--journal_check rank]|[--hot-standby][rank]]\n"
       << "  -m monitorip:port\n"
       << "        connect to monitor at given address\n"
       << "  --debug_mds n\n"
       << "        debug MDS level (e.g. 10)\n"
       << "  --journal-check rank\n"
       << "        replay the journal for rank, then exit\n"
       << "  --hot-standby rank\n"
       << "        start up as a hot standby for rank\n"
       << std::endl;
  generic_server_usage();
}


static int parse_rank(const char *opt_name, const std::string &val)
{
  std::string err;
  int ret = strict_strtol(val.c_str(), 10, &err);
  if (!err.empty()) {
    derr << "error parsing " << opt_name << ": failed to parse rank. "
	 << "It must be an int." << "\n" << dendl;
    usage();
  }
  return ret;
}



MDSDaemon *mds = NULL;


static void handle_mds_signal(int signum)
{
  if (mds)
    mds->handle_signal(signum);
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_MDS, CODE_ENVIRONMENT_DAEMON,
	      0, "mds_data");
  // FIXME
  // ceph_heap_profiler_init();

  std::string val, action;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    }
    else if (ceph_argparse_flag(args, i, "--help", "-h", (char*)NULL)) {
      // exit(1) will be called in the usage()
      usage();
    }
    else if (ceph_argparse_witharg(args, i, &val, "--hot-standby", (char*)NULL)) {
      int r = parse_rank("hot-standby", val);
      dout(0) << "requesting standby_replay for mds." << r << dendl;
      char rb[32];
      snprintf(rb, sizeof(rb), "%d", r);
      g_conf->set_val("mds_standby_for_rank", rb);
      g_conf->set_val("mds_standby_replay", "true");
      g_conf->apply_changes(NULL);
    }
    else {
      derr << "Error: can't understand argument: " << *i << "\n" << dendl;
      usage();
    }
  }

  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);

  // Normal startup
  if (g_conf->name.has_default_id()) {
    derr << "must specify '-i name' with the ceph-mds instance name" << dendl;
    usage();
  }

  if (g_conf->name.get_id().empty() ||
      (g_conf->name.get_id()[0] >= '0' && g_conf->name.get_id()[0] <= '9')) {
    derr << "deprecation warning: MDS id '" << g_conf->name
      << "' is invalid and will be forbidden in a future version.  "
      "MDS names may not start with a numeric digit." << dendl;
  }

  Messenger *msgr = Messenger::create(g_ceph_context, g_conf->ms_type,
				      entity_name_t::MDS(-1), "mds",
				      getpid(), 0, Messenger::HAS_MANY_CONNECTIONS);
  if (!msgr)
    exit(1);
  msgr->set_cluster_protocol(CEPH_MDS_PROTOCOL);

  cout << "starting " << g_conf->name << " at " << msgr->get_myaddr()
       << std::endl;
  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_DIRLAYOUTHASH |
    CEPH_FEATURE_MDS_INLINE_DATA |
    CEPH_FEATURE_PGID64 |
    CEPH_FEATURE_MSG_AUTH |
    CEPH_FEATURE_EXPORT_PEER |
    CEPH_FEATURE_MDS_QUOTA;
  uint64_t required =
    CEPH_FEATURE_OSDREPLYMUX;

  msgr->set_default_policy(Messenger::Policy::lossy_client(supported, required));
  msgr->set_policy(entity_name_t::TYPE_MON,
                   Messenger::Policy::lossy_client(supported,
                                                   CEPH_FEATURE_UID |
                                                   CEPH_FEATURE_PGID64));
  msgr->set_policy(entity_name_t::TYPE_MDS,
                   Messenger::Policy::lossless_peer(supported,
                                                    CEPH_FEATURE_UID));
  msgr->set_policy(entity_name_t::TYPE_CLIENT,
                   Messenger::Policy::stateful_server(supported, 0));

  int r = msgr->bind(g_conf->public_addr);
  if (r < 0)
    exit(1);

  global_init_daemonize(g_ceph_context);
  common_init_finish(g_ceph_context);

  // get monmap
  MonClient mc(g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;
  global_init_chdir(g_ceph_context);

  msgr->start();

  // start mds
  mds = new MDSDaemon(g_conf->name.get_id().c_str(), msgr, &mc);

  // in case we have to respawn...
  mds->orig_argc = argc;
  mds->orig_argv = argv;

  r = mds->init();
  if (r < 0) {
    msgr->wait();
    goto shutdown;
  }

  // set up signal handlers, now that we've daemonized/forked.
  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_mds_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_mds_signal);

  if (g_conf->inject_early_sigterm)
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

  // FIXME
  // only delete if it was a clean shutdown (to aid memory leak
  // detection, etc.).  don't bother if it was a suicide.
  // if (mds->is_clean_shutdown()) {
  if (true) {
    delete mds;
    delete msgr;
  }

  g_ceph_context->put();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    cerr << "ceph-mds: gmon.out should be in " << s << std::endl;
  }

  return 0;
}

