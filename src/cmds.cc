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

#include "common/config.h"
#include "common/strtol.h"

#include "mon/MonMap.h"
#include "mds/MDS.h"
#include "mds/Dumper.h"
#include "mds/Resetter.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"
#include "common/common_init.h"
#include "common/ceph_argparse.h"

#include "mon/MonClient.h"

#include "auth/KeyRing.h"

void usage()
{
  derr << "usage: cmds -i name [flags] [[--journal_check rank]|[--hot-standby][rank]]\n"
       << "  -m monitorip:port\n"
       << "        connect to monitor at given address\n"
       << "  --debug_mds n\n"
       << "        debug MDS level (e.g. 10)\n"
       << "  --dump-journal rank filename\n"
       << "        dump the MDS journal for rank. Defaults to mds.journal.dump\n"
       << "  --journal-check rank\n"
       << "        replay the journal for rank, then exit\n"
       << "  --hot-standby rank\n"
       << "        stat up as a hot standby for rank\n"
       << "  --reset-journal rank\n"
       << "        discard the MDS journal for rank, and replace it with a single\n"
       << "        event that updates/resets inotable and sessionmap on replay.\n"
       << dendl;
  generic_server_usage();
}

static int do_cmds_special_action(const std::string &action,
				  const std::string &dump_file, int rank)
{
  SimpleMessenger *messenger = new SimpleMessenger();
  messenger->bind(getpid());
  MonClient mc(&g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;

  if (action == "dump-journal") {
    dout(0) << "dumping journal for mds" << rank << " to " << dump_file << dendl;
    Dumper *journal_dumper = new Dumper(messenger, &mc);
    journal_dumper->init(rank);
    journal_dumper->dump(dump_file.c_str());
    mc.shutdown();
  }
  else if (action == "undump-journal") {
    dout(0) << "undumping journal for mds" << rank << " from " << dump_file << dendl;
    Dumper *journal_dumper = new Dumper(messenger, &mc);
    journal_dumper->init(rank);
    journal_dumper->undump(dump_file.c_str());
    mc.shutdown();
  }
  else if (action == "reset-journal") {
    dout(0) << "resetting journal" << dendl;
    Resetter *jr = new Resetter(messenger, &mc);
    jr->init(rank);
    jr->reset();
    mc.shutdown();
  }
  else {
    assert(0);
  }
  return 0;
}

static void set_special_action(std::string &dest, const std::string &act)
{
  if (!dest.empty()) {
    derr << "Parse error! Can't specify more than one action. You "
	 << "specified both " << act << " and " << dest << "\n" << dendl;
    usage();
    exit(1);
  }
  dest = act;
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

int main(int argc, const char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_init(args, CEPH_ENTITY_TYPE_MDS, CODE_ENVIRONMENT_DAEMON, 0);

  // mds specific args
  int shadow = 0;
  int rank = -1;
  std::string dump_file;

  std::string val, action;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_witharg(args, i, &val, "--dump-journal", (char*)NULL)) {
      set_special_action(action, "dump-journal");
      rank = parse_rank("dump-journal", val);
      if (i == args.end()) {
	derr << "error parsing --dump-journal: you must give a second "
	     << "dump-journal argument: the filename to dump the journal to. "
	     << "\n" << dendl;
	usage();
      }
      dump_file = *i++;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--undump-journal", (char*)NULL)) {
      set_special_action(action, "undump-journal");
      rank = parse_rank("undump-journal", val);
      if (i == args.end()) {
	derr << "error parsing --undump-journal: you must give a second "
	     << "undump-journal argument: the filename to undump the journal from. "
	     << "\n" << dendl;
	usage();
      }
      dump_file = *i++;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--reset-journal", (char*)NULL)) {
      set_special_action(action, "reset-journal");
      rank = parse_rank("reset-journal", val);
    }
    else if (ceph_argparse_witharg(args, i, &val, "--journal-check", (char*)NULL)) {
      int r = parse_rank("journal-check", val);
      if (shadow) {
        dout(0) << "Error: can only select one standby state" << dendl;
        return -1;
      }
      dout(0) << "requesting oneshot_replay for mds" << r << dendl;
      shadow = MDSMap::STATE_ONESHOT_REPLAY;
      g_conf.mds_standby_for_rank = r;
    }
    else if (ceph_argparse_witharg(args, i, &val, "--hot-standby", (char*)NULL)) {
      int r = parse_rank("hot-standby", val);
      if (shadow) {
        dout(0) << "Error: can only select one standby state" << dendl;
        return -1;
      }
      dout(0) << "requesting standby_replay for mds" << r << dendl;
      shadow = MDSMap::STATE_STANDBY_REPLAY;
      g_conf.mds_standby_for_rank = r;
    }
    else {
      derr << "Error: can't understand argument: " << *i << "\n" << dendl;
      usage();
    }
  }

  // Check for special actions
  if (!action.empty()) {
    return do_cmds_special_action(action, dump_file, rank);
  }

  // Normal startup
  if (g_conf.name.has_default_id()) {
    derr << "must specify '-i name' with the cmds instance name" << dendl;
    usage();
  }

  SimpleMessenger *messenger = new SimpleMessenger();
  messenger->bind(getpid());
  cout << "starting " << g_conf.name << " at " << messenger->get_ms_addr()
       << std::endl;
  messenger->register_entity(entity_name_t::MDS(-1));
  uint64_t supported =
    CEPH_FEATURE_UID |
    CEPH_FEATURE_NOSRCADDR |
    CEPH_FEATURE_DIRLAYOUTHASH;
  messenger->set_default_policy(SimpleMessenger::Policy::client(supported, 0));
  messenger->set_policy(entity_name_t::TYPE_MON,
			SimpleMessenger::Policy::client(supported,
							CEPH_FEATURE_UID));
  messenger->set_policy(entity_name_t::TYPE_MDS,
			SimpleMessenger::Policy::lossless_peer(supported,
							       CEPH_FEATURE_UID));
  messenger->set_policy(entity_name_t::TYPE_CLIENT,
			SimpleMessenger::Policy::stateful_server(supported, 0));

  if (shadow != MDSMap::STATE_ONESHOT_REPLAY)
    common_init_daemonize(&g_ceph_context, 0);
  common_init_finish(&g_ceph_context);

  // get monmap
  MonClient mc(&g_ceph_context);
  if (mc.build_initial_monmap() < 0)
    return -1;
  common_init_chdir(&g_ceph_context);

  messenger->start();

  // start mds
  MDS *mds = new MDS(g_conf.name.get_id().c_str(), messenger, &mc);

  // in case we have to respawn...
  mds->orig_argc = argc;
  mds->orig_argv = argv;

  if (shadow)
    mds->init(shadow);
  else
    mds->init();

  messenger->wait();

  // yuck: grab the mds lock, so we can be sure that whoever in *mds
  // called shutdown finishes what they were doing.
  mds->mds_lock.Lock();
  mds->mds_lock.Unlock();

  // only delete if it was a clean shutdown (to aid memory leak
  // detection, etc.).  don't bother if it was a suicide.
  if (mds->is_stopped())
    delete mds;

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  snprintf(s, sizeof(s), "gmon/%d", getpid());
  if ((mkdir(s, 0755) == 0) && (chdir(s) == 0)) {
    dout(0) << "cmds: gmon.out should be in " << s << dendl;
  }

  generic_dout(0) << "stopped." << dendl;
  return 0;
}

