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
  derr << "usage: cmds -i name [flags] [[--journal_check]|[--hot-standby][rank]]\n"
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

int main(int argc, const char **argv) 
{
  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_init(args, CEPH_ENTITY_TYPE_MDS, CODE_ENVIRONMENT_DAEMON, 0);
  keyring_init(&g_conf);

  // mds specific args
  int shadow = 0;
  int dump_journal = -1;
  const char *dump_file = NULL;
  int reset_journal = -1;
  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("dump-journal", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&dump_journal, OPT_INT);
      CEPH_ARGPARSE_SET_ARG_VAL(&dump_file, OPT_STR);
      dout(0) << "dumping journal for mds" << dump_journal << " to " << dump_file << dendl;
    } else if (CEPH_ARGPARSE_EQ("reset-journal", '\0')) {
      CEPH_ARGPARSE_SET_ARG_VAL(&reset_journal, OPT_INT);
    } else if (CEPH_ARGPARSE_EQ("journal-check", '\0')) {
      int check_rank;
      CEPH_ARGPARSE_SET_ARG_VAL(&check_rank, OPT_INT);
      
      if (shadow) {
        dout(0) << "Error: can only select one standby state" << dendl;
        return -1;
      }
      dout(0) << "requesting oneshot_replay for mds" << check_rank << dendl;
      shadow = MDSMap::STATE_ONESHOT_REPLAY;
      g_conf.mds_standby_for_rank = check_rank;
      ++i;
    } else if (CEPH_ARGPARSE_EQ("hot-standby", '\0')) {
      int check_rank;
      CEPH_ARGPARSE_SET_ARG_VAL(&check_rank, OPT_INT);
      if (shadow) {
        dout(0) << "Error: can only select one standby state" << dendl;
        return -1;
      }
      dout(0) << "requesting standby_replay for mds" << check_rank << dendl;
      shadow = MDSMap::STATE_STANDBY_REPLAY;
      g_conf.mds_standby_for_rank = check_rank;
    } else {
      derr << "unrecognized arg " << args[i] << dendl;
      usage();
    }
  }
  if (g_conf.name->has_default_id() && dump_journal < 0 && reset_journal < 0) {
    derr << "must specify '-i name' with the cmds instance name" << dendl;
    usage();
  }

  // get monmap
  RotatingKeyRing rkeys(CEPH_ENTITY_TYPE_MDS, &g_keyring);
  MonClient mc(&rkeys);
  if (mc.build_initial_monmap() < 0)
    return -1;

  SimpleMessenger *messenger = new SimpleMessenger();
  messenger->bind(getpid());
  if (dump_journal >= 0) {
    Dumper *journal_dumper = new Dumper(messenger, &mc);
    journal_dumper->init(dump_journal);
    journal_dumper->dump(dump_file);
    mc.shutdown();
  } else if (reset_journal >= 0) {
    Resetter *jr = new Resetter(messenger, &mc);
    jr->init(reset_journal);
    jr->reset();
    mc.shutdown();
  } else {
    cout << "starting " << *g_conf.name << " at " << messenger->get_ms_addr()
	 << std::endl;

    messenger->register_entity(entity_name_t::MDS(-1));
    assert_warn(messenger);
    if (!messenger)
      return 1;

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

    messenger->start(g_conf.daemonize);

    // start mds
    MDS *mds = new MDS(g_conf.name->get_id().c_str(), messenger, &mc);

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
  }
  return 0;
}

