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
#include "ebofs/Ebofs.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"
#include "common/common_init.h"

void usage() 
{
  cerr << "usage: cosd <device> [-j journalfileordev] [-m monitor] [--mkfs_for_osd <nodeid>]" << std::endl;
  cerr << "   -d              daemonize" << std::endl;
  cerr << "   --debug_osd N   set debug level (e.g. 10)" << std::endl;
  cerr << "   --debug_ms N    set message debug level (e.g. 1)" << std::endl;
  exit(1);
}


int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  configure_daemon_mode();
  common_init(args);

  if (g_conf.clock_tare) g_clock.tare();

  // osd specific args
  const char *dev = 0, *journaldev = 0;
  int whoami = -1;
  bool mkfs = 0;
  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"--mkfs_for_osd") == 0) {
      mkfs = 1; 
      whoami = atoi(args[++i]);
    } else if (strcmp(args[i],"-j") == 0)
      journaldev = args[++i];
    else if (!dev)
      dev = args[i];
    else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      usage();
    }
  }
  if (!dev) {
    cerr << "must specify device file" << std::endl;
    usage();
  }

  if (mkfs && whoami < 0) {
    cerr << "must specify '--osd #' where # is the osd number" << std::endl;
    usage();
  }

  // get monmap
  MonMap monmap;
  MonClient mc;
  if (mc.get_monmap(&monmap) < 0)
    return -1;

  if (mkfs) {
    int err = OSD::mkfs(dev, journaldev, monmap.fsid, whoami);
    if (err < 0) {
      cerr << "error creating empty object store in " << dev << ": " << strerror(-err) << std::endl;
      exit(1);
    }
    cout << "created object store for osd" << whoami << " fsid " << monmap.fsid << " on " << dev << std::endl;
    exit(0);
  }

  if (whoami < 0) {
    nstring magic;
    ceph_fsid_t fsid;
    int r = OSD::peek_super(dev, magic, fsid, whoami);
    if (r < 0) {
      cerr << "unable to determine OSD identity from superblock on " << dev << ": " << strerror(-r) << std::endl;
      exit(1);
    }
    if (strcmp(magic.c_str(), CEPH_OSD_ONDISK_MAGIC)) {
      cerr << "OSD magic " << magic << " != my " << CEPH_OSD_ONDISK_MAGIC << std::endl;
      exit(1);
    }
    if (ceph_fsid_compare(&fsid, &monmap.fsid)) {
      cerr << "OSD fsid " << fsid << " != monmap fsid " << monmap.fsid << std::endl;
      exit(1);
    }
  }

  _dout_create_courtesy_output_symlink("osd", whoami);


  // start up network
  rank.bind();

  cout << "starting osd" << whoami
       << " at " << rank.get_rank_addr() 
       << " dev " << dev << " " << (journaldev ? journaldev:"")
       << " fsid " << monmap.fsid
       << std::endl;

  g_timer.shutdown();

  Messenger *m = rank.register_entity(entity_name_t::OSD(whoami));
  assert_warn(m);
  if (!m)
    return 1;
  Messenger *hbm = rank.register_entity(entity_name_t::OSD(whoami));
  assert_warn(hbm);
  if (!hbm)
    return 1;

  rank.set_policy(entity_name_t::TYPE_MON, Rank::Policy::lossy_fast_fail());
  rank.set_policy(entity_name_t::TYPE_OSD, Rank::Policy::lossless());

  // make a _reasonable_ effort to send acks/replies to requests, but
  // don't get carried away, as the sender may go away and we won't
  // ever hear about it.
  rank.set_policy(entity_name_t::TYPE_MDS, Rank::Policy::lossy_fast_fail());
  rank.set_policy(entity_name_t::TYPE_CLIENT, Rank::Policy::lossy_fast_fail());

  rank.start();

  // start osd
  OSD *osd = new OSD(whoami, m, hbm, &monmap, dev, journaldev);
  if (osd->init() < 0) {
    cout << "error initializing osd" << std::endl;
    return 1;
  }

  rank.wait();

  // done
  delete osd;

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  sprintf(s, "gmon/%d", getpid());
  if (mkdir(s, 0755) == 0)
    chdir(s);

  return 0;
}

