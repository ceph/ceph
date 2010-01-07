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
  common_init(args, "osd", true, should_authenticate);

  if (g_conf.clock_tare) g_clock.tare();

  // osd specific args
  bool mkfs = false;
  bool mkjournal = false;
  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("mkfs", '\0')) {
      mkfs = true;
    } else if (CONF_ARG_EQ("mkjournal", '\0')) {
      mkjournal = true;
    } else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      ARGS_USAGE();
    }
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

  _dout_create_courtesy_output_symlink("osd", whoami);

  // get monmap
  MonClient mc;
  if (mc.build_initial_monmap() < 0)
    return -1;
  if (mc.get_monmap_privately() < 0)
    return -1;

  char buf[80];
  if (mkfs) {
    int err = OSD::mkfs(g_conf.osd_data, g_conf.osd_journal, mc.monmap.fsid, whoami);
    if (err < 0) {
      cerr << "error creating empty object store in " << g_conf.osd_data << ": " << strerror_r(-err, buf, sizeof(buf)) << std::endl;
      exit(1);
    }
    cout << "created object store for osd" << whoami << " fsid " << mc.monmap.fsid << " on " << g_conf.osd_data << std::endl;
    exit(0);
  }

  nstring magic;
  ceph_fsid_t fsid;
  int w;
  int r = OSD::peek_super(g_conf.osd_data, magic, fsid, w);
  if (r < 0) {
    cerr << TEXT_RED << " ** " << TEXT_HAZARD << "ERROR: " << TEXT_RED
         << "unable to open OSD superblock on " << g_conf.osd_data << ": " << strerror_r(-r, buf, sizeof(buf)) << TEXT_NORMAL << std::endl;
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
  if (ceph_fsid_compare(&fsid, &mc.monmap.fsid)) {
    cerr << "OSD fsid " << fsid << " != monmap fsid " << mc.monmap.fsid << std::endl;
    exit(1);
  }

  // start up network
  g_my_addr.ss_addr() = mc.get_my_addr().ss_addr();
  g_my_addr.set_port(0);

  SimpleMessenger *rank = new SimpleMessenger();
  SimpleMessenger *rank_hb = new SimpleMessenger();
  rank->bind();
  rank_hb->bind();

  cout << "starting osd" << whoami
       << " at " << rank->get_rank_addr() 
       << " osd_data " << g_conf.osd_data
       << " " << ((g_conf.osd_journal && g_conf.osd_journal[0]) ? g_conf.osd_journal:"(no journal)")
       << " fsid " << mc.monmap.fsid
       << std::endl;

  g_timer.shutdown();
  rank->register_entity(entity_name_t::OSD(whoami));
  Messenger *m = rank;
  assert_warn(m);
  if (!m)
    return 1;
  rank_hb->register_entity(entity_name_t::OSD(whoami));
  Messenger *hbm = rank_hb;
  assert_warn(hbm);
  if (!hbm)
    return 1;

  rank->set_default_policy(SimpleMessenger::Policy::stateless_server());
  rank->set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::client());
  rank->set_policy(entity_name_t::TYPE_OSD, SimpleMessenger::Policy::lossless_peer());

  rank->start();
  rank_hb->start(true);  // only need to daemon() once

  // start osd
  OSD *osd = new OSD(whoami, m, hbm, &mc, g_conf.osd_data, g_conf.osd_journal, mkjournal);
  if (osd->init() < 0) {
    cout << "error initializing osd" << std::endl;
    return 1;
  }

  rank->wait();
  rank_hb->wait();
  // done
  delete osd;
  rank->destroy();
  rank_hb->destroy();

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  sprintf(s, "gmon/%d", getpid());
  if (mkdir(s, 0755) == 0)
    chdir(s);

  return 0;
}

