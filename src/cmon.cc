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
#include "mon/Monitor.h"
#include "mon/MonitorStore.h"

#include "msg/SimpleMessenger.h"

#include "include/nstring.h"

#include "common/Timer.h"
#include "common/common_init.h"

void usage()
{
  cerr << "usage: cmon [flags] <monfsdir>" << std::endl;
  cerr << "  --debug_mon n\n";
  cerr << "        debug monitor level (e.g. 10)\n";
  generic_server_usage();
}

int main(int argc, const char **argv) 
{
  int err;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  configure_daemon_mode();
  common_init(args, "mon");

  // args
  const char *fsdir = 0;
  for (unsigned i=0; i<args.size(); i++) {
    if (args[i][0] != '-') {
      if (!fsdir)
        fsdir = args[i];
      else if (fsdir)
        usage();
    }
  }

  if (!fsdir)
    usage();

  if (g_conf.clock_tare) g_clock.tare();

  MonitorStore store(fsdir);
  err = store.mount();
  if (err < 0) {
    cerr << "problem opening monitor store in " << fsdir << ": " << strerror(-err) << std::endl;
    exit(1);
  }

  // whoami?
  if (!store.exists_bl_ss("whoami")) {
    cerr << "mon fs missing 'whoami'" << std::endl;
    exit(1);
  }
  int whoami = store.get_int("whoami");

  bufferlist magicbl;
  store.get_bl_ss(magicbl, "magic", 0);
  nstring magic(magicbl.length()-1, magicbl.c_str());  // ignore trailing \n
  if (strcmp(magic.c_str(), CEPH_MON_ONDISK_MAGIC)) {
    cerr << "mon fs magic '" << magic << "' != current '" << CEPH_MON_ONDISK_MAGIC << "'" << std::endl;
    exit(1);
  }

  // monmap?
  bufferlist mapbl;
  store.get_bl_ss(mapbl, "monmap/latest", 0);
  if (mapbl.length() == 0) {
    cerr << "mon fs missing 'monmap'" << std::endl;
    exit(1);
  }
  MonMap monmap;
  monmap.decode(mapbl);

  if ((unsigned)whoami >= monmap.size() || whoami < 0) {
    cerr << "mon" << whoami << " does not exist in monmap" << std::endl;
    exit(1);
  }

  // bind
  cout << "starting mon" << whoami 
       << " at " << monmap.get_inst(whoami).addr
       << " from " << fsdir << std::endl;
  g_my_addr = monmap.get_inst(whoami).addr;
  err = rank.bind();
  if (err < 0)
    return 1;

  _dout_create_courtesy_output_symlink("mon", whoami);
  
  // start monitor
  Messenger *m = rank.register_entity(entity_name_t::MON(whoami));
  m->set_default_send_priority(CEPH_MSG_PRIO_HIGH);
  Monitor *mon = new Monitor(whoami, &store, m, &monmap);

  rank.start();  // may daemonize

  rank.set_policy(entity_name_t::TYPE_MON, Rank::Policy::lossless());

  rank.set_policy(entity_name_t::TYPE_MDS, Rank::Policy::lossy_fast_fail());
  rank.set_policy(entity_name_t::TYPE_CLIENT, Rank::Policy::lossy_fast_fail());
  rank.set_policy(entity_name_t::TYPE_OSD, Rank::Policy::lossy_fast_fail());
  rank.set_policy(entity_name_t::TYPE_ADMIN, Rank::Policy::lossy_fast_fail());


  mon->init();
  rank.wait();

  store.umount();
  delete mon;

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  sprintf(s, "gmon/%d", getpid());
  if (mkdir(s, 0755) == 0)
    chdir(s);

  return 0;
}

