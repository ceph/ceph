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
  cerr << "usage: cmon -i monid [--mon-data=pathtodata] [flags]" << std::endl;
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
  common_init(args, "mon", true, true);

  // whoami
  char *end;
  int whoami = strtol(g_conf.id, &end, 10);
  if (*end || end == g_conf.id || whoami < 0) {
    cerr << "must specify '-i #' where # is the mon number" << std::endl;
    usage();
  }

  if (!g_conf.mon_data) {
    cerr << "must specify '--mon-data=foo' data path" << std::endl;
    usage();
  }

  if (g_conf.clock_tare) g_clock.tare();

  MonitorStore store(g_conf.mon_data);
  err = store.mount();
  if (err < 0) {
    char buf[80];
    cerr << "problem opening monitor store in " << g_conf.mon_data << ": " << strerror_r(-err, buf, sizeof(buf)) << std::endl;
    exit(1);
  }

  // whoami?
  if (!store.exists_bl_ss("whoami")) {
    cerr << "mon fs missing 'whoami'" << std::endl;
    exit(1);
  }
  int w = store.get_int("whoami");
  if (w != whoami) {
    cerr << "monitor data is for mon" << w << ", but you said i was mon" << whoami << std::endl;
    exit(1);
  }

  bufferlist magicbl;
  store.get_bl_ss(magicbl, "magic", 0);
  nstring magic(magicbl.length()-1, magicbl.c_str());  // ignore trailing \n
  if (strcmp(magic.c_str(), CEPH_MON_ONDISK_MAGIC)) {
    cerr << "mon fs magic '" << magic << "' != current '" << CEPH_MON_ONDISK_MAGIC << "'" << std::endl;
    exit(1);
  }

  // monmap?
  MonMap monmap;
  {
    bufferlist latest;
    store.get_bl_ss(latest, "monmap/latest", 0);
    if (latest.length() == 0) {
      cerr << "mon fs missing 'monmap'" << std::endl;
      exit(1);
    }
    bufferlist::iterator p = latest.begin();
    version_t v;
    ::decode(v, p);
    bufferlist mapbl;
    ::decode(mapbl, p);
    monmap.decode(mapbl);
    assert(v == monmap.get_epoch());
  }

  if ((unsigned)whoami >= monmap.size() || whoami < 0) {
    cerr << "mon" << whoami << " does not exist in monmap" << std::endl;
    exit(1);
  }

  entity_addr_t ipaddr = monmap.get_inst(whoami).addr;
  entity_addr_t conf_addr;
  char *mon_addr_str;

  if (conf_read_key(NULL, "mon addr", OPT_STR, &mon_addr_str, NULL) &&
      parse_ip_port(mon_addr_str, conf_addr, NULL) &&
      ipaddr != conf_addr)
    cerr << "WARNING: 'mon addr' config option does not match monmap file" << std::endl
	 << "         continuing with monmap configuration" << std::endl;

  // bind
  SimpleMessenger *rank = new SimpleMessenger();

  cout << "starting mon" << whoami 
       << " at " << monmap.get_inst(whoami).addr
       << " mon_data " << g_conf.mon_data
       << " fsid " << monmap.get_fsid()
       << std::endl;
  g_my_addr = monmap.get_inst(whoami).addr;
  err = rank->bind();
  if (err < 0)
    return 1;

  _dout_create_courtesy_output_symlink("mon", whoami);
  
  // start monitor
  Messenger *m = rank;
  rank->register_entity(entity_name_t::MON(whoami));
  m->set_default_send_priority(CEPH_MSG_PRIO_HIGH);
  Monitor *mon = new Monitor(whoami, &store, m, &monmap);

  rank->start();  // may daemonize

  rank->set_default_policy(SimpleMessenger::Policy::stateless_server());
  rank->set_policy(entity_name_t::TYPE_MON, SimpleMessenger::Policy::lossless_peer());

  mon->init();
  rank->wait();

  store.umount();
  delete mon;

  // cd on exit, so that gmon.out (if any) goes into a separate directory for each node.
  char s[20];
  sprintf(s, "gmon/%d", getpid());
  if (mkdir(s, 0755) == 0)
    chdir(s);

  return 0;
}

