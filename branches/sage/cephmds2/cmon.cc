// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"


class C_Die : public Context {
public:
  void finish(int) {
    cerr << "die" << endl;
    exit(1);
  }
};

class C_Debug : public Context {
  public:
  void finish(int) {
    int size = &g_conf.debug_after - &g_conf.debug;
    memcpy((char*)&g_conf.debug, (char*)&g_debug_after_conf.debug, size);
    dout(0) << "debug_after flipping debug settings" << endl;
  }
};


int main(int argc, char **argv) 
{
  vector<char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);
  if (g_conf.debug_after) 
    g_timer.add_event_after(g_conf.debug_after, new C_Debug);

  // let's assume a standalone monitor
  // FIXME: we want to start a cluster, eventually.
  cout << "starting standalone mon0" << endl;

  // start messenger
  rank.start_rank();

  // create monmap
  MonMap *monmap = new MonMap(1);
  monmap->mon_inst[0] = rank.my_inst;

  cout << "bound to " << rank.get_listen_addr() << endl;
  
  // start monitor
  Messenger *m = rank.register_entity(MSG_ADDR_MON(0));
  Monitor *mon = new Monitor(0, m, monmap);
  mon->init();

  // write monmap
  cout << "writing monmap to .ceph_monmap" << endl;
  bufferlist bl;
  monmap->encode(bl);
  int fd = ::open(".ceph_monmap", O_RDWR|O_CREAT);
  assert(fd >= 0);
  ::fchmod(fd, 0644);
  ::write(fd, (void*)bl.c_str(), bl.length());
  ::close(fd);

  // wait
  cout << "waiting for shutdown ..." << endl;
  rank.wait();

  // done
  delete mon;

  return 0;
}

