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

#include "osd/OSD.h"
#include "ebofs/Ebofs.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"


class C_Die : public Context {
public:
  void finish(int) {
    cerr << "die" << std::endl;
    exit(1);
  }
};

class C_Debug : public Context {
  public:
  void finish(int) {
    int size = &g_conf.debug_after - &g_conf.debug;
    memcpy((char*)&g_conf.debug, (char*)&g_debug_after_conf.debug, size);
    cout << "debug_after flipping debug settings" << std::endl;
  }
};


int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  parse_config_options(args);

  if (g_conf.kill_after) 
    g_timer.add_event_after(g_conf.kill_after, new C_Die);
  if (g_conf.debug_after) 
    g_timer.add_event_after(g_conf.debug_after, new C_Debug);

  if (g_conf.clock_tare) g_clock.tare();

  // osd specific args
  const char *dev = 0;
  char dev_default[20];
  int whoami = -1;
  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i],"--dev") == 0) 
      dev = args[++i];
    else if (strcmp(args[i],"--osd") == 0)
      whoami = atoi(args[++i]);
    else {
      cerr << "unrecognized arg " << args[i] << std::endl;
      return -1;
    }
  }
  if (whoami < 0) {
    cerr << "must specify '--osd #' where # is the osd number" << std::endl;
  }
  if (!dev) {
    sprintf(dev_default, "dev/osd%d", whoami);
    dev = dev_default;
  }
  cout << "dev " << dev << std::endl;
  

  if (whoami < 0) {
    // who am i?   peek at superblock!
    OSDSuperblock sb;
    ObjectStore *store = new Ebofs(dev);
    bufferlist bl;
    store->mount();
    int r = store->read(object_t(0,0), 0, sizeof(sb), bl);
    if (r < 0) {
      cerr << "couldn't read superblock object on " << dev << std::endl;
      exit(0);
    }
    bl.copy(0, sizeof(sb), (char*)&sb);
    store->umount();
    delete store;
    whoami = sb.whoami;
    
    cout << "osd fs says i am osd" << whoami << std::endl;
  } else {
    cout << "command line arg says i am osd" << whoami << std::endl;
  }

  // load monmap
  MonMap monmap;
  int r = monmap.read(".ceph_monmap");
  assert(r >= 0);

  // start up network
  rank.start_rank();

  // start osd
  Messenger *m = rank.register_entity(entity_name_t::OSD(whoami));
  assert(m);
  OSD *osd = new OSD(whoami, m, &monmap, dev);
  osd->init();

  // wait
  rank.wait();

  // done
  delete osd;

  return 0;
}

