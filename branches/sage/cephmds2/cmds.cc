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
#include "mds/MDS.h"

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

  // mds specific args
  bool standby = false;  // by default, i'll start active.
  for (unsigned i=0; i<args.size(); i++) {
    if (strcmp(args[i], "--standby") == 0) 
      standby = true;
    else {
      cerr << "unrecognized arg " << args[i] << endl;
      return -1;
    }
  }


  // load monmap
  bufferlist bl;
  int fd = ::open(".ceph_monmap", O_RDONLY);
  assert(fd >= 0);
  struct stat st;
  ::fstat(fd, &st);
  bufferptr bp(st.st_size);
  bl.append(bp);
  ::read(fd, (void*)bl.c_str(), bl.length());
  ::close(fd);
  
  MonMap *monmap = new MonMap;
  monmap->decode(bl);

  // start up network
  rank.start_rank();

  // start mds
  Messenger *m = rank.register_entity(MSG_ADDR_MDS_NEW);
  assert(m);
  
  MDS *mds = new MDS(m->get_myaddr().num(), m, monmap);
  mds->init(standby);
  
  // wait
  rank.wait();

  // done
  delete mds;

  return 0;
}

