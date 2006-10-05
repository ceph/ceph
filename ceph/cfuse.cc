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



#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"
#include "client/fuse.h"

#include "msg/NewMessenger.h"

#include "common/Timer.h"
       
#include <envz.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, char **argv, char *envp[]) {

  //cerr << "cfuse starting " << myrank << "/" << world << endl;
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args for fuse
  vec_to_argv(args, argc, argv);

  // load monmap
  bufferlist bl;
  int fd = ::open(".ceph_monmap", O_RDONLY);
  assert(fd >= 0);
  struct stat st;
  ::fstat(fd, &st);
  bufferptr bp = new buffer(st.st_size);
  bl.append(bp);
  ::read(fd, (void*)bl.c_str(), bl.length());
  ::close(fd);
  
  MonMap *monmap = new MonMap;
  monmap->decode(bl);

  // start up network
  rank.set_namer(monmap->get_inst(0).addr);
  rank.start_rank();

  // start client
  Client *client = new Client(rank.register_entity(MSG_ADDR_CLIENT_NEW), monmap);
  client->init();
    
  // start up fuse
  // use my argc, argv (make sure you pass a mount point!)
  cout << "mounting" << endl;
  client->mount();
  
  cerr << "starting fuse on pid " << getpid() << endl;
  ceph_fuse_main(client, argc, argv);
  cerr << "fuse finished on pid " << getpid() << endl;
  
  client->unmount();
  cout << "unmounted" << endl;
  client->shutdown();
  
  delete client;
  
  // wait for messenger to finish
  rank.wait();
  

  return 0;
}

