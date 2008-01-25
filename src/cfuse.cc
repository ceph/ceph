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

#include <sys/stat.h>
#include <iostream>
#include <string>
using namespace std;

#include "config.h"

#include "client/Client.h"
#include "client/fuse.h"
#include "client/fuse_ll.h"

#include "msg/SimpleMessenger.h"

#include "common/Timer.h"
       
#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, const char **argv, const char *envp[]) {

  //cerr << "cfuse starting " << myrank << "/" << world << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args for fuse
  vec_to_argv(args, argc, argv);

  // FUSE will chdir("/"); be ready.
  g_conf.use_abspaths = true;

  if (g_conf.clock_tare) g_clock.tare();

  // load monmap
  MonMap monmap;
  int r = monmap.read(".ceph_monmap");
  assert(r >= 0);

  // start up network
  rank.start_rank();

  // start client
  Client *client = new Client(rank.register_entity(entity_name_t::CLIENT()), &monmap);
  client->init();
    
  // start up fuse
  // use my argc, argv (make sure you pass a mount point!)
  cout << "mounting" << std::endl;
  client->mount();

  create_courtesy_output_symlink("client", client->get_nodeid());
  
  //cerr << "starting fuse on pid " << getpid() << std::endl;
  if (g_conf.fuse_ll)
    ceph_fuse_ll_main(client, argc, argv);
  else
    ceph_fuse_main(client, argc, argv);
  //cerr << "fuse finished on pid " << getpid() << std::endl;
  
  client->unmount();
  cout << "unmounted" << std::endl;
  client->shutdown();
  
  delete client;
  
  // wait for messenger to finish
  rank.wait();
  
  return 0;
}

