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

#include "mds/MDCluster.h"
#include "mds/MDS.h"
#include "osd/OSD.h"
#include "client/Client.h"
#include "client/fuse.h"

#include "msg/TCPMessenger.h"

#include "common/Timer.h"
       
#include <envz.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int main(int argc, char **argv, char *envp[]) {

  //cerr << "tcpfuse starting " << myrank << "/" << world << endl;
  vector<char*> args;
  argv_to_vec(argc, argv, args);
  parse_config_options(args);

  // args for fuse
  vec_to_argv(args, argc, argv);

  // start up tcpmessenger
  tcpaddr_t nsa;
  if (tcpmessenger_findns(nsa) < 0) exit(1);
  tcpmessenger_init();
  tcpmessenger_start();
  tcpmessenger_start_rankserver(nsa);
  
  Client *client = new Client(new TCPMessenger(MSG_ADDR_CLIENT_NEW));
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
  
  // wait for it to finish
  tcpmessenger_wait();
  tcpmessenger_shutdown();  // shutdown MPI

  return 0;
}

