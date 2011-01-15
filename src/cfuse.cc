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

#include "mon/MonClient.h"

#include "common/Timer.h"
#include "common/common_init.h"
       
#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

void usage()
{
  cerr << "usage: cfuse [-m mon-ip-addr:mon-port] <mount point>" << std::endl;
}

int main(int argc, const char **argv, const char *envp[]) {

  //cerr << "cfuse starting " << myrank << "/" << world << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_set_defaults(false);
  g_conf.daemonize = true;
  g_conf.log_per_instance = true;
  common_init(args, "cfuse", true);

  // args for fuse
  vec_to_argv(args, argc, argv);

  // FUSE will chdir("/"); be ready.
  g_conf.chdir = strdup("/");

  if (g_conf.clock_tare) g_clock.tare();

  // check for 32-bit arch
  if (sizeof(long) == 4) {
    cerr << std::endl;
    cerr << "WARNING: Ceph inode numbers are 64 bits wide, and FUSE on 32-bit kernels does" << std::endl;
    cerr << "         not cope well with that situation.  Expect to crash shortly." << std::endl;
    cerr << std::endl;
  }

  // get monmap
  MonClient mc;
  int ret = mc.build_initial_monmap();
  if (ret == -EINVAL)
    usage();

  if (ret < 0)
    return -1;

  // start up network
  SimpleMessenger *messenger = new SimpleMessenger();
  messenger->register_entity(entity_name_t::CLIENT());
  Client *client = new Client(messenger, &mc);

  // we need to handle the forking ourselves.
  bool daemonize = g_conf.daemonize;
  g_conf.daemonize = false;

  int fd[2] = {0, 0};  // parent's, child's
  pid_t childpid = 0;
  if (daemonize) {
    int r = socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
    if (r < 0) {
      cerr << "cfuse[" << getpid() << "]: unable to create socketpair: " << strerror(errno) << std::endl;
      exit(1);
    }

    childpid = fork();
  }

  if (childpid == 0) {
    //cout << "child, mounting" << std::endl;
    ::close(fd[0]);

    cout << "cfuse[" << getpid() << "]: starting ceph client" << std::endl;

    messenger->start();

    // start client
    client->init();
    
    // start up fuse
    // use my argc, argv (make sure you pass a mount point!)
    int r = client->mount(g_conf.client_mountpoint);
    if (r < 0) {
      cerr << "cfuse[" << getpid() << "]: ceph mount failed with " << strerror(-r) << std::endl;
      goto out_shutdown;
    }
    
    dout_create_rank_symlink(client->get_nodeid().v);
    cerr << "cfuse[" << getpid() << "]: starting fuse" << std::endl;
    if (g_conf.fuse_ll)
      r = ceph_fuse_ll_main(client, argc, argv, fd[1]);
    else
      r = ceph_fuse_main(client, argc, argv);
    cerr << "cfuse[" << getpid() << "]: fuse finished with error " << r << std::endl;
    
    client->unmount();
    //cout << "unmounted" << std::endl;
    
  out_shutdown:
    client->shutdown();
    delete client;
    
    // wait for messenger to finish
    messenger->wait();

    if (daemonize) {
      //cout << "child signalling parent with " << r << std::endl;
      ::write(fd[1], &r, sizeof(r));
    }

    //cout << "child done" << std::endl;
    return r;
  } else {
    // i am the parent
    //cout << "parent, waiting for signal" << std::endl;
    ::close(fd[1]);

    int r = -1;
    ::read(fd[0], &r, sizeof(r));
    if (r == 0) {
      // close stdout, etc.
      //cout << "success" << std::endl;
      ::close(0);
      ::close(1);
      ::close(2);
    } else {
      cerr << "cfuse[" << getpid() << "]: mount failed: " << strerror(-r) << std::endl;
    }
    return r;
  }
}

