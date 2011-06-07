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

#include "common/config.h"

#include "client/Client.h"
#include "client/fuse_ll.h"

#include "msg/SimpleMessenger.h"

#include "mon/MonClient.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/safe_io.h"
       
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
  DEFINE_CONF_VARS(usage);
  int filer_flags = 0;
  //cerr << "cfuse starting " << myrank << "/" << world << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  vector<const char*> nargs;
  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("localize-reads", '\0')) {
      cerr << "setting CEPH_OSD_FLAG_LOCALIZE_READS" << std::endl;
      filer_flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
    }
    else {
      nargs.push_back(args[i]);
    }
  }

  // args for fuse
  vec_to_argv(nargs, argc, argv);

  // FUSE will chdir("/"); be ready.
  g_conf->chdir = "/";

  // check for 32-bit arch
  if (sizeof(long) == 4) {
    cerr << std::endl;
    cerr << "WARNING: Ceph inode numbers are 64 bits wide, and FUSE on 32-bit kernels does" << std::endl;
    cerr << "         not cope well with that situation.  Expect to crash shortly." << std::endl;
    cerr << std::endl;
  }

  // get monmap
  MonClient mc(&g_ceph_context);
  int ret = mc.build_initial_monmap();
  if (ret == -EINVAL)
    usage();

  if (ret < 0)
    return -1;

  // start up network
  SimpleMessenger *messenger = new SimpleMessenger();
  messenger->register_entity(entity_name_t::CLIENT());
  Client *client = new Client(messenger, &mc);
  if (filer_flags) {
    client->set_filer_flags(filer_flags);
  }

  // we need to handle the forking ourselves.
  int fd[2] = {0, 0};  // parent's, child's
  pid_t childpid = 0;
  if (g_conf->daemonize) {
    int r = socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
    if (r < 0) {
      cerr << "cfuse[" << getpid() << "]: unable to create socketpair: " << strerror(errno) << std::endl;
      exit(1);
    }

    childpid = fork();
  }

  common_init_finish(&g_ceph_context);

  if (childpid == 0) {
    //cout << "child, mounting" << std::endl;
    ::close(fd[0]);

    cout << "cfuse[" << getpid() << "]: starting ceph client" << std::endl;
    messenger->start_with_nonce(getpid());

    // start client
    client->init();
    
    // start up fuse
    // use my argc, argv (make sure you pass a mount point!)
    int r = client->mount(g_conf->client_mountpoint.c_str());
    if (r < 0) {
      cerr << "cfuse[" << getpid() << "]: ceph mount failed with " << strerror(-r) << std::endl;
      goto out_shutdown;
    }
    
    cerr << "cfuse[" << getpid() << "]: starting fuse" << std::endl;
    r = ceph_fuse_ll_main(client, argc, argv, fd[1]);
    cerr << "cfuse[" << getpid() << "]: fuse finished with error " << r << std::endl;
    
    client->unmount();
    //cout << "unmounted" << std::endl;
    
  out_shutdown:
    client->shutdown();
    delete client;
    
    // wait for messenger to finish
    messenger->wait();

    if (g_conf->daemonize) {
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
    int err = safe_read_exact(fd[0], &r, sizeof(r));
    if (err == 0 && r == 0) {
      // close stdout, etc.
      //cout << "success" << std::endl;
      ::close(0);
      ::close(1);
      ::close(2);
    } else if (err)
      cerr << "cfuse[" << getpid() << "]: mount failed: " << strerror(-err) << std::endl;
    else
      cerr << "cfuse[" << getpid() << "]: mount failed: " << strerror(-r) << std::endl;
    return r;
  }
}

