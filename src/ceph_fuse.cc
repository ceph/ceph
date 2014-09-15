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
#include "common/errno.h"

#include "client/Client.h"
#include "client/fuse_ll.h"

#include "msg/Messenger.h"

#include "mon/MonClient.h"

#include "common/Timer.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/safe_io.h"
       
#ifndef DARWIN
#include <envz.h>
#endif // DARWIN

#include <sys/types.h>
#include <fcntl.h>

void usage()
{
  cerr << "usage: ceph-fuse [-m mon-ip-addr:mon-port] <mount point>" << std::endl;
}

int main(int argc, const char **argv, const char *envp[]) {
  int filer_flags = 0;
  //cerr << "ceph-fuse starting " << myrank << "/" << world << std::endl;
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "--localize-reads", (char*)NULL)) {
      cerr << "setting CEPH_OSD_FLAG_LOCALIZE_READS" << std::endl;
      filer_flags |= CEPH_OSD_FLAG_LOCALIZE_READS;
    } else {
      ++i;
    }
  }

  // args for fuse
  const char **newargv;
  int newargc;
  vec_to_argv(argv[0], args, &newargc, &newargv);

  // FUSE will chdir("/"); be ready.
  g_ceph_context->_conf->set_val("chdir", "/");
  g_ceph_context->_conf->apply_changes(NULL);

  // check for 32-bit arch
  if (sizeof(long) == 4) {
    cerr << std::endl;
    cerr << "WARNING: Ceph inode numbers are 64 bits wide, and FUSE on 32-bit kernels does" << std::endl;
    cerr << "         not cope well with that situation.  Expect to crash shortly." << std::endl;
    cerr << std::endl;
  }

  // we need to handle the forking ourselves.
  int fd[2] = {0, 0};  // parent's, child's
  pid_t childpid = 0;
  bool restart_log = false;
  if (g_conf->daemonize) {
    int r = socketpair(AF_UNIX, SOCK_STREAM, 0, fd);
    if (r < 0) {
      cerr << "ceph-fuse[" << getpid() << "]: unable to create socketpair: " << cpp_strerror(errno) << std::endl;
      exit(1);
    }

    g_ceph_context->_log->stop();
    restart_log = true;

    childpid = fork();
  }

  if (childpid == 0) {
    common_init_finish(g_ceph_context);

    //cout << "child, mounting" << std::endl;
    ::close(fd[0]);

    if (restart_log)
      g_ceph_context->_log->start();

    // get monmap
    Messenger *messenger = NULL;
    Client *client;
    CephFuse *cfuse;

    MonClient mc(g_ceph_context);
    int r = mc.build_initial_monmap();
    if (r == -EINVAL)
      usage();
    if (r < 0)
      goto out_mc_start_failed;

    // start up network
    messenger = Messenger::create(g_ceph_context,
				  entity_name_t::CLIENT(), "client",
				  getpid());
    messenger->set_default_policy(Messenger::Policy::lossy_client(0, 0));
    messenger->set_policy(entity_name_t::TYPE_MDS,
			  Messenger::Policy::lossless_client(0, 0));

    client = new Client(messenger, &mc);
    if (filer_flags) {
      client->set_filer_flags(filer_flags);
    }

    cfuse = new CephFuse(client, fd[1]);

    r = cfuse->init(newargc, newargv);
    if (r != 0) {
      cerr << "ceph-fuse[" << getpid() << "]: fuse failed to initialize" << std::endl;
      goto out_messenger_start_failed;
    }

    client->set_metadata("mount_point", cfuse->get_mount_point());

    cout << "ceph-fuse[" << getpid() << "]: starting ceph client" << std::endl;
    r = messenger->start();
    if (r < 0) {
      cerr << "ceph-fuse[" << getpid() << "]: ceph mount failed with " << cpp_strerror(-r) << std::endl;
      goto out_messenger_start_failed;
    }

    // start client
    r = client->init();
    if (r < 0) {
      cerr << "ceph-fuse[" << getpid() << "]: ceph mount failed with " << cpp_strerror(-r) << std::endl;
      goto out_init_failed;
    }
    
    // start up fuse
    // use my argc, argv (make sure you pass a mount point!)
    r = client->mount(g_conf->client_mountpoint.c_str());
    if (r < 0) {
      cerr << "ceph-fuse[" << getpid() << "]: ceph mount failed with " << cpp_strerror(-r) << std::endl;
      goto out_shutdown;
    }

    r = cfuse->start();
    if (r != 0) {
      cerr << "ceph-fuse[" << getpid() << "]: fuse failed to start" << std::endl;
      goto out_client_unmount;
    }
    cerr << "ceph-fuse[" << getpid() << "]: starting fuse" << std::endl;
    r = cfuse->loop();
    cerr << "ceph-fuse[" << getpid() << "]: fuse finished with error " << r << std::endl;

  out_client_unmount:
    client->unmount();
    //cout << "unmounted" << std::endl;

    cfuse->finalize();
    delete cfuse;

  out_shutdown:
    client->shutdown();
  out_init_failed:
    // wait for messenger to finish
    messenger->shutdown();
    messenger->wait();
  out_messenger_start_failed:
    delete client;
  out_mc_start_failed:

    if (g_conf->daemonize) {
      //cout << "child signalling parent with " << r << std::endl;
      static int foo = 0;
      foo += ::write(fd[1], &r, sizeof(r));
    }

    delete messenger;
    g_ceph_context->put();
    free(newargv);

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
    } else if (err) {
      cerr << "ceph-fuse[" << getpid() << "]: mount failed: " << cpp_strerror(-err) << std::endl;
    } else {
      cerr << "ceph-fuse[" << getpid() << "]: mount failed: " << cpp_strerror(-r) << std::endl;
    }
    return r;
  }
}

