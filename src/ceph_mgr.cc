// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat Inc
 *
 * Author: John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <Python.h>

#include <pthread.h>

#include "include/types.h"
#include "include/compat.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "common/pick_address.h"
#include "global/global_init.h"

#include "mgr/MgrStandby.h"

static void usage()
{
  cout << "usage: ceph-mgr -i <ID> [flags]\n"
       << std::endl;
  generic_server_usage();
}

/**
 * A short main() which just instantiates a MgrStandby and
 * hands over control to that.
 */
int main(int argc, const char **argv)
{
  ceph_pthread_setname(pthread_self(), "ceph-mgr");

  vector<const char*> args;
  argv_to_vec(argc, argv, args);

  map<string,string> defaults = {
    { "keyring", "$mgr_data/keyring" }
  };
  auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_MGR,
			 CODE_ENVIRONMENT_DAEMON, 0,
			 "mgr_data");

  // Handle --help
  if ((args.size() == 1 && (std::string(args[0]) == "--help" ||
			    std::string(args[0]) == "-h"))) {
    usage();
  }

  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);

  global_init_daemonize(g_ceph_context);
  global_init_chdir(g_ceph_context);
  common_init_finish(g_ceph_context);

  MgrStandby mgr(argc, argv);
  int rc = mgr.init();
  if (rc != 0) {
      std::cerr << "Error in initialization: " << cpp_strerror(rc) << std::endl;
      return rc;
  }

  return mgr.main(args);
}

