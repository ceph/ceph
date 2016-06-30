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

#include "mgr/Mgr.h"

#include "include/types.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  global_init(NULL, args, CEPH_ENTITY_TYPE_MGR, CODE_ENVIRONMENT_DAEMON, 0,
              "mgr_data");
  common_init_finish(g_ceph_context);
  // For consumption by KeyRing::from_ceph_context in MonClient
  g_conf->set_val("keyring", "$mgr_data/keyring", false);

  Mgr mgr;

  // Handle --help before calling init() so we don't depend on network.
  if ((args.size() == 1 && (std::string(args[0]) == "--help" || std::string(args[0]) == "-h"))) {
    mgr.usage();
    return 0;
  }

  global_init_daemonize(g_ceph_context);
  global_init_chdir(g_ceph_context);
  common_init_finish(g_ceph_context);

  // Connect to mon cluster, download MDS map etc
  int rc = mgr.init();
  if (rc != 0) {
      std::cerr << "Error in initialization: " << cpp_strerror(rc) << std::endl;
      return rc;
  }

  // Finally, execute the user's commands
  rc = mgr.main(args);
  if (rc != 0) {
    std::cerr << "Error (" << cpp_strerror(rc) << ")" << std::endl;
  }

  mgr.shutdown();

  return rc;
}

