// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */


#include "include/types.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"

#include "JournalTool.h"


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			     CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  JournalTool jt;

  // Handle --help before calling init() so we don't depend on network.
  if (args.empty() || (args.size() == 1 && (std::string(args[0]) == "--help" || std::string(args[0]) == "-h"))) {
    jt.usage();
    return 0;
  }

  // Connect to mon cluster, download MDS map etc
  int rc = jt.init();
  if (rc != 0) {
      std::cerr << "Error in initialization: " << cpp_strerror(rc) << std::endl;
      return rc;
  }

  // Finally, execute the user's commands
  rc = jt.main(args);
  if (rc != 0) {
    std::cerr << "Error (" << cpp_strerror(rc) << ")" << std::endl;
  }

  jt.shutdown();

  return rc;
}

