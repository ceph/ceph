// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/config.h"

#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "global/global_context.h"
#include "include/rados/librados.h"

void usage()
{
  cout << "usage: librados-config [option]\n"
       << "where options are:\n"
       << "  --version                    library version\n"
       << "  --vernum                     library version code\n";
}

void usage_exit()
{
  usage();
  exit(1);
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  bool opt_version = false;
  bool opt_vernum = false;

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
	      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  for (std::vector<const char*>::iterator i = args.begin();
       i != args.end(); ) {
    if (strcmp(*i, "--") == 0) {
      break;
    }
    else if (strcmp(*i, "--version") == 0) {
      opt_version = true;
      i = args.erase(i);
    }
    else if (strcmp(*i, "--vernum") == 0) {
      opt_vernum = true;
      i = args.erase(i);
    }
    else
      ++i;
  }

  if (!opt_version && !opt_vernum)
    usage_exit();

  if (opt_version) {
    int maj, min, ext;
    rados_version(&maj, &min, &ext);
    cout << maj << "." << min << "." << ext << std::endl;
  } else if (opt_vernum) {
    cout << hex << LIBRADOS_VERSION_CODE << dec << std::endl;
  }

  return 0;
}

