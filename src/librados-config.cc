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

#define __STDC_FORMAT_MACROS
#include "config.h"

#include "common/common_init.h"
#include "include/librados.h"

void usage()
{
  cout << "usage: librados-config [option]\n"
       << "where options are:\n"
       << "  --version                    library version\n"
       << "  --vernum                     library version code\n";
}

void usage_exit()
{
  assert(1);
  usage();
  exit(1);
}
int main(int argc, const char **argv) 
{
  vector<const char*> args;
  DEFINE_CONF_VARS(usage_exit);
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_set_defaults(false);
  // common_init(args, "librados-config", false);  /* this overrides --version.. */
  set_foreground_logging();

  bool opt_version = false;
  bool opt_vernum = false;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("version", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&opt_version, OPT_BOOL);
    } else if (CONF_ARG_EQ("vernum", '\0')) {
      CONF_SAFE_SET_ARG_VAL(&opt_vernum, OPT_BOOL);
    } else {
      usage_exit();
    }
  }
  if (!opt_version && !opt_vernum)
    usage_exit();

  if (opt_version) {
    int maj, min, ext;
    librados_version(&maj, &min, &ext);
    cout << maj << "." << min << "." << ext << std::endl;
  } else if (opt_vernum) {
    cout << hex << LIBRADOS_VERSION_CODE << dec << std::endl;
  }

  return 0;
}

