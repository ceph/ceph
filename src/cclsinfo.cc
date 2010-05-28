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

#include "config.h"

#include "common/common_init.h"
#include "common/arch.h"

#include <iostream>
#include <stdlib.h>
#include <dlfcn.h>

#include "include/rbd_types.h"

void usage()
{
  cout << "usage: cclsinfo [-n] [-v] [-a] <cls_file_name>\n"
       << "\t-n\tlist rbd images\n"
       << "\t-v\tshow information about image size, striping, etc.\n" << std::endl;

  exit(1);
}

static int char_count = 0;
static char print_char_maybe(char c)
{
  if (char_count++)
    return c;

  return  '\0';
}

int main(int argc, const char **argv) 
{
  vector<const char*> args;
  DEFINE_CONF_VARS(usage);
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  common_init(args, "cclsinfo", false, true);

  bool opt_name = false, opt_ver = false, opt_arch = false;
  const char *fname = NULL;

  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("name", 'n')) {
      CONF_SAFE_SET_ARG_VAL(&opt_name, OPT_BOOL);
    } else if (CONF_ARG_EQ("arch", 'a')) {
      CONF_SAFE_SET_ARG_VAL(&opt_arch, OPT_BOOL);
    } else if (CONF_ARG_EQ("ver", 'v')) {
      CONF_SAFE_SET_ARG_VAL(&opt_arch, OPT_BOOL);
    } else  {
      if (fname)
        usage();
      fname = CONF_VAL;
    }
  }

  if (!fname)
    usage();

  if (!opt_name && !opt_ver && !opt_arch) {
    opt_name = true;
    opt_ver = true;
    opt_arch = true;
  }

  void *handle = dlopen(fname, RTLD_LAZY);
  if (!handle) {
    perror("dlopen");
    exit(1);
  }

  const char *cls_name = *(const char **)dlsym(handle, "__cls_name");
  if (!cls_name) {
    cerr << "cls_name not defined, not a ceph class\n" << std::endl;
    dlclose(handle);
    exit(1);
  }

  if (opt_name) {
    cout << print_char_maybe(' ') << cls_name;
  }

  if (opt_ver) {
    int ver_maj = *(int *)dlsym(handle, "__cls_ver_maj");
    int ver_min = *(int *)dlsym(handle, "__cls_ver_min");
    cout << print_char_maybe(' ') << ver_maj << "." << ver_min;
  }

  if (opt_arch) {
    cout << print_char_maybe(' ') << get_arch();
  }

  cout << print_char_maybe('\n');

  dlclose(handle);
  exit(0);

}


