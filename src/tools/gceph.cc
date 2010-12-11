// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "config.h"

#include <iostream>
#include <vector>

// tool/gui.cc
int run_gui(int argc, char **argv);

using std::cerr;
using std::vector;

static std::ostringstream gss;

static void usage()
{
  cerr << "usage: gceph [options]" << std::endl;
  cerr << std::endl;
  cerr << "Runs the ceph graphical monitor" << std::endl;
  generic_client_usage(); // Will exit()
}

static int cephtool_run_gui()
{
  g.log = &gss;
  g.slog = &gss;

  // TODO: make sure that we capture the log this generates in the GUI
  g.lock.Lock();
  send_observe_requests();
  g.lock.Unlock();

  return run_gui(argc, (char **)argv);
}

int main(int argc, const char **argv)
{
  int ret = 0;

  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  ceph_set_default_id("admin");

  common_set_defaults(false);
  common_init(args, "cephtool", true);

  vec_to_argv(args, argc, argv);

  if (cephtool_common_init(CEPH_TOOL_MODE_GUI)) {
    cerr << "cephtool_common_init failed." << std::endl;
    return 1;
  }

  if (cephtool_run_gui())
    ret = 1;

  if (ceph_tool_messenger_shutdown())
    ret = 1;

  if (cephtool_common_shutdown())
    ret = 1;

  return ret;
}
