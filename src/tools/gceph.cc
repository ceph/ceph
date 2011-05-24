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

#include "common/common_init.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "tools/common.h"

#include <iostream>
#include <sstream>
#include <vector>

// tool/gui.cc
int run_gui(int argc, char **argv);

using std::vector;

static std::ostringstream gss;

static void usage()
{
  derr << "usage: gceph [options]" << dendl;
  derr << dendl;
  derr << "Runs the ceph graphical monitor" << dendl;
  generic_client_usage(); // Will exit()
}

static void parse_gceph_args(const vector<const char*> &args)
{
  DEFINE_CONF_VARS(usage);
  FOR_EACH_ARG(args) {
    if (CEPH_ARGPARSE_EQ("help", 'h')) {
      usage();
    }
  }
}

static int cephtool_run_gui(int argc, const char **argv)
{
  g.log = &gss;
  g.slog = &gss;

  // TODO: make sure that we capture the log this generates in the GUI
  g.lock.Lock();
  send_observe_requests();
  g.lock.Unlock();

  return run_gui(argc, (char **)argv);
}

void ceph_tool_common_shutdown_wrapper()
{
  ceph_tool_messenger_shutdown();
  ceph_tool_common_shutdown();
}

int main(int argc, const char **argv)
{
  int ret = 0;

  DEFINE_CONF_VARS(usage);
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_init(args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(&g_ceph_context);

  vec_to_argv(args, argc, argv);

  parse_gceph_args(args);

  if (ceph_tool_common_init(CEPH_TOOL_MODE_GUI)) {
    derr << "cephtool_common_init failed." << dendl;
    return 1;
  }

  atexit(ceph_tool_common_shutdown_wrapper);

  if (cephtool_run_gui(argc, argv))
    ret = 1;

  if (ceph_tool_messenger_shutdown())
    ret = 1;

  return ret;
}
