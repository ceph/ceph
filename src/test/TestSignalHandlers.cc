// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * TestSignalHandlers
 *
 * Test the Ceph signal handlers
 */
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/config.h"

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

using std::string;

// avoid compiler warning about dereferencing NULL pointer
static int* get_null()
{
  return 0;
}

static void simple_segv_test()
{
  generic_dout(-1) << "triggering SIGSEGV..." << dendl;
  // cppcheck-suppress nullPointer
  int i = *get_null();
  std::cout << "i = " << i << std::endl;
}

static void infinite_recursion_test_impl()
{
  infinite_recursion_test_impl();
}

static void infinite_recursion_test()
{
  generic_dout(0) << "triggering SIGSEGV with infinite recursion..." << dendl;
  infinite_recursion_test_impl();
}

static void usage()
{
  cerr << "usage: TestSignalHandlers [test]" << std::endl;
  cerr << "--simple_segv: run simple_segv test" << std::endl;
  cerr << "--infinite_recursion: run infinite_recursion test" << std::endl;
  generic_client_usage(); // Will exit()
}

typedef void (*test_fn_t)(void);

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  test_fn_t fn = NULL;
  for (std::vector<const char*>::iterator i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
    } else if (ceph_argparse_flag(args, i, "--infinite_recursion", (char*)NULL)) {
      fn = infinite_recursion_test;
    } else if (ceph_argparse_flag(args, i, "-s", "--simple_segv", (char*)NULL)) {
      fn = simple_segv_test;
    } else {
      cerr << "Garbage at end of command line." << std::endl;
      usage();
    }
  }
  if (!fn) {
    std::cerr << "Please select a test to run. Type -h for help." << std::endl;
    usage();
  }
  fn();
  return 0;
}
