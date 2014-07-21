// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <vector>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "Replayer.hpp"
#include "rbd_replay_debug.hpp"


using namespace std;
using namespace rbd_replay;


static const char* get_remainder(const char *string, const char *prefix) {
  while (*prefix) {
    if (*prefix++ != *string++) {
      return NULL;
    }
  }
  return string;
}

static void usage(const char* program) {
  cout << "Usage: " << program << " --conf=<config_file> <replay_file>" << std::endl;
  cout << "Options:" << endl;
  cout << "  --latency-multiplier <float>    Multiplies inter-request latencies.  Default: 1" << endl;
}

int main(int argc, const char **argv) {
  vector<const char*> args;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);

  std::vector<const char*>::iterator i;
  float latency_multiplier = 1;
  std::string val;
  std::ostringstream err;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_withfloat(args, i, &latency_multiplier, &err, "--latency-multiplier",
				     (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return 1;
      }
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage(argv[0]);
      return 0;
    } else if (get_remainder(*i, "-")) {
      cerr << "Unrecognized argument: " << *i << std::endl;
      return 1;
    } else {
      ++i;
    }
  }

  common_init_finish(g_ceph_context);

  string replay_file;
  if (!args.empty()) {
    replay_file = args[0];
  }

  if (replay_file.empty()) {
    cerr << "No replay file specified." << std::endl;
    return 1;
  }

  Replayer replayer;
  replayer.set_latency_multiplier(latency_multiplier);
  replayer.run(replay_file);
}
