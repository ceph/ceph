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
#include <boost/thread.hpp>
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "Replayer.hpp"
#include "rbd_replay_debug.hpp"
#include "ImageNameMap.hpp"


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
  cout << "Options:" << std::endl;
  cout << "  -p, --pool-name <pool>          Name of the pool to use.  Default: rbd" << std::endl;
  cout << "  --latency-multiplier <float>    Multiplies inter-request latencies.  Default: 1" << std::endl;
  cout << "  --read-only                     Only perform non-destructive operations." << std::endl;
  cout << "  --map-image <rule>              Add a rule to map image names in the trace to" << std::endl;
  cout << "                                  image names in the replay cluster." << std::endl;
  cout << "  --dump-perf-counters            *Experimental*" << std::endl;
  cout << "                                  Dump performance counters to standard out before" << std::endl;
  cout << "                                  an image is closed. Performance counters may be dumped" << std::endl;
  cout << "                                  multiple times if multiple images are closed, or if" << std::endl;
  cout << "                                  the same image is opened and closed multiple times." << std::endl;
  cout << "                                  Performance counters and their meaning may change between" << std::endl;
  cout << "                                  versions." << std::endl;
  cout << std::endl;
  cout << "Image mapping rules:" << std::endl;
  cout << "A rule of image1@snap1=image2@snap2 would map snap1 of image1 to snap2 of" << std::endl;
  cout << "image2." << std::endl;
}

int main(int argc, const char **argv) {
  vector<const char*> args;

  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY, 0);

  std::vector<const char*>::iterator i;
  string pool_name = "rbd";
  float latency_multiplier = 1;
  bool readonly = false;
  ImageNameMap image_name_map;
  std::string val;
  std::ostringstream err;
  bool dump_perf_counters = false;
  for (i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "-p", "--pool", (char*)NULL)) {
      pool_name = val;
    } else if (ceph_argparse_witharg(args, i, &latency_multiplier, err, "--latency-multiplier",
				     (char*)NULL)) {
      if (!err.str().empty()) {
	cerr << err.str() << std::endl;
	return 1;
      }
    } else if (ceph_argparse_flag(args, i, "--read-only", (char*)NULL)) {
      readonly = true;
    } else if (ceph_argparse_witharg(args, i, &val, "--map-image", (char*)NULL)) {
      ImageNameMap::Mapping mapping;
      if (image_name_map.parse_mapping(val, &mapping)) {
	image_name_map.add_mapping(mapping);
      } else {
	cerr << "Unable to parse mapping string: '" << val << "'" << std::endl;
	return 1;
      }
    } else if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage(argv[0]);
      return 0;
    } else if (ceph_argparse_flag(args, i, "--dump-perf-counters", (char*)NULL)) {
      dump_perf_counters = true;
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

  unsigned int nthreads = boost::thread::hardware_concurrency();
  Replayer replayer(2 * nthreads + 1);
  replayer.set_latency_multiplier(latency_multiplier);
  replayer.set_pool_name(pool_name);
  replayer.set_readonly(readonly);
  replayer.set_image_name_map(image_name_map);
  replayer.set_dump_perf_counters(dump_perf_counters);
  replayer.run(replay_file);
}
