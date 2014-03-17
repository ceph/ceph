// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 */

#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/algorithm/string.hpp>

#include "ceph_erasure_code_benchmark.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/Clock.h"
#include "include/utime.h"
#include "erasure-code/ErasureCodePlugin.h"

namespace po = boost::program_options;

int ErasureCodeBench::setup(int argc, char** argv) {

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("size,s", po::value<int>()->default_value(1024 * 1024),
     "size of the buffer to be encoded")
    ("iterations,i", po::value<int>()->default_value(1),
     "number of encode/decode runs")
    ("plugin,p", po::value<string>()->default_value("jerasure"),
     "erasure code plugin name")
    ("workload,w", po::value<string>()->default_value("encode"),
     "run either encode or decode")
    ("erasures,e", po::value<int>()->default_value(1),
     "number of erasures when decoding")
    ("parameter,P", po::value<vector<string> >(),
     "parameters")
    ;

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::store(
    parsed,
    vm);
  po::notify(vm);

  vector<const char *> ceph_options, def_args;
  vector<string> ceph_option_strings = po::collect_unrecognized(
    parsed.options, po::include_positional);
  ceph_options.reserve(ceph_option_strings.size());
  for (vector<string>::iterator i = ceph_option_strings.begin();
       i != ceph_option_strings.end();
       ++i) {
    ceph_options.push_back(i->c_str());
  }

  global_init(
    &def_args, ceph_options, CEPH_ENTITY_TYPE_CLIENT,
    CODE_ENVIRONMENT_UTILITY,
    CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);

  if (vm.count("help")) {
    cout << desc << std::endl;
    return 1;
  }

  if (vm.count("parameter")) {
    const vector<string> &p = vm["parameter"].as< vector<string> >();
    for (vector<string>::const_iterator i = p.begin();
	 i != p.end();
	 ++i) {
      std::vector<std::string> strs;
      boost::split(strs, *i, boost::is_any_of("="));
      if (strs.size() != 2) {
	cerr << "--parameter " << *i << " ignored because it does not contain exactly one =" << endl;
      } else {
	parameters[strs[0]] = strs[1];
      }
    }
  }

  if (parameters.count("directory") == 0)
    parameters["directory"] = ".libs";

  in_size = vm["size"].as<int>();
  max_iterations = vm["iterations"].as<int>();
  plugin = vm["plugin"].as<string>();
  workload = vm["workload"].as<string>();
  erasures = vm["erasures"].as<int>();

  return 0;
}

int ErasureCodeBench::run() {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  instance.disable_dlclose = true;

  if (workload == "encode")
    return encode();
  else
    return decode();
}

int ErasureCodeBench::encode()
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeInterfaceRef erasure_code;
  int code = instance.factory(plugin, parameters, &erasure_code, cerr);
  if (code)
    return code;
  int k = atoi(parameters["k"].c_str());
  int m = atoi(parameters["m"].c_str());

  bufferlist in;
  in.append(string(in_size, 'X'));
  set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }
  utime_t begin_time = ceph_clock_now(g_ceph_context);
  for (int i = 0; i < max_iterations; i++) {
    map<int,bufferlist> encoded;
    code = erasure_code->encode(want_to_encode, in, &encoded);
    if (code)
      return code;
  }
  utime_t end_time = ceph_clock_now(g_ceph_context);
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
  return 0;
}

int ErasureCodeBench::decode()
{
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeInterfaceRef erasure_code;
  int code = instance.factory(plugin, parameters, &erasure_code, cerr);
  if (code)
    return code;
  int k = atoi(parameters["k"].c_str());
  int m = atoi(parameters["m"].c_str());

  bufferlist in;
  in.append(string(in_size, 'X'));

  set<int> want_to_encode;
  for (int i = 0; i < k + m; i++) {
    want_to_encode.insert(i);
  }

  map<int,bufferlist> encoded;
  code = erasure_code->encode(want_to_encode, in, &encoded);
  if (code)
    return code;

  set<int> want_to_read = want_to_encode;

  utime_t begin_time = ceph_clock_now(g_ceph_context);
  for (int i = 0; i < max_iterations; i++) {
    map<int,bufferlist> chunks = encoded;
    for (int j = 0; j < erasures; j++) {
      int erasure;
      do {
	erasure = rand() % ( k + m );
      } while(chunks.count(erasure) == 0);
      chunks.erase(erasure);
    }
    map<int,bufferlist> decoded;
    code = erasure_code->decode(want_to_read, chunks, &decoded);
    if (code)
      return code;
  }
  utime_t end_time = ceph_clock_now(g_ceph_context);
  cout << (end_time - begin_time) << "\t" << (max_iterations * (in_size / 1024)) << endl;
  return 0;
}

int main(int argc, char** argv) {
  ErasureCodeBench ecbench;
  int err = ecbench.setup(argc, argv);
  if (err)
    return err;
  return ecbench.run();
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make ceph_erasure_code_benchmark &&
 *   valgrind --tool=memcheck --leak-check=full \
 *      ./ceph_erasure_code_benchmark \
 *      --plugin jerasure \
 *      --parameter directory=.libs \
 *      --parameter technique=reed_sol_van \
 *      --parameter k=2 \
 *      --parameter m=2 \
 *      --iterations 1
 * "
 * End:
 */
