// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph distributed storage system
 *
 * Copyright (C) 2014 Cloudwatt <libre.licensing@cloudwatt.com>
 * Copyright (C) 2014 Red Hat <contact@redhat.com>
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

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/Clock.h"
#include "include/utime.h"
#include "erasure-code/ErasureCodePlugin.h"

namespace po = boost::program_options;

class ErasureCodeCommand {
  po::variables_map vm;
  ErasureCodeProfile profile;
public:
  int setup(int argc, char** argv);
  int run();
  int plugin_exists();
  int display_information();
};

int ErasureCodeCommand::setup(int argc, char** argv) {

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("all", "implies "
     "--get_chunk_size 1024 "
     "--get_data_chunk_count "
     "--get_coding_chunk_count "
     "--get_chunk_count ")
    ("get_chunk_size", po::value<unsigned int>(),
     "display get_chunk_size(<object size>)")
    ("get_data_chunk_count", "display get_data_chunk_count()")
    ("get_coding_chunk_count", "display get_coding_chunk_count()")
    ("get_chunk_count", "display get_chunk_count()")
    ("parameter,P", po::value<vector<string> >(),
     "parameters")
    ("plugin_exists", po::value<string>(),
     "succeeds if the plugin given in argument exists and can be loaded")
    ;

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
  string directory = getenv("CEPH_LIB");
  g_conf->set_val("erasure_code_dir", directory, false, false);

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
	cerr << "--parameter " << *i
	     << " ignored because it does not contain exactly one =" << endl;
      } else {
	profile[strs[0]] = strs[1];
      }
    }
  }

  return 0;
}

int ErasureCodeCommand::run() {
  if (vm.count("plugin_exists"))
    return plugin_exists();
  else
    return display_information();
}

int ErasureCodeCommand::plugin_exists() {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodePlugin *plugin = 0;
  Mutex::Locker l(instance.lock);
  stringstream ss;
  int code = instance.load(vm["plugin_exists"].as<string>(),
			   g_conf->erasure_code_dir, &plugin, &ss);
  if (code)
    cerr << ss.str() << endl;
  return code;
}

int ErasureCodeCommand::display_information() {
  ErasureCodePluginRegistry &instance = ErasureCodePluginRegistry::instance();
  ErasureCodeInterfaceRef erasure_code;

  if (profile.count("plugin") == 0) {
    cerr << "--parameter plugin=<plugin> is mandatory" << endl;
    return 1;
  }

  int code = instance.factory(profile["plugin"],
			      g_conf->erasure_code_dir,
			      profile,
			      &erasure_code, &cerr);
  if (code)
    return code;

  if (vm.count("all") || vm.count("get_chunk_size")) {
    unsigned int object_size = 1024;
    if (vm.count("get_chunk_size"))
      object_size = vm["get_chunk_size"].as<unsigned int>();
    cout << "get_chunk_size(" << object_size << ")\t"
	 << erasure_code->get_chunk_size(object_size) << endl;
  }
  if (vm.count("all") || vm.count("get_data_chunk_count"))
    cout << "get_data_chunk_count\t"
      	 << erasure_code->get_data_chunk_count() << endl;
  if (vm.count("all") || vm.count("get_coding_chunk_count"))
    cout << "get_coding_chunk_count\t"
      	 << erasure_code->get_coding_chunk_count() << endl;
  if (vm.count("all") || vm.count("get_chunk_count"))
    cout << "get_chunk_count\t"
      	 << erasure_code->get_chunk_count() << endl;
  return 0;
}

int main(int argc, char** argv) {
  ErasureCodeCommand eccommand;
  try {
    int err = eccommand.setup(argc, argv);
    if (err)
      return err;
    return eccommand.run();
  } catch(po::error &e) {
    cerr << e.what() << endl; 
    return 1;
  }
}

/*
 * Local Variables:
 * compile-command: "cd ../.. ; make -j4 &&
 *   make -j4 ceph_erasure_code &&
 *   libtool --mode=execute valgrind --tool=memcheck --leak-check=full \
 *      ./ceph_erasure_code \
 *      --parameter plugin=jerasure \
 *      --parameter technique=reed_sol_van \
 *      --parameter k=2 \
 *      --parameter m=2 \
 *      --get_chunk_size 1024 \
 *      --get_data_chunk_count \
 *      --get_coding_chunk_count \
 *      --get_chunk_count \
 * "
 * End:
 */
