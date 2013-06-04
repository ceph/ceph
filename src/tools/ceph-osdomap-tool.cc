// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License kkjversion 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#include <boost/scoped_ptr.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <iostream>
#include <set>
#include <sstream>
#include <stdlib.h>
#include <fstream>
#include <string>
#include <sstream>
#include <map>
#include <set>
#include <boost/scoped_ptr.hpp>

#include "global/global_init.h"
#include "os/LevelDBStore.h"
#include "mon/MonitorDBStore.h"
#include "os/DBObjectMap.h"

namespace po = boost::program_options;
using namespace std;

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  string store_path, cmd, out_path;
  bool paranoid = false;
  desc.add_options()
    ("help", "produce help message")
    ("omap-path", po::value<string>(&store_path),
     "path to mon directory, mandatory (current/omap usually)")
    ("paranoid", po::value<bool>(&paranoid),
     "use paranoid checking")
    ("command", po::value<string>(&cmd),
     "command")
    ;
  po::positional_options_description p;
  p.add("command", 1);

  po::variables_map vm;
  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).positional(p).run();
  po::store(
    parsed,
    vm);
  try {
    po::notify(vm);
  } catch (...) {
    cout << desc << std::endl;
    return 1;
  }

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
    &def_args, ceph_options, CEPH_ENTITY_TYPE_OSD,
    CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);
  g_ceph_context->_conf->apply_changes(NULL);
  g_conf = g_ceph_context->_conf;

  if (vm.count("help")) {
    std::cerr << desc << std::endl;
    return 1;
  }

  LevelDBStore* store(new LevelDBStore(g_ceph_context, store_path));
  if (paranoid) {
    std::cerr << "Enabling paranoid checks" << std::endl;
    store->options.paranoid_checks = paranoid;
  }
  DBObjectMap omap(store);
  stringstream out;
  int r = store->open(out);
  if (r < 0) {
    std::cerr << "Store open got: " << cpp_strerror(r) << std::endl;
    std::cerr << "Output: " << out.str() << std::endl;
    goto done;
  }
  r = 0;


  if (cmd == "dump-raw-keys") {
    KeyValueDB::WholeSpaceIterator i = store->get_iterator();
    for (i->seek_to_first(); i->valid(); i->next()) {
      std::cout << i->raw_key() << std::endl;
    }
  } else if (cmd == "dump-raw-key-vals") {
    KeyValueDB::WholeSpaceIterator i = store->get_iterator();
    for (i->seek_to_first(); i->valid(); i->next()) {
      std::cout << i->raw_key() << std::endl;
      i->value().hexdump(std::cout);
    }
  } else if (cmd == "dump-objects") {
    vector<hobject_t> objects;
    r = omap.list_objects(&objects);
    if (r < 0) {
      std::cerr << "list_objects got: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    for (vector<hobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      std::cout << *i << std::endl;
    }
    r = 0;
  } else if (cmd == "dump-objects-with-keys") {
    vector<hobject_t> objects;
    r = omap.list_objects(&objects);
    if (r < 0) {
      std::cerr << "list_objects got: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    for (vector<hobject_t>::iterator i = objects.begin();
	 i != objects.end();
	 ++i) {
      std::cout << "Object: " << *i << std::endl;
      ObjectMap::ObjectMapIterator j = omap.get_iterator(*i);
      for (j->seek_to_first(); j->valid(); j->next()) {
	std::cout << j->key() << std::endl;
	j->value().hexdump(std::cout);
      }
    }
  } else if (cmd == "check") {
    r = omap.check(std::cout);
    if (!r) {
      std::cerr << "check got: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    std::cout << "check succeeded" << std::endl;
  } else {
    std::cerr << "Did not recognize command " << cmd << std::endl;
    goto done;
  }

  done:
  return r;
}
