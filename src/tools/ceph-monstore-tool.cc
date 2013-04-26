// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
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

namespace po = boost::program_options;
using namespace std;

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  int version = -1;
  string store_path, cmd, out_path;
  desc.add_options()
    ("help", "produce help message")
    ("mon-store-path", po::value<string>(&store_path),
     "path to mon directory, mandatory")
    ("out", po::value<string>(&out_path),
     "out path")
    ("version", po::value<int>(&version),
     "version requested")
    ("command", po::value<string>(&cmd),
     "command")
    ;
  po::positional_options_description p;
  p.add("command", 1);
  p.add("version", 1);

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

  int fd;
  if (vm.count("out")) {
    fd = open(out_path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0666);
  } else {
    fd = STDOUT_FILENO;
  }

  MonitorDBStore st(store_path);
  stringstream ss;
  int r = st.open(ss);
  if (r < 0) {
    std::cerr << ss.str() << std::endl;
    goto done;
  }
  if (cmd == "getosdmap") {
    version_t v;
    if (version == -1) {
      v = st.get("osdmap", "last_committed");
    } else {
      v = version;
    }

    bufferlist bl;
    /// XXX: this is not ok, osdmap and full should be abstracted somewhere
    int r = st.get("osdmap", st.combine_strings("full", v), bl);
    if (r < 0) {
      std::cerr << "Error getting map: " << cpp_strerror(r) << std::endl;
      goto done;
    }
    bl.write_fd(fd);
  } else {
    std::cerr << "Unrecognized command: " << cmd << std::endl;
    goto done;
  }

  done:
  if (vm.count("out")) {
    ::close(fd);
  }
  return 0;
}
