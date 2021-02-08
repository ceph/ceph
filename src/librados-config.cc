// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public
 * License version 2, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <iostream>

#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/option.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/program_options/variables_map.hpp>

#include "include/rados/librados.h"
#include "ceph_ver.h"

namespace po = boost::program_options;

int main(int argc, const char **argv) 
{
  po::options_description desc{"usage: librados-config [option]"};
  desc.add_options()
    ("help,h", "print this help message")
    ("version", "library version")
    ("vernum", "library version code")
    ("release", "print release name");

  po::parsed_options parsed =
    po::command_line_parser(argc, argv).options(desc).run();
  po::variables_map vm;
  po::store(parsed, vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
  } else if (vm.count("version")) {
    int maj, min, ext;
    rados_version(&maj, &min, &ext);
    std::cout << maj << "." << min << "." << ext << std::endl;
  } else if (vm.count("vernum")) {
    std::cout << std::hex << LIBRADOS_VERSION_CODE << std::dec << std::endl;
  } else if (vm.count("release")) {
    std::cout << CEPH_RELEASE_NAME << ' '
	      << '(' << CEPH_RELEASE_TYPE << ')'
	      << std::endl;
  } else {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    return 1;
  }
}

