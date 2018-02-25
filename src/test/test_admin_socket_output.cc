// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <algorithm>                                      // for move
#include <iostream>                                       // for ostream
#include <memory>                                         // for unique_ptr
#include <string>                                         // for operator<<
#include <vector>                                         // for vector
#include <boost/program_options/option.hpp>               // for program_opt...
#include <boost/program_options/options_description.hpp>  // for options_des...
#include <boost/program_options/parsers.hpp>              // for basic_comma...
#include <boost/program_options/variables_map.hpp>        // for variables_map
#include <boost/program_options/parsers.hpp>              // for basic_comma...

#include "admin_socket_output.h"
#include "admin_socket_output_tests.h"

namespace po = boost::program_options;

void usage(po::options_description desc) {
  std::cout << desc << std::endl;
}

void handle_unrecognised(std::vector<std::string>&& unrecognised) {
  for (auto& un : unrecognised) {
    std::cout << "Unrecognized Parameter: " << un << std::endl;
  }
}

// Test functions:
// See admin_socket_output_tests.h

int main(int argc, char** argv) {

  po::options_description desc("Allowed options");
  desc.add_options()
    ("help,h", "produce help message")
    ("all", "implies"
     " --osd"
     " --mon"
     " --mgr"
     " --mds"
     " --client"
     " --vstart")
    ("osd", "Test osd admin socket output")
    ("mon", "Test mon admin socket output")
    ("mgr", "Test mgr admin socket output")
    ("mds", "Test mds admin socket output")
    ("client", "Test client (includes rgw) admin socket output")
    ("vstart", po::value<std::string>()->implicit_value("./out"),
     "Modify to run in vstart environment")
    ;
  auto parsed =
    po::command_line_parser(argc, argv).options(desc).allow_unregistered().run();
  po::variables_map vm;
  po::store(parsed, vm);
  po::notify(vm);

  auto unrecognised = collect_unrecognized(parsed.options, po::include_positional);
  if(!unrecognised.empty()) {
    handle_unrecognised(std::move(unrecognised));
    usage(desc);
    return 1;
  }
  if (vm.count("help") || vm.empty()) {
    usage(desc);
    return 2;
  }

  std::unique_ptr<AdminSocketOutput> asockout(new AdminSocketOutput);

  if (vm.count("vstart")) {
    asockout->mod_for_vstart(vm["vstart"].as<std::string>());
  }

  if(vm.count("all")) {
    asockout->add_target("all");
  } else {
    if (vm.count("osd")) {
      asockout->add_target("osd");
    }
    if (vm.count("mon")) {
      asockout->add_target("mon");
    }
    if (vm.count("mgr")) {
      asockout->add_target("mgr");
    }
    if (vm.count("mds")) {
      asockout->add_target("mds");
    }
    if (vm.count("client")) {
      asockout->add_target("client");
    }
  }

  // Postpone commands that may affect later commands

  asockout->postpone("mds", "force_readonly");

  // Custom commands

  //Example:
  //asockout->add_command("osd", R"({"prefix":"config get", "var":"admin_socket"})");

  // End custom commands

  // Tests
  //Example:
  //asockout->add_test("osd", R"({"prefix":"config get", "var":"admin_socket"})", test_config_get_admin_socket);

  asockout->add_test("osd", R"({"prefix":"dump_pgstate_history"})", test_dump_pgstate_history);

  // End tests

  asockout->exec();

  return 0;
}
