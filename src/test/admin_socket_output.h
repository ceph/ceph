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

#ifndef CEPH_ADMIN_SOCKET_OUTPUT_H
#define CEPH_ADMIN_SOCKET_OUTPUT_H

#include <filesystem>
#include <iostream> // for std::cout
#include <string>
#include <map>
#include <set>
#include <vector>

namespace fs = std::filesystem;

using socket_results = std::map<std::string, std::string>;
using test_functions =
    std::vector<std::pair<std::string, bool (*)(std::string &)>>;

class AdminSocketClient;

class AdminSocketOutput {
public:
  AdminSocketOutput() {}

  void add_target(const std::string &target);
  void add_command(const std::string &target, const std::string &command);
  void add_test(const std::string &target, const std::string &command,
                bool (*test)(std::string &));
  void postpone(const std::string &target, const std::string &command);

  void exec();

  void mod_for_vstart(const std::string& dir) {
    socketdir = dir;
    prefix = "";
  }

private:
  bool init_directories() const {
    std::cout << "Checking " << socketdir << std::endl;
    return exists(socketdir) && is_directory(socketdir);
  }

  bool init_sockets();
  bool gather_socket_output();
  std::string get_result(const std::string &target, const std::string &command) const;

  std::pair<std::string, std::string>
  run_command(AdminSocketClient &client, const std::string &raw_command,
              bool send_untouched = false);

  bool run_tests() const;

  std::set<std::string> targets;
  std::map<std::string, std::string> sockets;
  std::map<std::string, socket_results> results;
  std::map<std::string, std::vector<std::string>> custom_commands;
  std::map<std::string, std::vector<std::string>> postponed_commands;
  std::map<std::string, test_functions> tests;

  std::string prefix = "ceph-";
  fs::path socketdir = "/var/run/ceph";
};

#endif // CEPH_ADMIN_SOCKET_OUTPUT_H
