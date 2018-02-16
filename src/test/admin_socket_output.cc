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

#include <iostream>
#include <regex>                 // For regex, regex_search
#include <experimental/filesystem> // For extension

#include "common/admin_socket_client.h"     // For AdminSocketClient
#include "common/ceph_json.h"               // For JSONParser, JSONObjIter
#include "include/buffer.h"                 // For bufferlist

#include "admin_socket_output.h"

void AdminSocketOutput::add_target(const std::string& target) {
  if (target == "all") {
    add_target("osd");
    add_target("mon");
    add_target("mgr");
    add_target("mds");
    add_target("client");
    return;
  }
  targets.insert(target);
}

void AdminSocketOutput::add_command(const std::string& target,
                                    const std::string& command) {
  auto seek = custom_commands.find(target);
  if (seek != custom_commands.end()) {
    seek->second.push_back(command);
  } else {
    std::vector<std::string> vec;
    vec.push_back(command);
    custom_commands.insert(std::make_pair(target, vec));
  }

}

void AdminSocketOutput::add_test(const std::string &target,
                                 const std::string &command,
                                 bool (*test)(std::string &)) {
  auto seek = tests.find(target);
  if (seek != tests.end()) {
    seek->second.push_back(std::make_pair(command, test));
  } else {
    std::vector<std::pair<std::string, bool (*)(std::string &)>> vec;
    vec.push_back(std::make_pair(command, test));
    tests.insert(std::make_pair(target, vec));
  }
}

void AdminSocketOutput::postpone(const std::string &target,
                                 const std::string& command) {
  auto seek = postponed_commands.find(target);
  if (seek != postponed_commands.end()) {
    seek->second.push_back(command);
  } else {
    std::vector<string> vec;
    vec.push_back(command);
    postponed_commands.insert(std::make_pair(target, vec));
  }
}

bool AdminSocketOutput::init_sockets() {
  std::cout << "Initialising sockets" << std::endl;
  for (const auto &x : fs::directory_iterator(socketdir)) {
    std::cout << x.path() << std::endl;
    if (x.path().extension() == ".asok") {
      for (auto &target : targets) {
        if (std::regex_search(x.path().filename().string(),
            std::regex(prefix + target + R"(\..*\.asok)"))) {
          std::cout << "Found " << target << " socket " << x.path()
                    << std::endl;
          sockets.insert(std::make_pair(target, x.path().string()));
          targets.erase(target);
        }
      }
      if (targets.empty()) {
        std::cout << "Found all required sockets" << std::endl;
        break;
      }
    }
  }

  return !sockets.empty() && targets.empty();
}

std::pair<std::string, std::string>
AdminSocketOutput::run_command(AdminSocketClient &client,
                               const std::string raw_command,
                               bool send_untouched) {
  std::cout << "Sending command \"" << raw_command << "\"" << std::endl;
  std::string command;
  std::string output;
  if (send_untouched) {
    command = raw_command;
  } else {
    command = "{\"prefix\":\"" + raw_command + "\"}";
  }
  std::string err = client.do_request(command, &output);
  if (!err.empty()) {
    std::cerr << __func__  << " AdminSocketClient::do_request errored with: "
              << err << std::endl;
    ceph_assert(false);
  }
  return std::make_pair(command, output);
}

bool AdminSocketOutput::gather_socket_output() {

  std::cout << "Gathering socket output" << std::endl;
  for (const auto& socket : sockets) {
    std::string response;
    AdminSocketClient client(socket.second);
    std::cout << std::endl
              << "Sending request to " << socket << std::endl
              << std::endl;
    std::string err = client.do_request("{\"prefix\":\"help\"}", &response);
    if (!err.empty()) {
      std::cerr << __func__  << " AdminSocketClient::do_request errored with: "
                << err << std::endl;
      return false;
    }
    std::cout << response << '\n';

    JSONParser parser;
    bool ret = parser.parse(response.c_str(), response.size());
    if (!ret) {
      cerr << "parse error" << std::endl;
      return false;
    }

    socket_results sresults;
    JSONObjIter iter = parser.find_first();
    const auto postponed_iter = postponed_commands.find(socket.first);
    std::vector<std::string> postponed;
    if (postponed_iter != postponed_commands.end()) {
      postponed = postponed_iter->second;
    }
    std::cout << "Sending commands to " << socket.first << " socket"
              << std::endl;
    for (; !iter.end(); ++iter) {
      if (std::find(postponed.begin(), postponed.end(), (*iter)->get_name())
          != std::end(postponed)) {
        std::cout << "Command \"" << (*iter)->get_name() << "\" postponed"
                  << std::endl;
        continue;
      }
      sresults.insert(run_command(client, (*iter)->get_name()));
    }

    if (sresults.empty()) {
      return false;
    }

    // Custom commands
    const auto seek = custom_commands.find(socket.first);
    if (seek != custom_commands.end()) {
      std::cout << std::endl << "Sending custom commands:" << std::endl;
      for (const auto& raw_command : seek->second) {
        sresults.insert(run_command(client, raw_command, true));
      }
    }

    // Postponed commands
    if (!postponed.empty())
      std::cout << std::endl << "Sending postponed commands" << std::endl;
    for (const auto& command : postponed) {
      sresults.insert(run_command(client, command));
    }

    results.insert(
        std::pair<std::string, socket_results>(socket.first, sresults));

  }

  return true;
}

std::string AdminSocketOutput::get_result(const std::string target,
                                          const std::string command) const {
  const auto& target_results = results.find(target);
  if (target_results == results.end())
    return std::string("");
  else {
    const auto& result = target_results->second.find(command);
    if (result == target_results->second.end())
      return std::string("");
    else
      return result->second;
  }
}

bool AdminSocketOutput::run_tests() const {
  for (const auto& socket : sockets) {
    const auto& seek = tests.find(socket.first);
    if (seek != tests.end()) {
      std::cout << std::endl;
      std::cout << "Running tests for " << socket.first << " socket" << std::endl;
      for (const auto& test : seek->second) {
          auto result = get_result(socket.first, test.first);
          if(result.empty()) {
            std::cout << "Failed to find result for command: " << test.first << std::endl;
            return false;
          } else {
            std::cout << "Running test for command: " << test.first << std::endl;
            const auto& test_func = test.second;
            bool res = test_func(result);
            if (res == false)
              return false;
            else
              std::cout << "Test passed" << std::endl;
          }
        }
      }
    }

  return true;
}

void AdminSocketOutput::exec() {
  ceph_assert(init_directories());
  ceph_assert(init_sockets());
  ceph_assert(gather_socket_output());
  ceph_assert(run_tests());
}
