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

#include <string>
#include <iostream>

#include "common/ceph_json.h"

// Test functions

// Example test function
/*
bool test_config_get_admin_socket(std::string& output) {
  return std::string::npos != output.find("admin_socket") &&
         std::string::npos != output.rfind(".asok");
}
*/

bool test_dump_pgstate_history(std::string &output) {
  JSONParser parser;
  bool ret = parser.parse(output.c_str(), output.size());
  if (!ret) {
    std::cerr << "test_dump_pgstate_history: parse error" << std::endl;
    return false;
  }

  JSONObjIter iter = parser.find_first();
  for (; !iter.end(); ++iter) {
    if ((*iter)->get_name() == "pg") {
      ret = !(*iter)->get_data().empty();
      if (ret == false) {
        std::cerr << "test_dump_pgstate_history: pg value empty, failing"
                  << std::endl;
        std::cerr << "Dumping full output: " << std::endl;
        std::cerr << output << std::endl;
        break;
      }
    } else if ((*iter)->get_name() == "history") {
      ret = std::string::npos != (*iter)->get_data().find("epoch") &&
            std::string::npos != (*iter)->get_data().find("state") &&
            std::string::npos != (*iter)->get_data().find("Initial") &&
            std::string::npos != (*iter)->get_data().find("enter") &&
            std::string::npos != (*iter)->get_data().find("exit");
      if (ret == false) {
        std::cerr << "test_dump_pgstate_history: Can't find expected values in "
                     "history object, failing"
                  << std::endl;
        std::cerr << "Problem output was:" << std::endl;
        std::cerr << (*iter)->get_data() << std::endl;
        break;
      }
    }
  }
  return ret;
}
