// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

/*
 * TestSyslogStreambuf
 *
 * Puts some output into the SyslogStreambuf class.
 * Check your syslog to see what it did.
 */
#include "common/SyslogStreambuf.h"
#include "common/common_init.h"
#include "config.h"

#include <iostream>
#include <sstream>
#include <string>

using std::cout;
using std::cerr;
using std::string;

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  ceph_set_default_id("admin");
  common_set_defaults(false);
  common_init(args, "ceph", true);

  std::ostream oss(new SyslogStreambuf<char>);

  oss << "I am logging to syslog now!" << std::endl;

  oss << "And here is another line!" << std::endl;

  oss.flush();

  oss << "Stuff ";
  oss << "that ";
  oss << "will ";
  oss << "all ";
  oss << "be ";
  oss << "on ";
  oss << "one ";
  oss << "line.";
  oss.flush();

  oss << "There will be no blank lines here." << std::endl;
  oss.flush();
  oss.flush();
  oss.flush();

  oss << "But here is a blank line:" << std::endl;
  oss << std::endl;

  return 0;
}
