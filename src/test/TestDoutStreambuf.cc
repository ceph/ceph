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
 * TestDoutStreambuf
 *
 * Puts some output into the DoutStreambuf class.
 * Check your syslog to see what it did.
 */
#include "common/DoutStreambuf.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/config.h"

#include <iostream>
#include <sstream>
#include <string>
#include <syslog.h>

using std::string;

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_set_defaults(false);
  common_init(args, "ceph", STARTUP_FLAG_FORCE_FG_LOGGING);

  DoutStreambuf<char> *dos = new DoutStreambuf<char>();

  {
    DoutLocker _dout_locker;
    dos->read_global_config();
  }
  derr << "using configuration: " << dos->config_to_str() << dendl;

  std::ostream oss(dos);
  syslog(LOG_USER | LOG_NOTICE, "TestDoutStreambuf: starting test\n");

  dos->set_prio(1);
  oss << "1. I am logging to dout now!" << std::endl;

  dos->set_prio(2);
  oss << "2. And here is another line!" << std::endl;

  oss.flush();

  dos->set_prio(3);
  oss << "3. And here is another line!" << std::endl;

  dos->set_prio(16);
  oss << "4. Stuff ";
  oss << "that ";
  oss << "will ";
  oss << "all ";
  oss << "be ";
  oss << "on ";
  oss << "one ";
  oss << "line.\n";
  oss.flush();

  dos->set_prio(10);
  oss << "5. There will be no blank lines here.\n" << std::endl;
  oss.flush();
  oss.flush();
  oss.flush();

  syslog(LOG_USER | LOG_NOTICE, "TestDoutStreambuf: ending test\n");

  return 0;
}
