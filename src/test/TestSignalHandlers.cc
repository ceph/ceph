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
 * TestSignalHandlers
 *
 * Test the Ceph signal handlers
 */
#include "common/DoutStreambuf.h"
#include "common/common_init.h"
#include "common/errno.h"
#include "config.h"

#include <errno.h>
#include <iostream>
#include <sstream>
#include <string>

using std::string;

// avoid compiler warning about dereferencing NULL pointer
static int* get_null()
{
  return 0;
}

static std::string get_tmp_filename()
{
  char tmp[PATH_MAX];
  memset(tmp, 0, sizeof(tmp));

  const char *tdir = getenv("TMPDIR");
  if (!tdir)
    tdir = "/tmp";

  snprintf(tmp, sizeof(tmp), "%s/%s", tdir, "test_signal_handlers_XXXXXX");
  int fd = TEMP_FAILURE_RETRY(::mkstemp(tmp));
  if (fd < 0) {
    std::cout << __PRETTY_FUNCTION__ << ": mkstemp failed: "
	      << cpp_strerror(errno) << std::endl;
    return "";
  }
  TEMP_FAILURE_RETRY(::close(fd));
  return string(tmp);
}

int main(int argc, const char **argv)
{
  string tmp_log_file(get_tmp_filename());
  if (tmp_log_file.empty())
    return 1;
  std::cout << "tmp_log_file = " << tmp_log_file << std::endl;

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);
  ceph_set_default_id("admin");
  common_set_defaults(false);
  g_conf.log_to_file = true;
  g_conf.log_file = tmp_log_file.c_str();
  common_init(args, "TestSignalHandlers", true);

  dout(0) << "triggering SIGSEGV..." << dendl;
  int i = *get_null();
  std::cout << "i = " << i << std::endl;

  return 0;
}
