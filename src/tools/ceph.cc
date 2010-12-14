// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2010 Sage Weil <sage@newdream.net>
 * Copyright (C) 2010 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/common_init.h"
#include "common/errno.h"
#include "config.h"
#include "tools/common.h"

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

using std::cerr;
using std::vector;

static void usage()
{
  cerr << "usage: ceph [options] [commands]" << std::endl;
  cerr << "If no commands are specified, enter interactive mode.\n";
  cerr << "Commands:" << std::endl;
  cerr << "   stop              -- cleanly shut down file system" << std::endl
       << "   (osd|pg|mds) stat -- get monitor subsystem status" << std::endl
       << "   ..." << std::endl;
  cerr << "Options:" << std::endl;
  cerr << "   -i infile\n";
  cerr << "   -o outfile\n";
  cerr << "        specify input or output file (for certain commands)\n";
  cerr << "   -s or --status\n";
  cerr << "        print current system status\n";
  cerr << "   -w or --watch\n";
  cerr << "        watch system status changes in real time (push)\n";
  generic_client_usage(); // Will exit()
}

static void parse_cmd_args(const vector<const char*> &args,
		const char **in_file, const char ** out_file,
		ceph_tool_mode_t *mode, vector<const char*> *nargs)
{
  DEFINE_CONF_VARS(usage);
  FOR_EACH_ARG(args) {
    if (CONF_ARG_EQ("in_file", 'i')) {
      CONF_SAFE_SET_ARG_VAL(in_file, OPT_STR);
    } else if (CONF_ARG_EQ("out_file", 'o')) {
      CONF_SAFE_SET_ARG_VAL(out_file, OPT_STR);
    } else if (CONF_ARG_EQ("status", 's')) {
      *mode = CEPH_TOOL_MODE_ONE_SHOT_OBSERVER;
    } else if (CONF_ARG_EQ("watch", 'w')) {
      *mode = CEPH_TOOL_MODE_OBSERVER;
    } else if (CONF_ARG_EQ("help", 'h')) {
      usage();
    } else if (args[i][0] == '-' && nargs->empty()) {
      cerr << "unrecognized option " << args[i] << std::endl;
      usage();
    } else {
      nargs->push_back(args[i]);
    }
  }
}

static int safe_read(int fd, char *buf, size_t count)
{
  if (count > SSIZE_MAX)
    return E2BIG;
  while (1) {
    int res;
    res = read(fd, buf, count);
    if (res < 0) {
      int err = errno;
      if (err == EINVAL)
	continue;
      return err;
    }
    count -= res;
    buf += res;
    if (count <= 0)
      return 0;
  }
}

static int get_indata(const char *in_file, bufferlist &indata)
{
  int fd = TEMP_FAILURE_RETRY(::open(in_file, O_RDONLY));
  if (fd < 0) {
    int err = errno;
    cerr << "error opening in_file '" << in_file << "': "
	 << cpp_strerror(err);
    return 1;
  }
  struct stat st;
  if (::fstat(fd, &st)) {
    int err = errno;
    cerr << "error getting size of in_file '" << in_file << "': "
	 << cpp_strerror(err);
    return 1;
  }

  indata.push_back(buffer::create(st.st_size));
  indata.zero();
  int ret = safe_read(fd, indata.c_str(), st.st_size);
  if (ret) {
    cerr << "error reading in_file '" << in_file << "': "
	 << cpp_strerror(ret);
    return 1;
  }

  TEMP_FAILURE_RETRY(::close(fd));
  cout << "read " << st.st_size << " bytes from " << in_file << std::endl;
  return 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  ceph_set_default_id("admin");

  common_set_defaults(false);
  common_init(args, "ceph", true);

  vec_to_argv(args, argc, argv);

  const char *in_file = NULL;
  const char *out_file = NULL;
  enum ceph_tool_mode_t mode = CEPH_TOOL_MODE_CLI_INPUT;
  vector<const char*> nargs;

  // parse user input
  parse_cmd_args(args, &in_file, &out_file, &mode, &nargs);

  bufferlist indata;

  if (in_file) {
    if (get_indata(in_file, indata)) {
      cerr << "failed to get data from '" << in_file << "'" << std::endl;
      return 1;
    }
  }

  if (ceph_tool_common_init(mode)) {
    cerr << "ceph_tool_common_init failed." << std::endl;
    return 1;
  }

  int ret = 0;
  switch (mode) {
    case CEPH_TOOL_MODE_ONE_SHOT_OBSERVER: // fall through
    case CEPH_TOOL_MODE_OBSERVER: {
      g.lock.Lock();
      send_observe_requests();
      g.lock.Unlock();
      break;
    }

    case CEPH_TOOL_MODE_CLI_INPUT: {
      vector<string> cmd;
      for (unsigned int i = 0; i < nargs.size(); ++i) {
	cmd.push_back(string(nargs[i]));
      }
      if (cmd.empty()) {
	if (ceph_tool_do_cli())
	  ret = 1;
      }
      else {
	if (ceph_tool_cli_input(cmd, out_file, indata))
	  ret = 1;
      }
      if (ceph_tool_messenger_shutdown())
	ret = 1;
      break;
    }

    default: {
      cerr << "logic error: illegal ceph command mode " << mode << std::endl;
      ret = 1;
      break;
    }
  }

  if (ceph_tool_common_shutdown())
    ret = 1;
  return ret;
}
