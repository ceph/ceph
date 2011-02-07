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
#include "common/safe_io.h"
#include "config.h"
#include "tools/common.h"

#include <limits.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>

using std::vector;

static void usage()
{
  cout << "usage: ceph [options] [commands]\n";
  cout << "If no commands are specified, enter interactive mode.\n";
  cout << "Commands:\n";
  cout << "   stop              -- cleanly shut down file system\n"
       << "   (osd|pg|mds) stat -- get monitor subsystem status\n"
       << "   ...\n";
  cout << "Options:\n";
  cout << "   -i infile\n"
       << "   -o outfile\n"
       << "        specify input or output file (for certain commands)\n"
       << "   -s or --status\n"
       << "        print current system status\n"
       << "   -w or --watch\n"
       << "        watch system status changes in real time (push)\n";
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
      derr << "unrecognized option " << args[i] << dendl;
      usage();
    } else {
      nargs->push_back(args[i]);
    }
  }
}

static int get_indata(const char *in_file, bufferlist &indata)
{
  int fd = TEMP_FAILURE_RETRY(::open(in_file, O_RDONLY));
  if (fd < 0) {
    int err = errno;
    derr << "error opening in_file '" << in_file << "': "
	 << cpp_strerror(err) << dendl;
    return 1;
  }
  struct stat st;
  if (::fstat(fd, &st)) {
    int err = errno;
    derr << "error getting size of in_file '" << in_file << "': "
	 << cpp_strerror(err) << dendl;
    return 1;
  }

  indata.push_back(buffer::create(st.st_size));
  indata.zero();
  int ret = safe_read(fd, indata.c_str(), st.st_size);
  if (ret) {
    derr << "error reading in_file '" << in_file << "': "
	 << cpp_strerror(ret) << dendl;
    return 1;
  }

  TEMP_FAILURE_RETRY(::close(fd));
  derr << "read " << st.st_size << " bytes from " << in_file << dendl;
  return 0;
}

int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  common_set_defaults(false);
  common_init(args, "ceph", true);
  set_foreground_logging();

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
      derr << "failed to get data from '" << in_file << "'" << dendl;
      return 1;
    }
  }

  if (ceph_tool_common_init(mode)) {
    derr << "ceph_tool_common_init failed." << dendl;
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
      derr << "logic error: illegal ceph command mode " << mode << dendl;
      ret = 1;
      break;
    }
  }

  if (ceph_tool_common_shutdown())
    ret = 1;
  return ret;
}
