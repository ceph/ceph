// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * SPDX-License-Identifier: LGPL-3.0-or-later
 * Copyright(c) 2020, Intel Corporation
 *
 * Author: Changcheng Liu <changcheng.liu@aliyun.com>
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <iostream>
#include <string>
#include <string_view>
#include <memory>

#include "include/ceph_features.h"
#include "include/ceph_assert.h"
#include "common/config.h"
#include "common/Timer.h"
#include "common/errno.h"
#include "common/ceph_argparse.h"
#include "common/pick_address.h"
#include "common/Preforker.h"

#include "global/global_init.h"
#include "global/signal_handler.h"

#include "msg/Messenger.h"

#include "mon/MonMap.h"
#include "mon/MonClient.h"

#include "ReplicaDaemon.h"

#define FN_NAME (__CEPH_ASSERT_FUNCTION == nullptr ? __func__ : __CEPH_ASSERT_FUNCTION)
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cache_replica

using std::cerr;
using std::cout;
using std::map;
using std::ostringstream;
using std::string;
using std::string_view;
using std::vector;
using std::shared_ptr;

using ceph::bufferlist;

static void usage()
{
  cout << "usage: ceph-replica -i <ID> [below_options]" << std::endl
       << " -m monitor_host_ip:port" << std::endl
       << "    connect to monitor at given address" << std::endl
       << " --debug_replica debug_level" << std::endl
       << "    debug replica level (e.g. 10)"
       << std::endl;
  generic_server_usage();
}

int main(int argc, const char **argv)
{
  ceph_pthread_setname(pthread_self(), "ceph-replica");

  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  map<string, string> default_config = {
    //overwrite "common/option" configuration here
  };

  auto gctx = global_init(&default_config, args,
                         CEPH_ENTITY_TYPE_REPLICA,
                         CODE_ENVIRONMENT_DAEMON, 0);

  /*
   * TODO: consider NUMA affinity
   */
  dout(1) << FN_NAME << " not setting numa affinity" << dendl;

  std::string val;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else {
      // #TODO: check other supported options
      ++i;
    }
  }
  if (!args.empty()) {
    cerr << "unregcognized arg " << args[0] << std::endl;
    for (auto& unknown_arg : args) {
      cerr << "\t" << unknown_arg << std::endl;
    }
    exit(1);
  }

  // make replica to be background daemon
  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  common_init_finish(g_ceph_context);
  global_init_chdir(g_ceph_context);

  return 0;
}
