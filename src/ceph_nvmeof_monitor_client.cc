// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 IBM Inc
 *
 * Author: Alexander Indenbaum <aindenba@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <pthread.h>

#include "include/types.h"
#include "include/compat.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "common/pick_address.h"
#include "global/global_init.h"

#include "nvmeof/NVMeofGwMonitorClient.h"

static void usage()
{
  std::cout << "usage: ceph-nvmeof-monitor-client\n"
               "        --gateway-name <GW_NAME>\n"
               "        --gateway-address <GW_ADDRESS>\n"
               "        --gateway-pool <CEPH_POOL>\n"
               "        --gateway-group <GW_GROUP>\n"
               "        --monitor-group-address <MONITOR_GROUP_ADDRESS>\n"
               "        [flags]\n"
	    << std::endl;
  generic_server_usage();
}

/**
 * A short main() which just instantiates a Nvme and
 * hands over control to that.
 */
int main(int argc, const char **argv)
{
  ceph_pthread_setname(pthread_self(), "ceph-nvmeof-monitor-client");

  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, // maybe later use CODE_ENVIRONMENT_DAEMON,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC);

  global_init_daemonize(g_ceph_context);
  global_init_chdir(g_ceph_context);
  common_init_finish(g_ceph_context);

  NVMeofGwMonitorClient gw_monitor_client(argc, argv);
  int rc = gw_monitor_client.init();
  if (rc != 0) {
      std::cerr << "Error in initialization: " << cpp_strerror(rc) << std::endl;
      return rc;
  }

  return gw_monitor_client.main(args);
}

