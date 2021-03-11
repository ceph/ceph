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
#include "common/async/context_pool.h"

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

ReplicaDaemon *cache_replica = nullptr;

static void handle_replica_signal(int signum)
{
  //TODO: deal with SIGINT & SIGTERM signal
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

  std::string rnic_addr; // RDMA NIC IP Address
  std::string val;
  for (auto i = args.begin(); i != args.end(); ) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--rnic", (char*)NULL)) {
      rnic_addr = val;
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

  //whoami, replica daemon ID
  char *end = nullptr;
  const char *id = g_conf()->name.get_id().c_str();
  int whoami = strtol(id, &end, 10);
  if (*end || end == id || whoami < 0) {
    cerr << "replica id should only be number" << std::endl;
    exit(1);
  }
  // get public network messenger type
  std::string public_msg_type = g_conf()->ms_public_type.empty() ?
                                g_conf().get_val<std::string>("ms_type") :
                                g_conf()->ms_public_type;
  uint64_t nonce = Messenger::get_pid_nonce();
  Messenger *msgr_public = Messenger::create(g_ceph_context, public_msg_type,
                                             entity_name_t::REPLICA(whoami),
                                             "replica_msgr",
                                             nonce);
  if (!msgr_public) {
    exit(1);
  }
  //default policy as client: lossy & only one connection
  msgr_public->set_default_policy(Messenger::Policy::lossy_client(0));
  //connect to monitor as the only one lossy client
  msgr_public->set_policy(entity_name_t::TYPE_MON,
                          Messenger::Policy::lossy_client(CEPH_FEATURE_UID |
                                                          CEPH_FEATURE_PGID64));
  //standby connection with peer replica daemon
  msgr_public->set_policy(entity_name_t::TYPE_REPLICA,
                          Messenger::Policy::lossless_peer(CEPH_FEATURE_UID));
  //server role as being connected with client
  msgr_public->set_policy(entity_name_t::TYPE_CLIENT,
                          Messenger::Policy::stateful_server(0));
  entity_addrvec_t public_addrs;
  int r = pick_addresses(g_ceph_context, CEPH_PICK_ADDRESS_PUBLIC, &public_addrs);
  if (r < 0) {
    derr << "Failed to pick public address" << dendl;
    delete msgr_public;
    msgr_public = nullptr;
    return r;
  }
  if (msgr_public->bindv(public_addrs) < 0) {
    r = -1;
    delete msgr_public;
    msgr_public = nullptr;
    return r;
  }

  // set up signal handlers, see: man 7 signal
  init_async_signal_handler();
  // 1) SIGHUP
  register_async_signal_handler(SIGHUP, sighup_handler);
  // 2) SIGINT: Interrupt from keyboard
  register_async_signal_handler_oneshot(SIGINT, handle_replica_signal);
  // 3) SIGTERM: Termination signal
  register_async_signal_handler_oneshot(SIGTERM, handle_replica_signal);

  msgr_public->start();

  //get monitor info(MonMap::mon_info) to construct MonClient::monmap(MonMap)
  ceph::async::io_context_pool ctxpool(2);
  MonClient mon_client(g_ceph_context, ctxpool);
  r = mon_client.build_initial_monmap();
  if (r < 0) {
    delete msgr_public;
    msgr_public = nullptr;
    return r;
  }

  // start cache replicadaemon
  cache_replica = new ReplicaDaemon(g_conf()->name.get_id().c_str(),
                                    msgr_public,
                                    &mon_client,
                                    ctxpool);

  r = cache_replica->init();

  msgr_public->wait();

  ctxpool.stop();

  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_replica_signal);
  unregister_async_signal_handler(SIGTERM, handle_replica_signal);

  return r;
}
