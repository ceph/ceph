// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/async/context_pool.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "Mirror.h"

#include <vector>

void usage() {
  std::cout << "usage: cephfs-mirror [options...]" << std::endl;
  std::cout << "options:\n";
  std::cout << "  --remote/-r FILE      remote cluster configuration\n";
  std::cout << "  --keyring=<path>      path to keyring for remote cluster\n";
  std::cout << "  --log-file=<logfile>  file to log debug output\n";
  std::cout << "  --debug-cephfs-mirror=<log-level>/<memory-level>  set cephfs-sync debug level\n";
  generic_server_usage();
}

cephfs::mirror::Mirror *mirror = nullptr;

static void handle_signal(int signum) {
  if (mirror) {
    mirror->handle_signal(signum);
  }
}

int main(int argc, const char **argv) {
  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    ::exit(1);
  }

  if (ceph_argparse_need_usage(args)) {
    usage();
    ::exit(0);
  }

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON,
                         CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }

  common_init_finish(g_ceph_context);

  init_async_signal_handler();
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  std::vector<const char*> cmd_args;
  argv_to_vec(argc, argv, cmd_args);

  Messenger *msgr = Messenger::create_client_messenger(g_ceph_context, "client");
  msgr->set_default_policy(Messenger::Policy::lossy_client(0));

  std::string reason;
  ceph::async::io_context_pool ctxpool(1);
  MonClient monc(MonClient(g_ceph_context, ctxpool));
  int r = monc.build_initial_monmap();
  if (r < 0) {
    cerr << "failed to generate initial monmap" << std::endl;
    goto cleanup_messenger;
  }

  msgr->start();

  mirror = new cephfs::mirror::Mirror(g_ceph_context, cmd_args, &monc, msgr);
  r = mirror->init(reason);
  if (r < 0) {
    std::cerr << "failed to initialize cephfs-mirror: " << reason << std::endl;
    goto cleanup;
  }

  mirror->run();

cleanup:
  monc.shutdown();
cleanup_messenger:
  msgr->shutdown();
  delete mirror;

  return r < 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
