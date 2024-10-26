// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/async/context_pool.h"
#include "common/Preforker.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "log/Log.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "Mirror.h"

#include <vector>

using namespace std;

void usage() {
  std::cout << "usage: cephfs-mirror [options...]" << std::endl;
  std::cout << "options:\n";
  std::cout << "  --mon-host monaddress[:port]  connect to specified monitor\n";
  std::cout << "  --keyring=<path>              path to keyring for local cluster\n";
  std::cout << "  --log-file=<logfile>          file to log debug output\n";
  std::cout << "  --debug-cephfs-mirror=<log-level>/<memory-level>  set cephfs-mirror debug level\n";
  generic_server_usage();
}

cephfs::mirror::Mirror *mirror = nullptr;

static void handle_signal(int signum) {
  if (mirror) {
    mirror->handle_signal(signum);
  }
}

int main(int argc, const char **argv) {
  auto args = argv_to_vec(argc, argv);
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

  Preforker forker;
  if (global_init_prefork(g_ceph_context) >= 0) {
    std::string err;
    int r = forker.prefork(err);
    if (r < 0) {
      cerr << err << std::endl;
      return r;
    }
    if (forker.is_parent()) {
      g_ceph_context->_log->start();
      if (forker.parent_wait(err) != 0) {
        return -ENXIO;
      }
      return 0;
    }
    global_init_postfork_start(g_ceph_context);
  }

  common_init_finish(g_ceph_context);

  bool daemonize = g_conf().get_val<bool>("daemonize");
  if (daemonize) {
    global_init_postfork_finish(g_ceph_context);
    forker.daemonize();
  }

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, handle_signal);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  auto cmd_args = argv_to_vec(argc, argv);

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
  delete mirror;

cleanup:
  monc.shutdown();
cleanup_messenger:
  msgr->shutdown();
  msgr->wait();
  delete msgr;

  unregister_async_signal_handler(SIGHUP, handle_signal);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();

  return forker.signal_exit(r);
}
