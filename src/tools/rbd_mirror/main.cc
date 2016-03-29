// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "Mirror.h"

#include <vector>

rbd::mirror::Mirror *mirror = nullptr;

void usage() {
  std::cout << "usage: rbd-mirror [options...]" << std::endl;
  std::cout << "options:\n";
  std::cout << "  -m monaddress[:port]      connect to specified monitor\n";
  std::cout << "  --keyring=<path>          path to keyring for local cluster\n";
  std::cout << "  --log-file=<logfile>       file to log debug output\n";
  std::cout << "  --debug-rbd-mirror=<log-level>/<memory-level>  set rbd-mirror debug level\n";
  generic_server_usage();
}

static void handle_signal(int signum)
{
  if (mirror)
    mirror->handle_signal(signum);
}

int main(int argc, const char **argv)
{
  std::vector<const char*> args;
  env_to_vec(args);
  argv_to_vec(argc, argv, args);

  global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
	      CODE_ENVIRONMENT_DAEMON,
	      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  for (auto i = args.begin(); i != args.end(); ++i) {
    if (ceph_argparse_flag(args, i, "-h", "--help", (char*)NULL)) {
      usage();
      return EXIT_SUCCESS;
    }
  }

  if (g_conf->daemonize) {
    global_init_daemonize(g_ceph_context);
  }
  g_ceph_context->enable_perf_counter();

  common_init_finish(g_ceph_context);

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  std::vector<const char*> cmd_args;
  argv_to_vec(argc, argv, cmd_args);

  mirror = new rbd::mirror::Mirror(g_ceph_context, cmd_args);
  int r = mirror->init();
  if (r < 0) {
    std::cerr << "failed to initialize: " << cpp_strerror(r) << std::endl;
    goto cleanup;
  }

  mirror->run();

 cleanup:
  unregister_async_signal_handler(SIGHUP, sighup_handler);
  unregister_async_signal_handler(SIGINT, handle_signal);
  unregister_async_signal_handler(SIGTERM, handle_signal);
  shutdown_async_signal_handler();

  delete mirror;
  g_ceph_context->put();

  return r < 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
