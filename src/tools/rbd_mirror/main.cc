// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/perf_counters.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "Mirror.h"
#include "Types.h"

#include <vector>

rbd::mirror::Mirror *mirror = nullptr;
PerfCounters *g_perf_counters = nullptr;

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
  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_DAEMON,
			 CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

  if (g_conf()->daemonize) {
    global_init_daemonize(g_ceph_context);
  }

  common_init_finish(g_ceph_context);

  init_async_signal_handler();
  register_async_signal_handler(SIGHUP, sighup_handler);
  register_async_signal_handler_oneshot(SIGINT, handle_signal);
  register_async_signal_handler_oneshot(SIGTERM, handle_signal);

  std::vector<const char*> cmd_args;
  argv_to_vec(argc, argv, cmd_args);

  // disable unnecessary librbd cache
  g_ceph_context->_conf.set_val_or_die("rbd_cache", "false");

  auto prio =
      g_ceph_context->_conf.get_val<int64_t>("rbd_mirror_perf_stats_prio");
  PerfCountersBuilder plb(g_ceph_context, "rbd_mirror",
                          rbd::mirror::l_rbd_mirror_first,
                          rbd::mirror::l_rbd_mirror_last);
  plb.add_u64_counter(rbd::mirror::l_rbd_mirror_replay, "replay", "Replays",
                      "r", prio);
  plb.add_u64_counter(rbd::mirror::l_rbd_mirror_replay_bytes, "replay_bytes",
                      "Replayed data", "rb", prio, unit_t(UNIT_BYTES));
  plb.add_time_avg(rbd::mirror::l_rbd_mirror_replay_latency, "replay_latency",
                   "Replay latency", "rl", prio);
  g_perf_counters = plb.create_perf_counters();
  g_ceph_context->get_perfcounters_collection()->add(g_perf_counters);

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

  g_ceph_context->get_perfcounters_collection()->remove(g_perf_counters);

  delete mirror;
  delete g_perf_counters;

  return r < 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
