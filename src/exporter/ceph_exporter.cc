#include "common/ceph_argparse.h"
#include "common/config.h"
#include "exporter/DaemonMetricCollector.h"
#include "exporter/http_server.h"
#include "global/global_init.h"
#include "global/global_context.h"

#include <boost/thread/thread.hpp>
#include <iostream>
#include <map>
#include <string>

#define dout_context g_ceph_context

static void usage() {
  std::cout << "usage: ceph-exporter [options]\n"
            << "options:\n"
               "  --sock-dir: The path to ceph daemons socket files dir\n"
               "  --addrs: Host ip address where exporter is deployed\n"
               "  --port: Port to deploy exporter on. Default is 9926\n"
               "  --prio-limit: Only perf counters greater than or equal to exporter_prio_limit are fetched\n"
               "  --stats-period: Time to wait before sending requests again to exporter server (seconds)"
            << std::endl;
  generic_server_usage();
}

int main(int argc, char **argv) {

  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_DAEMON, 0);
  std::string val, sock_dir, exporter_addrs, exporter_port, exporter_prio_limit;
  for (auto i = args.begin(); i != args.end();) {
    if (ceph_argparse_double_dash(args, i)) {
      break;
    } else if (ceph_argparse_witharg(args, i, &val, "--sock-dir", (char *)NULL)) {
      sock_dir = val;
      cct->_conf.set_val("sock_dir", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--addrs", (char *)NULL)) {
      exporter_addrs = val;
      cct->_conf.set_val("exporter_addrs", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--port", (char *)NULL)) {
      exporter_port = val;
      cct->_conf.set_val("exporter_port", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--prio-limit", (char *)NULL)) {
      exporter_prio_limit = val;
      cct->_conf.set_val("exporter_prio_limit", val);
    } else if (ceph_argparse_witharg(args, i, &val, "--stats-period", (char *)NULL)) {
      exporter_prio_limit = val;
      cct->_conf.set_val("exporter_stats_period", val);
    } else {
      ++i;
    }
  }
  common_init_finish(g_ceph_context);

  boost::thread server_thread(http_server_thread_entrypoint, exporter_addrs, exporter_port);
  DaemonMetricCollector &collector = collector_instance();
  collector.set_sock_dir(sock_dir);
  collector.main();
  server_thread.join();
}
