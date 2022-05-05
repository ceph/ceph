#include "common/ceph_argparse.h"
#include "exporter/DaemonMetricCollector.h"
#include "exporter/http_server.h"
#include <boost/thread/thread.hpp>
#include <iostream>
#include <map>
#include <string>

static void usage() {
  std::cout << "usage: ceph-exporter --cert cert.pem --key cert.key "
               "--tls_options no[flags]\n"
            << "--cert: TLS/SSL certificate in pem format\n"
            << "--key: TLS/SSL key in pem format\n"
            << "--tls_options: colon separated options for tls and ssl.\n"
            << "\tExample -> "
               "default_workarounds:no_compression:no_sslv2:no_sslv3:no_tlsv1:"
               "no_tlsv1_1:no_tlsv1_2:single_dh_use\n"
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
  std::string val, cert_path, key_path, tls_options;
  for (auto i = args.begin(); i != args.end();) {
    if (ceph_argparse_witharg(args, i, &val, "--cert", (char *)NULL)) {
      cert_path = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--key", (char *)NULL)) {
      key_path = val;
    } else if (ceph_argparse_witharg(args, i, &val, "--tls_options",
                                     (char *)NULL)) {
      tls_options = val;
    } else if (ceph_argparse_flag(args, i, "--help", (char *)NULL) ||
               ceph_argparse_flag(args, i, "-h", (char *)NULL)) {
      usage();
      exit(0);
    }
  }

  boost::thread server_thread(http_server_thread_entrypoint, cert_path,
                              key_path, tls_options);
  DaemonMetricCollector &collector = collector_instance();
  collector.main();
  server_thread.join();
}
