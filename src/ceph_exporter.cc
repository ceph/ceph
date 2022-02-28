#include "mgr/DaemonMetricCollector.h"
#include <iostream>
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include <map>


int main(int argc, char** argv) {
  // TODO: daemonize
  std::cout << "inside exporter" << std::endl;
  // std::map<std::string,std::string> defaults = {
  //   { "keyring", "$mgr_data/keyring" }
  // };
  // auto args = argv_to_vec(argc, argv);
  // auto cct = global_init(&defaults, args, CEPH_ENTITY_TYPE_EXPORTER,
	// 		 CODE_ENVIRONMENT_DAEMON, 0);
  DaemonMetricCollector collector;
  collector.main();
}
