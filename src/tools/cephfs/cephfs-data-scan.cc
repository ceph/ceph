
#include "include/types.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"

#include "DataScan.h"


int main(int argc, const char **argv)
{
  vector<const char*> args;
  argv_to_vec(argc, argv, args);
  env_to_vec(args);

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  DataScan data_scan;

  // Handle --help before calling init() so we don't depend on network.
  if (args.empty() || (args.size() == 1 && (std::string(args[0]) == "--help" || std::string(args[0]) == "-h"))) {
    data_scan.usage();
    return 0;
  }

  // Connect to mon cluster, download MDS map etc
  int rc = data_scan.init();
  if (rc != 0) {
      std::cerr << "Error in initialization: " << cpp_strerror(rc) << std::endl;
      return rc;
  }

  // Finally, execute the user's commands
  rc = data_scan.main(args);
  if (rc != 0) {
    std::cerr << "Error (" << cpp_strerror(rc) << ")" << std::endl;
  }

  data_scan.shutdown();

  return rc;
}

