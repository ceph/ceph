
#include "include/types.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"

#include "DataScan.h"

using namespace std;

int main(int argc, const char **argv)
{
  auto args = argv_to_vec(argc, argv);
  if (args.empty()) {
    cerr << argv[0] << ": -h or --help for usage" << std::endl;
    exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    DataScan::usage();
    exit(0);
  }

  auto cct = global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  DataScan data_scan;

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


  return rc;
}

