
#include "include/types.h"
#include "common/config.h"
#include "common/ceph_argparse.h"
#include "common/errno.h"
#include "global/global_init.h"

#include "DataScan.h"

#include "ToolsAuditLogger.h"

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

  rc = data_scan.connect_rados();
  if (rc != 0) {
    return rc;
  }

  std::unique_ptr<ToolsAuditLogger> logger = nullptr;
  if (auto logger_r = ToolsAuditLogger::create_for_tool(cct.get(), data_scan.get_rados_handle(), "cephfs_data_scan"); logger_r.has_value()) {
    logger = std::move(logger_r.value());
    logger->log_begin(argv[0], ToolsAuditLogger::get_audit_cmd_args(args), ceph_clock_now().sec());
  } else {
    std::cerr << "could not create audit db for cephfs-data-scan, moving on..." << std::endl;
  }

  // Finally, execute the user's commands
  rc = data_scan.main(args);

  if (logger) {
    logger->log_end(ceph_clock_now().sec(), rc < 0 ? data_scan.get_audit_status().empty() ? "unknown_error" : data_scan.get_audit_status() : "completed", rc);
  }

  if (rc != 0) {
    std::cerr << "Error (" << cpp_strerror(rc) << ")" << std::endl;
  }


  return rc;
}

