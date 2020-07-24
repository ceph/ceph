#include <vector>

#include <string.h>

#include "common/ceph_argparse.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/async/context_pool.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"

#include "SnapReplicator.h"

using namespace std::literals::string_literals;

void usage(const char **argv)
{
  std::cerr << "Usage:\n\t";
  std::cerr << argv[0] << " <old_snap_name> <new_snap_name>\n";
}

int main(int argc, const char **argv)
{
  std::vector<const char*> args;

  argv_to_vec(argc, argv, args);
  if (args.empty()) {
    std::cerr << argv[0] << ": -h or --help for usage" << std::endl;
    ::exit(1);
  }
  if (ceph_argparse_need_usage(args)) {
    usage(argv);
    ::exit(0);
  }

  std::string const src_conf = "/home/mchangir/src.conf";
  std::string const dst_conf = "/home/mchangir/dst.conf";
  std::string const src_keyring_path = "/home/mchangir/src.keyring";
  std::string const dst_keyring_path = "/home/mchangir/dst.keyring";
  std::string const src_path = "/mchangir";
  std::string const src_fs_name = "a";
  std::string const dst_fs_name = "a";
  std::string const source_auth_id = "fs_a";
  std::string const dest_auth_id   = "admin";
  std::string const old_snap_name  = args[0];
  std::string const new_snap_name  = args[1];

  SnapReplicator sr(src_conf, dst_conf,
                    src_keyring_path, dst_keyring_path,
                    src_path, src_fs_name, dst_fs_name,
                    old_snap_name, new_snap_name,
                    source_auth_id, dest_auth_id);

  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator created\n";

  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator connecting to source cluster\n";

  if (sr.connect_to_cluster(snap_cluster_type_t::SOURCE) < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: failed to connect to source cluster (" << sr.get_errno() << ":" << strerror(sr.get_errno()) << ")\n";
    return 1;
  }

  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator connected to source cluster\n";
  std::clog << __func__ << ":" << __LINE__ << ": \n\n";
  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator connecting to destination cluster\n";
  
  if (sr.connect_to_cluster(snap_cluster_type_t::DEST) < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: failed to connect to destination cluster (" << sr.get_errno() << ":" << strerror(sr.get_errno()) << ")\n";
    return 1;
  }
  
  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator connected to destination cluster\n";
  std::clog << __func__ << ":" << __LINE__ << ": \n\n";
  
  if (sr.prepare_to_replicate() < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: preparing to replicate (" << sr.get_errno() << ":" << strerror(sr.get_errno()) << ")\n";
    return 1;
  }
  
  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator prepare_to_replicate done\n";
  
  if (sr.replicate_phase1() < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: replicating in phase1 (" << sr.get_errno() << ":" << strerror(sr.get_errno()) << ")\n";
    return 1;
  }
  
  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator replicate_phase1 done\n";
  
  if (sr.replicate_phase2(""s) < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: replicating in phase2 (" << sr.get_errno() << ":" << strerror(sr.get_errno()) << ")\n";
    return 1;
  }
  
  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator replicate_phase2 done\n";
  
  if (sr.finish_replication() < 0) {
    std::clog << __func__ << ":" << __LINE__ << ": ERROR: finishing replication\n";
    return 1;
  }
  
  std::clog << __func__ << ":" << __LINE__ << ": snap-replicator finish_replication done\n";
  return 0;
}
