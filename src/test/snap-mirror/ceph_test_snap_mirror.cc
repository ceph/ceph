#include <vector>

#include <string.h>

#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/config.h"
#include "common/debug.h"
#include "common/errno.h"
#include "common/async/context_pool.h"
#include "global/global_init.h"
#include "global/signal_handler.h"

#include "SnapReplicator.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "SnapReplicator " << __func__

using namespace std::literals::string_literals;

enum snap_cluster_type_t {
  SOURCE = 1,
  DEST   = 2
};

class SnapReplicatorDispatcher
{
  public:
    SnapReplicatorDispatcher(const std::string& _src_conf_path,
                             const std::string& _dst_conf_path,
                             const std::string& _src_keyring_path,
                             const std::string& _dst_keyring_path,
                             const std::string& _src_dir,
                             const std::string& _src_fs_name,
                             const std::string& _dst_fs_name,
                             const std::string& _old_snap_name,
                             const std::string& _new_snap_name,
                             const std::string& _src_auth_id,
                             const std::string& _dst_auth_id) :
      src_conf_path(_src_conf_path),
      dst_conf_path(_dst_conf_path),
      src_keyring_path(_src_keyring_path),
      dst_keyring_path(_dst_keyring_path),
      src_dir(_src_dir),
      src_fs_name(_src_fs_name),
      dst_fs_name(_dst_fs_name),
      old_snap_name(_old_snap_name),
      new_snap_name(_new_snap_name),
      src_auth_id(_src_auth_id),
      dst_auth_id(_dst_auth_id)
    { }

    ~SnapReplicatorDispatcher()
    {
    }

    int dispatch();

    int finish_replication();

    int get_errno() {
      return my_errno;
    }

  private:
    const std::string src_conf_path;
    const std::string dst_conf_path;
    const std::string src_keyring_path;
    const std::string dst_keyring_path;
    const std::string src_dir;      // dir for which snapshots are being replicated
    const std::string src_fs_name;
    const std::string dst_fs_name;
    const std::string old_snap_name;
    const std::string new_snap_name;
    const std::string src_auth_id;
    const std::string dst_auth_id;
    std::string src_snap_dir;     // ".snap" or something user configured
    std::string dst_snap_dir;     // ".snap" or something user configured

    int my_errno = 0;
    bool is_src_mounted = false;
    bool is_dst_mounted = false;
    struct ceph_mount_info *src_mnt = nullptr;
    struct ceph_mount_info *dst_mnt = nullptr;

    int connect_to_cluster(snap_cluster_type_t sct);
    int create_directory(struct ceph_mount_info *mnt, const std::string& dir);
};

int SnapReplicatorDispatcher::create_directory(struct ceph_mount_info *mnt, const std::string& dir)
{
  int rv = ceph_mkdirs(mnt, dir.c_str(), 0755);
  if (rv < 0 && -rv != EEXIST) {
    my_errno = -rv;
    return -1;
  }
  return 0;
}

int SnapReplicatorDispatcher::connect_to_cluster(snap_cluster_type_t sct)
{
  ceph_assert(sct == snap_cluster_type_t::SOURCE || sct == snap_cluster_type_t::DEST);

  struct ceph_mount_info *& mnt = (sct == SOURCE ? src_mnt        : dst_mnt);
  const std::string& conf_path  = (sct == SOURCE ? src_conf_path  : dst_conf_path);
  const std::string& keyring_path  = (sct == SOURCE ? src_keyring_path  : dst_keyring_path);
  const std::string& fs_name    = (sct == SOURCE ? src_fs_name    : dst_fs_name);
  std::string& snap_dir         = (sct == SOURCE ? src_snap_dir   : dst_snap_dir);
  bool& is_mounted              = (sct == SOURCE ? is_src_mounted : is_dst_mounted);
  const std::string& auth_id    = (sct == SOURCE ? src_auth_id    : dst_auth_id);

  int rv = ceph_create(&mnt, auth_id.c_str());
  if (rv < 0) {
    dout(20) << ": ceph_create failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  dout(20) << ": created mount" << dendl;

  rv = ceph_conf_read_file(mnt, conf_path.c_str());
  if (rv < 0) {
    dout(20) << ": read cluster conf failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  dout(20) << ": read cluster conf completed" << dendl;

  rv = ceph_conf_set(mnt, "keyring", keyring_path.c_str());
  if (rv < 0) {
    dout(20) << ": ceph_conf_set(keyring, " << keyring_path << ") failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  dout(20) << ": keyring path set to " << keyring_path << "" << dendl;

  rv = ceph_conf_parse_env(mnt, NULL);
  if (rv < 0) {
    dout(20) << ": ceph_conf_parse_env failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  dout(20) << ": parse env completed" << dendl;

  rv = ceph_init(mnt);
  if (rv < 0) {
    dout(20) << ": ceph_init failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  dout(20) << ": init completed" << dendl;

  char buf[128] = {0,};
  rv = ceph_conf_get(mnt, "client_snapdir", buf, sizeof(buf));
  if (rv < 0) {
    dout(20) << ": ceph_conf_get client_snapdir failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  snap_dir = buf;
  dout(20) << ": client_snapdir is '" << snap_dir << "'" << dendl;

  rv = ceph_select_filesystem(mnt, fs_name.c_str());
  if (rv < 0) {
    dout(20) << ": ceph_select_filesystem failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  dout(20) << ": filesystem '" << fs_name << "' selected" << dendl;

  // we mount the root; then create the snap root dir; and unmount the root dir
  rv = ceph_mount(mnt, "/");
  if (rv < 0) {
    dout(20) << ": ceph_mount root failed!" << dendl;
    my_errno = -rv;
    return -1;
  }
  dout(20) << ": filesystem '/' dir mounted" << dendl;

  ceph_assert(ceph_is_mounted(mnt));

  if (sct == snap_cluster_type_t::DEST) {
    rv = create_directory(mnt, src_dir);
    if (rv < 0) {
      dout(20) << ": error creating snapshot destination dir '" << src_dir << "'" << dendl;
      return -1;
    }
    dout(20) << ": '" << src_dir << "' created" << dendl;
  }
  is_mounted = true;

  return 0;
}

int SnapReplicatorDispatcher::finish_replication()
{
  int rv = 0;

  if (is_dst_mounted) {
    rv = ceph_sync_fs(dst_mnt);
    ceph_unmount(dst_mnt);
    ceph_release(dst_mnt);
    dst_mnt = nullptr;
    is_dst_mounted = false;
  }

  if (is_src_mounted) {
    ceph_unmount(src_mnt);
    ceph_release(src_mnt);
    src_mnt = nullptr;
    is_src_mounted = false;
  }

  return rv;
}

int SnapReplicatorDispatcher::dispatch()
{
  dout(20) << ": snap-replicator created" << dendl;

  dout(20) << ": snap-replicator connecting to source cluster" << dendl;

  if (connect_to_cluster(snap_cluster_type_t::SOURCE) < 0) {
    dout(20) << ": ERROR: failed to connect to source cluster (" << get_errno() << ":" << strerror(get_errno()) << ")" << dendl;
    return 1;
  }

  dout(20) << ": snap-replicator connected to source cluster" << dendl;
  dout(20) << ": \n" << dendl;
  dout(20) << ": snap-replicator connecting to destination cluster" << dendl;
  
  if (connect_to_cluster(snap_cluster_type_t::DEST) < 0) {
    dout(20) << ": ERROR: failed to connect to destination cluster (" << get_errno() << ":" << strerror(get_errno()) << ")" << dendl;
    return 1;
  }
  
  dout(20) << ": snap-replicator connected to destination cluster" << dendl;
  dout(20) << ": \n" << dendl;

  SnapReplicator sr(src_mnt, dst_mnt, src_dir, std::make_pair(old_snap_name, new_snap_name));

  if (sr.replicate() < 0) {
    dout(20) << ": snap-replicator replicate failed" << dendl;
    return 1;
  }
  
  dout(20) << ": snap-replicator replicate done" << dendl;
  
  if (finish_replication() < 0) {
    dout(20) << ": ERROR: finishing replication" << dendl;
    return 1;
  }
  
  dout(20) << ": snap-replicator finish_replication done" << dendl;
  return 0;
}

void usage(const char **argv)
{
  std::cerr << "Usage:\n\t";
  std::cerr << argv[0] << " <old_snap_name> <new_snap_name>" << std::endl;
}

int main(int argc, const char **argv)
{
  if (argc < 10) {
    std::cerr << "Usage:\n\t";
    std::cerr << argv[0]
              << " <src conf> <dst conf> <src keyring path> <dst keyring path> "
                 "<src dir> <src fs> <dst fs> <src auth id> <dst auth id> "
                 "<old_snap_name> <new_snap_name>" << std::endl;
    ::exit(0);
  }

  // const std::string src_conf = "/etc/ceph/src.conf";
  // const std::string dst_conf = "/etc/ceph/dst.conf";
  // const std::string src_keyring_path = "/etc/ceph/src.keyring";
  // const std::string dst_keyring_path = "/etc/ceph/dst.keyring";
  // const std::string src_path = "/mchangir";
  // const std::string src_fs_name = "a";
  // const std::string dst_fs_name = "a";
  // const std::string src_auth_id = "fs_a";
  // const std::string dst_auth_id   = "admin";
  // const std::string old_snap_name  = argv[1];
  // const std::string new_snap_name  = argv[2];

  std::vector<const char*> args;
  argv_to_vec(argc, argv, args);

  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);

  common_init_finish(g_ceph_context);

  const std::string src_conf = argv[1];
  const std::string dst_conf = argv[2];
  const std::string src_keyring_path = argv[3];
  const std::string dst_keyring_path = argv[4];
  const std::string src_path = argv[5];
  const std::string src_fs_name = argv[6];
  const std::string dst_fs_name = argv[7];
  const std::string src_auth_id = argv[8];
  const std::string dst_auth_id   = argv[9];
  const std::string old_snap_name  = argv[10];
  const std::string new_snap_name  = argv[11];

  SnapReplicatorDispatcher srd(src_conf, dst_conf,
                               src_keyring_path, dst_keyring_path,
                               src_path, src_fs_name, dst_fs_name,
                               old_snap_name, new_snap_name,
                               src_auth_id, dst_auth_id);

  srd.dispatch();

  return 0;
}
