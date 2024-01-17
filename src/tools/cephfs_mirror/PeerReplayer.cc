// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stack>
#include <fcntl.h>
#include <algorithm>
#include <sys/time.h>
#include <sys/file.h>
#include <boost/scope_exit.hpp>

#include "common/admin_socket.h"
#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "FSMirror.h"
#include "PeerReplayer.h"
#include "Utils.h"

#include "json_spirit/json_spirit.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::PeerReplayer("   \
                           << m_peer.uuid << ") " << __func__

using namespace std;

namespace cephfs {
namespace mirror {

namespace {

const std::string PEER_CONFIG_KEY_PREFIX = "cephfs/mirror/peer";

std::string snapshot_dir_path(CephContext *cct, const std::string &path) {
  return path + "/" + cct->_conf->client_snapdir;
}

std::string snapshot_path(const std::string &snap_dir, const std::string &snap_name) {
  return snap_dir + "/" + snap_name;
}

std::string snapshot_path(CephContext *cct, const std::string &path, const std::string &snap_name) {
  return path + "/" + cct->_conf->client_snapdir + "/" + snap_name;
}

std::string entry_path(const std::string &dir, const std::string &name) {
  return dir + "/" + name;
}

std::map<std::string, std::string> decode_snap_metadata(snap_metadata *snap_metadata,
                                                        size_t nr_snap_metadata) {
  std::map<std::string, std::string> metadata;
  for (size_t i = 0; i < nr_snap_metadata; ++i) {
    metadata.emplace(snap_metadata[i].key, snap_metadata[i].value);
  }

  return metadata;
}

std::string peer_config_key(const std::string &fs_name, const std::string &uuid) {
  return PEER_CONFIG_KEY_PREFIX + "/" + fs_name + "/" + uuid;
}

class PeerAdminSocketCommand {
public:
  virtual ~PeerAdminSocketCommand() {
  }
  virtual int call(Formatter *f) = 0;
};

class StatusCommand : public PeerAdminSocketCommand {
public:
  explicit StatusCommand(PeerReplayer *peer_replayer)
    : peer_replayer(peer_replayer) {
  }

  int call(Formatter *f) override {
    peer_replayer->peer_status(f);
    return 0;
  }

private:
  PeerReplayer *peer_replayer;
};

// helper to open a directory relative to a file descriptor
int opendirat(MountRef mnt, int dirfd, const std::string &relpath, int flags,
              ceph_dir_result **dirp) {
  int r = ceph_openat(mnt, dirfd, relpath.c_str(), flags, 0);
  if (r < 0) {
    return r;
  }

  int fd = r;
  r = ceph_fdopendir(mnt, fd, dirp);
  ceph_close(mnt, fd);
  return r;
}

} // anonymous namespace

class PeerReplayerAdminSocketHook : public AdminSocketHook {
public:
  PeerReplayerAdminSocketHook(CephContext *cct, const Filesystem &filesystem,
                              const Peer &peer, PeerReplayer *peer_replayer)
    : admin_socket(cct->get_admin_socket()) {
    int r;
    std::string cmd;

    // mirror peer status format is name@id uuid
    cmd = "fs mirror peer status "
          + stringify(filesystem.fs_name) + "@" + stringify(filesystem.fscid)
          + " "
          + stringify(peer.uuid);
    r = admin_socket->register_command(
      cmd, this, "get peer mirror status");
    if (r == 0) {
      commands[cmd] = new StatusCommand(peer_replayer);
    }
  }

  ~PeerReplayerAdminSocketHook() override {
    admin_socket->unregister_commands(this);
    for (auto &[command, cmdptr] : commands) {
      delete cmdptr;
    }
  }

  int call(std::string_view command, const cmdmap_t& cmdmap,
           const bufferlist&,
           Formatter *f, std::ostream &errss, bufferlist &out) override {
    auto p = commands.at(std::string(command));
    return p->call(f);
  }

private:
  typedef std::map<std::string, PeerAdminSocketCommand*, std::less<>> Commands;

  AdminSocket *admin_socket;
  Commands commands;
};

PeerReplayer::PeerReplayer(CephContext *cct, FSMirror *fs_mirror,
                           RadosRef local_cluster, const Filesystem &filesystem,
                           const Peer &peer, const std::set<std::string, std::less<>> &directories,
                           MountRef mount, ServiceDaemon *service_daemon)
  : m_cct(cct),
    m_fs_mirror(fs_mirror),
    m_local_cluster(local_cluster),
    m_filesystem(filesystem),
    m_peer(peer),
    m_directories(directories.begin(), directories.end()),
    m_local_mount(mount),
    m_service_daemon(service_daemon),
    m_asok_hook(new PeerReplayerAdminSocketHook(cct, filesystem, peer, this)),
    m_lock(ceph::make_mutex("cephfs::mirror::PeerReplayer::" + stringify(peer.uuid))) {
  // reset sync stats sent via service daemon
  m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                 SERVICE_DAEMON_FAILED_DIR_COUNT_KEY, (uint64_t)0);
  m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                 SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY, (uint64_t)0);
}

PeerReplayer::~PeerReplayer() {
  delete m_asok_hook;
}

int PeerReplayer::init() {
  dout(20) << ": initial dir list=[" << m_directories << "]" << dendl;
  for (auto &dir_root : m_directories) {
    m_snap_sync_stats.emplace(dir_root, SnapSyncStat());
  }

  auto &remote_client = m_peer.remote.client_name;
  auto &remote_cluster = m_peer.remote.cluster_name;
  auto remote_filesystem = Filesystem{0, m_peer.remote.fs_name};

  std::string key = peer_config_key(m_filesystem.fs_name, m_peer.uuid);
  std::string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" + key + "\""
    "}";

  bufferlist in_bl;
  bufferlist out_bl;

  int r = m_local_cluster->mon_command(cmd, in_bl, &out_bl, nullptr);
  dout(5) << ": mon command r=" << r << dendl;
  if (r < 0 && r != -ENOENT) {
    return r;
  }

  std::string mon_host;
  std::string cephx_key;
  if (!r) {
    json_spirit::mValue root;
    if (!json_spirit::read(out_bl.to_str(), root)) {
      derr << ": invalid config-key JSON" << dendl;
      return -EBADMSG;
    }
    try {
      auto &root_obj = root.get_obj();
      mon_host = root_obj.at("mon_host").get_str();
      cephx_key = root_obj.at("key").get_str();
      dout(0) << ": remote monitor host=" << mon_host << dendl;
    } catch (std::runtime_error&) {
      derr << ": unexpected JSON received" << dendl;
      return -EBADMSG;
    }
  }

  r = connect(remote_client, remote_cluster, &m_remote_cluster, mon_host, cephx_key);
  if (r < 0) {
    derr << ": error connecting to remote cluster: " << cpp_strerror(r)
         << dendl;
    return r;
  }

  r = mount(m_remote_cluster, remote_filesystem, false, &m_remote_mount);
  if (r < 0) {
    m_remote_cluster.reset();
    derr << ": error mounting remote filesystem=" << remote_filesystem << dendl;
    return r;
  }

  std::scoped_lock locker(m_lock);
  auto nr_replayers = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_concurrent_directory_syncs");
  dout(20) << ": spawning " << nr_replayers << " snapshot replayer(s)" << dendl;

  while (nr_replayers-- > 0) {
    std::unique_ptr<SnapshotReplayerThread> replayer(
      new SnapshotReplayerThread(this));
    std::string name("replayer-" + stringify(nr_replayers));
    replayer->create(name.c_str());
    m_replayers.push_back(std::move(replayer));
  }

  return 0;
}

void PeerReplayer::shutdown() {
  dout(20) << dendl;

  {
    std::scoped_lock locker(m_lock);
    ceph_assert(!m_stopping);
    m_stopping = true;
    m_cond.notify_all();
  }

  for (auto &replayer : m_replayers) {
    replayer->join();
  }
  m_replayers.clear();
  ceph_unmount(m_remote_mount);
  ceph_release(m_remote_mount);
  m_remote_mount = nullptr;
  m_remote_cluster.reset();
}

void PeerReplayer::add_directory(string_view dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  std::scoped_lock locker(m_lock);
  m_directories.emplace_back(dir_root);
  m_snap_sync_stats.emplace(dir_root, SnapSyncStat());
  m_cond.notify_all();
}

void PeerReplayer::remove_directory(string_view dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  auto _dir_root = std::string(dir_root);

  std::scoped_lock locker(m_lock);
  auto it = std::find(m_directories.begin(), m_directories.end(), _dir_root);
  if (it != m_directories.end()) {
    m_directories.erase(it);
  }

  auto it1 = m_registered.find(_dir_root);
  if (it1 == m_registered.end()) {
    m_snap_sync_stats.erase(_dir_root);
  } else {
    it1->second.canceled = true;
  }
  m_cond.notify_all();
}

boost::optional<std::string> PeerReplayer::pick_directory() {
  dout(20) << dendl;

  auto now = clock::now();
  auto retry_timo = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_retry_failed_directories_interval");

  boost::optional<std::string> candidate;
  for (auto &dir_root : m_directories) {
    auto &sync_stat = m_snap_sync_stats.at(dir_root);
    if (sync_stat.failed) {
      std::chrono::duration<double> d = now - *sync_stat.last_failed;
      if (d.count() < retry_timo) {
        continue;
      }
    }
    if (!m_registered.count(dir_root)) {
      candidate = dir_root;
      break;
    }
  }

  std::rotate(m_directories.begin(), m_directories.begin() + 1, m_directories.end());
  return candidate;
}

int PeerReplayer::register_directory(const std::string &dir_root,
                                     SnapshotReplayerThread *replayer) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  ceph_assert(m_registered.find(dir_root) == m_registered.end());

  DirRegistry registry;
  int r = try_lock_directory(dir_root, replayer, &registry);
  if (r < 0) {
    return r;
  }

  dout(5) << ": dir_root=" << dir_root << " registered with replayer="
          << replayer << dendl;
  m_registered.emplace(dir_root, std::move(registry));
  return 0;
}

void PeerReplayer::unregister_directory(const std::string &dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  auto it = m_registered.find(dir_root);
  ceph_assert(it != m_registered.end());

  unlock_directory(it->first, it->second);
  m_registered.erase(it);
  if (std::find(m_directories.begin(), m_directories.end(), dir_root) == m_directories.end()) {
    m_snap_sync_stats.erase(dir_root);
  }
}

int PeerReplayer::try_lock_directory(const std::string &dir_root,
                                     SnapshotReplayerThread *replayer, DirRegistry *registry) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  int r = ceph_open(m_remote_mount, dir_root.c_str(), O_RDONLY | O_DIRECTORY, 0);
  if (r < 0 && r != -ENOENT) {
    derr << ": failed to open remote dir_root=" << dir_root << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  if (r == -ENOENT) {
    // we snap under dir_root, so mode does not matter much
    r = ceph_mkdirs(m_remote_mount, dir_root.c_str(), 0755);
    if (r < 0) {
      derr << ": failed to create remote directory=" << dir_root << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }

    r = ceph_open(m_remote_mount, dir_root.c_str(), O_RDONLY | O_DIRECTORY, 0);
    if (r < 0) {
      derr << ": failed to open remote dir_root=" << dir_root << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }
  }

  int fd = r;
  r = ceph_flock(m_remote_mount, fd, LOCK_EX | LOCK_NB, (uint64_t)replayer->get_thread_id());
  if (r != 0) {
    if (r == -EWOULDBLOCK) {
      dout(5) << ": dir_root=" << dir_root << " is locked by cephfs-mirror, "
              << "will retry again" << dendl;
    } else {
      derr << ": failed to lock dir_root=" << dir_root << ": " << cpp_strerror(r)
           << dendl;
    }

    if (ceph_close(m_remote_mount, fd) < 0) {
      derr << ": failed to close (cleanup) remote dir_root=" << dir_root << ": "
           << cpp_strerror(r) << dendl;
    }
    return r;
  }

  dout(10) << ": dir_root=" << dir_root << " locked" << dendl;

  registry->fd = fd;
  registry->replayer = replayer;
  return 0;
}

void PeerReplayer::unlock_directory(const std::string &dir_root, const DirRegistry &registry) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  int r = ceph_flock(m_remote_mount, registry.fd, LOCK_UN,
                     (uint64_t)registry.replayer->get_thread_id());
  if (r < 0) {
    derr << ": failed to unlock remote dir_root=" << dir_root << ": " << cpp_strerror(r)
         << dendl;
    return;
  }

  r = ceph_close(m_remote_mount, registry.fd);
  if (r < 0) {
    derr << ": failed to close remote dir_root=" << dir_root << ": " << cpp_strerror(r)
         << dendl;
  }

  dout(10) << ": dir_root=" << dir_root << " unlocked" << dendl;
}

int PeerReplayer::build_snap_map(const std::string &dir_root,
                                 std::map<uint64_t, std::string> *snap_map, bool is_remote) {
  auto snap_dir = snapshot_dir_path(m_cct, dir_root);
  dout(20) << ": dir_root=" << dir_root << ", snap_dir=" << snap_dir
           << ", is_remote=" << is_remote << dendl;

  auto lr_str = is_remote ? "remote" : "local";
  auto mnt = is_remote ? m_remote_mount : m_local_mount;

  ceph_dir_result *dirp = nullptr;
  int r = ceph_opendir(mnt, snap_dir.c_str(), &dirp);
  if (r < 0) {
    if (is_remote && r == -ENOENT) {
      return 0;
    }
    derr << ": failed to open " << lr_str << " snap directory=" << snap_dir
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  std::set<std::string> snaps;
  auto entry = ceph_readdir(mnt, dirp);
  while (entry != NULL) {
    auto d_name = std::string(entry->d_name);
    dout(20) << ": entry=" << d_name << dendl;
    if (d_name != "." && d_name != ".." && d_name.rfind("_", 0) != 0) {
      snaps.emplace(d_name);
    }

    entry = ceph_readdir(mnt, dirp);
  }

  int rv = 0;
  for (auto &snap : snaps) {
    snap_info info;
    auto snap_path = snapshot_path(snap_dir, snap);
    r = ceph_get_snap_info(mnt, snap_path.c_str(), &info);
    if (r < 0) {
      derr << ": failed to fetch " << lr_str << " snap info for snap_path=" << snap_path
           << ": " << cpp_strerror(r) << dendl;
      rv = r;
      break;
    }

    uint64_t snap_id;
    if (is_remote) {
      if (!info.nr_snap_metadata) {
        derr << ": snap_path=" << snap_path << " has invalid metadata in remote snapshot"
             << dendl;
        rv = -EINVAL;
      } else {
        auto metadata = decode_snap_metadata(info.snap_metadata, info.nr_snap_metadata);
        dout(20) << ": snap_path=" << snap_path << ", metadata=" << metadata << dendl;
        auto it = metadata.find(PRIMARY_SNAP_ID_KEY);
        if (it == metadata.end()) {
          derr << ": snap_path=" << snap_path << " has missing \"" << PRIMARY_SNAP_ID_KEY
               << "\" in metadata" << dendl;
          rv = -EINVAL;
        } else {
          snap_id = std::stoull(it->second);
        }
        ceph_free_snap_info_buffer(&info);
      }
    } else {
      snap_id = info.id;
    }

    if (rv != 0) {
      break;
    }
    snap_map->emplace(snap_id, snap);
  }

  r = ceph_closedir(mnt, dirp);
  if (r < 0) {
    derr << ": failed to close " << lr_str << " snap directory=" << snap_dir
         << ": " << cpp_strerror(r) << dendl;
  }

  dout(10) << ": " << lr_str << " snap_map=" << *snap_map << dendl;
  return rv;
}

int PeerReplayer::propagate_snap_deletes(const std::string &dir_root,
                                         const std::set<std::string> &snaps) {
  dout(5) << ": dir_root=" << dir_root << ", deleted snapshots=" << snaps << dendl;

  for (auto &snap : snaps) {
    dout(20) << ": deleting dir_root=" << dir_root << ", snapshot=" << snap
             << dendl;
    int r = ceph_rmsnap(m_remote_mount, dir_root.c_str(), snap.c_str());
    if (r < 0) {
      derr << ": failed to delete remote snap dir_root=" << dir_root
           << ", snapshot=" << snaps << ": " << cpp_strerror(r) << dendl;
      return r;
    }
    inc_deleted_snap(dir_root);
  }

  return 0;
}

int PeerReplayer::propagate_snap_renames(
    const std::string &dir_root,
    const std::set<std::pair<std::string,std::string>> &snaps) {
  dout(10) << ": dir_root=" << dir_root << ", renamed snapshots=" << snaps << dendl;

  for (auto &snapp : snaps) {
    auto from = snapshot_path(m_cct, dir_root, snapp.first);
    auto to = snapshot_path(m_cct, dir_root, snapp.second);
    dout(20) << ": renaming dir_root=" << dir_root << ", snapshot from="
             << from << ", to=" << to << dendl;
    int r = ceph_rename(m_remote_mount, from.c_str(), to.c_str());
    if (r < 0) {
      derr << ": failed to rename remote snap dir_root=" << dir_root
           << ", snapshot from =" << from << ", to=" << to << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    inc_renamed_snap(dir_root);
  }

  return 0;
}

int PeerReplayer::remote_mkdir(const std::string &epath, const struct ceph_statx &stx,
                               const FHandles &fh) {
  dout(10) << ": remote epath=" << epath << dendl;

  int r = ceph_mkdirat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), stx.stx_mode & ~S_IFDIR);
  if (r < 0 && r != -EEXIST) {
    derr << ": failed to create remote directory=" << epath << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  r = ceph_chownat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), stx.stx_uid, stx.stx_gid,
                   AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to chown remote directory=" << epath << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  r = ceph_chmodat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), stx.stx_mode & ~S_IFMT,
                   AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to chmod remote directory=" << epath << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  struct timespec times[] = {{stx.stx_atime.tv_sec, stx.stx_atime.tv_nsec},
                             {stx.stx_mtime.tv_sec, stx.stx_mtime.tv_nsec}};
  r = ceph_utimensat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), times, AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to change [am]time on remote directory=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

#define NR_IOVECS 8 // # iovecs
#define IOVEC_SIZE (8 * 1024 * 1024) // buffer size for each iovec
int PeerReplayer::copy_to_remote(const std::string &dir_root,  const std::string &epath,
                                 const struct ceph_statx &stx, const FHandles &fh) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath << dendl;
  int l_fd;
  int r_fd;
  void *ptr;
  struct iovec iov[NR_IOVECS];

  int r = ceph_openat(m_local_mount, fh.c_fd, epath.c_str(), O_RDONLY | O_NOFOLLOW, 0);
  if (r < 0) {
    derr << ": failed to open local file path=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  l_fd = r;
  r = ceph_openat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(),
                  O_CREAT | O_TRUNC | O_WRONLY | O_NOFOLLOW, stx.stx_mode);
  if (r < 0) {
    derr << ": failed to create remote file path=" << epath << ": "
         << cpp_strerror(r) << dendl;
    goto close_local_fd;
  }

  r_fd = r;
  ptr = malloc(NR_IOVECS * IOVEC_SIZE);
  if (!ptr) {
    r = -ENOMEM;
    derr << ": failed to allocate memory" << dendl;
    goto close_remote_fd;
  }

  while (true) {
    if (should_backoff(dir_root, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    for (int i = 0; i < NR_IOVECS; ++i) {
      iov[i].iov_base = (char*)ptr + IOVEC_SIZE*i;
      iov[i].iov_len = IOVEC_SIZE;
    }

    r = ceph_preadv(m_local_mount, l_fd, iov, NR_IOVECS, -1);
    if (r < 0) {
      derr << ": failed to read local file path=" << epath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }
    if (r == 0) {
      break;
    }

    int iovs = (int)(r / IOVEC_SIZE);
    int t = r % IOVEC_SIZE;
    if (t) {
      iov[iovs].iov_len = t;
      ++iovs;
    }

    r = ceph_pwritev(m_remote_mount, r_fd, iov, iovs, -1);
    if (r < 0) {
      derr << ": failed to write remote file path=" << epath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }
  }

  if (r == 0) {
    r = ceph_fsync(m_remote_mount, r_fd, 0);
    if (r < 0) {
      derr << ": failed to sync data for file path=" << epath << ": "
           << cpp_strerror(r) << dendl;
    }
  }

  free(ptr);

close_remote_fd:
  if (ceph_close(m_remote_mount, r_fd) < 0) {
    derr << ": failed to close remote fd path=" << epath << ": " << cpp_strerror(r)
         << dendl;
    return -EINVAL;
  }

close_local_fd:
  if (ceph_close(m_local_mount, l_fd) < 0) {
    derr << ": failed to close local fd path=" << epath << ": " << cpp_strerror(r)
         << dendl;
    return -EINVAL;
  }

  return r == 0 ? 0 : r;
}

int PeerReplayer::remote_file_op(const std::string &dir_root, const std::string &epath,
                                 const struct ceph_statx &stx, const FHandles &fh,
                                 bool need_data_sync, bool need_attr_sync) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath << ", need_data_sync=" << need_data_sync
           << ", need_attr_sync=" << need_attr_sync << dendl;

  int r;
  if (need_data_sync) {
    if (S_ISREG(stx.stx_mode)) {
      r = copy_to_remote(dir_root, epath, stx, fh);
      if (r < 0) {
        derr << ": failed to copy path=" << epath << ": " << cpp_strerror(r) << dendl;
        return r;
      }
    } else if (S_ISLNK(stx.stx_mode)) {
      // free the remote link before relinking
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), 0);
      if (r < 0 && r != -ENOENT) {
        derr << ": failed to remove remote symlink=" << epath << dendl;
        return r;
      }
      char *target = (char *)alloca(stx.stx_size+1);
      r = ceph_readlinkat(m_local_mount, fh.c_fd, epath.c_str(), target, stx.stx_size);
      if (r < 0) {
        derr << ": failed to readlink local path=" << epath << ": " << cpp_strerror(r)
             << dendl;
        return r;
      }

      target[stx.stx_size] = '\0';
      r = ceph_symlinkat(m_remote_mount, target, fh.r_fd_dir_root, epath.c_str());
      if (r < 0 && r != EEXIST) {
        derr << ": failed to symlink remote path=" << epath << " to target=" << target
             << ": " << cpp_strerror(r) << dendl;
        return r;
      }
    } else {
      dout(5) << ": skipping entry=" << epath << ": unsupported mode=" << stx.stx_mode
              << dendl;
      return 0;
    }
  }

  if (need_attr_sync) {
    r = ceph_chownat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), stx.stx_uid, stx.stx_gid,
                     AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chown remote directory=" << epath << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }

    r = ceph_chmodat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), stx.stx_mode & ~S_IFMT,
                     AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chmod remote directory=" << epath << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }

    struct timespec times[] = {{stx.stx_atime.tv_sec, stx.stx_atime.tv_nsec},
                               {stx.stx_mtime.tv_sec, stx.stx_mtime.tv_nsec}};
    r = ceph_utimensat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), times, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to change [am]time on remote directory=" << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  return 0;
}

int PeerReplayer::cleanup_remote_dir(const std::string &dir_root,
                                     const std::string &epath, const FHandles &fh) {
  dout(20) << ": dir_root=" << dir_root << ", epath=" << epath
           << dendl;

  struct ceph_statx tstx;
  int r = ceph_statxat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), &tstx,
                       CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                       CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                       AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat remote directory=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  ceph_dir_result *tdirp;
  r = opendirat(m_remote_mount, fh.r_fd_dir_root, epath, AT_SYMLINK_NOFOLLOW,
                &tdirp);
  if (r < 0) {
    derr << ": failed to open remote directory=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  std::stack<SyncEntry> rm_stack;
  rm_stack.emplace(SyncEntry(epath, tdirp, tstx));
  while (!rm_stack.empty()) {
    if (should_backoff(dir_root, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    dout(20) << ": " << rm_stack.size() << " entries in stack" << dendl;
    std::string e_name;
    auto &entry = rm_stack.top();
    dout(20) << ": top of stack path=" << entry.epath << dendl;
    if (entry.is_directory()) {
      struct ceph_statx stx;
      struct dirent de;
      while (true) {
        r = ceph_readdirplus_r(m_remote_mount, entry.dirp, &de, &stx,
                               CEPH_STATX_MODE, AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
        if (r < 0) {
          derr << ": failed to read remote directory=" << entry.epath << dendl;
          break;
        }
        if (r == 0) {
          break;
        }

        auto d_name = std::string(de.d_name);
        if (d_name != "." && d_name != "..") {
          e_name = d_name;
          break;
        }
      }

      if (r == 0) {
        r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(), AT_REMOVEDIR);
        if (r < 0) {
          derr << ": failed to remove remote directory=" << entry.epath << ": "
               << cpp_strerror(r) << dendl;
          break;
        }

        dout(10) << ": done for remote directory=" << entry.epath << dendl;
        if (ceph_closedir(m_remote_mount, entry.dirp) < 0) {
          derr << ": failed to close remote directory=" << entry.epath << dendl;
        }
        rm_stack.pop();
        continue;
      }
      if (r < 0) {
        break;
      }

      auto epath = entry_path(entry.epath, e_name);
      if (S_ISDIR(stx.stx_mode)) {
        ceph_dir_result *dirp;
        r = opendirat(m_remote_mount, fh.r_fd_dir_root, epath, AT_SYMLINK_NOFOLLOW,
                      &dirp);
        if (r < 0) {
          derr << ": failed to open remote directory=" << epath << ": "
               << cpp_strerror(r) << dendl;
          break;
        }
        rm_stack.emplace(SyncEntry(epath, dirp, stx));
      } else {
        rm_stack.emplace(SyncEntry(epath, stx));
      }
    } else {
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(), 0);
      if (r < 0) {
        derr << ": failed to remove remote directory=" << entry.epath << ": "
             << cpp_strerror(r) << dendl;
        break;
      }
      dout(10) << ": done for remote file=" << entry.epath << dendl;
      rm_stack.pop();
    }
  }

  while (!rm_stack.empty()) {
    auto &entry = rm_stack.top();
    if (entry.is_directory()) {
      dout(20) << ": closing remote directory=" << entry.epath << dendl;
      if (ceph_closedir(m_remote_mount, entry.dirp) < 0) {
        derr << ": failed to close remote directory=" << entry.epath << dendl;
      }
    }

    rm_stack.pop();
  }

  return r;
}

int PeerReplayer::should_sync_entry(const std::string &epath, const struct ceph_statx &cstx,
                                    const FHandles &fh, bool *need_data_sync, bool *need_attr_sync) {
  dout(10) << ": epath=" << epath << dendl;

  *need_data_sync = false;
  *need_attr_sync = false;
  struct ceph_statx pstx;
  int r = ceph_statxat(fh.p_mnt, fh.p_fd, epath.c_str(), &pstx,
                       CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                       CEPH_STATX_SIZE | CEPH_STATX_CTIME | CEPH_STATX_MTIME,
                       AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0 && r != -ENOENT && r != -ENOTDIR) {
    derr << ": failed to stat prev entry= " << epath << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  if (r < 0) {
    // inode does not exist in prev snapshot or file type has changed
    // (file was S_IFREG earlier, S_IFDIR now).
    dout(5) << ": entry=" << epath << ", r=" << r << dendl;
    *need_data_sync = true;
    *need_attr_sync = true;
    return 0;
  }

  dout(10) << ": local cur statx: mode=" << cstx.stx_mode << ", uid=" << cstx.stx_uid
           << ", gid=" << cstx.stx_gid << ", size=" << cstx.stx_size << ", ctime="
           << cstx.stx_ctime << ", mtime=" << cstx.stx_mtime << dendl;
  dout(10) << ": local prev statx: mode=" << pstx.stx_mode << ", uid=" << pstx.stx_uid
           << ", gid=" << pstx.stx_gid << ", size=" << pstx.stx_size << ", ctime="
           << pstx.stx_ctime << ", mtime=" << pstx.stx_mtime << dendl;
  if ((cstx.stx_mode & S_IFMT) != (pstx.stx_mode & S_IFMT)) {
    dout(5) << ": entry=" << epath << " has mode mismatch" << dendl;
    *need_data_sync = true;
    *need_attr_sync = true;
  } else {
    *need_data_sync = (cstx.stx_size != pstx.stx_size) || (cstx.stx_mtime != pstx.stx_mtime);
    *need_attr_sync = (cstx.stx_ctime != pstx.stx_ctime);
  }

  return 0;
}

int PeerReplayer::propagate_deleted_entries(const std::string &dir_root,
                                            const std::string &epath, const FHandles &fh) {
  dout(10) << ": dir_root=" << dir_root << ", epath=" << epath << dendl;

  ceph_dir_result *dirp;
  int r = opendirat(fh.p_mnt, fh.p_fd, epath, AT_SYMLINK_NOFOLLOW, &dirp);
  if (r < 0) {
    if (r == -ELOOP) {
      dout(5) << ": epath=" << epath << " is a symbolic link -- mode sync"
              << " done when traversing parent" << dendl;
      return 0;
    }
    if (r == -ENOTDIR) {
      dout(5) << ": epath=" << epath << " is not a directory -- mode sync"
              << " done when traversing parent" << dendl;
      return 0;
    }
    if (r == -ENOENT) {
      dout(5) << ": epath=" << epath << " missing in previous-snap/remote dir-root"
              << dendl;
    }
    return r;
  }

  struct dirent *dire = (struct dirent *)alloca(512 * sizeof(struct dirent));
  while (true) {
    if (should_backoff(dir_root, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    int len = ceph_getdents(fh.p_mnt, dirp, (char *)dire, 512);
    if (len < 0) {
      derr << ": failed to read directory entries: " << cpp_strerror(len) << dendl;
      r = len;
      // flip errno to signal that we got an err (possible the
      // snapshot getting deleted in midst).
      if (r == -ENOENT) {
        r = -EINVAL;
      }
      break;
    }
    if (len == 0) {
      dout(10) << ": reached EOD" << dendl;
      break;
    }
    int nr = len / sizeof(struct dirent);
    for (int i = 0; i < nr; ++i) {
      if (should_backoff(dir_root, &r)) {
        dout(0) << ": backing off r=" << r << dendl;
        break;
      }
      std::string d_name = std::string(dire[i].d_name);
      if (d_name == "." || d_name == "..") {
        continue;
      }

      struct ceph_statx pstx;
      auto dpath = entry_path(epath, d_name);
      r = ceph_statxat(fh.p_mnt, fh.p_fd, dpath.c_str(), &pstx,
                       CEPH_STATX_MODE, AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
      if (r < 0) {
        derr << ": failed to stat (prev) directory=" << dpath << ": "
             << cpp_strerror(r) << dendl;
        // flip errno to signal that we got an err (possible the
        // snapshot getting deleted in midst).
        if (r == -ENOENT) {
          r = -EINVAL;
        }
        return r;
      }

      struct ceph_statx cstx;
      r = ceph_statxat(m_local_mount, fh.c_fd, dpath.c_str(), &cstx,
                       CEPH_STATX_MODE, AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
      if (r < 0 && r != -ENOENT) {
        derr << ": failed to stat local (cur) directory=" << dpath << ": "
             << cpp_strerror(r) << dendl;
        return r;
      }

      bool purge_remote = true;
      if (r == 0) {
        // directory entry present in both snapshots -- check inode
        // type
        if ((pstx.stx_mode & S_IFMT) == (cstx.stx_mode & S_IFMT)) {
          dout(5) << ": mode matches for entry=" << d_name << dendl;
          purge_remote = false;
        } else {
          dout(5) << ": mode mismatch for entry=" << d_name << dendl;
        }
      } else {
        dout(5) << ": entry=" << d_name << " missing in current snapshot" << dendl;
      }

      if (purge_remote) {
        dout(5) << ": purging remote entry=" << dpath << dendl;
        if (S_ISDIR(pstx.stx_mode)) {
          r = cleanup_remote_dir(dir_root, dpath, fh);
        } else {
          r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, dpath.c_str(), 0);
        }

        if (r < 0 && r != -ENOENT) {
          derr << ": failed to cleanup remote entry=" << d_name << ": "
               << cpp_strerror(r) << dendl;
          return r;
        }
      }
    }
  }

  ceph_closedir(fh.p_mnt, dirp);
  return r;
}

int PeerReplayer::open_dir(MountRef mnt, const std::string &dir_path,
                           boost::optional<uint64_t> snap_id) {
  dout(20) << ": dir_path=" << dir_path << dendl;
  if (snap_id) {
    dout(20) << ": expected snapshot id=" << *snap_id << dendl;
  }

  int fd = ceph_open(mnt, dir_path.c_str(), O_DIRECTORY | O_RDONLY, 0);
  if (fd < 0) {
    derr << ": cannot open dir_path=" << dir_path << ": " << cpp_strerror(fd)
         << dendl;
    return fd;
  }

  if (!snap_id) {
    return fd;
  }

  snap_info info;
  int r = ceph_get_snap_info(mnt, dir_path.c_str(), &info);
  if (r < 0) {
    derr << ": failed to fetch snap_info for path=" << dir_path
         << ": " << cpp_strerror(r) << dendl;
    ceph_close(mnt, fd);
    return r;
  }

  if (info.id != *snap_id) {
    dout(5) << ": got mismatching snapshot id for path=" << dir_path << " (" << info.id
            << " vs " << *snap_id << ") -- possible recreate" << dendl;
    ceph_close(mnt, fd);
    return -EINVAL;
  }

  return fd;
}

int PeerReplayer::pre_sync_check_and_open_handles(
    const std::string &dir_root,
    const Snapshot &current, boost::optional<Snapshot> prev,
    FHandles *fh) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": prev=" << prev << dendl;
  }

  auto cur_snap_path = snapshot_path(m_cct, dir_root, current.first);
  auto fd = open_dir(m_local_mount, cur_snap_path, current.second);
  if (fd < 0) {
    return fd;
  }

  // current snapshot file descriptor
  fh->c_fd = fd;

  MountRef mnt;
  if (prev) {
    mnt = m_local_mount;
    auto prev_snap_path = snapshot_path(m_cct, dir_root, (*prev).first);
    fd = open_dir(mnt, prev_snap_path, (*prev).second);
  } else {
    mnt = m_remote_mount;
    fd = open_dir(mnt, dir_root, boost::none);
  }

  if (fd < 0) {
    if (!prev || fd != -ENOENT) {
      ceph_close(m_local_mount, fh->c_fd);
      return fd;
    }

    // ENOENT of previous snap
    dout(5) << ": previous snapshot=" << *prev << " missing" << dendl;
    mnt = m_remote_mount;
    fd = open_dir(mnt, dir_root, boost::none);
    if (fd < 0) {
      ceph_close(m_local_mount, fh->c_fd);
      return fd;
    }
  }

  // "previous" snapshot or dir_root file descriptor
  fh->p_fd = fd;
  fh->p_mnt = mnt;

  {
    std::scoped_lock locker(m_lock);
    auto it = m_registered.find(dir_root);
    ceph_assert(it != m_registered.end());
    fh->r_fd_dir_root = it->second.fd;
  }

  dout(5) << ": using " << ((fh->p_mnt == m_local_mount) ? "local (previous) snapshot" : "remote dir_root")
          << " for incremental transfer" << dendl;
  return 0;
}

// sync the mode of the remote dir_root with that of the local dir_root
int PeerReplayer::sync_perms(const std::string& path) {
  int r = 0;
  struct ceph_statx tstx;

  r = ceph_statx(m_local_mount, path.c_str(), &tstx, CEPH_STATX_MODE,
		 AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to fetch stat for local path: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  r = ceph_chmod(m_remote_mount, path.c_str(), tstx.stx_mode);
  if (r < 0) {
    derr << ": failed to set mode for remote path: "
	 << cpp_strerror(r) << dendl;
    return r;
  }
  return 0;
}

void PeerReplayer::post_sync_close_handles(const FHandles &fh) {
  dout(20) << dendl;

  // @FHandles.r_fd_dir_root is closed in @unregister_directory since
  // its used to acquire an exclusive lock on remote dir_root.
  ceph_close(m_local_mount, fh.c_fd);
  ceph_close(fh.p_mnt, fh.p_fd);
}

int PeerReplayer::do_synchronize(const std::string &dir_root, const Snapshot &current,
                                 boost::optional<Snapshot> prev) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": incremental sync check from prev=" << prev << dendl;
  }

  FHandles fh;
  int r = pre_sync_check_and_open_handles(dir_root, current, prev, &fh);
  if (r < 0) {
    dout(5) << ": cannot proceeed with sync: " << cpp_strerror(r) << dendl;
    return r;
  }

  BOOST_SCOPE_EXIT_ALL( (this)(&fh) ) {
    post_sync_close_handles(fh);
  };

  // record that we are going to "dirty" the data under this
  // directory root
  auto snap_id_str{stringify(current.second)};
  r = ceph_fsetxattr(m_remote_mount, fh.r_fd_dir_root, "ceph.mirror.dirty_snap_id",
                     snap_id_str.c_str(), snap_id_str.size(), 0);
  if (r < 0) {
    derr << ": error setting \"ceph.mirror.dirty_snap_id\" on dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  struct ceph_statx tstx;
  r = ceph_fstatx(m_local_mount, fh.c_fd, &tstx,
                  CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                  CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                  AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat snap=" << current.first << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  ceph_dir_result *tdirp;
  r = ceph_fdopendir(m_local_mount, fh.c_fd, &tdirp);
  if (r < 0) {
    derr << ": failed to open local snap=" << current.first << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  std::stack<SyncEntry> sync_stack;
  sync_stack.emplace(SyncEntry(".", tdirp, tstx));
  while (!sync_stack.empty()) {
    if (should_backoff(dir_root, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    dout(20) << ": " << sync_stack.size() << " entries in stack" << dendl;
    std::string e_name;
    auto &entry = sync_stack.top();
    dout(20) << ": top of stack path=" << entry.epath << dendl;
    if (entry.is_directory()) {
      // entry is a directory -- propagate deletes for missing entries
      // (and changed inode types) to the remote filesystem.
      if (!entry.needs_remote_sync()) {
        r = propagate_deleted_entries(dir_root, entry.epath, fh);
        if (r < 0 && r != -ENOENT) {
          derr << ": failed to propagate missing dirs: " << cpp_strerror(r) << dendl;
          break;
        }
        entry.set_remote_synced();
      }

      struct ceph_statx stx;
      struct dirent de;
      while (true) {
        r = ceph_readdirplus_r(m_local_mount, entry.dirp, &de, &stx,
                               CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                               CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                               AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
        if (r < 0) {
          derr << ": failed to local read directory=" << entry.epath << dendl;
          break;
        }
        if (r == 0) {
          break;
        }

        auto d_name = std::string(de.d_name);
        if (d_name != "." && d_name != "..") {
          e_name = d_name;
          break;
        }
      }

      if (r == 0) {
        dout(10) << ": done for directory=" << entry.epath << dendl;
        if (ceph_closedir(m_local_mount, entry.dirp) < 0) {
          derr << ": failed to close local directory=" << entry.epath << dendl;
        }
        sync_stack.pop();
        continue;
      }
      if (r < 0) {
        break;
      }

      auto epath = entry_path(entry.epath, e_name);
      if (S_ISDIR(stx.stx_mode)) {
        r = remote_mkdir(epath, stx, fh);
        if (r < 0) {
          break;
        }
        ceph_dir_result *dirp;
        r = opendirat(m_local_mount, fh.c_fd, epath, AT_SYMLINK_NOFOLLOW, &dirp);
        if (r < 0) {
          derr << ": failed to open local directory=" << epath << ": "
               << cpp_strerror(r) << dendl;
          break;
        }
        sync_stack.emplace(SyncEntry(epath, dirp, stx));
      } else {
        sync_stack.emplace(SyncEntry(epath, stx));
      }
    } else {
      bool need_data_sync = true;
      bool need_attr_sync = true;
      r = should_sync_entry(entry.epath, entry.stx, fh,
                            &need_data_sync, &need_attr_sync);
      if (r < 0) {
        break;
      }

      dout(5) << ": entry=" << entry.epath << ", data_sync=" << need_data_sync
              << ", attr_sync=" << need_attr_sync << dendl;
      if (need_data_sync || need_attr_sync) {
        r = remote_file_op(dir_root, entry.epath, entry.stx, fh, need_data_sync,
                           need_attr_sync);
        if (r < 0) {
          break;
        }
      }
      dout(10) << ": done for epath=" << entry.epath << dendl;
      sync_stack.pop();
    }
  }

  while (!sync_stack.empty()) {
    auto &entry = sync_stack.top();
    if (entry.is_directory()) {
      dout(20) << ": closing local directory=" << entry.epath << dendl;
      if (ceph_closedir(m_local_mount, entry.dirp) < 0) {
        derr << ": failed to close local directory=" << entry.epath << dendl;
      }
    }

    sync_stack.pop();
  }

  return r;
}

int PeerReplayer::synchronize(const std::string &dir_root, const Snapshot &current,
                              boost::optional<Snapshot> prev) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": prev=" << prev << dendl;
  }

  int r = ceph_getxattr(m_remote_mount, dir_root.c_str(), "ceph.mirror.dirty_snap_id", nullptr, 0);
  if (r < 0 && r != -ENODATA) {
    derr << ": failed to fetch primary_snap_id length from dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
    return r;
  }

  // no xattr, can't determine which snap the data belongs to!
  if (r < 0) {
    dout(5) << ": missing \"ceph.mirror.dirty_snap_id\" xattr on remote -- using"
            << " incremental sync with remote scan" << dendl;
    r = do_synchronize(dir_root, current, boost::none);
  } else {
    size_t xlen = r;
    char *val = (char *)alloca(xlen+1);
    r = ceph_getxattr(m_remote_mount, dir_root.c_str(), "ceph.mirror.dirty_snap_id", (void*)val, xlen);
    if (r < 0) {
      derr << ": failed to fetch \"dirty_snap_id\" for dir_root: " << dir_root
           << ": " << cpp_strerror(r) << dendl;
      return r;
    }

    val[xlen] = '\0';
    uint64_t dirty_snap_id = atoll(val);

    dout(20) << ": dirty_snap_id: " << dirty_snap_id << " vs (" << current.second
             << "," << (prev ? stringify((*prev).second) : "~") << ")" << dendl;
    if (prev && (dirty_snap_id == (*prev).second || dirty_snap_id == current.second)) {
      dout(5) << ": match -- using incremental sync with local scan" << dendl;
      r = do_synchronize(dir_root, current, prev);
    } else {
      dout(5) << ": mismatch -- using incremental sync with remote scan" << dendl;
      r = do_synchronize(dir_root, current, boost::none);
    }
  }

  // snap sync failed -- bail out!
  if (r < 0) {
    return r;
  }

  auto cur_snap_id_str{stringify(current.second)};
  snap_metadata snap_meta[] = {{PRIMARY_SNAP_ID_KEY.c_str(), cur_snap_id_str.c_str()}};
  r = ceph_mksnap(m_remote_mount, dir_root.c_str(), current.first.c_str(), 0755,
                  snap_meta, sizeof(snap_meta)/sizeof(snap_metadata));
  if (r < 0) {
    derr << ": failed to snap remote directory dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
  }

  return r;
}

int PeerReplayer::do_sync_snaps(const std::string &dir_root) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  std::map<uint64_t, std::string> local_snap_map;
  std::map<uint64_t, std::string> remote_snap_map;

  int r = build_snap_map(dir_root, &local_snap_map);
  if (r < 0) {
    derr << ": failed to build local snap map" << dendl;
    return r;
  }

  r = build_snap_map(dir_root, &remote_snap_map, true);
  if (r < 0) {
    derr << ": failed to build remote snap map" << dendl;
    return r;
  }

  // infer deleted and renamed snapshots from local and remote
  // snap maps
  std::set<std::string> snaps_deleted;
  std::set<std::pair<std::string,std::string>> snaps_renamed;
  for (auto &[primary_snap_id, snap_name] : remote_snap_map) {
    auto it = local_snap_map.find(primary_snap_id);
    if (it == local_snap_map.end()) {
      snaps_deleted.emplace(snap_name);
    } else if (it->second != snap_name) {
      snaps_renamed.emplace(std::make_pair(snap_name, it->second));
    }
  }

  r = propagate_snap_deletes(dir_root, snaps_deleted);
  if (r < 0) {
    derr << ": failed to propgate deleted snapshots" << dendl;
    return r;
  }

  r = propagate_snap_renames(dir_root, snaps_renamed);
  if (r < 0) {
    derr << ": failed to propgate renamed snapshots" << dendl;
    return r;
  }

  // start mirroring snapshots from the last snap-id synchronized
  uint64_t last_snap_id = 0;
  std::string last_snap_name;
  if (!remote_snap_map.empty()) {
    auto last = remote_snap_map.rbegin();
    last_snap_id = last->first;
    last_snap_name = last->second;
    set_last_synced_snap(dir_root, last_snap_id, last_snap_name);
  }

  dout(5) << ": last snap-id transferred=" << last_snap_id << dendl;
  auto it = local_snap_map.upper_bound(last_snap_id);
  if (it == local_snap_map.end()) {
    dout(20) << ": nothing to synchronize" << dendl;
    return 0;
  }

  auto snaps_per_cycle = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_snapshot_sync_per_cycle");

  dout(10) << ": synchronizing from snap-id=" << it->first << dendl;
  for (; it != local_snap_map.end(); ++it) {
    set_current_syncing_snap(dir_root, it->first, it->second);
    auto start = clock::now();
    boost::optional<Snapshot> prev = boost::none;
    if (last_snap_id != 0) {
      prev = std::make_pair(last_snap_name, last_snap_id);
    }
    r = synchronize(dir_root, std::make_pair(it->second, it->first), prev);
    if (r < 0) {
      derr << ": failed to synchronize dir_root=" << dir_root
           << ", snapshot=" << it->second << dendl;
      clear_current_syncing_snap(dir_root);
      return r;
    }
    std::chrono::duration<double> duration = clock::now() - start;
    set_last_synced_stat(dir_root, it->first, it->second, duration.count());
    if (--snaps_per_cycle == 0) {
      break;
    }

    last_snap_name = it->second;
    last_snap_id = it->first;
  }

  return 0;
}

void PeerReplayer::sync_snaps(const std::string &dir_root,
                              std::unique_lock<ceph::mutex> &locker) {
  dout(20) << ": dir_root=" << dir_root << dendl;
  locker.unlock();
  int r = do_sync_snaps(dir_root);
  if (r < 0) {
    derr << ": failed to sync snapshots for dir_root=" << dir_root << dendl;
  }
  locker.lock();
  if (r < 0) {
    _inc_failed_count(dir_root);
  } else {
    _reset_failed_count(dir_root);
  }
}

void PeerReplayer::run(SnapshotReplayerThread *replayer) {
  dout(10) << ": snapshot replayer=" << replayer << dendl;

  time last_directory_scan = clock::zero();
  auto scan_interval = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_directory_scan_interval");

  std::unique_lock locker(m_lock);
  while (true) {
    // do not check if client is blocklisted under lock
    m_cond.wait_for(locker, 1s, [this]{return is_stopping();});
    if (is_stopping()) {
      dout(5) << ": exiting" << dendl;
      break;
    }

    locker.unlock();

    if (m_fs_mirror->is_blocklisted()) {
      dout(5) << ": exiting as client is blocklisted" << dendl;
      break;
    }

    locker.lock();

    auto now = clock::now();
    std::chrono::duration<double> timo = now - last_directory_scan;
    if (timo.count() >= scan_interval && m_directories.size()) {
      dout(20) << ": trying to pick from " << m_directories.size() << " directories" << dendl;
      auto dir_root = pick_directory();
      if (dir_root) {
        dout(5) << ": picked dir_root=" << *dir_root << dendl;
        int r = register_directory(*dir_root, replayer);
        if (r == 0) {
	  r = sync_perms(*dir_root);
	  if (r < 0) {
	    _inc_failed_count(*dir_root);
	  } else {
	    sync_snaps(*dir_root, locker);
	  }
	  unregister_directory(*dir_root);
        }
      }

      last_directory_scan = now;
    }
  }
}

void PeerReplayer::peer_status(Formatter *f) {
  std::scoped_lock locker(m_lock);
  f->open_object_section("stats");
  for (auto &[dir_root, sync_stat] : m_snap_sync_stats) {
    f->open_object_section(dir_root);
    if (sync_stat.failed) {
      f->dump_string("state", "failed");
    } else if (!sync_stat.current_syncing_snap) {
      f->dump_string("state", "idle");
    } else {
      f->dump_string("state", "syncing");
      f->open_object_section("current_sycning_snap");
      f->dump_unsigned("id", (*sync_stat.current_syncing_snap).first);
      f->dump_string("name", (*sync_stat.current_syncing_snap).second);
      f->close_section();
    }
    if (sync_stat.last_synced_snap) {
      f->open_object_section("last_synced_snap");
      f->dump_unsigned("id", (*sync_stat.last_synced_snap).first);
      f->dump_string("name", (*sync_stat.last_synced_snap).second);
      if (sync_stat.last_sync_duration) {
        f->dump_float("sync_duration", *sync_stat.last_sync_duration);
        f->dump_stream("sync_time_stamp") << sync_stat.last_synced;
      }
      f->close_section();
    }
    f->dump_unsigned("snaps_synced", sync_stat.synced_snap_count);
    f->dump_unsigned("snaps_deleted", sync_stat.deleted_snap_count);
    f->dump_unsigned("snaps_renamed", sync_stat.renamed_snap_count);
    f->close_section(); // dir_root
  }
  f->close_section(); // stats
}

void PeerReplayer::reopen_logs() {
  std::scoped_lock locker(m_lock);

  if (m_remote_cluster) {
    reinterpret_cast<CephContext *>(m_remote_cluster->cct())->reopen_logs();
  }
}

void PeerReplayer::register_perf_counters() {
}

void PeerReplayer::unregister_perf_counters() {
}

} // namespace mirror
} // namespace cephfs
