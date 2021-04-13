// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <stack>
#include <fcntl.h>
#include <algorithm>
#include <sys/time.h>
#include <sys/file.h>

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
  for (auto &dir_path : m_directories) {
    m_snap_sync_stats.emplace(dir_path, SnapSyncStat());
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

void PeerReplayer::add_directory(string_view dir_path) {
  dout(20) << ": dir_path=" << dir_path << dendl;

  std::scoped_lock locker(m_lock);
  m_directories.emplace_back(dir_path);
  m_snap_sync_stats.emplace(dir_path, SnapSyncStat());
  m_cond.notify_all();
}

void PeerReplayer::remove_directory(string_view dir_path) {
  dout(20) << ": dir_path=" << dir_path << dendl;
  auto _dir_path = std::string(dir_path);

  std::scoped_lock locker(m_lock);
  auto it = std::find(m_directories.begin(), m_directories.end(), _dir_path);
  if (it != m_directories.end()) {
    m_directories.erase(it);
  }

  auto it1 = m_registered.find(_dir_path);
  if (it1 == m_registered.end()) {
    m_snap_sync_stats.erase(_dir_path);
  } else {
    it1->second.replayer->cancel();
  }
  m_cond.notify_all();
}

boost::optional<std::string> PeerReplayer::pick_directory() {
  dout(20) << dendl;

  auto now = clock::now();
  auto retry_timo = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_retry_failed_directories_interval");

  boost::optional<std::string> candidate;
  for (auto &dir_path : m_directories) {
    auto &sync_stat = m_snap_sync_stats.at(dir_path);
    if (sync_stat.failed) {
      std::chrono::duration<double> d = now - *sync_stat.last_failed;
      if (d.count() < retry_timo) {
        continue;
      }
    }
    if (!m_registered.count(dir_path)) {
      candidate = dir_path;
      break;
    }
  }

  std::rotate(m_directories.begin(), m_directories.begin() + 1, m_directories.end());
  return candidate;
}

int PeerReplayer::register_directory(const std::string &dir_path,
                                     SnapshotReplayerThread *replayer) {
  dout(20) << ": dir_path=" << dir_path << dendl;
  ceph_assert(m_registered.find(dir_path) == m_registered.end());

  DirRegistry registry;
  int r = try_lock_directory(dir_path, replayer, &registry);
  if (r < 0) {
    return r;
  }

  dout(5) << ": dir_path=" << dir_path << " registered with replayer="
          << replayer << dendl;
  m_registered.emplace(dir_path, std::move(registry));
  return 0;
}

void PeerReplayer::unregister_directory(const std::string &dir_path) {
  dout(20) << ": dir_path=" << dir_path << dendl;

  auto it = m_registered.find(dir_path);
  ceph_assert(it != m_registered.end());

  unlock_directory(it->first, it->second);
  m_registered.erase(it);
  if (std::find(m_directories.begin(), m_directories.end(), dir_path) == m_directories.end()) {
    m_snap_sync_stats.erase(dir_path);
  }
}

int PeerReplayer::try_lock_directory(const std::string &dir_path,
                                     SnapshotReplayerThread *replayer, DirRegistry *registry) {
  dout(20) << ": dir_path=" << dir_path << dendl;

  int r = ceph_open(m_remote_mount, dir_path.c_str(), O_RDONLY | O_DIRECTORY, 0);
  if (r < 0 && r != -ENOENT) {
    derr << ": failed to open remote dir_path=" << dir_path << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  if (r == -ENOENT) {
    // we snap under dir_path, so mode does not matter much
    r = ceph_mkdirs(m_remote_mount, dir_path.c_str(), 0755);
    if (r < 0) {
      derr << ": failed to create remote directory=" << dir_path << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }

    r = ceph_open(m_remote_mount, dir_path.c_str(), O_RDONLY | O_DIRECTORY, 0);
    if (r < 0) {
      derr << ": failed to open remote dir_path=" << dir_path << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }
  }

  int fd = r;
  r = ceph_flock(m_remote_mount, fd, LOCK_EX | LOCK_NB, (uint64_t)replayer->get_thread_id());
  if (r != 0) {
    if (r == -EWOULDBLOCK) {
      dout(5) << ": dir_path=" << dir_path << " is locked by cephfs-mirror, "
              << "will retry again" << dendl;
    } else {
      derr << ": failed to lock dir_path=" << dir_path << ": " << cpp_strerror(r)
           << dendl;
    }

    if (ceph_close(m_remote_mount, fd) < 0) {
      derr << ": failed to close (cleanup) remote dir_path=" << dir_path << ": "
           << cpp_strerror(r) << dendl;
    }
    return r;
  }

  dout(10) << ": dir_path=" << dir_path << " locked" << dendl;

  registry->fd = fd;
  registry->replayer = replayer;
  return 0;
}

void PeerReplayer::unlock_directory(const std::string &dir_path, const DirRegistry &registry) {
  dout(20) << ": dir_path=" << dir_path << dendl;

  int r = ceph_flock(m_remote_mount, registry.fd, LOCK_UN,
                     (uint64_t)registry.replayer->get_thread_id());
  if (r < 0) {
    derr << ": failed to unlock remote dir_path=" << dir_path << ": " << cpp_strerror(r)
         << dendl;
    return;
  }

  r = ceph_close(m_remote_mount, registry.fd);
  if (r < 0) {
    derr << ": failed to close remote dir_path=" << dir_path << ": " << cpp_strerror(r)
         << dendl;
  }

  dout(10) << ": dir_path=" << dir_path << " unlocked" << dendl;
}

int PeerReplayer::build_snap_map(const std::string &dir_path,
                                 std::map<uint64_t, std::string> *snap_map, bool is_remote) {
  auto snap_dir = snapshot_dir_path(m_cct, dir_path);
  dout(20) << ": dir_path=" << dir_path << ", snap_dir=" << snap_dir
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
    if (d_name != "." && d_name != "..") {
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

int PeerReplayer::propagate_snap_deletes(const std::string &dir_path,
                                         const std::set<std::string> &snaps) {
  dout(5) << ": dir_path=" << dir_path << ", deleted snapshots=" << snaps << dendl;

  for (auto &snap : snaps) {
    dout(20) << ": deleting dir_path=" << dir_path << ", snapshot=" << snap
             << dendl;
    int r = ceph_rmsnap(m_remote_mount, dir_path.c_str(), snap.c_str());
    if (r < 0) {
      derr << ": failed to delete remote snap dir_path=" << dir_path
           << ", snapshot=" << snaps << ": " << cpp_strerror(r) << dendl;
      return r;
    }
    inc_deleted_snap(dir_path);
  }

  return 0;
}

int PeerReplayer::propagate_snap_renames(
    const std::string &dir_path,
    const std::set<std::pair<std::string,std::string>> &snaps) {
  dout(10) << ": dir_path=" << dir_path << ", renamed snapshots=" << snaps << dendl;

  for (auto &snapp : snaps) {
    auto from = snapshot_path(m_cct, dir_path, snapp.first);
    auto to = snapshot_path(m_cct, dir_path, snapp.second);
    dout(20) << ": renaming dir_path=" << dir_path << ", snapshot from="
             << from << ", to=" << to << dendl;
    int r = ceph_rename(m_remote_mount, from.c_str(), to.c_str());
    if (r < 0) {
      derr << ": failed to rename remote snap dir_path=" << dir_path
           << ", snapshot from =" << from << ", to=" << to << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    inc_renamed_snap(dir_path);
  }

  return 0;
}

int PeerReplayer::remote_mkdir(const std::string &local_path,
                               const std::string &remote_path,
                               const struct ceph_statx &stx) {
  dout(10) << ": local_path=" << local_path << ", remote_path=" << remote_path
           << dendl;

  int r = ceph_mkdir(m_remote_mount, remote_path.c_str(), stx.stx_mode & ~S_IFDIR);
  if (r < 0 && r != -EEXIST) {
    derr << ": failed to create remote directory=" << remote_path << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  r = ceph_lchown(m_remote_mount, remote_path.c_str(), stx.stx_uid, stx.stx_gid);
  if (r < 0) {
    derr << ": failed to chown remote directory=" << remote_path << ": " << cpp_strerror(r)
         << dendl;
    return r;
  }

  struct timeval times[] = {{stx.stx_atime.tv_sec, stx.stx_atime.tv_nsec / 1000},
                            {stx.stx_mtime.tv_sec, stx.stx_mtime.tv_nsec / 1000}};
  r = ceph_lutimes(m_remote_mount, remote_path.c_str(), times);
  if (r < 0) {
    derr << ": failed to change [am]time on remote directory=" << remote_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

#define NR_IOVECS 8 // # iovecs
#define IOVEC_SIZE (8 * 1024 * 1024) // buffer size for each iovec
int PeerReplayer::remote_copy(const std::string &dir_path,
                              const std::string &local_path,
                              const std::string &remote_path,
                              const struct ceph_statx &stx) {
  dout(10) << ": dir_path=" << dir_path << ", local_path=" << local_path
           << ", remote_path=" << remote_path << dendl;
  int l_fd;
  int r_fd;
  void *ptr;
  struct iovec iov[NR_IOVECS];

  int r = ceph_open(m_local_mount, local_path.c_str(), O_RDONLY, 0);
  if (r < 0) {
    derr << ": failed to open local file path=" << local_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  l_fd = r;
  r = ceph_open(m_remote_mount, remote_path.c_str(),
                O_CREAT | O_TRUNC | O_WRONLY, stx.stx_mode);
  if (r < 0) {
    derr << ": failed to create remote file path=" << remote_path << ": "
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
    if (should_backoff(dir_path, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    for (int i = 0; i < NR_IOVECS; ++i) {
      iov[i].iov_base = (char*)ptr + IOVEC_SIZE*i;
      iov[i].iov_len = IOVEC_SIZE;
    }

    r = ceph_preadv(m_local_mount, l_fd, iov, NR_IOVECS, -1);
    if (r < 0) {
      derr << ": failed to read local file path=" << local_path << ": "
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
      derr << ": failed to write remote file path=" << remote_path << ": "
           << cpp_strerror(r) << dendl;
      break;
    }
  }

  if (r == 0) {
    r = ceph_fsync(m_remote_mount, r_fd, 0);
    if (r < 0) {
      derr << ": failed to sync data for dir_path=" << remote_path << ": "
           << cpp_strerror(r) << dendl;
    }
  }

  free(ptr);

close_remote_fd:
  if (ceph_close(m_remote_mount, r_fd) < 0) {
    derr << ": failed to close remote fd path=" << remote_path << ": " << cpp_strerror(r)
         << dendl;
    return -EINVAL;
  }

close_local_fd:
  if (ceph_close(m_local_mount, l_fd) < 0) {
    derr << ": failed to close local fd path=" << local_path << ": " << cpp_strerror(r)
         << dendl;
    return -EINVAL;
  }

  return r == 0 ? 0 : r;
}

int PeerReplayer::remote_file_op(const std::string &dir_path,
                                 const std::string &local_path,
                                 const std::string &remote_path,
                                 const struct ceph_statx &stx) {
  dout(10) << ": dir_path=" << dir_path << ", local_path=" << local_path
           << ", remote_path=" << remote_path << dendl;

  int r;
  if (S_ISREG(stx.stx_mode)) {
    r = remote_copy(dir_path, local_path, remote_path, stx);
    if (r < 0) {
      derr << ": failed to copy path=" << local_path << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }
  } else if (S_ISLNK(stx.stx_mode)) {
    char *target = (char *)alloca(stx.stx_size+1);
    r = ceph_readlink(m_local_mount, local_path.c_str(), target, stx.stx_size);
    if (r < 0) {
      derr << ": failed to readlink local path=" << local_path << ": " << cpp_strerror(r)
           << dendl;
      return r;
    }

    target[stx.stx_size] = '\0';
    r = ceph_symlink(m_remote_mount, target, remote_path.c_str());
    if (r < 0 && r != EEXIST) {
      derr << ": failed to symlink remote path=" << remote_path << " to target=" << target
           << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  } else {
    dout(5) << ": skipping entry=" << local_path << ": unsupported mode=" << stx.stx_mode
            << dendl;
    return 0;
  }

  r = ceph_lchown(m_remote_mount, remote_path.c_str(), stx.stx_uid, stx.stx_gid);
  if (r < 0) {
    derr << ": failed to chown remote directory=" << remote_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  struct timeval times[] = {{stx.stx_atime.tv_sec, stx.stx_atime.tv_nsec / 1000},
                            {stx.stx_mtime.tv_sec, stx.stx_mtime.tv_nsec / 1000}};
  r = ceph_lutimes(m_remote_mount, remote_path.c_str(), times);
  if (r < 0) {
    derr << ": failed to change [am]time on remote directory=" << remote_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  return 0;
}

int PeerReplayer::cleanup_remote_dir(const std::string &dir_path) {
  dout(20) << ": dir_path=" << dir_path << dendl;

  std::stack<SyncEntry> rm_stack;
  ceph_dir_result *tdirp;
  int r = ceph_opendir(m_remote_mount, dir_path.c_str(), &tdirp);
  if (r < 0) {
    derr << ": failed to open remote directory=" << dir_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  struct ceph_statx tstx;
  r = ceph_statx(m_remote_mount, dir_path.c_str(), &tstx,
                 CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                 CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                 AT_NO_ATTR_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat remote directory=" << dir_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  rm_stack.emplace(SyncEntry(dir_path, tdirp, tstx));
  while (!rm_stack.empty()) {
    if (should_backoff(dir_path, &r)) {
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
                               CEPH_STATX_MODE, AT_NO_ATTR_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
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
        if (rm_stack.size() > 1) {
          r = ceph_rmdir(m_remote_mount, entry.epath.c_str());
          if (r < 0) {
            derr << ": failed to remove remote directory=" << entry.epath << ": "
                 << cpp_strerror(r) << dendl;
            break;
          }
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
        r = ceph_opendir(m_remote_mount, epath.c_str(), &dirp);
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
      r = ceph_unlink(m_remote_mount, entry.epath.c_str());
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

int PeerReplayer::do_synchronize(const std::string &dir_path, const std::string &snap_name) {
  dout(20) << ": dir_path=" << dir_path << ", snap_name=" << snap_name << dendl;

  auto snap_path = snapshot_path(m_cct, dir_path, snap_name);
  std::stack<SyncEntry> sync_stack;

  ceph_dir_result *tdirp;
  int r = ceph_opendir(m_local_mount, snap_path.c_str(), &tdirp);
  if (r < 0) {
    derr << ": failed to open local directory=" << snap_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  struct ceph_statx tstx;
  r = ceph_statx(m_local_mount, snap_path.c_str(), &tstx,
                 CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                 CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                 AT_NO_ATTR_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat local directory=" << snap_path << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  sync_stack.emplace(SyncEntry("/", tdirp, tstx));
  while (!sync_stack.empty()) {
    if (should_backoff(dir_path, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }

    dout(20) << ": " << sync_stack.size() << " entries in stack" << dendl;
    std::string e_name;
    auto &entry = sync_stack.top();
    dout(20) << ": top of stack path=" << entry.epath << dendl;
    if (entry.is_directory()) {
      struct ceph_statx stx;
      struct dirent de;
      while (true) {
        r = ceph_readdirplus_r(m_local_mount, entry.dirp, &de, &stx,
                               CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                               CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                               AT_NO_ATTR_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
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
      auto l_path = entry_path(snap_path, epath);
      auto r_path = entry_path(dir_path, epath);
      if (S_ISDIR(stx.stx_mode)) {
        r = remote_mkdir(l_path, r_path, stx);
        if (r < 0) {
          break;
        }
        ceph_dir_result *dirp;
        r = ceph_opendir(m_local_mount, l_path.c_str(), &dirp);
        if (r < 0) {
          derr << ": failed to open local directory=" << l_path << ": "
               << cpp_strerror(r) << dendl;
          break;
        }
        sync_stack.emplace(SyncEntry(epath, dirp, stx));
      } else {
        sync_stack.emplace(SyncEntry(epath, stx));
      }
    } else {
      auto l_path = entry_path(snap_path, entry.epath);
      auto r_path = entry_path(dir_path, entry.epath);
      r = remote_file_op(dir_path, l_path, r_path, entry.stx);
      if (r < 0) {
        break;
      }
      dout(10) << ": done for file=" << entry.epath << dendl;
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

int PeerReplayer::synchronize(const std::string &dir_path, uint64_t snap_id,
                              const std::string &snap_name) {
  dout(20) << ": dir_path=" << dir_path << ", snap_id=" << snap_id
           << ", snap_name=" << snap_name << dendl;

  auto snap_path = snapshot_path(m_cct, dir_path, snap_name);

  int r = cleanup_remote_dir(dir_path);
  if (r < 0) {
    derr << ": failed to cleanup remote directory=" << dir_path << dendl;
    return r;
  }

  r = do_synchronize(dir_path, snap_name);
  if (r < 0) {
    derr << ": failed to synchronize dir_path=" << dir_path << ", snapshot="
         << snap_path << dendl;
    return r;
  }

  auto snap_id_str{stringify(snap_id)};
  snap_metadata snap_meta[] = {{PRIMARY_SNAP_ID_KEY.c_str(), snap_id_str.c_str()}};
  r = ceph_mksnap(m_remote_mount, dir_path.c_str(), snap_name.c_str(), 0755,
                  snap_meta, 1);
  if (r < 0) {
    derr << ": failed to snap remote directory dir_path=" << dir_path
         << ": " << cpp_strerror(r) << dendl;
  }

  return r;
}

int PeerReplayer::do_sync_snaps(const std::string &dir_path) {
  dout(20) << ": dir_path=" << dir_path << dendl;

  std::map<uint64_t, std::string> local_snap_map;
  std::map<uint64_t, std::string> remote_snap_map;

  int r = build_snap_map(dir_path, &local_snap_map);
  if (r < 0) {
    derr << ": failed to build local snap map" << dendl;
    return r;
  }

  r = build_snap_map(dir_path, &remote_snap_map, true);
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

  r = propagate_snap_deletes(dir_path, snaps_deleted);
  if (r < 0) {
    derr << ": failed to propgate deleted snapshots" << dendl;
    return r;
  }

  r = propagate_snap_renames(dir_path, snaps_renamed);
  if (r < 0) {
    derr << ": failed to propgate renamed snapshots" << dendl;
    return r;
  }

  // start mirroring snapshots from the last snap-id synchronized
  uint64_t last_snap_id = 0;
  if (!remote_snap_map.empty()) {
    auto last = remote_snap_map.rbegin();
    last_snap_id = last->first;
    set_last_synced_snap(dir_path, last_snap_id, last->second);
  }

  dout(5) << ": last snap-id transferred=" << last_snap_id << dendl;
  auto it = local_snap_map.upper_bound(last_snap_id);
  if (it == local_snap_map.end()) {
    dout(20) << ": nothing to synchronize" << dendl;
    return 0;
  }

  auto snaps_per_cycle = g_ceph_context->_conf.get_val<uint64_t>(
    "cephfs_mirror_max_snapshot_sync_per_cycle");

  dout(10) << ": synzhronizing from snap-id=" << it->first << dendl;
  for (; it != local_snap_map.end(); ++it) {
    set_current_syncing_snap(dir_path, it->first, it->second);
    auto start = clock::now();
    r = synchronize(dir_path, it->first, it->second);
    if (r < 0) {
      derr << ": failed to synchronize dir_path=" << dir_path
           << ", snapshot=" << it->second << dendl;
      clear_current_syncing_snap(dir_path);
      return r;
    }
    std::chrono::duration<double> duration = clock::now() - start;
    set_last_synced_stat(dir_path, it->first, it->second, duration.count());
    if (--snaps_per_cycle == 0) {
      break;
    }
  }

  return 0;
}

void PeerReplayer::sync_snaps(const std::string &dir_path,
                              std::unique_lock<ceph::mutex> &locker) {
  dout(20) << ": dir_path=" << dir_path << dendl;
  locker.unlock();
  int r = do_sync_snaps(dir_path);
  if (r < 0) {
    derr << ": failed to sync snapshots for dir_path=" << dir_path << dendl;
  }
  locker.lock();
  if (r < 0) {
    _inc_failed_count(dir_path);
  } else {
    _reset_failed_count(dir_path);
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
      auto dir_path = pick_directory();
      if (dir_path) {
        dout(5) << ": picked dir_path=" << *dir_path << dendl;
        int r = register_directory(*dir_path, replayer);
        if (r == 0) {
          sync_snaps(*dir_path, locker);
          unregister_directory(*dir_path);
        }
      }

      last_directory_scan = now;
    }
  }
}

void PeerReplayer::peer_status(Formatter *f) {
  std::scoped_lock locker(m_lock);
  f->open_object_section("stats");
  for (auto &[dir_path, sync_stat] : m_snap_sync_stats) {
    f->open_object_section(dir_path);
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
    f->close_section(); // dir_path
  }
  f->close_section(); // stats
}

} // namespace mirror
} // namespace cephfs
