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
#include "common/perf_counters.h"
#include "common/perf_counters_collection.h"
#include "common/perf_counters_key.h"
#include "include/stringify.h"
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

// Performance Counters
enum {
  l_cephfs_mirror_peer_replayer_first = 6000,
  l_cephfs_mirror_peer_replayer_snaps_synced,
  l_cephfs_mirror_peer_replayer_snaps_deleted,
  l_cephfs_mirror_peer_replayer_snaps_renamed,
  l_cephfs_mirror_peer_replayer_snap_sync_failures,
  l_cephfs_mirror_peer_replayer_avg_sync_time,
  l_cephfs_mirror_peer_replayer_sync_bytes,
  l_cephfs_mirror_peer_replayer_last_synced_start,
  l_cephfs_mirror_peer_replayer_last_synced_end,
  l_cephfs_mirror_peer_replayer_last_synced_duration,
  l_cephfs_mirror_peer_replayer_last_synced_bytes,
  l_cephfs_mirror_peer_replayer_last,
};

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
  int r = ceph_openat(mnt, dirfd, relpath.c_str(), flags | O_DIRECTORY, 0);
  if (r < 0) {
    return r;
  }

  int fd = r;
  r = ceph_fdopendir(mnt, fd, dirp);
  if (r < 0) {
    ceph_close(mnt, fd);
  }
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
    m_lock(ceph::make_mutex("cephfs::mirror::PeerReplayer::" + stringify(peer.uuid))),
    file_sync_pool(g_ceph_context->_conf->cephfs_mirror_file_sync_thread, m_peer) {
  g_conf().add_observer(this);
  // reset sync stats sent via service daemon
  m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                 SERVICE_DAEMON_FAILED_DIR_COUNT_KEY, (uint64_t)0);
  m_service_daemon->add_or_update_peer_attribute(m_filesystem.fscid, m_peer,
                                                 SERVICE_DAEMON_RECOVERED_DIR_COUNT_KEY, (uint64_t)0);

  std::string labels = ceph::perf_counters::key_create("cephfs_mirror_peers",
						       {{"source_fscid", stringify(m_filesystem.fscid)},
							{"source_filesystem", m_filesystem.fs_name},
							{"peer_cluster_name", m_peer.remote.cluster_name},
							{"peer_cluster_filesystem", m_peer.remote.fs_name}});
  PerfCountersBuilder plb(m_cct, labels, l_cephfs_mirror_peer_replayer_first,
			  l_cephfs_mirror_peer_replayer_last);
  auto prio = m_cct->_conf.get_val<int64_t>("cephfs_mirror_perf_stats_prio");
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snaps_synced,
		      "snaps_synced", "Snapshots Synchronized", "sync", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snaps_deleted,
		      "snaps_deleted", "Snapshots Deleted", "del", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snaps_renamed,
		      "snaps_renamed", "Snapshots Renamed", "ren", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_snap_sync_failures,
		      "sync_failures", "Snapshot Sync Failures", "fail", prio);
  plb.add_time_avg(l_cephfs_mirror_peer_replayer_avg_sync_time,
		   "avg_sync_time", "Average Sync Time", "asyn", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_sync_bytes,
		      "sync_bytes", "Sync Bytes", "sbye", prio);
  plb.add_time(l_cephfs_mirror_peer_replayer_last_synced_start,
	       "last_synced_start", "Last Synced Start", "lsst", prio);
  plb.add_time(l_cephfs_mirror_peer_replayer_last_synced_end,
	       "last_synced_end", "Last Synced End", "lsen", prio);
  plb.add_time(l_cephfs_mirror_peer_replayer_last_synced_duration,
	       "last_synced_duration", "Last Synced Duration", "lsdn", prio);
  plb.add_u64_counter(l_cephfs_mirror_peer_replayer_last_synced_bytes,
		      "last_synced_bytes", "Last Synced Bytes", "lsbt", prio);
  m_perf_counters = plb.create_perf_counters();
  m_cct->get_perfcounters_collection()->add(m_perf_counters);
}

PeerReplayer::~PeerReplayer() {
  delete m_asok_hook;
  PerfCounters *perf_counters = nullptr;
  std::swap(perf_counters, m_perf_counters);
  if (perf_counters != nullptr) {
    m_cct->get_perfcounters_collection()->remove(perf_counters);
    delete perf_counters;
  }
}

void PeerReplayer::handle_conf_change(const ConfigProxy &conf,
                                      const std::set<std::string> &changed) {
  if (changed.count("cephfs_mirror_file_sync_thread")) {
    file_sync_pool.update_state();
  }
  if (changed.count("cephfs_mirror_dir_scanning_thread")) {
    std::unique_lock locker(m_lock);
    for (auto& [dir, registry]: m_registered) {
      if (registry->sync_pool) {
        registry->sync_pool->update_state();
      }
    }
  }
}

std::vector<std::string> PeerReplayer::get_tracked_keys() const noexcept {
  return {"cephfs_mirror_file_sync_thread"s,
          "cephfs_mirror_dir_scanning_thread"s};
}

int PeerReplayer::init() {
  dout(20) << ": initial dir list=[" << m_directories << "]" << dendl;
  for (auto &dir_root : m_directories) {
    m_snap_sync_stats.emplace(dir_root, new SnapSyncStat());
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
  dout(0) << ": Activating file sync thread pool having, "
          << g_ceph_context->_conf->cephfs_mirror_file_sync_thread
          << "number of threads" << dendl;
  file_sync_pool.activate();
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

  file_sync_pool.deactivate();
  dout(0) << ": File sync thread pool deactivated" << dendl;

  {
    std::scoped_lock locker(m_lock);
    ceph_assert(!m_stopping);
    m_stopping = true;
    for (auto &[dir_root, registry] : m_registered) {
      if (registry->sync_pool) {
        registry->sync_pool->deactivate();
      }
    }
    dout(0) << ": Directory scanning thread pools deactivated" << dendl;
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
  m_snap_sync_stats.emplace(dir_root, new SnapSyncStat());
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
    it1->second->canceled.store(true);
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
    if (sync_stat->failed) {
      std::chrono::duration<double> d = now - *sync_stat->last_failed;
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

  DirRegistry* registry = new DirRegistry();
  int r = try_lock_directory(dir_root, replayer, registry);
  if (r < 0) {
    return r;
  }

  dout(5) << ": dir_root=" << dir_root << " registered with replayer="
          << replayer << dendl;
  m_registered.emplace(dir_root, registry);
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

    r = ceph_close(m_remote_mount, fd);
    if (r < 0) {
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

void PeerReplayer::unlock_directory(const std::string &dir_root, DirRegistry* registry) {
  dout(20) << ": dir_root=" << dir_root << dendl;

  int r = ceph_flock(m_remote_mount, registry->fd, LOCK_UN,
                     (uint64_t)registry->replayer->get_thread_id());
  if (r < 0) {
    derr << ": failed to unlock remote dir_root=" << dir_root << ": " << cpp_strerror(r)
         << dendl;
    return;
  }

  r = ceph_close(m_remote_mount, registry->fd);
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
        std::string failed_reason = "snapshot '" + snap  + "' has invalid metadata";
        derr << ": " << failed_reason << dendl;
        m_snap_sync_stats.at(dir_root)->last_failed_reason = failed_reason;
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
    if (m_perf_counters) {
      m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snaps_deleted);
    }
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
    if (m_perf_counters) {
      m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snaps_renamed);
    }
  }

  return 0;
}

int PeerReplayer::sync_attributes(std::shared_ptr<SyncEntry> &cur_entry,
                                  const FHandles &fh) {
  int r = 0;
  if ((cur_entry->change_mask & CEPH_STATX_UID) ||
      (cur_entry->change_mask & CEPH_STATX_GID)) {
    r = ceph_chownat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                     cur_entry->stx.stx_uid, cur_entry->stx.stx_gid,
                     AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chown remote directory=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if ((cur_entry->change_mask & CEPH_STATX_MODE)) {
    r = ceph_chmodat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                     cur_entry->stx.stx_mode & ~S_IFMT, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to chmod remote directory=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
  }

  if (!cur_entry->is_directory() &&
      (cur_entry->change_mask & CEPH_STATX_MTIME)) {
    struct timespec times[] = {
        {cur_entry->stx.stx_atime.tv_sec, cur_entry->stx.stx_atime.tv_nsec},
        {cur_entry->stx.stx_mtime.tv_sec, cur_entry->stx.stx_mtime.tv_nsec}};
    r = ceph_utimensat(m_remote_mount, fh.r_fd_dir_root,
                       cur_entry->epath.c_str(), times, AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to change [am]time on remote directory="
           << cur_entry->epath << ": " << cpp_strerror(r) << dendl;
      return r;
    }
  }
  return 0;
}

int PeerReplayer::_remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry,
                                const FHandles &fh, SnapSyncStat *sync_stat) {
  int r =
      ceph_mkdirat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                   cur_entry->stx.stx_mode & ~S_IFDIR);
  if (r < 0 && r != -EEXIST) {
    derr << ": failed to create remote directory=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  sync_stat->current_stat.inc_dir_created_count();
  return 0;
}

int PeerReplayer::remote_mkdir(std::shared_ptr<SyncEntry> &cur_entry,
                               const FHandles &fh, SnapSyncStat *sync_stat) {
  dout(10) << ": remote epath=" << cur_entry->epath << dendl;
  int r = 0;
  /*
    We know that if it is not create fresh and purge remote
    then remote already has the dir entry, we just need to
    sync_attributes, thus saving a ceph_mkdirat call.
  */
  if (cur_entry->create_fresh() || cur_entry->purge_remote()) {
    r = _remote_mkdir(cur_entry, fh, sync_stat);
    if (r < 0) {
      return r;
    }
  }

  r = sync_attributes(cur_entry, fh);
  return r;
}

#define NR_IOVECS 8 // # iovecs
#define IOVEC_SIZE (8 * 1024 * 1024) // buffer size for each iovec
int PeerReplayer::copy_to_remote(std::shared_ptr<SyncEntry> &cur_entry,
                                 DirRegistry *registry, const FHandles &fh,
                                 uint64_t num_blocks, struct cblock *b, bool trunc) {
  dout(10) << ": dir_root=" << registry->dir_root
           << ", epath=" << cur_entry->epath << ", num_blocks=" << num_blocks
           << dendl;
  int l_fd;
  int r_fd;
  void *ptr;
  struct iovec iov[NR_IOVECS];

  int r = ceph_openat(m_local_mount, fh.c_fd, cur_entry->epath.c_str(),
                      O_RDONLY | O_NOFOLLOW, 0);
  if (r < 0) {
    derr << ": failed to open local file path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  l_fd = r;
  r = ceph_openat(m_remote_mount, fh.r_fd_dir_root, cur_entry->epath.c_str(),
                  O_CREAT | O_WRONLY | O_NOFOLLOW | (trunc ? O_TRUNC : 0),
                  cur_entry->stx.stx_mode);
  if (r < 0) {
    derr << ": failed to create remote file path=" << cur_entry->epath << ": "
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

  while (num_blocks > 0) {
    auto offset = b->offset;
    auto len = b->len;

    dout(10) << ": dir_root=" << registry->dir_root
             << ", epath=" << cur_entry->epath << ", block: [" << offset << "~"
             << len << "]" << dendl;

    auto end_offset = offset + len;
    dout(20) << ": start offset=" << offset << ", end offset=" << end_offset
             << dendl;
    while (offset < end_offset) {
      if (should_backoff(registry, &r)) {
        dout(0) << ": backing off r=" << r << dendl;
        break;
      }

      auto cut_off = len;
      if (cut_off > NR_IOVECS*IOVEC_SIZE) {
        cut_off = NR_IOVECS*IOVEC_SIZE;
      }

      int num_buffers = cut_off / IOVEC_SIZE;
      if (cut_off % IOVEC_SIZE) {
        ++num_buffers;
      }

      dout(20) << ": num_buffers=" << num_buffers << dendl;
      for (int i = 0; i < num_buffers; ++i) {
        iov[i].iov_base = (char *)ptr + IOVEC_SIZE*i;
        if (cut_off < IOVEC_SIZE) {
          ceph_assert(i+1 == num_buffers);
          iov[i].iov_len = cut_off;
        } else {
          iov[i].iov_len = IOVEC_SIZE;
          cut_off -= IOVEC_SIZE;
        }
      }

      r = ceph_preadv(m_local_mount, l_fd, iov, num_buffers, offset);
      if (r < 0) {
        derr << ": failed to read local file path=" << cur_entry->epath << ": "
             << cpp_strerror(r) << dendl;
        break;
      }
      dout(10) << ": read: " << r << " bytes" << dendl;
      if (r == 0) {
        break;
      }

      int iovs = (int)(r / IOVEC_SIZE);
      int t = r % IOVEC_SIZE;
      if (t) {
        iov[iovs].iov_len = t;
        ++iovs;
      }

      dout(10) << ": writing to offset: " << offset << dendl;
      r = ceph_pwritev(m_remote_mount, r_fd, iov, iovs, offset);
      if (r < 0) {
        derr << ": failed to write remote file path=" << cur_entry->epath
             << ": " << cpp_strerror(r) << dendl;
        break;
      }

      offset += r;
      len -= r;
    }

    --num_blocks;
    ++b;
  }

  if (num_blocks == 0 && r >= 0) { // handle blocklist case
    dout(20) << ": truncating epath=" << cur_entry->epath << " to "
             << cur_entry->stx.stx_size << " bytes" << dendl;
    r = ceph_ftruncate(m_remote_mount, r_fd, cur_entry->stx.stx_size);
    if (r < 0) {
      derr << ": failed to truncate remote file path=" << cur_entry->epath
           << ": " << cpp_strerror(r) << dendl;
      goto freeptr;
    }
    r = ceph_fsync(m_remote_mount, r_fd, 0);
    if (r < 0) {
      derr << ": failed to sync data for file path=" << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
    }
  }

freeptr:
  free(ptr);

close_remote_fd:
  if (ceph_close(m_remote_mount, r_fd) < 0) {
    derr << ": failed to close remote fd path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return -EINVAL;
  }

close_local_fd:
  if (ceph_close(m_local_mount, l_fd) < 0) {
    derr << ": failed to close local fd path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return -EINVAL;
  }
  return r == 0 ? 0 : r;
}

int PeerReplayer::remote_file_op(std::shared_ptr<SyncEntry> &cur_entry,
                                 DirRegistry *registry, SnapSyncStat *sync_stat,
                                 const FHandles &fh) {
  bool need_data_sync = cur_entry->create_fresh() ||
                        cur_entry->purge_remote() ||
                        ((cur_entry->change_mask & CEPH_STATX_SIZE) > 0) ||
                        (((cur_entry->change_mask & CEPH_STATX_MTIME) > 0));

  dout(10) << ": dir_root=" << registry->dir_root
           << ", epath=" << cur_entry->epath
           << ", need_data_sync=" << need_data_sync
           << ", stat_change_mask=" << cur_entry->change_mask << dendl;

  int r;
  if (need_data_sync) {
    if (S_ISREG(cur_entry->stx.stx_mode)) {
      sync_stat->current_stat.inc_file_in_flight_count();
      FileSyncMechanism *syncm = new FileSyncMechanism(
          m_local_mount, std::move(cur_entry), registry, sync_stat, fh, this);
      file_sync_pool.sync_file_data(syncm);
      return 0;
    } else if (S_ISLNK(cur_entry->stx.stx_mode)) {
      // free the remote link before relinking
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root,
                        cur_entry->epath.c_str(), 0);
      if (r < 0 && r != -ENOENT) {
        derr << ": failed to remove remote symlink=" << cur_entry->epath << dendl;
        return r;
      }
      char *target = (char *)alloca(cur_entry->stx.stx_size + 1);
      r = ceph_readlinkat(m_local_mount, fh.c_fd, cur_entry->epath.c_str(),
                          target, cur_entry->stx.stx_size);
      if (r < 0) {
        derr << ": failed to readlink local path=" << cur_entry->epath << ": "
             << cpp_strerror(r) << dendl;
        return r;
      }

      target[cur_entry->stx.stx_size] = '\0';
      r = ceph_symlinkat(m_remote_mount, target, fh.r_fd_dir_root,
                         cur_entry->epath.c_str());
      if (r < 0 && r != EEXIST) {
        derr << ": failed to symlink remote path=" << cur_entry->epath
             << " to target=" << target << ": " << cpp_strerror(r) << dendl;
        return r;
      }
      cur_entry->change_mask =
          (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID | CEPH_STATX_GID |
           CEPH_STATX_MTIME);
    } else {
      dout(5) << ": skipping entry=" << cur_entry->epath
              << ": unsupported mode=" << cur_entry->stx.stx_mode << dendl;
      return 0;
    }
  }
  unsigned int all_change =
      (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID | CEPH_STATX_GID |
       CEPH_STATX_MTIME);
  r = sync_attributes(cur_entry, fh);
  if (r < 0) {
    return r;
  }
  if (cur_entry->change_mask & all_change) {
    sync_stat->current_stat.inc_files_synced(false, cur_entry->stx.stx_size);
  }

  return 0;
}

int PeerReplayer::cleanup_remote_entry(const std::string &epath,
                                       DirRegistry *registry,
                                       const FHandles &fh,
                                       SnapSyncStat *sync_stat, int not_dir) {
  dout(20) << ": dir_root=" << registry->dir_root << ", epath=" << epath
           << dendl;
  int r = 0;
  /*
    not_dir is a hint whether in the base state it's directory or not.
    This hint works always.
  */
  if (not_dir == 1) {
    r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), 0);
    if (r < 0 && r != -ENOENT && r != -EISDIR) {
      derr << ": failed to remove remote entry=" << epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    if (r == -ENOENT) {
      dout(10) << ": entry already removed=" << epath << dendl;
      return r;
    }
    if (r == 0) {
      sync_stat->current_stat.inc_file_del_count();
      return r;
    }
  }

  r = 0;
  struct ceph_statx tstx;
  r = ceph_statxat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), &tstx,
                   CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                       CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                   AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r == -ENOENT) {
    dout(10) << ": entry already removed=" << epath << dendl;
    return r;
  }
  if (r < 0) {
    derr << ": failed to remove remote entry=" << epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }

  /*
    if the hint didn't work(it should work always but for the sanity check), 
    that means we thought it as a directory in base state, our goal is to 
    just clean it up. No matter it's file or directory.
  */
  if (!S_ISDIR(tstx.stx_mode)) {
    r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, epath.c_str(), 0);
    if (r < 0) {
      derr << ": failed to remove remote entry=" << epath << ": "
           << cpp_strerror(r) << dendl;
    }
    sync_stat->current_stat.inc_file_del_count();
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
    if (should_backoff(registry, &r)) {
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
                              CEPH_STATX_MODE,
                              AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
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
        r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(),
                          AT_REMOVEDIR);
        if (r < 0) {
          derr << ": failed to remove remote directory=" << entry.epath << ": "
              << cpp_strerror(r) << dendl;
          break;
        }
        sync_stat->current_stat.inc_dir_deleted_count();

        dout(10) << ": done for remote directory=" << entry.epath << dendl;
        if (entry.dirp && ceph_closedir(m_remote_mount, entry.dirp) < 0) {
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
        r = opendirat(m_remote_mount, fh.r_fd_dir_root, epath,
                      AT_SYMLINK_NOFOLLOW, &dirp);
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
      r = ceph_unlinkat(m_remote_mount, fh.r_fd_dir_root, entry.epath.c_str(),
                        0);
      if (r < 0) {
        derr << ": failed to remove remote directory=" << entry.epath << ": "
            << cpp_strerror(r) << dendl;
        break;
      }
      dout(10) << ": done for remote file=" << entry.epath << dendl;
      sync_stat->current_stat.inc_file_del_count();
      rm_stack.pop();
    }
  }

  while (!rm_stack.empty()) {
    auto &entry = rm_stack.top();
    if (entry.is_directory()) {
      dout(20) << ": closing remote directory=" << entry.epath << dendl;
      if (entry.dirp && ceph_closedir(m_remote_mount, entry.dirp) < 0) {
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

int PeerReplayer::propagate_deleted_entries(
    const std::string &epath, DirRegistry *registry, SnapSyncStat *sync_stat,
    const FHandles &fh,
    std::unordered_map<std::string, unsigned int> &change_mask_map,
    int &change_mask_map_size) {
  dout(10) << ": dir_root=" << registry->dir_root << ", epath=" << epath
           << dendl;
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
      dout(5) << ": epath=" << epath
              << " missing in previous-snap/remote dir-root" << dendl;
    }
    return r;
  }
  struct ceph_statx pstx;
  struct dirent de;
  while (true) {
    if (should_backoff(registry, &r)) {
      dout(0) << ": backing off r=" << r << dendl;
      break;
    }
    unsigned int extra_flags = (change_mask_map_size < max_change_mask_map_size)
                                   ? (CEPH_STATX_UID | CEPH_STATX_GID |
                                      CEPH_STATX_SIZE | CEPH_STATX_MTIME)
                                   : 0;
    r = ceph_readdirplus_r(fh.p_mnt, dirp, &de, &pstx,
                           CEPH_STATX_MODE | extra_flags,
                           AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);

    if (r < 0) {
      derr << ": failed to read directory entries: " << cpp_strerror(r)
           << dendl;
      // flip errno to signal that we got an err (possible the
      // snapshot getting deleted in midst).
      if (r == -ENOENT) {
        r = -EINVAL;
      }
      break;
    }
    if (r == 0) {
      dout(10) << ": reached EOD" << dendl;
      break;
    }
    std::string d_name = std::string(de.d_name);
    if (d_name == "." || d_name == "..") {
      continue;
    }
    auto dpath = entry_path(epath, d_name);

    struct ceph_statx cstx;
    r = ceph_statxat(m_local_mount, fh.c_fd, dpath.c_str(), &cstx,
                     CEPH_STATX_MODE | extra_flags,
                     AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
    if (r < 0 && r != -ENOENT) {
      derr << ": failed to stat local (cur) directory=" << dpath << ": "
           << cpp_strerror(r) << dendl;
      break;
    }

    bool purge_remote = true;
    bool entry_present = false;
    if (r == 0) {
      // directory entry present in both snapshots -- check inode
      // type
      entry_present = true;
      if ((pstx.stx_mode & S_IFMT) == (cstx.stx_mode & S_IFMT)) {
        dout(5) << ": mode matches for entry=" << d_name << dendl;
        purge_remote = false;
      } else {
        dout(5) << ": mode mismatch for entry=" << d_name << dendl;
      }
    } else {
      dout(5) << ": entry=" << d_name << " missing in current snapshot"
              << dendl;
    }

    if (purge_remote && !entry_present) {
      dout(5) << ": purging remote entry=" << dpath << dendl;
      SyncMechanism *syncm = new DeleteMechanism(
          m_local_mount, std::move(std::make_shared<SyncEntry>(dpath)),
          registry, sync_stat, fh, this, !S_ISDIR(pstx.stx_mode));
      registry->sync_pool->sync_anyway(syncm);
    } else if (extra_flags) {
      unsigned int change_mask = 0;
      build_change_mask(pstx, cstx, false, purge_remote, change_mask);
      /*
        saving some of change_mask, such that we can avoid doing
        stat in previous snapshot/remote if it is found in the chage_mask map.
        This map is strategically maintained such that we can best use of
        max_change_mask_map_size threshold.
      */
      change_mask_map[d_name] = change_mask;
      ++change_mask_map_size;
    }
  }
  if (dirp) {
    ceph_closedir(fh.p_mnt, dirp);
  }
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
    FHandles *snapdiff_fh, FHandles* remotediff_fh) {
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  if (prev) {
    dout(20) << ": prev=" << prev << dendl;
  }

  auto cur_snap_path = snapshot_path(m_cct, dir_root, current.first);
  auto fd = open_dir(m_local_mount, cur_snap_path, current.second);
  if (fd < 0) {
    return fd;
  }
  snapdiff_fh->c_fd = snapdiff_fh->p_fd = remotediff_fh->c_fd =
      remotediff_fh->p_fd = -1;

  // current snapshot file descriptor
  snapdiff_fh->m_current = current;
  snapdiff_fh->m_prev = prev;
  snapdiff_fh->c_fd = fd;
  remotediff_fh->c_fd = fd;

  if (prev) {
    auto prev_snap_path = snapshot_path(m_cct, dir_root, (*prev).first);
    snapdiff_fh->p_fd = open_dir(m_local_mount, prev_snap_path, (*prev).second);
    snapdiff_fh->p_mnt = m_local_mount;
  }
  remotediff_fh->p_fd = open_dir(m_remote_mount, dir_root, boost::none);
  remotediff_fh->p_mnt = m_remote_mount;

  if (remotediff_fh->p_fd < 0) {
    ceph_close(m_local_mount, remotediff_fh->c_fd);
    if (snapdiff_fh->p_fd >= 0) {
      ceph_close(m_local_mount, snapdiff_fh->p_fd);
    }
    return remotediff_fh->p_fd;
  }

  {
    std::scoped_lock locker(m_lock);
    auto it = m_registered.find(dir_root);
    ceph_assert(it != m_registered.end());
    snapdiff_fh->r_fd_dir_root = it->second->fd;
    remotediff_fh->r_fd_dir_root = it->second->fd;
  }

  dout(0) << ": using "
          << ((snapdiff_fh->p_fd >= 0)
                  ? ("local (previous) snapshot " + (*prev).first)
                  : "remote dir_root")
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

PeerReplayer::FileSyncPool::FileSyncPool(int num_threads, const Peer &m_peer)
    : num_threads(num_threads), qlimit(max(10 * num_threads, 5000)),
      stop_thread(num_threads, true), pool_stats(this), sync_count(0),
      active(false), m_peer(m_peer) {}

void PeerReplayer::FileSyncPool::activate() {
  {
    std::scoped_lock lock(mtx);
    active = true;
    for (int i = 0; i < num_threads; ++i) {
      stop_thread[i] = false;
    }
  }
  for (int i = 0; i < num_threads; ++i) {
    workers.emplace_back(
        std::make_unique<std::thread>(&FileSyncPool::run, this, i));
  }
}

void PeerReplayer::FileSyncPool::deactivate() {
  {
    std::scoped_lock lock(mtx);
    active = false;
    for (int i = 0; i < workers.size(); ++i) {
      stop_thread[i] = true;
    }
  }
  pick_cv.notify_all();
  give_cv.notify_all();
  for (auto &worker : workers) {
    if (worker->joinable()) {
      worker->join();
    }
  }
  drain_queue();
}

void PeerReplayer::FileSyncPool::drain_queue() {
  std::scoped_lock lock(mtx);
  for (int i = 0; i < sync_queue.size(); ++i) {
    while (!sync_queue[i].empty()) {
      auto &task = sync_queue[i].front();
      task->complete(-1);
      sync_queue[i].pop();
    }
  }
}

void PeerReplayer::FileSyncPool::run(int thread_idx) {
  while (true) {
    FileSyncMechanism *task;
    int sync_idx = 0;
    {
      std::unique_lock<std::mutex> lock(mtx);
      pick_cv.wait(lock, [this, thread_idx] {
        return (stop_thread[thread_idx] || !sync_ring.empty());
      });
      if (stop_thread[thread_idx]) {
        return;
      }
      task = sync_ring.front();
      sync_idx = task->get_file_sync_queue_idx();
      ceph_assert(sync_idx < sync_queue.size() &&
                  !sync_queue[sync_idx].empty());
      sync_queue[sync_idx].pop();
      sync_ring.pop();
      if (!sync_queue[sync_idx].empty()) {
        sync_ring.emplace(sync_queue[sync_idx].front());
      }
      pool_stats.remove_file(task->get_file_size());
    }
    give_cv.notify_one();
    task->complete(0);
  }
}

void PeerReplayer::FileSyncPool::sync_file_data(FileSyncMechanism *task) {
  int sync_idx = task->get_file_sync_queue_idx();
  {
    std::unique_lock<std::mutex> lock(mtx);
    give_cv.wait(lock, [this, sync_idx] {
      return (!active || sync_queue[sync_idx].size() < qlimit);
    });
    if (!active) {
      return;
    }
    if (sync_queue[sync_idx].empty()) {
      sync_ring.emplace(task);
    }
    sync_queue[sync_idx].emplace(task);
    pool_stats.add_file(task->get_file_size());
    task->sync_in_flight();
  }
  pick_cv.notify_one();
}

int PeerReplayer::FileSyncPool::sync_start() {
  std::scoped_lock lock(mtx);
  int sync_idx = 0;
  if (!unassigned_sync_ids.empty()) {
    sync_idx = unassigned_sync_ids.back();
    sync_queue[sync_idx] = std::queue<FileSyncMechanism *>();
    unassigned_sync_ids.pop_back();
  } else {
    sync_idx = sync_count++;
    sync_queue.emplace_back(std::queue<FileSyncMechanism *>());
  }
  return sync_idx;
}

void PeerReplayer::FileSyncPool::sync_finish(int sync_idx) {
  std::scoped_lock lock(mtx);
  ceph_assert(sync_idx >= 0 && sync_idx < sync_queue.size());
  while (!sync_queue[sync_idx].empty()) {
    auto &task = sync_queue[sync_idx].front();
    task->complete(-1);
    sync_queue[sync_idx].pop();
  }
  unassigned_sync_ids.push_back(sync_idx);
}

void PeerReplayer::FileSyncPool::update_state() {
  int thread_count = g_ceph_context->_conf->cephfs_mirror_file_sync_thread;
  std::unique_lock<std::mutex> lock(mtx);
  if (!active) {
    return;
  }
  if (thread_count == workers.size()) {
    return;
  }

  if (thread_count < workers.size()) {
    for (int i = thread_count; i < workers.size(); ++i) {
      stop_thread[i] = true;
    }
    lock.unlock();
    pick_cv.notify_all();
    for (int i = thread_count; i < workers.size(); ++i) {
      if (workers[i]->joinable()) {
        workers[i]->join();
        dout(0) << ": shutdown of thread having thread idx=" << i
                << ", from file sync thread pool" << dendl;
      }
    }
    lock.lock();
    while (workers.size() > thread_count) {
      workers.pop_back();
      stop_thread.pop_back();
    }
  } else {
    for (int i = workers.size(); i < thread_count; ++i) {
      stop_thread.emplace_back(false);
      workers.emplace_back(
          std::make_unique<std::thread>(&FileSyncPool::run, this, i));
      dout(0) << ": creating thread having thread idx=" << i
              << ", in file sync thread pool" << dendl;
    }
  }
  num_threads = thread_count;

  dout(0) << ": updating number of threads in file sync threadpool to "
          << thread_count << dendl;
}

void PeerReplayer::DirSyncPool::activate() {
  {
    std::scoped_lock lock(mtx);
    active = true;
    qlimit = 1 << 15;
  }
  for (int i = 0; i < num_threads; ++i) {
    thread_status.emplace_back(new ThreadStatus());
    thread_status[i]->stop_called = false;
    workers.emplace_back(std::make_unique<std::thread>(&DirSyncPool::run, this,
                                                       thread_status[i]));
    thread_status[i]->active = true;
  }
}

void PeerReplayer::DirSyncPool::drain_queue() {
  std::scoped_lock lock(mtx);
  while (!sync_queue.empty()) {
    auto &task = sync_queue.front();
    task->complete(-1);
    sync_queue.pop();
    queued--;
  }
}

void PeerReplayer::DirSyncPool::deactivate() {
  std::unique_lock<std::mutex> lock(mtx);
  active = false;
  for (int i = 0; i < thread_status.size(); ++i) {
    thread_status[i]->stop_called = true;
  }
  lock.unlock();
  pick_cv.notify_all();
  for (auto &worker : workers) {
    if (worker->joinable()) {
      worker->join();
    }
  }
  lock.lock();
  workers.clear();
  for (int i = 0; i < thread_status.size(); ++i) {
    if (thread_status[i]) {
      delete thread_status[i];
      thread_status[i] = nullptr;
    }
  }
  thread_status.clear();
  lock.unlock();
  drain_queue();
}

void PeerReplayer::DirSyncPool::run(ThreadStatus *status) {
  while (true) {
    SyncMechanism *task;
    {
      std::unique_lock<std::mutex> lock(mtx);
      pick_cv.wait(lock, [this, status] {
        return (status->stop_called || !sync_queue.empty());
      });
      if (status->stop_called) {
        status->active = false;
        break;
      }
      task = sync_queue.front();
      sync_queue.pop();
      queued--;
    }
    task->complete(0);
  }
}

bool PeerReplayer::DirSyncPool::_try_sync(SyncMechanism *task) {
  {
    std::unique_lock<std::mutex> lock(mtx);
    if (sync_queue.size() >= qlimit) {
      return false;
    }
    task->sync_in_flight();
    sync_queue.emplace(task);
    queued++;
  }
  pick_cv.notify_one();
  return true;
}

bool PeerReplayer::DirSyncPool::try_sync(SyncMechanism *task) {
  bool success = false;
  if (sync_queue.size() < qlimit) {
    success = _try_sync(task);
  }
  return success;
}

void PeerReplayer::DirSyncPool::sync_anyway(SyncMechanism *task) {
  if (!try_sync(task)) {
    sync_direct(task);
  }
}

void PeerReplayer::DirSyncPool::sync_direct(SyncMechanism *task) {
  task->sync_in_flight();
  task->complete(1);
}

void PeerReplayer::DirSyncPool::update_state() {
  std::unique_lock<std::mutex> lock(mtx);
  int thread_count = g_ceph_context->_conf->cephfs_mirror_dir_scanning_thread;
  if (!active) {
    return;
  }
  if (thread_count == num_threads) {
    return;
  }

  if (thread_count < num_threads) {
    for (int i = thread_count; i < num_threads; ++i) {
      thread_status[i]->stop_called = true;
      dout(0) << ": Lazy shutdown of thread no " << i
              << " from directory scanning threadpool for " << epath << dendl;
    }
    num_threads = thread_count;
    lock.unlock();
    pick_cv.notify_all();
  } else {
    int i;
    for (i = num_threads; i < workers.size() && num_threads < thread_count;
         ++i) {
      if (thread_status[i]->active) {
        swap(workers[i], workers[num_threads]);
        swap(thread_status[i], thread_status[num_threads]);
        thread_status[num_threads++]->stop_called = false;
        dout(0) << ": Reactivating thread no " << i
                << " of directory scanning threadpool for " << epath << dendl;
      }
    }
    for (int i = (int)workers.size() - 1; i >= num_threads; --i) {
      if (num_threads == thread_count && thread_status[i]->active) {
        break;
      }
    }
    lock.unlock();
    pick_cv.notify_all();
    if (i < num_threads) {
      for (i = (int)workers.size() - 1; i >= num_threads; --i) {
        if (workers[i]->joinable()) {
          workers[i]->join();
          dout(0) << ": Force shutdown of already lazy shut thread having "
                     "thread no "
                  << i << " from directory scanning threadpool for " << epath
                  << dendl;
        }
        workers.pop_back();
        if (thread_status[i]) {
          delete thread_status[i];
          thread_status[i] = nullptr;
        }
        thread_status.pop_back();
      }
    }

    for (i = num_threads; i < thread_count; ++i) {
      thread_status.emplace_back(new ThreadStatus());
      thread_status[i]->stop_called = false;
      workers.emplace_back(std::make_unique<std::thread>(
          &DirSyncPool::run, this, thread_status[i]));
      thread_status[i]->active = true;
      dout(0) << ": Creating thread no " << i
              << " in directory scanning threadpool for " << epath << dendl;
    }
    num_threads = thread_count;
  }
  dout(0) << ": updating number of threads in directory scanner threadpool for "
             "directory="
          << epath << " to" << thread_count << dendl;
}

int PeerReplayer::SyncMechanism::populate_current_stat(const FHandles& fh) {
  int r = 0;
  if (!cur_entry->stat_known) { // stat populated
    r = ceph_statxat(m_local, fh.c_fd, cur_entry->epath.c_str(),
                     &cur_entry->stx,
                     CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                         CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                     AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
    if (r < 0) {
      derr << ": failed to stat cur entry= " << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      derr << ": failed to stat cur entry= " << cur_entry->epath << ", "
           << cur_entry->sync_is_snapdiff() << dendl;
      return r;
    }
    cur_entry->set_stat_known();
  }
  return r;
}

int PeerReplayer::SyncMechanism::populate_change_mask(
    const FHandles &fh) {
  int r = 0;
  bool purge_remote = cur_entry->purge_remote();
  bool create_fresh = cur_entry->create_fresh();
  struct ceph_statx pstx;
  if (cur_entry->change_mask_populated()) {
    return 0;
  }
  if (!create_fresh && !purge_remote) {
    int pstat_r =
        ceph_statxat(fh.p_mnt, fh.p_fd, cur_entry->epath.c_str(), &pstx,
                     CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                         CEPH_STATX_SIZE | CEPH_STATX_ATIME | CEPH_STATX_MTIME,
                     AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
    if (pstat_r < 0 && pstat_r != -ENOENT && pstat_r != -ENOTDIR) {
      r = pstat_r;
      derr << ": failed to stat prev entry= " << cur_entry->epath << ": "
           << cpp_strerror(r) << dendl;
      return r;
    }
    purge_remote = pstat_r == 0 && ((cur_entry->stx.stx_mode & S_IFMT) !=
                                    (pstx.stx_mode & S_IFMT));
    create_fresh = (pstat_r < 0);
  }
  replayer->build_change_mask(pstx, cur_entry->stx, create_fresh, purge_remote,
                              cur_entry->change_mask);
  return 0;
}


void PeerReplayer::DeleteMechanism::finish(int r) {
  if (r < 0) {
    goto notify_finish;
  }
  r = replayer->cleanup_remote_entry(cur_entry->epath, registry, fh, sync_stat,
                                     not_dir);
  if (r < 0 && r != -ENOENT) {
    derr << ": failed to cleanup remote entry=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    registry->set_failed(r);
  }
notify_finish:
  registry->dec_sync_indicator();
}

void PeerReplayer::FileSyncMechanism::finish(int r) {
  if (r < 0) {
    goto notify_finish;
  }
  r = sync_file();
  if (r < 0) {
    registry->set_failed(r);
  }
notify_finish:
  sync_stat->current_stat.dec_file_in_flight_count();
  registry->dec_sync_indicator();
}

int PeerReplayer::FileSyncMechanism::_sync_file() {
  dout(20) << ": epath=" << cur_entry->epath << dendl;

  struct cblock b;
  // extent covers the whole file
  b.offset = 0;
  b.len = cur_entry->stx.stx_size;

  struct ceph_file_blockdiff_changedblocks block;
  block.num_blocks = 1;
  block.b = &b;
  int r = replayer->copy_to_remote(cur_entry, registry, fh, block.num_blocks,
                                   block.b, true);
  if (r < 0) {
    derr << ": failed to copy path=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  r = replayer->sync_attributes(cur_entry, fh);
  sync_stat->current_stat.inc_files_synced(true, cur_entry->stx.stx_size);
  return r;
}

int PeerReplayer::FileSyncMechanism::sync_file() {
  dout(20) << ": dir_root=" << registry->dir_root
           << ", epath=" << cur_entry->epath
           << ", is_snapdiff=" << cur_entry->sync_is_snapdiff() << dendl;
  int r = 0;
  if (!cur_entry->sync_is_snapdiff() || cur_entry->create_fresh() ||
      cur_entry->purge_remote()) {
    return _sync_file();
  }

  ceph_file_blockdiff_info info;
  r = ceph_file_blockdiff_init(
      m_local, registry->dir_root.c_str(), cur_entry->epath.c_str(),
      (*fh.m_prev).first.c_str(), fh.m_current.first.c_str(), &info);
  if (r != 0) {
    dout(5) << ": failed to init file blockdiff: r=" << r << dendl;
    return r;
  }

  if (r < 0) {
    if (r == -ENOENT) {
      dout(20) << ": new file epath=" << cur_entry->epath << dendl;
    }
    /*
      Even if block diff failed with an error which is not ENOENT,
      which should not be the case. But still we should just try 
      the brute copy once.
    */
    return _sync_file();
  }

  r = 1;
  while (true) {
    ceph_file_blockdiff_changedblocks blocks;
    r = ceph_file_blockdiff(&info, &blocks);
    if (r < 0) {
      derr << " failed to get next changed block: ret:" << r << dendl;
      break;
    }

    int rr = r;
    if (blocks.num_blocks) {
      r = replayer->copy_to_remote(cur_entry, registry, fh, blocks.num_blocks,
                                   blocks.b);
      ceph_free_file_blockdiff_buffer(&blocks);
      if (r < 0) {
        derr << ": blockdiff callback returned error: r=" << r << dendl;
        break;
      }
    }

    if (rr == 0) {
      break;
    }
    // else fetch next changed blocks
  }

  ceph_file_blockdiff_finish(&info);
  if (r >= 0) {
    r = replayer->sync_attributes(cur_entry, fh);
  }
  sync_stat->current_stat.inc_files_synced(true, cur_entry->stx.stx_size);
  return r;
}

void PeerReplayer::build_change_mask(const struct ceph_statx &pstx,
                                     const struct ceph_statx &cstx,
                                     bool create_fresh, bool purge_remote,
                                     unsigned int &change_mask) {
  change_mask |= SyncEntry::CHANGE_MASK_POPULATED;
  if (!S_ISDIR(pstx.stx_mode)) {
    change_mask |= SyncEntry::WASNT_DIR_IN_PREV_SNAPSHOT;
  }
  if (create_fresh || purge_remote) {
    change_mask |= (CEPH_STATX_MODE | CEPH_STATX_SIZE | CEPH_STATX_UID |
                    CEPH_STATX_GID | CEPH_STATX_MTIME);
    if (create_fresh) {
      change_mask |= SyncEntry::CREATE_FRESH;
    }
    if (purge_remote) {
      change_mask |= SyncEntry::PURGE_REMOTE;
    }
    return;
  }

  if ((cstx.stx_mode & ~S_IFMT) != (pstx.stx_mode & ~S_IFMT)) {
    change_mask = change_mask | CEPH_STATX_MODE;
  }
  if (cstx.stx_size != pstx.stx_size) {
    change_mask = change_mask | CEPH_STATX_SIZE;
  }
  if (cstx.stx_uid != pstx.stx_uid) {
    change_mask = change_mask | CEPH_STATX_UID;
  }
  if (cstx.stx_gid != pstx.stx_gid) {
    change_mask = change_mask | CEPH_STATX_GID;
  }
  if (cstx.stx_mtime != pstx.stx_mtime) {
    change_mask = change_mask | CEPH_STATX_MTIME;
  }
}

void PeerReplayer::DirSyncMechanism::finish(int r) {
  if (r < 0) {
    goto notify_finish;
  }
  r = sync_tree();
  if (r < 0) {
    registry->set_failed(r);
  }
notify_finish:
  registry->dec_sync_indicator();
}

int PeerReplayer::DirSyncMechanism::sync_tree() {

  int r = 0;
  while (cur_entry && !replayer->should_backoff(registry, &r)) {
    if (cur_entry->needs_remote_sync()) {
      // **--> sync remote
      r = sync_current_entry();
      if (r < 0) {
        break;
      }
    }
    r = go_next();
    if (r < 0) {
      break;
    }
  }
  finish_sync();
  return r;
}

bool PeerReplayer::DirSyncMechanism::try_spawning(SyncMechanism *syncm) {
  if (registry->sync_pool->try_sync(syncm)) {
    cur_entry = nullptr;
    return true;
  }
  cur_entry = syncm->move_cur_entry();
  delete syncm;
  return false;
}

int PeerReplayer::DirSnapDiffSync::go_next() {
  bool failed_prev =
      (sync_stat->nr_failures > 0 || sync_stat->synced_snap_count == 0);
  int r = 0;
  while (!m_sync_stack.empty()) {
    std::shared_ptr<SyncEntry> &entry = m_sync_stack.top();
    dout(20) << ": top of stack path=" << entry->epath << dendl;
    std::string e_name;
    ceph_snapdiff_entry_t sd_entry;
    while (true) {
      if (replayer->should_backoff(registry, &r)) {
        dout(0) << ": backing off r=" << r << dendl;
        return r;
      }
      r = ceph_readdir_snapdiff(&entry->info, &sd_entry);
      if (r < 0) {
        derr << ": failed to read directory=" << entry->epath << dendl;
        return r;
      }
      if (r == 0) {
        break;
      }
      std::string d_name = sd_entry.dir_entry.d_name;
      if (d_name == "." || d_name == "..") {
        continue;
      }
      e_name = d_name;
      break;
    }

    std::string epath = entry_path(entry->epath, e_name);
    if (entry->deleted_sibling_exist &&
        entry->deleted_sibling.snapid == (*fh.m_prev).second) {
      std::string deleted_sibling_d_name =
          entry->deleted_sibling.dir_entry.d_name;
      if (r == 0 || sd_entry.snapid == (*fh.m_prev).second ||
          e_name != deleted_sibling_d_name) {
        // prev sibling is a deleted entry, we need to delete it now.
        std::shared_ptr<SyncEntry> deleted_entry = std::make_shared<SyncEntry>(
            entry_path(entry->epath, deleted_sibling_d_name));
        SyncMechanism *syncm = new DeleteMechanism(
            m_local, std::move(deleted_entry), registry, sync_stat, fh,
            replayer, (DT_DIR != entry->deleted_sibling.dir_entry.d_type));
        registry->sync_pool->sync_anyway(syncm);
        entry->deleted_sibling_exist = false;
      }
    }

    if (r == 0) {
      dout(10) << ": done for directory=" << entry->epath << dendl;
      if (!(m_sync_stack.size() == 1 && entry->epath == ".")) {
        if (ceph_close_snapdiff(&entry->info) < 0) {
          derr << ": failed to close directory=" << entry->epath << dendl;
        }
      }
      m_sync_stack.pop();
      continue;
    }

    if (sd_entry.snapid == (*fh.m_prev).second) {
      entry->deleted_sibling = sd_entry;
      entry->deleted_sibling_exist = true;
      continue;
    }

    struct ceph_statx cstx;
    cur_entry =
        std::make_shared<SyncEntry>(epath, ceph_snapdiff_info(), cstx, 0);
    if (entry->deleted_sibling_exist &&
        entry->deleted_sibling.snapid == (*fh.m_prev).second &&
        sd_entry.snapid == fh.m_current.second &&
        std::string(entry->deleted_sibling.dir_entry.d_name) == e_name) {
      // this is an entry where previous need to be purged
      cur_entry->change_mask |= SyncEntry::PURGE_REMOTE;
      entry->deleted_sibling_exist = false;
    }

    SyncMechanism *syncm = new DirSnapDiffSync(
        m_local, std::move(cur_entry), registry, sync_stat, fh, replayer);
    if (try_spawning(syncm)) {
      continue;
    }
    r = 0;
    break;
  }
  return r;
}

int PeerReplayer::DirSnapDiffSync::sync_current_entry() {
  int r = 0;
  bool failed_prev =
      (sync_stat->nr_failures > 0 || sync_stat->synced_snap_count == 0);
  /*
  Our goal here, when this is a fresh syncing we will go for DirSnapDiffSync.
  When we already know that previous sync failed, then our goal is to, for the
  unchanged part(same as previous snapshot) we will scan through DirSnapDiffSync
  for other parts we will scan through DirBruteDiffSync comparing with the remote
  state(more specifically keeping remote as diff base). With this strategy we
  will get rid of transferring already transferred file for a failed scenario.
  Also, we are traversing the unchanged part(between previous and current snapshot)
  of the tree using snapdiff.

  Scenario when previous sync failed:
  1. For file, we completely skipped file blockdiff strategy and based on 
    remote we synced the file. Which will eventually save us from already 
    synced file.
  2. For directory, we used local previous snapshot as diff base for
    change mask building to find out whether is a newly created entry
    (or previous snapshot has same entry with different type). Some cases are 
    give below,
    * If directory doesn't exist in prev snapshot, it might be possible
      that remote state has some patial subtree under that directory. So
      we let DirBruteDiffSync class to handle by using remote diff base
      by sending registry->remotediff_fh.
    * Same case for when entry type is different in previous snapshot
    * If both is not the case, then we went for snapdiff optimized strategy.

  Scenario when previous sync didn't fail:
    For every new entry or entry need to be purged in remote before creating
    We let DirBruteDiffSync class handle this matter as we know snapdiff
    has no impact here. And as remote state is not corrupted, we let DirBruteDiffSync
    class to use local previous snapshot as diff base.
  */

  r = populate_current_stat(fh);
  if (r < 0) {
    return r;
  }

  bool remote_fh = failed_prev && (!cur_entry->is_directory());
  if (remote_fh) {
    // because I am rebuilding change mask for remote state as diff base
    cur_entry->reset_change_mask();
  }

  r = populate_change_mask(remote_fh ? registry->remotediff_fh : fh);
  if (r < 0) {
    return r;
  }

  if ((failed_prev && !cur_entry->is_directory()) ||
      cur_entry->create_fresh() || cur_entry->purge_remote()) {
    if (failed_prev && cur_entry->is_directory()) {
      /*
      we need to reset it when it's a directory, because DirBruteDiffSync
      will rebuild the change mask keeping remote state as diff base as
      the remote state is inconsistent because of failed sync attempt.
      For file we don't need to reset as we already built the change mask
      keeping remote state as diff base(using remote_fh flag), So 
      DirBruteDiffSync will automatically skip it.
      */
      cur_entry->reset_change_mask();
    }
    cur_entry->set_is_snapdiff(false);

    SyncMechanism *syncm = new DirBruteDiffSync(
        m_local, std::move(cur_entry), registry, sync_stat,
        failed_prev ? registry->remotediff_fh : fh, replayer);
    registry->sync_pool->sync_direct(syncm);
    return 0;
  }

  if (S_ISDIR(cur_entry->stx.stx_mode)) { // is a directory
    r = replayer->remote_mkdir(cur_entry, fh, sync_stat);
    if (r < 0) {
      return r;
    }
    sync_stat->current_stat.inc_dir_scanned_count();
  } else {
    r = replayer->remote_file_op(cur_entry, registry, sync_stat, fh);
    cur_entry = nullptr;
    return r;
  }
  r = ceph_open_snapdiff(fh.p_mnt, registry->dir_root.c_str(),
                         cur_entry->epath.c_str(), (*fh.m_prev).first.c_str(),
                         fh.m_current.first.c_str(), &cur_entry->info);
  if (r != 0) {
    derr << ": failed to open snapdiff, entry=" << cur_entry->epath
         << ", r=" << r << dendl;
    return r;
  }
  cur_entry->set_remote_synced();
  m_sync_stack.emplace(std::move(cur_entry));
  cur_entry = nullptr;
  return 0;
}

void PeerReplayer::DirSnapDiffSync::finish_sync() {
  dout(20) << dendl;

  while (!m_sync_stack.empty()) {
    auto &entry = m_sync_stack.top();
    if (entry->is_directory() &&
        !(m_sync_stack.size() == 1 && entry->epath == ".")) {
      dout(20) << ": closing local directory=" << entry->epath << dendl;
      if (ceph_close_snapdiff(&(entry->info)) < 0) {
        derr << ": failed to close snapdiff directory=" << entry->epath
             << dendl;
      }
    }

    m_sync_stack.pop();
  }
}

int PeerReplayer::DirBruteDiffSync::go_next() {
  int r = 0;
  while (!m_sync_stack.empty()) {
    std::shared_ptr<SyncEntry> &entry = m_sync_stack.top();
    dout(20) << ": top of stack path=" << entry->epath << dendl;
    std::string e_name;
    struct dirent de;
    struct ceph_statx stx;
    while (true) {
      if (replayer->should_backoff(registry, &r)) {
        dout(0) << ": backing off r=" << r << dendl;
        return r;
      }
      r = ceph_readdirplus_r(m_local, entry->dirp, &de, &stx,
                             CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                                 CEPH_STATX_SIZE | CEPH_STATX_ATIME |
                                 CEPH_STATX_MTIME,
                             AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW, NULL);
      if (r < 0) {
        derr << ": failed to local read directory=" << entry->epath << dendl;
        return r;
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
      dout(10) << ": done for directory=" << entry->epath << dendl;
      if (!(m_sync_stack.size() == 1 && entry->epath == ".")) {
        if (entry->dirp && ceph_closedir(m_local, entry->dirp) < 0) {
          derr << ": failed to close local directory=" << entry->epath << dendl;
        }
      }
      m_sync_stack.pop();
      change_mask_map_size -= (int)m_change_mask_map_stack.top().size();
      m_change_mask_map_stack.pop();
      continue;
    }
    std::shared_ptr<SyncEntry> &par_entry = m_sync_stack.top();
    auto& par_change_mask_map = m_change_mask_map_stack.top();
    uint64_t change_mask = 0;
    bool create_fresh = par_entry->create_fresh() || par_entry->purge_remote();

    if (!create_fresh) {
      auto it = par_change_mask_map.find(e_name);
      /*
        found in the map so, we don't need to do extra stat in prev
        snapshot or remote.
      */
      if (it != par_change_mask_map.end()) {
        change_mask = it->second;
        // we already take the change mask and deleting it from memory, to insert
        // more entry
        par_change_mask_map.erase(it);
        change_mask_map_size--;
      }
    } else {
      change_mask |= SyncEntry::CREATE_FRESH;
    }
    cur_entry = std::make_shared<SyncEntry>(
        entry_path(par_entry->epath, e_name), nullptr, stx, change_mask);
    

    // this will help us to avoid doing stat again in current snapshot
    // in populate_current_stat function.
    cur_entry->set_stat_known();
    SyncMechanism *syncm = new DirBruteDiffSync(
        m_local, std::move(cur_entry), registry, sync_stat, fh, replayer);
    if (try_spawning(syncm)) {
      continue;
    }
    r = 0;
    break;
  }
  return r;
}

int PeerReplayer::DirBruteDiffSync::sync_current_entry() {
  int r = 0, rem_r = 0;
  r = populate_current_stat(fh);
  if (r < 0) {
    return r;
  }
  r = populate_change_mask(fh);
  if (r < 0) {
    return r;
  }
  if (cur_entry->purge_remote()) {
    rem_r = replayer->cleanup_remote_entry(
        cur_entry->epath, registry, fh, sync_stat,
        cur_entry->wasnt_dir_in_prev_snapshot());
    if (rem_r < 0 && rem_r != -ENOENT) {
      derr << ": failed to cleanup remote entry=" << cur_entry->epath << ": "
           << cpp_strerror(rem_r) << dendl;
      r = rem_r;
      return r;
    }
  }
  if (S_ISDIR(cur_entry->stx.stx_mode)) {
    r = replayer->remote_mkdir(cur_entry, fh, sync_stat);
    if (r < 0) {
      return r;
    }
    sync_stat->current_stat.inc_dir_scanned_count();
  } else {
    r = replayer->remote_file_op(cur_entry, registry, sync_stat, fh);
    cur_entry = nullptr;
    return r;
  }
  r = opendirat(m_local, fh.c_fd, cur_entry->epath, AT_SYMLINK_NOFOLLOW,
                &cur_entry->dirp);
  if (r < 0) {
    derr << ": failed to open local directory=" << cur_entry->epath << ": "
         << cpp_strerror(r) << dendl;
    return r;
  }
  std::unordered_map<std::string, unsigned int> change_mask_map;

  /*
    when cur_entry is fresh or entry in previous snapshot need to
    be purged, then we don't need unnecessary propagate_deleted_entries
    Saving some api calls.
  */

  if (!cur_entry->create_fresh() && !cur_entry->purge_remote()) {
    r = replayer->propagate_deleted_entries(cur_entry->epath, registry,
                                            sync_stat, fh, change_mask_map,
                                            change_mask_map_size);
    if (r < 0 && r != -ENOENT) {
      derr << ": failed to propagate missing dirs: " << cpp_strerror(r)
           << dendl;
      return r;
    }
  }

  cur_entry->set_remote_synced();
  m_sync_stack.emplace(std::move(cur_entry));
  m_change_mask_map_stack.emplace(std::move(change_mask_map));
  return 0;
}

void PeerReplayer::DirBruteDiffSync::finish_sync() {
  dout(20) << dendl;

  while (!m_sync_stack.empty()) {
    auto &entry = m_sync_stack.top();
    if (entry && entry->is_directory() &&
        !(m_sync_stack.size() == 1 && entry->epath == ".")) {
      dout(20) << ": closing local directory=" << entry->epath << dendl;
      if (ceph_closedir(m_local, entry->dirp) < 0) {
        derr << ": failed to close local directory=" << entry->epath << dendl;
      }
    }

    m_sync_stack.pop();
  }
}

int PeerReplayer::do_synchronize(const std::string &dir_root, const Snapshot &current,
                                 boost::optional<Snapshot> prev) {
  dout(0) << ": sync start-->" << dir_root << dendl;
  dout(20) << ": dir_root=" << dir_root << ", current=" << current << dendl;
  std::unique_lock<ceph::mutex> lock(m_lock);
  DirRegistry *registry = m_registered.at(dir_root);
  SnapSyncStat* sync_stat = m_snap_sync_stats.at(dir_root);
  lock.unlock();
  int r = pre_sync_check_and_open_handles(dir_root, current, prev,
                                          &registry->snapdiff_fh,
                                          &registry->remotediff_fh);
  if (r < 0) {
    dout(5) << ": cannot proceed with sync: " << cpp_strerror(r) << dendl;
    return r;
  }
  registry->dir_root = dir_root, registry->failed = registry->canceled = false;
  registry->failed_reason = 0;
  FHandles *fh;
  if (registry->snapdiff_fh.p_fd >= 0) {
    fh = &registry->snapdiff_fh;
  } else {
    fh = &registry->remotediff_fh;
  }

  auto close_p_fd = [&registry]() {
    if (registry->snapdiff_fh.p_fd >= 0) {
      ceph_close(registry->snapdiff_fh.p_mnt, registry->snapdiff_fh.p_fd);
    }
    if (registry->remotediff_fh.p_fd >= 0) {
      ceph_close(registry->remotediff_fh.p_mnt, registry->remotediff_fh.p_fd);
    }
  };

  // record that we are going to "dirty" the data under this
  // directory root
  auto snap_id_str{stringify(current.second)};
  r = ceph_fsetxattr(m_remote_mount, fh->r_fd_dir_root, "ceph.mirror.dirty_snap_id",
                     snap_id_str.c_str(), snap_id_str.size(), 0);
  if (r < 0) {
    derr << ": error setting \"ceph.mirror.dirty_snap_id\" on dir_root=" << dir_root
         << ": " << cpp_strerror(r) << dendl;
    ceph_close(m_local_mount, fh->c_fd);
    close_p_fd();
    return r;
  }

  struct ceph_statx stx;
  r = ceph_fstatx(m_local_mount, fh->c_fd, &stx,
                  CEPH_STATX_MODE | CEPH_STATX_UID | CEPH_STATX_GID |
                      CEPH_STATX_SIZE | CEPH_STATX_MTIME,
                  AT_STATX_DONT_SYNC | AT_SYMLINK_NOFOLLOW);
  if (r < 0) {
    derr << ": failed to stat snap=" << current.first << ": " << cpp_strerror(r)
         << dendl;
    ceph_close(m_local_mount, fh->c_fd);
    close_p_fd();
    return r;
  }

  SyncMechanism *syncm;
  std::shared_ptr <SyncEntry> root_entry;

  lock.lock();
  dout(0) << ": Number of dir scanning threads="
          << g_ceph_context->_conf->cephfs_mirror_dir_scanning_thread << dendl;
  registry->sync_start(file_sync_pool.sync_start());
  sync_stat->current_stat.start_timer();
  registry->sync_pool = std::make_unique<DirSyncPool>(
      g_ceph_context->_conf->cephfs_mirror_dir_scanning_thread, dir_root,
      m_peer);
  registry->sync_pool->activate();
  lock.unlock();

  if (registry->snapdiff_fh.p_fd >= 0) {
    std::shared_ptr<SyncEntry> cur_entry =
        std::make_shared<SyncEntry>(".", ceph_snapdiff_info(), stx, 0);
    cur_entry->set_stat_known();
    root_entry = cur_entry;
    syncm = new DirSnapDiffSync(m_local_mount, std::move(cur_entry), registry,
                                sync_stat, registry->snapdiff_fh, this);
  } else {
    std::shared_ptr<SyncEntry> cur_entry =
        std::make_shared<SyncEntry>(".", nullptr, stx, 0);
    cur_entry->set_stat_known();
    root_entry = cur_entry;
    syncm = new DirBruteDiffSync(m_local_mount, std::move(cur_entry), registry,
                                 sync_stat, registry->remotediff_fh, this);
  }
  registry->sync_pool->sync_anyway(syncm);
  registry->wait_for_sync_finish();
  if (!prev || registry->snapdiff_fh.p_fd < 0) {
    if (root_entry->dirp &&
        ceph_closedir(m_local_mount, root_entry->dirp) < 0) {
      derr << ": failed to close local directory=." << dendl;
    }
  } else if (ceph_close_snapdiff(&root_entry->info) < 0) {
    derr << ": failed to close local directory=." << dendl;
  }

  should_backoff(registry, &r);
  if (registry->failed) {
    r = registry->failed_reason;
  }

  lock.lock();
  registry->sync_pool->deactivate();
  registry->sync_pool = nullptr;
  sync_stat->reset_stats();
  lock.unlock();

  dout(20) << " cur:" << fh->c_fd
           << " prev:" << fh->p_fd
           << " ret = " << r
           << dendl;

  // @FHandles.r_fd_dir_root is closed in @unregister_directory since
  // its used to acquire an exclusive lock on remote dir_root.

  // c_fd has been used in ceph_fdopendir call so
  // there is no need to close this fd manually.
  ceph_close(m_local_mount, fh->c_fd);
  close_p_fd();
  dout(0) << ": sync done-->" << dir_root << ", " << current.first << dendl;

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
    r = do_synchronize(dir_root, current);
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
      r = do_synchronize(dir_root, current);
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
  double start = 0;
  double end = 0;
  double duration = 0;
  for (; it != local_snap_map.end(); ++it) {
    if (m_perf_counters) {
      start = std::chrono::duration_cast<std::chrono::seconds>(clock::now().time_since_epoch()).count();
      utime_t t;
      t.set_from_double(start);
      m_perf_counters->tset(l_cephfs_mirror_peer_replayer_last_synced_start, t);
    }
    set_current_syncing_snap(dir_root, it->first, it->second);
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
    if (m_perf_counters) {
      m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snaps_synced);
      end = std::chrono::duration_cast<std::chrono::seconds>(clock::now().time_since_epoch()).count();
      utime_t t;
      t.set_from_double(end);
      m_perf_counters->tset(l_cephfs_mirror_peer_replayer_last_synced_end, t);
      duration = end - start;
      t.set_from_double(duration);
      m_perf_counters->tinc(l_cephfs_mirror_peer_replayer_avg_sync_time, t);
      m_perf_counters->tset(l_cephfs_mirror_peer_replayer_last_synced_duration, t);
      m_perf_counters->set(l_cephfs_mirror_peer_replayer_last_synced_bytes, m_snap_sync_stats.at(dir_root)->sync_bytes);
    }

    set_last_synced_stat(dir_root, it->first, it->second, duration);
    if (--snaps_per_cycle == 0) {
      break;
    }

    last_snap_name = it->second;
    last_snap_id = it->first;
  }

  return 0;
}

int PeerReplayer::sync_snaps(const std::string &dir_root,
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
  return r;
}

void PeerReplayer::run(SnapshotReplayerThread *replayer) {
  dout(10) << ": snapshot replayer=" << replayer << dendl;

  monotime last_directory_scan = clock::zero();
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
          if (r == 0) {
            r = sync_snaps(*dir_root, locker);
            if (r < 0 && m_perf_counters) {
              m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snap_sync_failures);
            }
          } else {
            _inc_failed_count(*dir_root);
            if (m_perf_counters) {
              m_perf_counters->inc(l_cephfs_mirror_peer_replayer_snap_sync_failures);
            }
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
    if (sync_stat->failed) {
      f->dump_string("state", "failed");
      if (sync_stat->last_failed_reason) {
	f->dump_string("failure_reason", *sync_stat->last_failed_reason);
      }
    } else if (!sync_stat->current_syncing_snap) {
      f->dump_string("state", "idle");
    } else {
      f->dump_string("state", "syncing");
      f->open_object_section("current_syncing_snap");
      f->dump_unsigned("id", (*sync_stat->current_syncing_snap).first);
      f->dump_string("name", (*sync_stat->current_syncing_snap).second);
      sync_stat->current_stat.dump(f);
      auto &dir_sync_pool = m_registered.at(dir_root)->sync_pool;
      if (dir_sync_pool) {
        dir_sync_pool->dump_stats(f);
      }
      f->close_section();
    }
    if (sync_stat->last_synced_snap) {
      f->open_object_section("last_synced_snap");
      f->dump_unsigned("id", (*sync_stat->last_synced_snap).first);
      f->dump_string("name", (*sync_stat->last_synced_snap).second);
      if (sync_stat->last_sync_duration) {
        f->dump_float("sync_duration", *sync_stat->last_sync_duration);
        f->dump_stream("sync_time_stamp") << sync_stat->last_synced;
      }
      if (sync_stat->last_sync_bytes) {
        f->dump_unsigned("sync_bytes", *sync_stat->last_sync_bytes);
      }
      sync_stat->last_stat.dump(f);
      f->close_section();
    }
    f->dump_unsigned("snaps_synced", sync_stat->synced_snap_count);
    f->dump_unsigned("snaps_deleted", sync_stat->deleted_snap_count);
    f->dump_unsigned("snaps_renamed", sync_stat->renamed_snap_count);
    f->close_section(); // dir_root
  }
  f->open_object_section("file_sync_pool");
  file_sync_pool.dump_stats(f);
  f->close_section();
  f->close_section(); // stats
}

void PeerReplayer::reopen_logs() {
  std::scoped_lock locker(m_lock);

  if (m_remote_cluster) {
    reinterpret_cast<CephContext *>(m_remote_cluster->cct())->reopen_logs();
  }
}

} // namespace mirror
} // namespace cephfs
