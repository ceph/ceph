// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_FS_MIRROR_H
#define CEPHFS_MIRROR_FS_MIRROR_H

#include "common/Formatter.h"
#include "common/Thread.h"
#include "mds/FSMap.h"
#include "Types.h"
#include "InstanceWatcher.h"
#include "MirrorWatcher.h"

class ContextWQ;

namespace cephfs {
namespace mirror {

class MirrorAdminSocketHook;
class PeerReplayer;
class ServiceDaemon;

// handle mirroring for a filesystem to a set of peers

class FSMirror {
public:
  FSMirror(CephContext *cct, const Filesystem &filesystem, uint64_t pool_id,
           ServiceDaemon *service_daemon, std::vector<const char*> args,
           ContextWQ *work_queue);
  ~FSMirror();

  void init(Context *on_finish);
  void shutdown(Context *on_finish);

  void add_peer(const Peer &peer);
  void remove_peer(const Peer &peer);

  bool is_stopping() {
    std::scoped_lock locker(m_lock);
    return m_stopping;
  }

  bool is_init_failed() {
    std::scoped_lock locker(m_lock);
    return m_init_failed;
  }

  bool is_failed() {
    std::scoped_lock locker(m_lock);
    return m_init_failed ||
           m_instance_watcher->is_failed() ||
           m_mirror_watcher->is_failed();
  }

  utime_t get_failed_ts() {
    std::scoped_lock locker(m_lock);
    if (m_instance_watcher) {
      return m_instance_watcher->get_failed_ts();
    }
    if (m_mirror_watcher) {
      return m_mirror_watcher->get_failed_ts();
    }

    return utime_t();
  }

  bool is_blocklisted() {
    std::scoped_lock locker(m_lock);
    return is_blocklisted(locker);
  }

  utime_t get_blocklisted_ts() {
    std::scoped_lock locker(m_lock);
    if (m_instance_watcher) {
      return m_instance_watcher->get_blocklisted_ts();
    }
    if (m_mirror_watcher) {
      return m_mirror_watcher->get_blocklisted_ts();
    }

    return utime_t();
  }

  Peers get_peers() {
    std::scoped_lock locker(m_lock);
    return m_all_peers;
  }

  std::string get_instance_addr() {
    std::scoped_lock locker(m_lock);
    return m_addrs;
  }

  // admin socket helpers
  void mirror_status(Formatter *f);

  void reopen_logs();

private:
  bool is_blocklisted(const std::scoped_lock<ceph::mutex> &locker) const {
    bool blocklisted = false;
    if (m_instance_watcher) {
      blocklisted = m_instance_watcher->is_blocklisted();
    }
    if (m_mirror_watcher) {
      blocklisted |= m_mirror_watcher->is_blocklisted();
    }

    return blocklisted;
  }

  struct SnapListener : public InstanceWatcher::Listener {
    FSMirror *fs_mirror;

    SnapListener(FSMirror *fs_mirror)
      : fs_mirror(fs_mirror) {
    }

    void acquire_directory(std::string_view dir_path) override {
      fs_mirror->handle_acquire_directory(dir_path);
    }

    void release_directory(std::string_view dir_path) override {
      fs_mirror->handle_release_directory(dir_path);
    }
  };

  CephContext *m_cct;
  Filesystem m_filesystem;
  uint64_t m_pool_id;
  ServiceDaemon *m_service_daemon;
  std::vector<const char *> m_args;
  ContextWQ *m_work_queue;

  ceph::mutex m_lock = ceph::make_mutex("cephfs::mirror::fs_mirror");
  SnapListener m_snap_listener;
  std::set<std::string, std::less<>> m_directories;
  Peers m_all_peers;
  std::map<Peer, std::unique_ptr<PeerReplayer>> m_peer_replayers;

  RadosRef m_cluster;
  std::string m_addrs;
  librados::IoCtx m_ioctx;
  InstanceWatcher *m_instance_watcher = nullptr;
  MirrorWatcher *m_mirror_watcher = nullptr;

  int m_retval = 0;
  bool m_stopping = false;
  bool m_init_failed = false;
  Context *m_on_init_finish = nullptr;
  Context *m_on_shutdown_finish = nullptr;

  MirrorAdminSocketHook *m_asok_hook = nullptr;

  MountRef m_mount;

  PerfCounters *m_perf_counters;

  int init_replayer(PeerReplayer *peer_replayer);
  void shutdown_replayer(PeerReplayer *peer_replayer);
  void cleanup();

  void init_instance_watcher(Context *on_finish);
  void handle_init_instance_watcher(int r);

  void init_mirror_watcher();
  void handle_init_mirror_watcher(int r);

  void shutdown_peer_replayers();

  void shutdown_mirror_watcher();
  void handle_shutdown_mirror_watcher(int r);

  void shutdown_instance_watcher();
  void handle_shutdown_instance_watcher(int r);

  void handle_acquire_directory(std::string_view dir_path);
  void handle_release_directory(std::string_view dir_path);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_FS_MIRROR_H
