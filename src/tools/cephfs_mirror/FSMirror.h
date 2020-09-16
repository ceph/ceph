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

// handle mirroring for a filesystem to a set of peers

class FSMirror {
public:
  FSMirror(CephContext *cct, std::string_view fs_name, uint64_t pool_id,
           std::vector<const char*> args, ContextWQ *work_queue);
  ~FSMirror();

  void init(Context *on_finish);
  void shutdown(Context *on_finish);

  void add_peer(const Peer &peer);
  void remove_peer(const Peer &peer);

  bool is_stopping() {
    std::scoped_lock locker(m_lock);
    return m_stopping;
  }

  bool is_failed() {
    std::scoped_lock locker(m_lock);
    return m_init_failed;
  }

  // admin socket helpers
  void mirror_status(Formatter *f);

private:
  struct SnapListener : public InstanceWatcher::Listener {
    FSMirror *fs_mirror;

    SnapListener(FSMirror *fs_mirror)
      : fs_mirror(fs_mirror) {
    }

    void acquire_directory(string_view dir_path) override {
      fs_mirror->handle_acquire_directory(dir_path);
    }

    void release_directory(string_view dir_path) override {
      fs_mirror->handle_release_directory(dir_path);
    }
  };

  struct PeerReplayer;
  class SnapshotReplayerThread : public Thread {
  public:
    SnapshotReplayerThread(FSMirror *fs_mirror, PeerReplayer *peer_replayer)
      : m_fs_mirror(fs_mirror),
        m_peer_replayer(peer_replayer) {
    }

    void *entry() override {
      m_fs_mirror->run(m_peer_replayer);
      return 0;
    }

  private:
    FSMirror *m_fs_mirror;
    PeerReplayer *m_peer_replayer;
  };

  typedef std::vector<std::unique_ptr<SnapshotReplayerThread>> SnapshotReplayers;
  struct PeerReplayer {
    SnapshotReplayers replayers;
    bool stopping = false;

    bool is_stopping() {
      return stopping;
    }
  };

  std::string m_fs_name;
  uint64_t m_pool_id;
  std::vector<const char *> m_args;
  ContextWQ *m_work_queue;

  ceph::mutex m_lock = ceph::make_mutex("cephfs::mirror::fs_mirror");
  ceph::condition_variable m_cond;
  SnapListener m_snap_listener;
  std::set<std::string, std::less<>> m_directories;
  std::map<Peer, PeerReplayer> m_peer_replayers;

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

  void run(PeerReplayer *peer_replayer);
  void init_replayers(PeerReplayer *peer_replayer);
  void shutdown_replayers(PeerReplayer *peer_replayer,
                          std::unique_lock<ceph::mutex> &locker);

  int connect(std::string_view cluster_name, std::string_view client_name,
              RadosRef *cluster);

  void init_instance_watcher(Context *on_finish);
  void handle_init_instance_watcher(int r);

  void init_mirror_watcher();
  void handle_init_mirror_watcher(int r);

  void shutdown_mirror_watcher();
  void handle_shutdown_mirror_watcher(int r);

  void shutdown_instance_watcher();
  void handle_shutdown_instance_watcher(int r);

  void handle_acquire_directory(string_view dir_path);
  void handle_release_directory(string_view dir_path);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_FS_MIRROR_H
