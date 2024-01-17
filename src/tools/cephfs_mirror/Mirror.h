// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_H
#define CEPHFS_MIRROR_H

#include <map>
#include <set>
#include <vector>

#include "common/ceph_mutex.h"
#include "common/WorkQueue.h"
#include "mds/FSMap.h"
#include "ClusterWatcher.h"
#include "FSMirror.h"
#include "ServiceDaemon.h"
#include "Types.h"

class Messenger;
class MonClient;
class ContextWQ;

namespace cephfs {
namespace mirror {

// this wraps up ClusterWatcher and FSMirrors to implement mirroring
// for ceph filesystems.

class Mirror {
public:
  Mirror(CephContext *cct, const std::vector<const char*> &args,
         MonClient *monc, Messenger *msgr);
  ~Mirror();

  int init(std::string &reason);
  void shutdown();
  void run();

  void handle_signal(int signum);

private:
  static constexpr std::string_view MIRRORING_MODULE = "mirroring";

  struct C_EnableMirroring;
  struct C_DisableMirroring;
  struct C_PeerUpdate;
  struct C_RestartMirroring;

  struct ClusterListener : ClusterWatcher::Listener {
    Mirror *mirror;

    ClusterListener(Mirror *mirror)
      : mirror(mirror) {
    }

    void handle_mirroring_enabled(const FilesystemSpec &spec) override {
      mirror->mirroring_enabled(spec.filesystem, spec.pool_id);
    }

    void handle_mirroring_disabled(const Filesystem &filesystem) override {
      mirror->mirroring_disabled(filesystem);
    }

    void handle_peers_added(const Filesystem &filesystem, const Peer &peer) override {
      mirror->peer_added(filesystem, peer);
    }

    void handle_peers_removed(const Filesystem &filesystem, const Peer &peer) override {
      mirror->peer_removed(filesystem, peer);
    }
  };

  struct MirrorAction {
    MirrorAction(uint64_t pool_id) :
      pool_id(pool_id) {
    }

    uint64_t pool_id; // for restarting blocklisted mirror instance
    bool action_in_progress = false;
    bool restarting = false;
    std::list<Context *> action_ctxs;
    std::unique_ptr<FSMirror> fs_mirror;
  };

  ceph::mutex m_lock = ceph::make_mutex("cephfs::mirror::Mirror");
  ceph::condition_variable m_cond;

  CephContext *m_cct;
  std::vector<const char *> m_args;
  MonClient *m_monc;
  Messenger *m_msgr;
  ClusterListener m_listener;

  ThreadPool *m_thread_pool = nullptr;
  ContextWQ *m_work_queue = nullptr;
  SafeTimer *m_timer = nullptr;
  ceph::mutex *m_timer_lock = nullptr;
  Context *m_timer_task = nullptr;

  bool m_stopping = false;
  std::unique_ptr<ClusterWatcher> m_cluster_watcher;
  std::map<Filesystem, MirrorAction> m_mirror_actions;

  RadosRef m_local;
  std::unique_ptr<ServiceDaemon> m_service_daemon;

  int init_mon_client();

  // called via listener
  void mirroring_enabled(const Filesystem &filesystem, uint64_t local_pool_id);
  void mirroring_disabled(const Filesystem &filesystem);
  void peer_added(const Filesystem &filesystem, const Peer &peer);
  void peer_removed(const Filesystem &filesystem, const Peer &peer);

  // mirror enable callback
  void enable_mirroring(const Filesystem &filesystem, uint64_t local_pool_id,
                        Context *on_finish, bool is_restart=false);
  void handle_enable_mirroring(const Filesystem &filesystem, int r);
  void handle_enable_mirroring(const Filesystem &filesystem, const Peers &peers, int r);

  // mirror disable callback
  void disable_mirroring(const Filesystem &filesystem, Context *on_finish);
  void handle_disable_mirroring(const Filesystem &filesystem, int r);

  // peer update callback
  void add_peer(const Filesystem &filesystem, const Peer &peer);
  void remove_peer(const Filesystem &filesystem, const Peer &peer);

  void schedule_mirror_update_task();
  void update_fs_mirrors();

  void reopen_logs();

  void _set_restarting(const Filesystem &filesystem) {
    auto &mirror_action = m_mirror_actions.at(filesystem);
    mirror_action.restarting = true;
  }

  void _unset_restarting(const Filesystem &filesystem) {
    auto &mirror_action = m_mirror_actions.at(filesystem);
    mirror_action.restarting = false;
  }

  bool _is_restarting(const Filesystem &filesystem) {
    auto &mirror_action = m_mirror_actions.at(filesystem);
    return mirror_action.restarting;
  }

};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_H
