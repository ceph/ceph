// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_H
#define CEPHFS_MIRROR_H

#include <map>
#include <set>
#include <vector>

#include "common/ceph_mutex.h"
#include "mds/FSMap.h"
#include "ClusterWatcher.h"
#include "FSMirror.h"
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

  struct ClusterListener : ClusterWatcher::Listener {
    Mirror *mirror;

    ClusterListener(Mirror *mirror)
      : mirror(mirror) {
    }

    void handle_mirroring_enabled(const FilesystemSpec &spec) override {
      mirror->mirroring_enabled(spec.fs_name, spec.pool_id);
    }

    void handle_mirroring_disabled(const std::string &fs_name) override {
      mirror->mirroring_disabled(fs_name);
    }

    void handle_peers_added(const std::string &fs_name, const Peer &peer) override {
      mirror->peer_added(fs_name, peer);
    }

    void handle_peers_removed(const std::string &fs_name, const Peer &peer) override {
      mirror->peer_removed(fs_name, peer);
    }
  };

  ceph::mutex m_lock = ceph::make_mutex("cephfs::mirror::Mirror");
  ceph::condition_variable m_cond;

  CephContext *m_cct;
  std::vector<const char *> m_args;
  MonClient *m_monc;
  Messenger *m_msgr;
  ClusterListener m_listener;

  ContextWQ *m_work_queue = nullptr;
  SafeTimer *m_timer = nullptr;

  bool m_stopping = false;
  bool m_stopped = false;
  std::unique_ptr<ClusterWatcher> m_cluster_watcher;
  std::map<std::string, std::unique_ptr<FSMirror>> m_fs_mirrors;

  int init_mon_client();

  void handle_mirroring_enabled(const std::string &fs_name, int r);
  void mirroring_enabled(const std::string &fs_name, uint64_t local_pool_id);
  void mirroring_disabled(const std::string &fs_name);

  void peer_added(const std::string &fs_name, const Peer &peer);
  void peer_removed(const std::string &fs_name, const Peer &peer);

  void handle_shutdown(const std::string &fs_name, int r);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_H
