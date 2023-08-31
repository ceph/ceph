// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <mutex>
#include <vector>

#include "common/ceph_context.h"
#include "common/debug.h"
#include "common/errno.h"
#include "mon/MonClient.h"

#include "ClusterWatcher.h"
#include "ServiceDaemon.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::ClusterWatcher " << __func__

namespace cephfs {
namespace mirror {

ClusterWatcher::ClusterWatcher(CephContext *cct, MonClient *monc, ServiceDaemon *service_daemon,
                               Listener &listener)
  : Dispatcher(cct),
    m_monc(monc),
    m_service_daemon(service_daemon),
    m_listener(listener) {
}

ClusterWatcher::~ClusterWatcher() {
}

bool ClusterWatcher::ms_can_fast_dispatch2(const cref_t<Message> &m) const {
  return m->get_type() == CEPH_MSG_FS_MAP;
}

void ClusterWatcher::ms_fast_dispatch2(const ref_t<Message> &m) {
  bool handled = ms_dispatch2(m);
  ceph_assert(handled);
}

bool ClusterWatcher::ms_dispatch2(const ref_t<Message> &m) {
  if (m->get_type() == CEPH_MSG_FS_MAP) {
    if (m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
      handle_fsmap(ref_cast<MFSMap>(m));
    }
    return true;
  }

  return false;
}

int ClusterWatcher::init() {
  dout(20) << dendl;

  bool sub = m_monc->sub_want("fsmap", 0, 0);
  if (!sub) {
    derr << ": failed subscribing to FSMap" << dendl;
    return -1;
  }

  m_monc->renew_subs();
  dout(10) << ": subscribed to FSMap" << dendl;
  return 0;
}

void ClusterWatcher::shutdown() {
  dout(20) << dendl;
  std::scoped_lock locker(m_lock);
  m_stopping = true;
  m_monc->sub_unwant("fsmap");
}

void ClusterWatcher::handle_fsmap(const cref_t<MFSMap> &m) {
  dout(20) << dendl;

  auto fsmap = m->get_fsmap();

  std::vector<Filesystem> mirroring_enabled;
  std::vector<Filesystem> mirroring_disabled;
  std::map<Filesystem, Peers> peers_added;
  std::map<Filesystem, Peers> peers_removed;
  std::map<Filesystem, uint64_t> fs_metadata_pools;
  {
    std::scoped_lock locker(m_lock);
    if (m_stopping) {
      return;
    }

    // deleted filesystems are considered mirroring disabled
    for (auto it = m_filesystem_peers.begin(); it != m_filesystem_peers.end();) {
      if (!fsmap.filesystem_exists(it->first.fscid)) {
        mirroring_disabled.emplace_back(it->first);
        it = m_filesystem_peers.erase(it);
        continue;
      }
      ++it;
    }

    for (auto& [fscid, _fs] : std::as_const(fsmap)) {
      auto& mds_map = _fs.get_mds_map();
      auto fs = Filesystem{fscid, std::string(mds_map.get_fs_name())};
      auto pool_id = mds_map.get_metadata_pool();
      auto& mirror_info = _fs.get_mirror_info();

      if (!mirror_info.is_mirrored()) {
        auto it = m_filesystem_peers.find(fs);
        if (it != m_filesystem_peers.end()) {
          mirroring_disabled.emplace_back(fs);
          m_filesystem_peers.erase(it);
        }
      } else {
        auto [fspeersit, enabled] = m_filesystem_peers.emplace(fs, Peers{});
        auto &peers = fspeersit->second;

        if (enabled) {
          mirroring_enabled.emplace_back(fs);
          fs_metadata_pools.emplace(fs, pool_id);
        }

        // peers added
        Peers added;
        std::set_difference(mirror_info.peers.begin(), mirror_info.peers.end(),
                            peers.begin(), peers.end(), std::inserter(added, added.end()));

        // peers removed
        Peers removed;
        std::set_difference(peers.begin(), peers.end(),
                            mirror_info.peers.begin(), mirror_info.peers.end(),
                            std::inserter(removed, removed.end()));

        // update set
        if (!added.empty()) {
          peers_added.emplace(fs, added);
          peers.insert(added.begin(), added.end());
        }
        if (!removed.empty()) {
          peers_removed.emplace(fs, removed);
          for (auto &p : removed) {
            peers.erase(p);
          }
        }
      }
    }
  }

  dout(5) << ": mirroring enabled=" << mirroring_enabled << ", mirroring_disabled="
          << mirroring_disabled << dendl;
  for (auto &fs : mirroring_enabled) {
    m_service_daemon->add_filesystem(fs.fscid, fs.fs_name);
    m_listener.handle_mirroring_enabled(FilesystemSpec(fs, fs_metadata_pools.at(fs)));
  }
  for (auto &fs : mirroring_disabled) {
    m_service_daemon->remove_filesystem(fs.fscid);
    m_listener.handle_mirroring_disabled(fs);
  }

  dout(5) << ": peers added=" << peers_added << ", peers removed=" << peers_removed << dendl;

  for (auto &[fs, peers] : peers_added) {
    for (auto &peer : peers) {
      m_service_daemon->add_peer(fs.fscid, peer);
      m_listener.handle_peers_added(fs, peer);
    }
  }
  for (auto &[fs, peers] : peers_removed) {
    for (auto &peer : peers) {
      m_service_daemon->remove_peer(fs.fscid, peer);
      m_listener.handle_peers_removed(fs, peer);
    }
  }

  std::scoped_lock locker(m_lock);
  if (!m_stopping) {
    m_monc->sub_got("fsmap", fsmap.get_epoch());
  } // else we have already done a sub_unwant()
}

} // namespace mirror
} // namespace cephfs
