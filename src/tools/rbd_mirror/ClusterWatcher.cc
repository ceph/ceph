// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ClusterWatcher.h"
#include "include/stringify.h"
#include "common/ceph_json.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/internal.h"
#include "librbd/api/Mirror.h"
#include "tools/rbd_mirror/ServiceDaemon.h"
#include "json_spirit/json_spirit.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ClusterWatcher:" << this << " " \
                           << __func__ << ": "

using std::list;
using std::map;
using std::pair;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;

namespace rbd {
namespace mirror {

ClusterWatcher::ClusterWatcher(RadosRef cluster, ceph::mutex &lock,
                               ServiceDaemon<librbd::ImageCtx>* service_daemon)
  : m_cluster(cluster), m_lock(lock), m_service_daemon(service_daemon)
{
}

const ClusterWatcher::PoolPeers& ClusterWatcher::get_pool_peers() const
{
  ceph_assert(ceph_mutex_is_locked(m_lock));
  return m_pool_peers;
}

std::string ClusterWatcher::get_site_name() const {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  return m_site_name;
}

void ClusterWatcher::refresh_pools()
{
  dout(20) << "enter" << dendl;

  PoolPeers pool_peers;
  read_pool_peers(&pool_peers);

  std::string site_name;
  int r = read_site_name(&site_name);

  std::lock_guard l{m_lock};
  m_pool_peers = pool_peers;

  if (r >= 0) {
    m_site_name = site_name;
  }

  // TODO: perhaps use a workqueue instead, once we get notifications
  // about config changes for existing pools
}

void ClusterWatcher::read_pool_peers(PoolPeers *pool_peers)
{
  int r = m_cluster->wait_for_latest_osdmap();
  if (r < 0) {
    derr << "error waiting for OSD map: " << cpp_strerror(r) << dendl;
    return;
  }

  list<pair<int64_t, string> > pools;
  r = m_cluster->pool_list2(pools);
  if (r < 0) {
    derr << "error listing pools: " << cpp_strerror(r) << dendl;
    return;
  }

  std::set<int64_t> service_pool_ids;
  for (auto& kv : pools) {
    int64_t pool_id = kv.first;
    auto& pool_name = kv.second;
    int64_t base_tier;
    r = m_cluster->pool_get_base_tier(pool_id, &base_tier);
    if (r == -ENOENT) {
      dout(10) << "pool " << pool_name << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      derr << "Error retrieving base tier for pool " << pool_name << dendl;
      continue;
    }
    if (pool_id != base_tier) {
      // pool is a cache; skip it
      continue;
    }

    IoCtx ioctx;
    r = m_cluster->ioctx_create2(pool_id, ioctx);
    if (r == -ENOENT) {
      dout(10) << "pool " << pool_id << " no longer exists" << dendl;
      continue;
    } else if (r < 0) {
      derr << "Error accessing pool " << pool_name << cpp_strerror(r) << dendl;
      continue;
    }

    cls::rbd::MirrorMode mirror_mode_internal;
    r = librbd::cls_client::mirror_mode_get(&ioctx, &mirror_mode_internal);
    if (r == 0 && mirror_mode_internal == cls::rbd::MIRROR_MODE_DISABLED) {
      dout(10) << "mirroring is disabled for pool " << pool_name << dendl;
      continue;
    }

    service_pool_ids.insert(pool_id);
    if (m_service_pools.find(pool_id) == m_service_pools.end()) {
      m_service_pools[pool_id] = {};
      m_service_daemon->add_pool(pool_id, pool_name);
    }

    if (r == -EPERM) {
      dout(10) << "access denied querying pool " << pool_name << dendl;
      m_service_pools[pool_id] = m_service_daemon->add_or_update_callout(
        pool_id, m_service_pools[pool_id],
        service_daemon::CALLOUT_LEVEL_WARNING, "access denied");
      continue;
    } else if (r < 0) {
      derr << "could not tell whether mirroring was enabled for " << pool_name
	   << " : " << cpp_strerror(r) << dendl;
      m_service_pools[pool_id] = m_service_daemon->add_or_update_callout(
        pool_id, m_service_pools[pool_id],
        service_daemon::CALLOUT_LEVEL_WARNING, "mirroring mode query failed");
      continue;
    }

    vector<librbd::mirror_peer_site_t> configs;
    r = librbd::api::Mirror<>::peer_site_list(ioctx, &configs);
    if (r < 0) {
      derr << "error reading mirroring config for pool " << pool_name
	   << cpp_strerror(r) << dendl;
      m_service_pools[pool_id] = m_service_daemon->add_or_update_callout(
        pool_id, m_service_pools[pool_id],
        service_daemon::CALLOUT_LEVEL_ERROR, "mirroring peer list failed");
      continue;
    }

    std::vector<PeerSpec> peers;
    peers.reserve(configs.size());
    for (auto& peer : configs) {
      if (peer.direction != RBD_MIRROR_PEER_DIRECTION_TX) {
        peers.push_back(peer);
      }
    }

    for (auto& peer : peers) {
      r = resolve_peer_site_config_keys(pool_id, pool_name, &peer);
      if (r < 0) {
        break;
      }
    }

    if (m_service_pools[pool_id] != service_daemon::CALLOUT_ID_NONE) {
      m_service_daemon->remove_callout(pool_id, m_service_pools[pool_id]);
      m_service_pools[pool_id] = service_daemon::CALLOUT_ID_NONE;
    }

    pool_peers->emplace(pool_id, Peers{peers.begin(), peers.end()});
  }

  for (auto it = m_service_pools.begin(); it != m_service_pools.end(); ) {
    auto current_it(it++);
    if (service_pool_ids.find(current_it->first) == service_pool_ids.end()) {
      m_service_daemon->remove_pool(current_it->first);
      m_service_pools.erase(current_it->first);
    }
  }
}

int ClusterWatcher::read_site_name(std::string* site_name) {
  librbd::RBD rbd;
  int r = rbd.mirror_site_name_get(*m_cluster, site_name);
  dout(10) << "site_name=" << *site_name << ", r=" << r << dendl;
  return r;
}

int ClusterWatcher::resolve_peer_site_config_keys(int64_t pool_id,
                                                  const std::string& pool_name,
                                                  PeerSpec* peer) {
  dout(10) << "retrieving config-key: pool_id=" << pool_id << ", "
           << "pool_name=" << pool_name << ", "
           << "peer_uuid=" << peer->uuid << dendl;

  std::string cmd =
    "{"
      "\"prefix\": \"config-key get\", "
      "\"key\": \"" RBD_MIRROR_PEER_CONFIG_KEY_PREFIX + stringify(pool_id) +
        "/" + peer->uuid + "\""
    "}";

  bufferlist in_bl;
  bufferlist out_bl;
  int r = m_cluster->mon_command(cmd, in_bl, &out_bl, nullptr);
  if (r == -ENOENT || out_bl.length() == 0) {
    return 0;
  } else if (r < 0) {
    derr << "error reading mirroring peer config for pool " << pool_name << ": "
         << cpp_strerror(r) << dendl;
    m_service_pools[pool_id] = m_service_daemon->add_or_update_callout(
      pool_id, m_service_pools[pool_id],
      service_daemon::CALLOUT_LEVEL_WARNING,
      "mirroring peer config-key query failed");
    return r;
  }

  bool json_valid = false;
  json_spirit::mValue json_root;
  if(json_spirit::read(out_bl.to_str(), json_root)) {
    try {
      auto& json_obj = json_root.get_obj();
      if (json_obj.count("mon_host")) {
        peer->mon_host = json_obj["mon_host"].get_str();
      }
      if (json_obj.count("key")) {
        peer->key = json_obj["key"].get_str();
      }
      json_valid = true;
    } catch (std::runtime_error&) {
    }
  }

  if (!json_valid) {
    derr << "error parsing mirroring peer config for pool " << pool_name << ", "
         << "peer " << peer->uuid << dendl;
    m_service_pools[pool_id] = m_service_daemon->add_or_update_callout(
      pool_id, m_service_pools[pool_id],
      service_daemon::CALLOUT_LEVEL_WARNING,
      "mirroring peer config-key decode failed");
  }

  return 0;
}

} // namespace mirror
} // namespace rbd
