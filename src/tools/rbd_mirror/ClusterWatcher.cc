// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "librbd/internal.h"

#include "ClusterWatcher.h"

#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ClusterWatcher:" << this << " " \
                           << __func__ << ": "

using std::list;
using std::map;
using std::set;
using std::string;
using std::unique_ptr;
using std::vector;

using librados::Rados;
using librados::IoCtx;

namespace rbd {
namespace mirror {

ClusterWatcher::ClusterWatcher(RadosRef cluster, Mutex &lock) :
  m_lock(lock),
  m_cluster(cluster)
{
}

const ClusterWatcher::PoolPeers& ClusterWatcher::get_pool_peers() const
{
  assert(m_lock.is_locked());
  return m_pool_peers;
}

const ClusterWatcher::PoolNames& ClusterWatcher::get_pool_names() const
{
  assert(m_lock.is_locked());
  return m_pool_names;
}

void ClusterWatcher::refresh_pools()
{
  dout(20) << "enter" << dendl;

  PoolPeers pool_peers;
  PoolNames pool_names;
  read_pool_peers(&pool_peers, &pool_names);

  Mutex::Locker l(m_lock);
  m_pool_peers = pool_peers;
  m_pool_names = pool_names;
  // TODO: perhaps use a workqueue instead, once we get notifications
  // about config changes for existing pools
}

void ClusterWatcher::read_pool_peers(PoolPeers *pool_peers,
				     PoolNames *pool_names)
{
  list<pair<int64_t, string> > pools;
  int r = m_cluster->pool_list2(pools);
  if (r < 0) {
    derr << "error listing pools: " << cpp_strerror(r) << dendl;
    return;
  }

  for (auto kv : pools) {
    int64_t pool_id = kv.first;
    string pool_name = kv.second;
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

    rbd_mirror_mode_t mirror_mode;
    r = librbd::mirror_mode_get(ioctx, &mirror_mode);
    if (r < 0) {
      derr << "could not tell whether mirroring was enabled for " << pool_name
	   << " : " << cpp_strerror(r) << dendl;
      continue;
    }
    if (mirror_mode == RBD_MIRROR_MODE_DISABLED) {
      dout(10) << "mirroring is disabled for pool " << pool_name << dendl;
      continue;
    }

    vector<librbd::mirror_peer_t> configs;
    r = librbd::mirror_peer_list(ioctx, &configs);
    if (r < 0) {
      derr << "error reading mirroring config for pool " << pool_name
	   << cpp_strerror(r) << dendl;
      continue;
    }

    pool_peers->insert({pool_id, Peers{configs.begin(), configs.end()}});
    pool_names->insert(pool_name);
  }
}

} // namespace mirror
} // namespace rbd
