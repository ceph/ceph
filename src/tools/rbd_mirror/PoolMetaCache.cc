// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/dout.h"
#include "tools/rbd_mirror/PoolMetaCache.h"
#include <shared_mutex>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::PoolMetaCache: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

int PoolMetaCache::get_local_pool_meta(
    int64_t pool_id,
    LocalPoolMeta* local_pool_meta) const {
  dout(15) << "pool_id=" << pool_id << dendl;

  std::shared_lock locker{m_lock};
  auto it = m_local_pool_metas.find(pool_id);
  if (it == m_local_pool_metas.end()) {
    return -ENOENT;
  }

  *local_pool_meta = it->second;
  return 0;
}

void PoolMetaCache::set_local_pool_meta(
    int64_t pool_id,
    const LocalPoolMeta& local_pool_meta) {
  dout(15) << "pool_id=" << pool_id << ", "
           << "local_pool_meta=" << local_pool_meta << dendl;

  std::unique_lock locker(m_lock);
  m_local_pool_metas[pool_id] = local_pool_meta;
}

void PoolMetaCache::remove_local_pool_meta(int64_t pool_id) {
  dout(15) << "pool_id=" << pool_id << dendl;

  std::unique_lock locker(m_lock);
  m_local_pool_metas.erase(pool_id);
}

int PoolMetaCache::get_remote_pool_meta(
    int64_t pool_id,
    RemotePoolMeta* remote_pool_meta) const {
  dout(15) << "pool_id=" << pool_id << dendl;

  std::shared_lock locker{m_lock};
  auto it = m_remote_pool_metas.find(pool_id);
  if (it == m_remote_pool_metas.end()) {
    return -ENOENT;
  }

  *remote_pool_meta = it->second;
  return 0;
}

void PoolMetaCache::set_remote_pool_meta(
    int64_t pool_id,
    const RemotePoolMeta& remote_pool_meta) {
  dout(15) << "pool_id=" << pool_id << ", "
           << "remote_pool_meta=" << remote_pool_meta << dendl;

  std::unique_lock locker(m_lock);
  m_remote_pool_metas[pool_id] = remote_pool_meta;
}

void PoolMetaCache::remove_remote_pool_meta(int64_t pool_id) {
  dout(15) << "pool_id=" << pool_id << dendl;

  std::unique_lock locker(m_lock);
  m_remote_pool_metas.erase(pool_id);
}

} // namespace mirror
} // namespace rbd
