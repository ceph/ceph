// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_META_CACHE_H
#define CEPH_RBD_MIRROR_POOL_META_CACHE_H

#include "include/int_types.h"
#include "common/ceph_mutex.h"
#include "tools/rbd_mirror/Types.h"
#include <map>

namespace rbd {
namespace mirror {

class PoolMetaCache {
public:
  PoolMetaCache(CephContext* cct)
    : m_cct(cct) {
  }
  PoolMetaCache(const PoolMetaCache&) = delete;
  PoolMetaCache& operator=(const PoolMetaCache&) = delete;

  int get_local_pool_meta(int64_t pool_id,
                          LocalPoolMeta* local_pool_meta) const;
  void set_local_pool_meta(int64_t pool_id,
                           const LocalPoolMeta& local_pool_meta);
  void remove_local_pool_meta(int64_t pool_id);

  int get_remote_pool_meta(int64_t pool_id,
                           RemotePoolMeta* remote_pool_meta) const;
  void set_remote_pool_meta(int64_t pool_id,
                            const RemotePoolMeta& remote_pool_meta);
  void remove_remote_pool_meta(int64_t pool_id);

private:
  CephContext* m_cct;

  mutable ceph::shared_mutex m_lock =
    ceph::make_shared_mutex("rbd::mirror::PoolMetaCache::m_lock");
  std::map<int64_t, LocalPoolMeta> m_local_pool_metas;
  std::map<int64_t, RemotePoolMeta> m_remote_pool_metas;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_POOL_META_CACHE_H
