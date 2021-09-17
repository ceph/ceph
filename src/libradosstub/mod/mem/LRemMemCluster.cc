// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemMemCluster.h"
#include "LRemMemRadosClient.h"

namespace librados {

LRemMemCluster::File::File()
  : objver(0), snap_id(), exists(true) {
}

LRemMemCluster::File::File(const File &rhs)
  : data(rhs.data),
    mtime(rhs.mtime),
    objver(rhs.objver),
    snap_id(rhs.snap_id),
    exists(rhs.exists) {
}

LRemMemCluster::Pool::Pool() = default;

LRemMemCluster::LRemMemCluster()
  : m_next_nonce(static_cast<uint32_t>(reinterpret_cast<uint64_t>(this))) {
}

LRemMemCluster::~LRemMemCluster() {
  for (auto pool_pair : m_pools) {
    pool_pair.second->put();
  }
}

LRemRadosClient *LRemMemCluster::create_rados_client(CephContext *cct) {
  return new LRemMemRadosClient(cct, this);
}

int LRemMemCluster::register_object_handler(LRemRadosClient *rados_client,
                                            int64_t pool_id,
                                            const ObjectLocator& locator,
                                            ObjectHandler* object_handler) {
  std::lock_guard locker{m_lock};
  auto pool = get_pool(m_lock, pool_id);
  if (pool == nullptr) {
    return -ENOENT;
  }

  std::unique_lock pool_locker{pool->file_lock};
  auto file_it = pool->files.find(locator);
  if (file_it == pool->files.end()) {
    return -ENOENT;
  }

  auto& object_handlers = pool->file_handlers[locator];
  auto it = object_handlers.find(object_handler);
  ceph_assert(it == object_handlers.end());

  object_handlers.insert(object_handler);
  return 0;
}

void LRemMemCluster::unregister_object_handler(LRemRadosClient *rados_client,
                                               int64_t pool_id,
                                               const ObjectLocator& locator,
                                               ObjectHandler* object_handler) {
  std::lock_guard locker{m_lock};
  auto pool = get_pool(m_lock, pool_id);
  if (pool == nullptr) {
    return;
  }

  std::unique_lock pool_locker{pool->file_lock};
  auto handlers_it = pool->file_handlers.find(locator);
  if (handlers_it == pool->file_handlers.end()) {
    return;
  }

  auto& object_handlers = handlers_it->second;
  object_handlers.erase(object_handler);
}

int LRemMemCluster::pool_create(const std::string &pool_name) {
  std::lock_guard locker{m_lock};
  if (m_pools.find(pool_name) != m_pools.end()) {
    return -EEXIST;
  }
  Pool *pool = new Pool();
  pool->pool_id = ++m_pool_id;
  m_pools[pool_name] = pool;
  return 0;
}

int LRemMemCluster::pool_delete(const std::string &pool_name) {
  std::lock_guard locker{m_lock};
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  iter->second->put();
  m_pools.erase(iter);
  return 0;
}

int LRemMemCluster::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int LRemMemCluster::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  std::lock_guard locker{m_lock};
  v.clear();
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    v.push_back(std::make_pair(iter->second->pool_id, iter->first));
  }
  return 0;
}

int64_t LRemMemCluster::pool_lookup(const std::string &pool_name) {
  std::lock_guard locker{m_lock};
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  return iter->second->pool_id;
}

int LRemMemCluster::pool_reverse_lookup(int64_t id, std::string *name) {
  std::lock_guard locker{m_lock};
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    if (iter->second->pool_id == id) {
      *name = iter->first;
      return 0;
    }
  }
  return -ENOENT;
}

LRemMemCluster::Pool *LRemMemCluster::get_pool(int64_t pool_id) {
  std::lock_guard locker{m_lock};
  return get_pool(m_lock, pool_id);
}

LRemMemCluster::Pool *LRemMemCluster::get_pool(const ceph::mutex& lock,
                                               int64_t pool_id) {
  for (auto &pool_pair : m_pools) {
    if (pool_pair.second->pool_id == pool_id) {
      return pool_pair.second;
    }
  }
  return nullptr;
}

LRemMemCluster::Pool *LRemMemCluster::get_pool(const std::string &pool_name) {
  std::lock_guard locker{m_lock};
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter != m_pools.end()) {
    return iter->second;
  }
  return nullptr;
}

void LRemMemCluster::allocate_client(uint32_t *nonce, uint64_t *global_id) {
  std::lock_guard locker{m_lock};
  *nonce = m_next_nonce++;
  *global_id = m_next_global_id++;
}

void LRemMemCluster::deallocate_client(uint32_t nonce) {
  std::lock_guard locker{m_lock};
  m_blocklist.erase(nonce);
}

bool LRemMemCluster::is_blocklisted(uint32_t nonce) const {
  std::lock_guard locker{m_lock};
  return (m_blocklist.find(nonce) != m_blocklist.end());
}

void LRemMemCluster::blocklist(LRemRadosClient *rados_client, uint32_t nonce) {
  m_watch_notify.blocklist(rados_client, nonce);

  std::lock_guard locker{m_lock};
  m_blocklist.insert(nonce);
}

void LRemMemCluster::transaction_start(LRemTransactionStateRef& ref) {
  const auto& locator = ref->locator;
  std::unique_lock locker{m_lock};
  m_transaction_cond.wait(locker, [&locator, this] {
    return m_transactions.count(locator) == 0;
  });
  auto result = m_transactions.insert(locator);
  ceph_assert(result.second);
}

void LRemMemCluster::transaction_finish(LRemTransactionStateRef& ref) {
  const auto& locator = ref->locator;
  std::lock_guard locker{m_lock};
  size_t count = m_transactions.erase(locator);
  ceph_assert(count == 1);
  m_transaction_cond.notify_all();
}

} // namespace librados

