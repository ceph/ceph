// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemDBCluster.h"
#include "LRemDBRadosClient.h"

#define dout_subsys ceph_subsys_rados
#undef dout_prefix
#define dout_prefix *_dout << "LRemDBCluster: " << this << " " << __func__ \
                           << ": "

namespace librados {

LRemDBCluster::File::File() {
}

LRemDBCluster::File::File(const File &rhs) {
}

LRemDBCluster::Pool::Pool() = default;

LRemDBCluster::LRemDBCluster(CephContext *_cct)
  : cct(_cct),
    m_next_nonce(static_cast<uint32_t>(reinterpret_cast<uint64_t>(this))) {
}

LRemDBCluster::~LRemDBCluster() {
}

LRemRadosClient *LRemDBCluster::create_rados_client(CephContext *cct) {
  return new LRemDBRadosClient(cct, this);
}

int LRemDBCluster::register_object_handler(LRemRadosClient *client,
                                           int64_t pool_id,
                                           const ObjectLocator& locator,
                                           ObjectHandler* object_handler) {
#if 0
  auto uuid = client->cct()->_conf.get_val<uuid_d>("fsid");
  auto dbc = std::make_shared<LRemDBStore::Cluster>(uuid.to_string());

  string pool_name;

  int r = pool_reverse_lookup(pool_id, &pool_name);
  if (r < 0) {
    return r;
  }

  LRemDBCluster::PoolRef pool;
  {
    std::lock_guard locker{m_lock};

    pool = get_pool(*dbc, pool_id);
    if (!pool) {
      return -ENOENT;
    }
  }

  LRemDBStore::PoolRef pool_store;

  r = dbc->get_pool(pool_name, &pool_store);
  if (r < 0) {
    return r;
  }

  auto obj = pool_store->get_obj_handler(locator.nspace, locator.name);
  LRemDBStore::Obj::Meta meta;

  r = obj->read_meta(&meta);
  if (r < 0) {
    return r;
  }
#endif

  std::lock_guard locker{m_lock};
  auto pool = get_cached_pool(m_lock, pool_id);
  if (pool == nullptr) {
    return -ENOENT;
  }

  std::unique_lock pool_locker{pool->file_lock};
#if 0
  auto file_it = pool->files.find(locator);
  if (file_it == pool->files.end()) {
    return -ENOENT;
  }
#endif

  auto& object_handlers = pool->file_handlers[locator];
  auto it = object_handlers.find(object_handler);
  ceph_assert(it == object_handlers.end());

  object_handlers.insert(object_handler);
  return 0;
}

void LRemDBCluster::unregister_object_handler(LRemRadosClient *client,
                                              int64_t pool_id,
                                              const ObjectLocator& locator,
                                              ObjectHandler* object_handler) {
  std::lock_guard locker{m_lock};
  auto pool = get_cached_pool(m_lock, pool_id);
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

LRemDBCluster::PoolRef LRemDBCluster::make_pool(const string& name, int id) {
  auto pool = std::make_shared<Pool>();
  pool->pool_id = id;
  m_pools[name] = pool;

  return pool;
}

int LRemDBCluster::init(LRemDBStore::Cluster& dbc) {
  map<string, LRemDBStore::PoolRef> pools;

  int r = dbc.list_pools(&pools);
  if (r < 0) {
    return r;
  }

  for (auto& iter : pools) {
    auto& name = iter.first;
    auto& pool_info = *iter.second;

    make_pool(name, pool_info.get_id());
  }

  return 0;
}

int LRemDBCluster::pool_create(LRemDBStore::Cluster& dbc,
                               const std::string &pool_name) {
  if (pool_name.empty()) {
    return -EINVAL;
  }

  std::lock_guard locker{m_lock};

  LRemDBTransactionState dbtrans(cct);

  int r = dbtrans.dbo->exec("PRAGMA journal_mode = wal;");
  if (r < 0) {
    return r;
  }

#if 0
  LRemDBOps::Transaction transaction(dbtrans.dbo->new_transaction());
#endif
  auto pool = get_pool(m_lock, dbc, pool_name);
  if (pool) {
    return -EEXIST;
  }

  string v;
  r = dbc.create_pool(pool_name, v);
  if (r < 0) {
    return r;
  }
#if 0
  transaction.complete_op(r);
#endif
  dbtrans.db_trans->complete_op(r);

  make_pool(pool_name, r);

  return r;
}

int LRemDBCluster::pool_delete(LRemDBStore::Cluster& dbc,
                               const std::string &pool_name) {
  std::lock_guard locker{m_lock};
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  m_pools.erase(iter);
#warning FIXME --  remove pool from store
  return 0;
}

int LRemDBCluster::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int LRemDBCluster::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  std::lock_guard locker{m_lock};
  v.clear();
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    v.push_back(std::make_pair(iter->second->pool_id, iter->first));
  }
  return 0;
}

int64_t LRemDBCluster::pool_lookup(const std::string &pool_name) {
  std::lock_guard locker{m_lock};
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  return iter->second->pool_id;
}

int LRemDBCluster::pool_reverse_lookup(int64_t id, std::string *name) {
  std::lock_guard locker{m_lock};
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    if (iter->second->pool_id == id) {
      *name = iter->first;
      return 0;
    }
  }
  return -ENOENT;
}

LRemDBCluster::PoolRef LRemDBCluster::get_pool(LRemDBStore::Cluster& dbc,
                                               int64_t pool_id) {
  std::lock_guard locker{m_lock};
  return get_pool(m_lock, dbc, pool_id);
}

LRemDBCluster::PoolRef LRemDBCluster::get_pool(const ceph::mutex& lock,
                                               LRemDBStore::Cluster& dbc,
                                               int64_t pool_id) {
  for (auto &pool_pair : m_pools) {
    if (pool_pair.second->pool_id == pool_id) {
      return pool_pair.second;
    }
  }

  LRemDBStore::PoolRef pool;
  int r = dbc.get_pool(pool_id, &pool);
  if (r < 0) {
    return nullptr;
  }

  return make_pool(pool->get_name(), pool->get_id());
}

LRemDBCluster::PoolRef LRemDBCluster::get_cached_pool(const ceph::mutex& lock,
                                                      int64_t pool_id) {
  for (auto &pool_pair : m_pools) {
    if (pool_pair.second->pool_id == pool_id) {
      return pool_pair.second;
    }
  }

  return nullptr;
}

LRemDBCluster::PoolRef LRemDBCluster::get_pool(LRemDBStore::Cluster& dbc,
                                               const std::string &pool_name) {
  std::lock_guard locker{m_lock};
  return get_pool(m_lock, dbc, pool_name);
}

LRemDBCluster::PoolRef LRemDBCluster::get_pool(const ceph::mutex& lock,
                                               LRemDBStore::Cluster& dbc,
                                               const std::string &pool_name) {
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter != m_pools.end()) {
    return iter->second;
  }

  LRemDBStore::PoolRef pool;
  int r = dbc.get_pool(pool_name, &pool);
  if (r < 0) {
    return nullptr;
  }

  return make_pool(pool_name, pool->get_id());
}

void LRemDBCluster::allocate_client(uint32_t *nonce, uint64_t *global_id) {
  std::lock_guard locker{m_lock};
#warning move global_id, nonce into pool storage
  *nonce = m_next_nonce++;
  *global_id = m_next_global_id++;
}

void LRemDBCluster::deallocate_client(uint32_t nonce) {
  std::lock_guard locker{m_lock};
  m_blocklist.erase(nonce);
}

bool LRemDBCluster::is_blocklisted(uint32_t nonce) const {
  std::lock_guard locker{m_lock};
  return (m_blocklist.find(nonce) != m_blocklist.end());
}

void LRemDBCluster::blocklist(LRemRadosClient *rados_client, uint32_t nonce) {
  m_watch_notify.blocklist(rados_client, nonce);

  std::lock_guard locker{m_lock};
  m_blocklist.insert(nonce);
}

void LRemDBCluster::transaction_start(LRemTransactionStateRef& ref) {
  const auto& locator = ref->locator;
  std::unique_lock locker{m_lock};
  m_transaction_cond.wait(locker, [&locator, this] {
    return m_transactions.count(locator) == 0;
  });
  auto result = m_transactions.insert(locator);
  ceph_assert(result.second);
}

void LRemDBCluster::transaction_finish(LRemTransactionStateRef& ref) {
  const auto& locator = ref->locator;
  std::lock_guard locker{m_lock};
  size_t count = m_transactions.erase(locator);
  ceph_assert(count == 1);
  m_transaction_cond.notify_all();
}

} // namespace librados

