// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestMemCluster.h"
#include "test/librados_test_stub/TestMemRadosClient.h"

namespace librados {

TestMemCluster::File::File()
  : snap_id(), exists(true), lock("TestMemCluster::File::lock") {
}

TestMemCluster::File::File(const File &rhs)
  : data(rhs.data),
    mtime(rhs.mtime),
    snap_id(rhs.snap_id),
    exists(rhs.exists),
    lock("TestMemCluster::File::lock") {
}

TestMemCluster::Pool::Pool()
  : file_lock("TestMemCluster::Pool::file_lock") {
}

TestMemCluster::TestMemCluster()
  : m_lock("TestMemCluster::m_lock"),
    m_next_nonce(static_cast<uint32_t>(reinterpret_cast<uint64_t>(this))) {
}

TestMemCluster::~TestMemCluster() {
  for (auto pool_pair : m_pools) {
    pool_pair.second->put();
  }
}

TestRadosClient *TestMemCluster::create_rados_client(CephContext *cct) {
  return new TestMemRadosClient(cct, this);
}

int TestMemCluster::pool_create(const std::string &pool_name) {
  Mutex::Locker locker(m_lock);
  if (m_pools.find(pool_name) != m_pools.end()) {
    return -EEXIST;
  }
  Pool *pool = new Pool();
  pool->pool_id = ++m_pool_id;
  m_pools[pool_name] = pool;
  return 0;
}

int TestMemCluster::pool_delete(const std::string &pool_name) {
  Mutex::Locker locker(m_lock);
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  iter->second->put();
  m_pools.erase(iter);
  return 0;
}

int TestMemCluster::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int TestMemCluster::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  Mutex::Locker locker(m_lock);
  v.clear();
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    v.push_back(std::make_pair(iter->second->pool_id, iter->first));
  }
  return 0;
}

int64_t TestMemCluster::pool_lookup(const std::string &pool_name) {
  Mutex::Locker locker(m_lock);
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  return iter->second->pool_id;
}

int TestMemCluster::pool_reverse_lookup(int64_t id, std::string *name) {
  Mutex::Locker locker(m_lock);
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    if (iter->second->pool_id == id) {
      *name = iter->first;
      return 0;
    }
  }
  return -ENOENT;
}

TestMemCluster::Pool *TestMemCluster::get_pool(int64_t pool_id) {
  Mutex::Locker locker(m_lock);
  for (auto &pool_pair : m_pools) {
    if (pool_pair.second->pool_id == pool_id) {
      return pool_pair.second;
    }
  }
  return nullptr;
}

TestMemCluster::Pool *TestMemCluster::get_pool(const std::string &pool_name) {
  Mutex::Locker locker(m_lock);
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter != m_pools.end()) {
    return iter->second;
  }
  return nullptr;
}

void TestMemCluster::allocate_client(uint32_t *nonce, uint64_t *global_id) {
  Mutex::Locker locker(m_lock);
  *nonce = m_next_nonce++;
  *global_id = m_next_global_id++;
}

void TestMemCluster::deallocate_client(uint32_t nonce) {
  Mutex::Locker locker(m_lock);
  m_blacklist.erase(nonce);
}

bool TestMemCluster::is_blacklisted(uint32_t nonce) const {
  Mutex::Locker locker(m_lock);
  return (m_blacklist.find(nonce) != m_blacklist.end());
}

void TestMemCluster::blacklist(uint32_t nonce) {
  m_watch_notify.blacklist(nonce);

  Mutex::Locker locker(m_lock);
  m_blacklist.insert(nonce);
}

void TestMemCluster::transaction_start(const std::string &oid) {
  Mutex::Locker locker(m_lock);
  while (m_transactions.count(oid)) {
    m_transaction_cond.Wait(m_lock);
  }
  std::pair<std::set<std::string>::iterator, bool> result =
    m_transactions.insert(oid);
  assert(result.second);
}

void TestMemCluster::transaction_finish(const std::string &oid) {
  Mutex::Locker locker(m_lock);
  size_t count = m_transactions.erase(oid);
  assert(count == 1);
  m_transaction_cond.Signal();
}

} // namespace librados

