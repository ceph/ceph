// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestMemRadosClient.h"
#include "test/librados_test_stub/TestMemIoCtxImpl.h"
#include <errno.h>

namespace librados {

TestMemRadosClient::TestMemRadosClient(CephContext *cct)
  : TestRadosClient(cct), m_pool_id() {
}

TestMemRadosClient::~TestMemRadosClient() {
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    delete iter->second;
  }
}

TestMemRadosClient::File::File()
  : snap_id(), exists(true), lock("TestMemRadosClient::File::lock")
{
}

TestMemRadosClient::File::File(const File &rhs)
  : data(rhs.data),
    mtime(rhs.mtime),
    snap_id(rhs.snap_id),
    exists(rhs.exists),
    lock("TestMemRadosClient::File::lock")
{
}

TestMemRadosClient::Pool::Pool()
  : pool_id(), snap_id(1), file_lock("TestMemRadosClient::Pool::file_lock")
{
}

TestIoCtxImpl *TestMemRadosClient::create_ioctx(int64_t pool_id,
						const std::string &pool_name) {
  Pools::iterator iter = m_pools.find(pool_name);
  assert(iter != m_pools.end());

  return new TestMemIoCtxImpl(*this, pool_id, pool_name, iter->second);
}

void TestMemRadosClient::object_list(int64_t pool_id,
 				     std::list<librados::TestRadosClient::Object> *list) {
  list->clear();

  for (Pools::iterator p_it = m_pools.begin(); p_it != m_pools.end(); ++p_it) {
    Pool *pool = p_it->second;
    if (pool->pool_id == pool_id) {
      RWLock::RLocker l(pool->file_lock);
      for (Files::iterator it = pool->files.begin();
	   it != pool->files.end(); ++it) {
	Object obj;
	obj.oid = it->first;
	list->push_back(obj);
      } 
      break;
    }
  }
} 

int TestMemRadosClient::pool_create(const std::string &pool_name) {
  if (m_pools.find(pool_name) != m_pools.end()) {
    return -EEXIST;
  }
  Pool *pool = new Pool();
  pool->pool_id = ++m_pool_id;
  m_pools[pool_name] = pool;
  return 0;
}

int TestMemRadosClient::pool_delete(const std::string &pool_name) {
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  iter->second->put();
  m_pools.erase(iter);
  return 0;
}

int TestMemRadosClient::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int TestMemRadosClient::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  v.clear();
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    v.push_back(std::make_pair(iter->second->pool_id, iter->first));
  }
  return 0;
}

int64_t TestMemRadosClient::pool_lookup(const std::string &pool_name) {
  Pools::iterator iter = m_pools.find(pool_name);
  if (iter == m_pools.end()) {
    return -ENOENT;
  }
  return iter->second->pool_id;
}

int TestMemRadosClient::pool_reverse_lookup(int64_t id, std::string *name) {
  for (Pools::iterator iter = m_pools.begin(); iter != m_pools.end(); ++iter) {
    if (iter->second->pool_id == id) {
      *name = iter->first;
      return 0;
    }
  }
  return -ENOENT;
}

int TestMemRadosClient::watch_flush() {
  get_watch_notify().flush();
  return 0;
}

int TestMemRadosClient::blacklist_add(const std::string& client_address,
				      uint32_t expire_seconds) {
  return 0;
}

} // namespace librados
