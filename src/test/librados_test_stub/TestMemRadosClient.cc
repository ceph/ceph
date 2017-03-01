// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/librados_test_stub/TestMemRadosClient.h"
#include "test/librados_test_stub/TestMemCluster.h"
#include "test/librados_test_stub/TestMemIoCtxImpl.h"
#include <errno.h>

namespace librados {

TestMemRadosClient::TestMemRadosClient(CephContext *cct,
                                       TestMemCluster *test_mem_cluster)
  : TestRadosClient(cct, test_mem_cluster->get_watch_notify()),
    m_mem_cluster(test_mem_cluster) {
}

TestIoCtxImpl *TestMemRadosClient::create_ioctx(int64_t pool_id,
						const std::string &pool_name) {
  return new TestMemIoCtxImpl(this, pool_id, pool_name,
                              m_mem_cluster->get_pool(pool_name));
}

void TestMemRadosClient::object_list(int64_t pool_id,
 				     std::list<librados::TestRadosClient::Object> *list) {
  list->clear();

  auto pool = m_mem_cluster->get_pool(pool_id);
  if (pool != nullptr) {
    RWLock::RLocker file_locker(pool->file_lock);
    for (auto &file_pair : pool->files) {
      Object obj;
      obj.oid = file_pair.first;
      list->push_back(obj);
    }
  }
}

int TestMemRadosClient::pool_create(const std::string &pool_name) {
  return m_mem_cluster->pool_create(pool_name);
}

int TestMemRadosClient::pool_delete(const std::string &pool_name) {
  return m_mem_cluster->pool_delete(pool_name);
}

int TestMemRadosClient::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int TestMemRadosClient::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  return m_mem_cluster->pool_list(v);
}

int64_t TestMemRadosClient::pool_lookup(const std::string &pool_name) {
  return m_mem_cluster->pool_lookup(pool_name);
}

int TestMemRadosClient::pool_reverse_lookup(int64_t id, std::string *name) {
  return m_mem_cluster->pool_reverse_lookup(id, name);
}

int TestMemRadosClient::watch_flush() {
  get_watch_notify()->flush(this);
  return 0;
}

int TestMemRadosClient::blacklist_add(const std::string& client_address,
				      uint32_t expire_seconds) {
  return 0;
}

void TestMemRadosClient::transaction_start(const std::string &oid) {
  m_mem_cluster->transaction_start(oid);
}

void TestMemRadosClient::transaction_finish(const std::string &oid) {
  m_mem_cluster->transaction_finish(oid);
}

} // namespace librados
