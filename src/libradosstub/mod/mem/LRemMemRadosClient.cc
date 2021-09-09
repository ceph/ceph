// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemMemRadosClient.h"
#include "LRemMemCluster.h"
#include "LRemMemIoCtxImpl.h"
#include <errno.h>
#include <sstream>

namespace librados {

LRemMemRadosClient::LRemMemRadosClient(CephContext *cct,
                                       LRemMemCluster *lrem_mem_cluster)
  : LRemRadosClient(cct, lrem_mem_cluster->get_watch_notify()),
    m_mem_cluster(lrem_mem_cluster) {
  m_mem_cluster->allocate_client(&m_nonce, &m_global_id);
}

LRemMemRadosClient::~LRemMemRadosClient() {
  m_mem_cluster->deallocate_client(m_nonce);
}

LRemIoCtxImpl *LRemMemRadosClient::create_ioctx(int64_t pool_id,
						const std::string &pool_name) {
  return new LRemMemIoCtxImpl(this, pool_id, pool_name,
                              m_mem_cluster->get_pool(pool_name));
}

void LRemMemRadosClient::object_list(int64_t pool_id,
 				     std::list<librados::LRemRadosClient::Object> *list) {
  list->clear();

  auto pool = m_mem_cluster->get_pool(pool_id);
  if (pool != nullptr) {
    std::shared_lock file_locker{pool->file_lock};
    for (auto &file_pair : pool->files) {
      Object obj;
      obj.oid = file_pair.first.name;
      list->push_back(obj);
    }
  }
}

int LRemMemRadosClient::pool_create(const std::string &pool_name) {
  if (is_blocklisted()) {
    return -EBLOCKLISTED;
  }
  return m_mem_cluster->pool_create(pool_name);
}

int LRemMemRadosClient::pool_delete(const std::string &pool_name) {
  if (is_blocklisted()) {
    return -EBLOCKLISTED;
  }
  return m_mem_cluster->pool_delete(pool_name);
}

int LRemMemRadosClient::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int LRemMemRadosClient::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  return m_mem_cluster->pool_list(v);
}

int64_t LRemMemRadosClient::pool_lookup(const std::string &pool_name) {
  return m_mem_cluster->pool_lookup(pool_name);
}

int LRemMemRadosClient::pool_reverse_lookup(int64_t id, std::string *name) {
  return m_mem_cluster->pool_reverse_lookup(id, name);
}

int LRemMemRadosClient::watch_flush() {
  get_watch_notify()->flush(this);
  return 0;
}

bool LRemMemRadosClient::is_blocklisted() const {
  return m_mem_cluster->is_blocklisted(m_nonce);
}

int LRemMemRadosClient::blocklist_add(const std::string& client_address,
				      uint32_t expire_seconds) {
  if (is_blocklisted()) {
    return -EBLOCKLISTED;
  }

  // extract the nonce to use as a unique key to the client
  auto idx = client_address.find("/");
  if (idx == std::string::npos || idx + 1 >= client_address.size()) {
    return -EINVAL;
  }

  std::stringstream nonce_ss(client_address.substr(idx + 1));
  uint32_t nonce;
  nonce_ss >> nonce;
  if (!nonce_ss) {
    return -EINVAL;
  }

  m_mem_cluster->blocklist(nonce);
  return 0;
}

void LRemMemRadosClient::transaction_start(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_start(state);
}

void LRemMemRadosClient::transaction_finish(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_finish(state);
}

} // namespace librados
