// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemDBRadosClient.h"
#include "LRemDBCluster.h"
#include "LRemDBIoCtxImpl.h"
#include <errno.h>
#include <sstream>

namespace librados {

LRemDBRadosClient::LRemDBRadosClient(CephContext *cct,
                                       LRemDBCluster *lrem_mem_cluster)
  : LRemRadosClient(cct, lrem_mem_cluster->get_watch_notify()),
    m_mem_cluster(lrem_mem_cluster) {
  m_mem_cluster->allocate_client(&m_nonce, &m_global_id);
}

LRemDBRadosClient::~LRemDBRadosClient() {
  m_mem_cluster->deallocate_client(m_nonce);
}

LRemIoCtxImpl *LRemDBRadosClient::create_ioctx(int64_t pool_id,
						const std::string &pool_name) {
  return new LRemDBIoCtxImpl(this, pool_id, pool_name,
                              m_mem_cluster->get_pool(pool_name));
}

void LRemDBRadosClient::object_list(int64_t pool_id,
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

int LRemDBRadosClient::pool_create(const std::string &pool_name) {
  if (is_blocklisted()) {
    return -EBLOCKLISTED;
  }
  return m_mem_cluster->pool_create(pool_name);
}

int LRemDBRadosClient::pool_delete(const std::string &pool_name) {
  if (is_blocklisted()) {
    return -EBLOCKLISTED;
  }
  return m_mem_cluster->pool_delete(pool_name);
}

int LRemDBRadosClient::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int LRemDBRadosClient::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  return m_mem_cluster->pool_list(v);
}

int LRemDBRadosClient::watch_flush() {
  get_watch_notify()->flush(this);
  return 0;
}

bool LRemDBRadosClient::is_blocklisted() const {
  return m_mem_cluster->is_blocklisted(m_nonce);
}

int LRemDBRadosClient::blocklist_add(const std::string& client_address,
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

void LRemDBRadosClient::transaction_start(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_start(state);
}

void LRemDBRadosClient::transaction_finish(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_finish(state);
}

} // namespace librados
