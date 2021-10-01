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

class LRemMemObjListOp : public ObjListOp {
  CephContext *cct;

  LRemMemCluster *mem_cluster;
  int64_t pool_id;

  ObjectCursor cursor;
  std::optional<string> nspace;
  bufferlist filter;

  uint32_t hash(const string& s) const {
    char result[sizeof(uint32_t)];
    const char *cs = s.c_str();
    int len = min(s.size(), sizeof(result) - 1);
    result[0] = (char)len;
    memcpy(&result[1], cs, len); /* memcpy and not strcpy because s might hold null chars */

    return *(uint32_t *)result;
  }

  string unhash(uint32_t h) const {
    char *buf = (char *)&h;

    int len = (int)buf[0];

    return string(&buf[1], len);
  }
public:
  LRemMemObjListOp(CephContext *_cct,
                   LRemMemCluster *_mem_cluster,
                   int64_t _pool_id) : cct(_cct),
                                       mem_cluster(_mem_cluster),
                                       pool_id(_pool_id) {}

  int open() {
    return 0;
  }

  uint32_t seek(const ObjectCursor& _cursor) override {
    cursor = _cursor;
    return get_pg_hash_position();
  }

  uint32_t seek(uint32_t pos) override {
    cursor.from_str(unhash(pos));
    return pos;
  }

  virtual librados::ObjectCursor get_cursor() const override {
    return cursor;
  }

  uint32_t get_pg_hash_position() const override {
    return hash(cursor.to_str());
  }

  void set_filter(const bufferlist& bl) override {
    filter = bl;
  }

  void set_nspace(const string& ns) override {
    nspace = ns;
  }

  int list_objs(int max, std::list<librados::LRemRadosClient::Object> *result, bool *more) override;
};

int LRemMemObjListOp::list_objs(int max, std::list<librados::LRemRadosClient::Object> *result, bool *more) {
  result->clear();

  auto pool = mem_cluster->get_pool(pool_id);
  if (!pool) {
    return -ENOENT;
  }

  std::shared_lock file_locker{pool->file_lock};
  LRemCluster::ObjectLocator loc_cursor( nspace.value_or(string()), cursor.to_str() );
  auto iter = pool->files.lower_bound(loc_cursor);
  for (int i = 0;
       i < max && iter != pool->files.end();
       ++i, ++iter) {
    librados::LRemRadosClient::Object obj;
    obj.oid = iter->first.name;
    result->push_back(obj);

    cursor.from_str(obj.oid);
  }

  return 0;
}

int LRemMemRadosClient::object_list_open(int64_t pool_id,
                                        std::shared_ptr<ObjListOp> *op) {
  LRemMemObjListOp *_op = new LRemMemObjListOp(cct(), m_mem_cluster, pool_id);
  int r = _op->open();
  if (r < 0) {
    delete _op;
    return r;
  }
  *op = std::shared_ptr<ObjListOp>(_op);

  return 0;
};

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

  m_mem_cluster->blocklist(this, nonce);
  return 0;
}

void LRemMemRadosClient::transaction_start(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_start(state);
}

void LRemMemRadosClient::transaction_finish(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_finish(state);
}

} // namespace librados
