// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LRemDBRadosClient.h"
#include "LRemDBCluster.h"
#include "LRemDBIoCtxImpl.h"
#include "LRemDBStore.h"
#include <errno.h>
#include <sstream>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rados

using namespace std;

namespace librados {

LRemDBRadosClient::LRemDBRadosClient(CephContext *_cct,
                                       LRemDBCluster *lrem_mem_cluster)
  : LRemRadosClient(_cct, lrem_mem_cluster->get_watch_notify()),
    m_mem_cluster(lrem_mem_cluster) {
  m_mem_cluster->allocate_client(&m_nonce, &m_global_id);
  int r = init();
  assert( r >= 0 );
}

LRemDBRadosClient::~LRemDBRadosClient() {
  m_mem_cluster->deallocate_client(m_nonce);
}

LRemIoCtxImpl *LRemDBRadosClient::create_ioctx(int64_t pool_id,
						const std::string &pool_name) {
  LRemDBTransactionState trans(cct());
  return new LRemDBIoCtxImpl(this, pool_id, pool_name,
                              m_mem_cluster->get_pool(*trans.dbc, pool_name));
}

int LRemDBRadosClient::init() {
  LRemDBTransactionState trans(cct());
  int r = trans.dbc->init_cluster();
  if (r < 0) {
    return r;
  }

  return m_mem_cluster->init(*trans.dbc);
}

class LRemDBObjListOp : public ObjListOp {
  CephContext *cct;
  LRemDBTransactionState trans;
  int64_t pool_id;

  LRemDBStore::PoolRef pool_db;

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
  LRemDBObjListOp(CephContext *_cct, int64_t _pool_id) : cct(_cct), trans(_cct),
                                                         pool_id(_pool_id) {}

  int open() {
    int r = trans.dbc->get_pool(pool_id, &pool_db);
    if (r < 0) {
      return r;
    }
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


int LRemDBObjListOp::list_objs(int max, std::list<librados::LRemRadosClient::Object> *result, bool *more) {
  std::optional<string> opt_filter;
  if (filter.length() > 0) {
    opt_filter = filter.to_str();
  }
  std::list<LRemCluster::ObjectLocator> objs;
  int r = pool_db->list(nspace, cursor.to_str(),
                        filter.to_str(), max,
                        &objs, more);
  if (r < 0) {
    return r;
  }

  for (auto& loc : objs) {
    LRemDBRadosClient::Object obj;
    obj.oid = loc.name;
    obj.nspace = loc.nspace;
    result->push_back(obj);

    cursor.from_str(obj.oid);
  }

  return 0;
}

int LRemDBRadosClient::object_list_open(int64_t pool_id,
                                        std::shared_ptr<ObjListOp> *op) {
  LRemDBObjListOp *_op = new LRemDBObjListOp(cct(), pool_id);
  int r = _op->open();
  if (r < 0) {
    delete _op;
    return r;
  }
  *op = std::shared_ptr<ObjListOp>(_op);

  return 0;
};

int LRemDBRadosClient::pool_create(const std::string &pool_name) {
  if (is_blocklisted()) {
    return -EBLOCKLISTED;
  }
  LRemDBTransactionState trans(cct());
  return m_mem_cluster->pool_create(*trans.dbc, pool_name);
}

int LRemDBRadosClient::pool_delete(const std::string &pool_name) {
  if (is_blocklisted()) {
    return -EBLOCKLISTED;
  }
  LRemDBTransactionState trans(cct());
  return m_mem_cluster->pool_delete(*trans.dbc, pool_name);
}

int LRemDBRadosClient::pool_get_base_tier(int64_t pool_id, int64_t* base_tier) {
  // TODO
  *base_tier = pool_id;
  return 0;
}

int LRemDBRadosClient::pool_list(std::list<std::pair<int64_t, std::string> >& v) {
  return m_mem_cluster->pool_list(v);
}

int64_t LRemDBRadosClient::pool_lookup(const std::string &pool_name) {
  return m_mem_cluster->pool_lookup(pool_name);
}

int LRemDBRadosClient::pool_reverse_lookup(int64_t id, std::string *name) {
  return m_mem_cluster->pool_reverse_lookup(id, name);
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

  m_mem_cluster->blocklist(this, nonce);
  return 0;
}

void LRemDBRadosClient::transaction_start(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_start(state);
}

void LRemDBRadosClient::transaction_finish(LRemTransactionStateRef& state) {
  m_mem_cluster->transaction_finish(state);
}

} // namespace librados
