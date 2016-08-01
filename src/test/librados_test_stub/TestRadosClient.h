// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_RADOS_CLIENT_H
#define CEPH_TEST_RADOS_CLIENT_H

#include "include/rados/librados.hpp"
#include "common/config.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "include/atomic.h"
#include "include/buffer_fwd.h"
#include "test/librados_test_stub/TestWatchNotify.h"
#include <boost/function.hpp>
#include <boost/functional/hash.hpp>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

class Finisher;

namespace librados {

class TestIoCtxImpl;

class TestRadosClient {
public:

  static void Deallocate(librados::TestRadosClient* client)
  {
    client->put();
  }

  typedef boost::function<int()> AioFunction;

  struct Object {
    std::string oid;
    std::string locator;
    std::string nspace;
  };

  class Transaction {
  public:
    Transaction(TestRadosClient *rados_client, const std::string &oid)
      : rados_client(rados_client), oid(oid) {
      rados_client->transaction_start(oid);
    }
    ~Transaction() {
      rados_client->transaction_finish(oid);
    }
  private:
    TestRadosClient *rados_client;
    std::string oid;
  };

  TestRadosClient(CephContext *cct);

  void get();
  void put();

  virtual CephContext *cct();

  virtual uint64_t get_instance_id();

  virtual int connect();
  virtual void shutdown();
  virtual int wait_for_latest_osdmap();

  virtual TestIoCtxImpl *create_ioctx(int64_t pool_id,
                                      const std::string &pool_name) = 0;

  virtual int mon_command(const std::vector<std::string>& cmd,
                          const bufferlist &inbl,
                          bufferlist *outbl, std::string *outs);

  virtual void object_list(int64_t pool_id,
			   std::list<librados::TestRadosClient::Object> *list) = 0;

  virtual int pool_create(const std::string &pool_name) = 0;
  virtual int pool_delete(const std::string &pool_name) = 0;
  virtual int pool_get_base_tier(int64_t pool_id, int64_t* base_tier) = 0;
  virtual int pool_list(std::list<std::pair<int64_t, std::string> >& v) = 0;
  virtual int64_t pool_lookup(const std::string &name) = 0;
  virtual int pool_reverse_lookup(int64_t id, std::string *name) = 0;

  virtual int aio_watch_flush(AioCompletionImpl *c);
  virtual int watch_flush() = 0;

  virtual int blacklist_add(const std::string& client_address,
			    uint32_t expire_seconds) = 0;

  TestWatchNotify &get_watch_notify() {
    return m_watch_notify;
  }

  void add_aio_operation(const std::string& oid, bool queue_callback,
			 const AioFunction &aio_function, AioCompletionImpl *c);
  void flush_aio_operations();
  void flush_aio_operations(AioCompletionImpl *c);

  void finish_aio_completion(AioCompletionImpl *c, int r);

protected:
  virtual ~TestRadosClient();

private:

  CephContext *m_cct;
  atomic_t m_refcount;

  Finisher *get_finisher(const std::string& oid);

  Finisher *m_aio_finisher;
  std::vector<Finisher *> m_finishers;
  boost::hash<std::string> m_hash;

  TestWatchNotify m_watch_notify;

  Mutex m_transaction_lock;
  Cond m_transaction_cond;
  std::set<std::string> m_transactions;

  void transaction_start(const std::string &oid);
  void transaction_finish(const std::string &oid);

};

} // namespace librados

#endif // CEPH_TEST_RADOS_CLIENT_H
