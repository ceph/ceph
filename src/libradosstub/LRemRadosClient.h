// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <memory>
#include <list>
#include <string>
#include <vector>
#include <atomic>

#include <boost/function.hpp>
#include <boost/functional/hash.hpp>

#include "include/rados/librados.hpp"
#include "common/config.h"
#include "common/config_obs.h"
#include "include/buffer_fwd.h"
#include "LRemWatchNotify.h"
#include "LRemTransaction.h"

class Finisher;

namespace boost { namespace asio { struct io_context; }}
namespace ceph { namespace async { struct io_context_pool; }}

namespace librados {

class LRemIoCtxImpl;

class ObjListOp;

class LRemRadosClient : public md_config_obs_t {
public:

  static void Deallocate(librados::LRemRadosClient* client)
  {
    client->put();
  }

  typedef boost::function<int()> AioFunction;

  struct Object {
    std::string oid;
    std::string locator;
    std::string nspace;
  };

  LRemRadosClient(CephContext *cct, LRemWatchNotify *watch_notify);

  void get();
  void put();

  virtual CephContext *cct();

  virtual uint32_t get_nonce() = 0;
  virtual uint64_t get_instance_id() = 0;

  virtual int get_min_compatible_osd(int8_t* require_osd_release) = 0;
  virtual int get_min_compatible_client(int8_t* min_compat_client,
                                        int8_t* require_min_compat_client) = 0;

  virtual int connect();
  virtual void shutdown();
  virtual int wait_for_latest_osdmap();

  virtual LRemIoCtxImpl *create_ioctx(int64_t pool_id,
                                      const std::string &pool_name) = 0;

  virtual int mon_command(const std::vector<std::string>& cmd,
                          const bufferlist &inbl,
                          bufferlist *outbl, std::string *outs);

  virtual int object_list_open(int64_t pool_id,
                               std::shared_ptr<ObjListOp> *op) = 0;

  virtual int object_list(int64_t pool_id,
                          std::list<librados::LRemRadosClient::Object> *list) = 0;

  virtual int service_daemon_register(const std::string& service,
                                      const std::string& name,
                                      const std::map<std::string,std::string>& metadata) = 0;
  virtual int service_daemon_update_status(std::map<std::string,std::string>&& status) = 0;

  virtual int pool_create(const std::string &pool_name) = 0;
  virtual int pool_create_async(const char *name, PoolAsyncCompletionImpl *c);
  virtual int pool_delete(const std::string &pool_name) = 0;
  virtual int pool_get_base_tier(int64_t pool_id, int64_t* base_tier) = 0;
  virtual int pool_list(std::list<std::pair<int64_t, std::string> >& v) = 0;
  virtual int64_t pool_lookup(const std::string &name) = 0;
  virtual int pool_reverse_lookup(int64_t id, std::string *name) = 0;

  virtual int aio_watch_flush(AioCompletionImpl *c);
  virtual int watch_flush() = 0;

  virtual bool is_blocklisted() const = 0;
  virtual int blocklist_add(const std::string& client_address,
			    uint32_t expire_seconds) = 0;

  virtual int wait_for_latest_osd_map() {
    return 0;
  }

  Finisher *get_aio_finisher() {
    return m_aio_finisher;
  }
  LRemWatchNotify *get_watch_notify() {
    return m_watch_notify;
  }

  void add_aio_operation(const std::string& oid, bool queue_callback,
			 const AioFunction &aio_function, AioCompletionImpl *c);
  void add_pool_aio_operation(bool queue_callback,
                              const AioFunction &aio_function,
                              PoolAsyncCompletionImpl *c);

  void flush_aio_operations();
  void flush_aio_operations(AioCompletionImpl *c);

  void finish_aio_completion(AioCompletionImpl *c, int r);

  void flush_pool_aio_operations();
  void flush_pool_aio_operations(PoolAsyncCompletionImpl *c);

  virtual int cluster_stat(cluster_stat_t& result) = 0;

  boost::asio::io_context& get_io_context();

protected:
  virtual ~LRemRadosClient();

  virtual void transaction_start(LRemTransactionStateRef& state) = 0;
  virtual void transaction_finish(LRemTransactionStateRef& state) = 0;

  const char** get_tracked_conf_keys() const override;
  void handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string> &changed) override;

private:
  struct IOContextPool;

  CephContext *m_cct;
  std::atomic<uint64_t> m_refcount = { 0 };

  LRemWatchNotify *m_watch_notify;

  Finisher *get_finisher(const std::string& oid);

  Finisher *m_aio_finisher;
  std::vector<Finisher *> m_finishers;
  boost::hash<std::string> m_hash;

  Finisher *m_pool_finisher;

  std::unique_ptr<ceph::async::io_context_pool> m_io_context_pool;
};

class ObjListOp {
public:
  virtual ~ObjListOp() {}
  virtual uint32_t seek(const ObjectCursor& cursor) = 0;
  virtual uint32_t seek(uint32_t pos) = 0;
  virtual librados::ObjectCursor get_cursor() const = 0;
  virtual uint32_t get_pg_hash_position() const = 0;

  virtual void set_nspace(const string& nspace) = 0;
  virtual void set_filter(const bufferlist& bl) = 0;

  virtual int list_objs(int max, std::list<librados::LRemRadosClient::Object> *result, bool *more) = 0;
};

struct ObjListCtx {
  std::shared_ptr<ObjListOp> op;
//  ObjectCursor cursor;
  bool at_end{false};
  bool more{true};
  std::list<librados::LRemRadosClient::Object> objs;

  uint32_t seek(const ObjectCursor& cursor) {
    return op->seek(cursor);
  }

  uint32_t seek(uint32_t pos) {
    return op->seek(pos);
  }

  uint32_t get_pg_hash_position() const {
    return op->get_pg_hash_position();
  }

  librados::ObjectCursor get_cursor() {
    return op->get_cursor();
  }

  void set_nspace(const string& nspace) {
    op->set_nspace(nspace);
  }

  void set_filter(const bufferlist& bl) {
    op->set_filter(bl);
  }
};

} // namespace librados
