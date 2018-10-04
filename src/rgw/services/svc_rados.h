#ifndef CEPH_RGW_SERVICES_RADOS_H
#define CEPH_RGW_SERVICES_RADOS_H


#include "rgw/rgw_service.h"

#include "include/rados/librados.hpp"

class RGWAccessListFilter {
public:
  virtual ~RGWAccessListFilter() {}
  virtual bool filter(string& name, string& key) = 0;
};

struct RGWAccessListFilterPrefix : public RGWAccessListFilter {
  string prefix;

  explicit RGWAccessListFilterPrefix(const string& _prefix) : prefix(_prefix) {}
  bool filter(string& name, string& key) override {
    return (prefix.compare(key.substr(0, prefix.size())) == 0);
  }
};

struct rgw_rados_ref {
  rgw_pool pool;
  string oid;
  string key;
  bool has_ioctx{false};
  librados::IoCtx ioctx;

  rgw_rados_ref() {}
  ~rgw_rados_ref() {}

  rgw_rados_ref(const rgw_rados_ref& r) : pool(r.pool),
                                          oid(r.oid),
                                          key(r.key),
                                          has_ioctx(r.has_ioctx) {
    if (r.has_ioctx) {
      ioctx = r.ioctx;
    }
  }

  rgw_rados_ref(const rgw_rados_ref&& r) : pool(std::move(r.pool)),
                                           oid(std::move(r.oid)),
                                           key(std::move(r.key)),
                                           has_ioctx(r.has_ioctx) {
    if (r.has_ioctx) {
      ioctx = r.ioctx;
    }
  }

  rgw_rados_ref& operator=(rgw_rados_ref&& r) {
    pool = std::move(r.pool);
    oid = std::move(r.oid);
    key = std::move(r.key);
    has_ioctx = r.has_ioctx;
    if (has_ioctx) {
      ioctx = r.ioctx;
    }
    return *this;
  }

  rgw_rados_ref& operator=(rgw_rados_ref& r) {
    pool = r.pool;
    oid = r.oid;
    key = r.key;
    has_ioctx = r.has_ioctx;
    if (has_ioctx) {
      ioctx = r.ioctx;
    }
    return *this;
  }
};

class RGWSI_RADOS : public RGWServiceInstance
{
  std::vector<librados::Rados> rados;
  uint32_t next_rados_handle{0};
  RWLock handle_lock;
  std::map<pthread_t, int> rados_map;

  int do_start() override;

  librados::Rados* get_rados_handle(int rados_handle);
  int open_pool_ctx(const rgw_pool& pool, librados::IoCtx& io_ctx, int rados_handle);
  int pool_iterate(librados::IoCtx& ioctx,
                   librados::NObjectIterator& iter,
                   uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                   RGWAccessListFilter *filter,
                   bool *is_truncated);

public:
  RGWSI_RADOS(CephContext *cct): RGWServiceInstance(cct),
                                 handle_lock("rados_handle_lock") {}

  void init() {}

  uint64_t instance_id();

  class Handle;

  class Obj {
    friend class RGWSI_RADOS;
    friend class Handle;

    RGWSI_RADOS *rados_svc{nullptr};
    int rados_handle{-1};
    rgw_rados_ref ref;

    void init(const rgw_raw_obj& obj);

    Obj(RGWSI_RADOS *_rados_svc, const rgw_raw_obj& _obj, int _rados_handle) : rados_svc(_rados_svc), rados_handle(_rados_handle) {
      init(_obj);
    }

  public:
    Obj() {}
    Obj(const Obj& o) : rados_svc(o.rados_svc),
                        rados_handle(o.rados_handle),
                        ref(o.ref) {}

    Obj(Obj&& o) : rados_svc(o.rados_svc),
                   rados_handle(o.rados_handle),
                   ref(std::move(o.ref)) {}

    Obj& operator=(Obj&& o) {
      rados_svc = o.rados_svc;
      rados_handle = o.rados_handle;
      ref = std::move(o.ref);
      return *this;
    }

    int open();

    int operate(librados::ObjectWriteOperation *op);
    int operate(librados::ObjectReadOperation *op, bufferlist *pbl);
    int aio_operate(librados::AioCompletion *c, librados::ObjectWriteOperation *op);

    int watch(uint64_t *handle, librados::WatchCtx2 *ctx);
    int aio_watch(librados::AioCompletion *c, uint64_t *handle, librados::WatchCtx2 *ctx);
    int unwatch(uint64_t handle);
    int notify(bufferlist& bl,
               uint64_t timeout_ms,
               bufferlist *pbl);
    void notify_ack(uint64_t notify_id,
                    uint64_t cookie,
                    bufferlist& bl);

    uint64_t get_last_version();

    rgw_rados_ref& get_ref() {
      return ref;
    }
  };

  class Pool {
    friend class RGWSI_RADOS;
    friend class Handle;

    RGWSI_RADOS *rados_svc{nullptr};
    int rados_handle{-1};
    rgw_pool pool;

    Pool(RGWSI_RADOS *_rados_svc,
         const rgw_pool& _pool,
         int _rados_handle) : rados_svc(_rados_svc),
                              rados_handle(_rados_handle),
                              pool(_pool) {}

    Pool(RGWSI_RADOS *_rados_svc) : rados_svc(_rados_svc) {}
  public:
    Pool() {}
    Pool(const Pool& p) : rados_svc(p.rados_svc),
                          rados_handle(p.rados_handle),
                          pool(p.pool) {}

    int create(const std::vector<rgw_pool>& pools, std::vector<int> *retcodes);
    int lookup(const rgw_pool& pool);

    struct List {
      Pool& pool;

      struct Ctx {
        bool initialized{false};
        librados::IoCtx ioctx;
        librados::NObjectIterator iter;
        RGWAccessListFilter *filter{nullptr};
      } ctx;

      List(Pool& _pool) : pool(_pool) {}

      int init(const string& marker, RGWAccessListFilter *filter = nullptr);
      int get_next(int max,
                   std::list<string> *oids,
                   bool *is_truncated);
    };

    List op() {
      return List(*this);
    }

    friend class List;
  };

  class Handle {
    friend class RGWSI_RADOS;

    RGWSI_RADOS *rados_svc{nullptr};
    int rados_handle{-1};

    Handle(RGWSI_RADOS *_rados_svc, int _rados_handle) : rados_svc(_rados_svc),
                                                         rados_handle(_rados_handle) {}
  public:
    Obj obj(const rgw_raw_obj& o) {
      return Obj(rados_svc, o, rados_handle);
    }

    Pool pool(const rgw_pool& p) {
      return Pool(rados_svc, p, rados_handle);
    }

    int watch_flush();
  };

  Handle handle(int rados_handle) {
    return Handle(this, rados_handle);
  }

  Obj obj(const rgw_raw_obj& o) {
    return Obj(this, o, -1);
  }

  Pool pool() {
    return Pool(this);
  }

  Pool pool(const rgw_pool& p) {
    return Pool(this, p, -1);
  }

  friend class Obj;
  friend class Pool;
  friend class Pool::List;
};

#endif
