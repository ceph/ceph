#ifndef CEPH_RGW_SERVICES_RADOS_H
#define CEPH_RGW_SERVICES_RADOS_H


#include "rgw/rgw_service.h"

#include "include/rados/librados.hpp"
#include "common/async/yield_context.h"

class RGWAccessListFilter {
public:
  virtual ~RGWAccessListFilter() {}
  virtual bool filter(const string& name, string& key) = 0;
};

struct RGWAccessListFilterPrefix : public RGWAccessListFilter {
  string prefix;

  explicit RGWAccessListFilterPrefix(const string& _prefix) : prefix(_prefix) {}
  bool filter(const string& name, string& key) override {
    return (prefix.compare(key.substr(0, prefix.size())) == 0);
  }
};

struct rgw_rados_ref {
  rgw_raw_obj obj;
  librados::IoCtx ioctx;
};

class RGWSI_RADOS : public RGWServiceInstance
{
  librados::Rados rados;

  int do_start() override;

  librados::Rados* get_rados_handle();
  int open_pool_ctx(const rgw_pool& pool, librados::IoCtx& io_ctx);
  int pool_iterate(librados::IoCtx& ioctx,
                   librados::NObjectIterator& iter,
                   uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                   RGWAccessListFilter *filter,
                   bool *is_truncated);

public:
  RGWSI_RADOS(CephContext *cct) : RGWServiceInstance(cct) {}

  void init() {}

  uint64_t instance_id();

  class Handle;

  class Obj {
    friend class RGWSI_RADOS;
    friend Handle;

    RGWSI_RADOS *rados_svc{nullptr};
    rgw_rados_ref ref;

    void init(const rgw_raw_obj& obj);

    Obj(RGWSI_RADOS *_rados_svc, const rgw_raw_obj& _obj)
      : rados_svc(_rados_svc) {
      init(_obj);
    }

  public:
    Obj() {}

    int open();

    int operate(librados::ObjectWriteOperation *op, optional_yield y);
    int operate(librados::ObjectReadOperation *op, bufferlist *pbl,
                optional_yield y);
    int aio_operate(librados::AioCompletion *c, librados::ObjectWriteOperation *op);
    int aio_operate(librados::AioCompletion *c, librados::ObjectReadOperation *op,
                    bufferlist *pbl);

    int watch(uint64_t *handle, librados::WatchCtx2 *ctx);
    int aio_watch(librados::AioCompletion *c, uint64_t *handle, librados::WatchCtx2 *ctx);
    int unwatch(uint64_t handle);
    int notify(bufferlist& bl, uint64_t timeout_ms,
               bufferlist *pbl, optional_yield y);
    void notify_ack(uint64_t notify_id,
                    uint64_t cookie,
                    bufferlist& bl);

    uint64_t get_last_version();

    rgw_rados_ref& get_ref() { return ref; }
    const rgw_rados_ref& get_ref() const { return ref; }
  };

  class Pool {
    friend class RGWSI_RADOS;
    friend Handle;

    RGWSI_RADOS *rados_svc{nullptr};
    rgw_pool pool;

    Pool(RGWSI_RADOS *_rados_svc,
         const rgw_pool& _pool) : rados_svc(_rados_svc),
                                  pool(_pool) {}

    Pool(RGWSI_RADOS *_rados_svc) : rados_svc(_rados_svc) {}
  public:
    Pool() {}

    int create();
    int create(const std::vector<rgw_pool>& pools, std::vector<int> *retcodes);
    int lookup();

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

    friend List;
  };

  class Handle {
    friend class RGWSI_RADOS;

    RGWSI_RADOS *rados_svc{nullptr};

    Handle(RGWSI_RADOS *_rados_svc) : rados_svc(_rados_svc) {}
  public:
    Obj obj(const rgw_raw_obj& o) {
      return Obj(rados_svc, o);
    }

    Pool pool(const rgw_pool& p) {
      return Pool(rados_svc, p);
    }

    int watch_flush();
  };

  Handle handle() {
    return Handle(this);
  }

  Obj obj(const rgw_raw_obj& o) {
    return Obj(this, o);
  }

  Pool pool() {
    return Pool(this);
  }

  Pool pool(const rgw_pool& p) {
    return Pool(this, p);
  }

  friend Obj;
  friend Pool;
  friend Pool::List;
};

#endif
