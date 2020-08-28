// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw/rgw_service.h"

#include "include/rados/librados.hpp"
#include "common/async/yield_context.h"
#include "common/RWLock.h"

class RGWAsyncRadosProcessor;

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

class RGWSI_RADOS : public RGWServiceInstance
{
  librados::Rados rados;
  std::unique_ptr<RGWAsyncRadosProcessor> async_processor;

  int do_start() override;

public:
  struct OpenParams {
    bool create{true};
    bool mostly_omap{false};

    OpenParams() {}

    OpenParams& set_create(bool _create) {
      create = _create;
      return *this;
    }
    OpenParams& set_mostly_omap(bool _mostly_omap) {
      mostly_omap = _mostly_omap;
      return *this;
    }
  };

private:
  librados::Rados* get_rados_handle();
  int open_pool_ctx(const rgw_pool& pool, librados::IoCtx& io_ctx,
                    const OpenParams& params = {}, const Span& parent_span = nullptr);
  int pool_iterate(librados::IoCtx& ioctx,
                   librados::NObjectIterator& iter,
                   uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                   RGWAccessListFilter *filter,
                   bool *is_truncated);

public:
  RGWSI_RADOS(CephContext *cct);
  ~RGWSI_RADOS();

  void init() {}
  void shutdown() override;

  uint64_t instance_id();
  bool check_secure_mon_conn() const;

  RGWAsyncRadosProcessor *get_async_processor() {
    return async_processor.get();
  }

  int clog_warn(const string& msg);

  class Handle;

  class Pool {
    friend class RGWSI_RADOS;
    friend Handle;
    friend class Obj;

    RGWSI_RADOS *rados_svc{nullptr};
    rgw_pool pool;

    struct State {
      librados::IoCtx ioctx;
    } state;

    Pool(RGWSI_RADOS *_rados_svc,
         const rgw_pool& _pool) : rados_svc(_rados_svc),
                                  pool(_pool) {}

    Pool(RGWSI_RADOS *_rados_svc) : rados_svc(_rados_svc) {}
  public:
    Pool() {}

    int create();
    int create(const std::vector<rgw_pool>& pools, std::vector<int> *retcodes);
    int lookup();
    int open(const OpenParams& params = {}, const Span& parent_span = nullptr);

    const rgw_pool& get_pool() {
      return pool;
    }

    librados::IoCtx& ioctx() {
      return state.ioctx;
    }

    struct List {
      Pool *pool{nullptr};

      struct Ctx {
        bool initialized{false};
        librados::IoCtx ioctx;
        librados::NObjectIterator iter;
        RGWAccessListFilter *filter{nullptr};
      } ctx;

      List() {}
      List(Pool *_pool) : pool(_pool) {}

      int init(const string& marker, RGWAccessListFilter *filter = nullptr);
      int get_next(int max,
                   std::vector<string> *oids,
                   bool *is_truncated);

      int get_marker(string *marker);
    };

    List op() {
      return List(this);
    }

    friend List;
  };


  struct rados_ref {
    RGWSI_RADOS::Pool pool;
    rgw_raw_obj obj;
  };

  class Obj {
    friend class RGWSI_RADOS;
    friend class Handle;

    RGWSI_RADOS *rados_svc{nullptr};
    rados_ref ref;

    void init(const rgw_raw_obj& obj);

    Obj(RGWSI_RADOS *_rados_svc, const rgw_raw_obj& _obj)
      : rados_svc(_rados_svc) {
      init(_obj);
    }

    Obj(Pool& pool, const string& oid);

  public:
    Obj() {}

    int open();

    int operate(librados::ObjectWriteOperation *op, optional_yield y,
		int flags = 0);
    int operate(librados::ObjectReadOperation *op, bufferlist *pbl,
                optional_yield y, int flags = 0);
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

    rados_ref& get_ref() { return ref; }
    const rados_ref& get_ref() const { return ref; }

    const rgw_raw_obj& get_raw_obj() const {
      return ref.obj;
    }
  };

  class Handle {
    friend class RGWSI_RADOS;

    RGWSI_RADOS *rados_svc{nullptr};

    Handle(RGWSI_RADOS *_rados_svc) : rados_svc(_rados_svc) {}
  public:
    Obj obj(const rgw_raw_obj& o);

    Pool pool(const rgw_pool& p) {
      return Pool(rados_svc, p);
    }

    int watch_flush();

    int mon_command(std::string cmd,
                    const bufferlist& inbl,
                    bufferlist *outbl,
                    std::string *outs);
  };

  Handle handle() {
    return Handle(this);
  }

  Obj obj(const rgw_raw_obj& o) {
    return Obj(this, o);
  }

  Obj obj(Pool& pool, const string& oid) {
    return Obj(pool, oid);
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

using rgw_rados_ref = RGWSI_RADOS::rados_ref;

inline ostream& operator<<(ostream& out, const RGWSI_RADOS::Obj& obj) {
  return out << obj.get_raw_obj();
}
