#ifndef CEPH_RGW_SERVICES_RADOS_H
#define CEPH_RGW_SERVICES_RADOS_H


#include "rgw/rgw_service.h"

#include "include/rados/librados.hpp"

class RGWS_RADOS : public RGWService
{
  std::vector<std::string> get_deps();
public:
  RGWS_RADOS(CephContext *cct) : RGWService(cct, "rados") {}

  int create_instance(const string& conf, RGWServiceInstanceRef *instance);
};

struct rgw_rados_ref {
  rgw_pool pool;
  string oid;
  string key;
  librados::IoCtx ioctx;
};

class RGWSI_RADOS : public RGWServiceInstance
{
  std::vector<librados::Rados> rados;
  uint32_t next_rados_handle{0};
  RWLock handle_lock;
  std::map<pthread_t, int> rados_map;

  int init(const string& conf, std::map<std::string, RGWServiceInstanceRef>& deps);
  librados::Rados* get_rados_handle();
  int open_pool_ctx(const rgw_pool& pool, librados::IoCtx& io_ctx);
public:
  RGWSI_RADOS(RGWService *svc, CephContext *cct): RGWServiceInstance(svc, cct),
                                                  handle_lock("rados_handle_lock") {}

  uint64_t instance_id();

  class Obj {
    friend class RGWSI_RADOS;

    RGWSI_RADOS *rados_svc;
    rgw_rados_ref ref;

    void init(const rgw_raw_obj& obj);

    Obj(RGWSI_RADOS *_rados_svc, const rgw_raw_obj& _obj) : rados_svc(_rados_svc) {
      init(_obj);
    }

  public:
    Obj(const Obj& o) : rados_svc(o.rados_svc),
                        ref(o.ref) {}

    Obj(const Obj&& o) : rados_svc(o.rados_svc),
                         ref(std::move(o.ref)) {}

    int open();

    int operate(librados::ObjectWriteOperation *op);
    int operate(librados::ObjectReadOperation *op, bufferlist *pbl);
    int aio_operate(librados::AioCompletion *c, librados::ObjectWriteOperation *op);
  };

  Obj obj(const rgw_raw_obj& o) {
    return Obj(this, o);
  }

};

#endif
