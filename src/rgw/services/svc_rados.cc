#include "svc_rados.h"

#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "osd/osd_types.h"

#define dout_subsys ceph_subsys_rgw

int RGWS_RADOS::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_RADOS(this, cct));
  return 0;
}

static int init_ioctx(CephContext *cct, librados::Rados *rados, const rgw_pool& pool, librados::IoCtx& ioctx, bool create)
{
  int r = rados->ioctx_create(pool.name.c_str(), ioctx);
  if (r == -ENOENT && create) {
    r = rados->pool_create(pool.name.c_str());
    if (r == -ERANGE) {
      ldout(cct, 0)
        << __func__
        << " ERROR: librados::Rados::pool_create returned " << cpp_strerror(-r)
        << " (this can be due to a pool or placement group misconfiguration, e.g."
        << " pg_num < pgp_num or mon_max_pg_per_osd exceeded)"
        << dendl;
    }
    if (r < 0 && r != -EEXIST) {
      return r;
    }

    r = rados->ioctx_create(pool.name.c_str(), ioctx);
    if (r < 0) {
      return r;
    }

    r = ioctx.application_enable(pg_pool_t::APPLICATION_NAME_RGW, false);
    if (r < 0 && r != -EOPNOTSUPP) {
      return r;
    }
  } else if (r < 0) {
    return r;
  }
  if (!pool.ns.empty()) {
    ioctx.set_namespace(pool.ns);
  }
  return 0;
}

int RGWSI_RADOS::load(const string& conf, map<string, RGWServiceInstanceRef>& deps)
{
  auto handles = std::vector<librados::Rados>{static_cast<size_t>(cct->_conf->rgw_num_rados_handles)};

  for (auto& r : handles) {
    int ret = r.init_with_context(cct);
    if (ret < 0) {
      return ret;
    }
    ret = r.connect();
    if (ret < 0) {
      return ret;
    }
  }
  return 0;
}

librados::Rados* RGWSI_RADOS::get_rados_handle()
{
  if (rados.size() == 1) {
    return &rados[0];
  }
  handle_lock.get_read();
  pthread_t id = pthread_self();
  std::map<pthread_t, int>:: iterator it = rados_map.find(id);

  if (it != rados_map.end()) {
    handle_lock.put_read();
    return &rados[it->second];
  }
  handle_lock.put_read();
  handle_lock.get_write();
  const uint32_t handle = next_rados_handle;
  rados_map[id] = handle;
  if (++next_rados_handle == rados.size()) {
    next_rados_handle = 0;
  }
  handle_lock.put_write();
  return &rados[handle];
}

uint64_t RGWSI_RADOS::instance_id()
{
  return get_rados_handle()->get_instance_id();
}

int RGWSI_RADOS::open_pool_ctx(const rgw_pool& pool, librados::IoCtx& io_ctx)
{
  constexpr bool create = true; // create the pool if it doesn't exist
  return init_ioctx(cct, get_rados_handle(), pool, io_ctx, create);
}

void RGWSI_RADOS::Obj::init(const rgw_raw_obj& obj)
{
  ref.oid = obj.oid;
  ref.key = obj.loc;
  ref.pool = obj.pool;
}

int RGWSI_RADOS::Obj::open()
{
  int r = rados_svc->open_pool_ctx(ref.pool, ref.ioctx);
  if (r < 0)
    return r;

  ref.ioctx.locator_set_key(ref.key);

  return 0;
}

int RGWSI_RADOS::Obj::operate(librados::ObjectWriteOperation *op)
{
  return ref.ioctx.operate(ref.oid, op);
}

int RGWSI_RADOS::Obj::operate(librados::ObjectReadOperation *op, bufferlist *pbl)
{
  return ref.ioctx.operate(ref.oid, op, pbl);
}

int RGWSI_RADOS::Obj::aio_operate(librados::AioCompletion *c, librados::ObjectWriteOperation *op)
{
  return ref.ioctx.aio_operate(ref.oid, c, op);
}

uint64_t RGWSI_RADOS::Obj::get_last_version()
{
  return ref.ioctx.get_last_version();
}
