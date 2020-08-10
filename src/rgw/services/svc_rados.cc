// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_rados.h"

#include "include/rados/librados.hpp"
#include "common/errno.h"
#include "osd/osd_types.h"
#include "rgw/rgw_tools.h"
#include "rgw/rgw_cr_rados.h"

#include "auth/AuthRegistry.h"

#define dout_subsys ceph_subsys_rgw

RGWSI_RADOS::RGWSI_RADOS(CephContext *cct) : RGWServiceInstance(cct)
{
}

RGWSI_RADOS::~RGWSI_RADOS()
{
}

int RGWSI_RADOS::do_start()
{
  int ret = rados.init_with_context(cct);
  if (ret < 0) {
    return ret;
  }
  ret = rados.connect();
  if (ret < 0) {
    return ret;
  }

  async_processor.reset(new RGWAsyncRadosProcessor(cct, cct->_conf->rgw_num_async_rados_threads));
  async_processor->start();

  return 0;
}

void RGWSI_RADOS::shutdown()
{
  if (async_processor) {
    async_processor->stop();
  }
}

librados::Rados* RGWSI_RADOS::get_rados_handle()
{
  return &rados;
}

uint64_t RGWSI_RADOS::instance_id()
{
  return get_rados_handle()->get_instance_id();
}

int RGWSI_RADOS::open_pool_ctx(const rgw_pool& pool, librados::IoCtx& io_ctx,
                               const OpenParams& params, const Span& parent_span)
{
  return rgw_init_ioctx(get_rados_handle(), pool, io_ctx,
                        params.create,
                        params.mostly_omap, parent_span);
}

int RGWSI_RADOS::pool_iterate(librados::IoCtx& io_ctx,
                              librados::NObjectIterator& iter,
                              uint32_t num, vector<rgw_bucket_dir_entry>& objs,
                              RGWAccessListFilter *filter,
                              bool *is_truncated)
{
  if (iter == io_ctx.nobjects_end())
    return -ENOENT;

  uint32_t i;

  for (i = 0; i < num && iter != io_ctx.nobjects_end(); ++i, ++iter) {
    rgw_bucket_dir_entry e;

    string oid = iter->get_oid();
    ldout(cct, 20) << "RGWRados::pool_iterate: got " << oid << dendl;

    // fill it in with initial values; we may correct later
    if (filter && !filter->filter(oid, oid))
      continue;

    e.key = oid;
    objs.push_back(e);
  }

  if (is_truncated)
    *is_truncated = (iter != io_ctx.nobjects_end());

  return objs.size();
}

RGWSI_RADOS::Obj::Obj(Pool& pool, const string& oid) : rados_svc(pool.rados_svc)
{
  ref.pool = pool;
  ref.obj = rgw_raw_obj(pool.get_pool(), oid);
}

void RGWSI_RADOS::Obj::init(const rgw_raw_obj& obj)
{
  ref.pool = RGWSI_RADOS::Pool(rados_svc, obj.pool);
  ref.obj = obj;
}

int RGWSI_RADOS::Obj::open()
{
  int r = ref.pool.open();
  if (r < 0) {
    return r;
  }

  ref.pool.ioctx().locator_set_key(ref.obj.loc);

  return 0;
}

int RGWSI_RADOS::Obj::operate(librados::ObjectWriteOperation *op,
                              optional_yield y, int flags)
{
  return rgw_rados_operate(ref.pool.ioctx(), ref.obj.oid, op, y, flags);
}

int RGWSI_RADOS::Obj::operate(librados::ObjectReadOperation *op,
			      bufferlist *pbl, optional_yield y, int flags)
{
  return rgw_rados_operate(ref.pool.ioctx(), ref.obj.oid, op, pbl, y, flags);
}

int RGWSI_RADOS::Obj::aio_operate(librados::AioCompletion *c, librados::ObjectWriteOperation *op)
{
  return ref.pool.ioctx().aio_operate(ref.obj.oid, c, op);
}

int RGWSI_RADOS::Obj::aio_operate(librados::AioCompletion *c, librados::ObjectReadOperation *op,
                                  bufferlist *pbl)
{
  return ref.pool.ioctx().aio_operate(ref.obj.oid, c, op, pbl);
}

int RGWSI_RADOS::Obj::watch(uint64_t *handle, librados::WatchCtx2 *ctx)
{
  return ref.pool.ioctx().watch2(ref.obj.oid, handle, ctx);
}

int RGWSI_RADOS::Obj::aio_watch(librados::AioCompletion *c, uint64_t *handle, librados::WatchCtx2 *ctx)
{
  return ref.pool.ioctx().aio_watch(ref.obj.oid, c, handle, ctx);
}

int RGWSI_RADOS::Obj::unwatch(uint64_t handle)
{
  return ref.pool.ioctx().unwatch2(handle);
}

int RGWSI_RADOS::Obj::notify(bufferlist& bl, uint64_t timeout_ms,
                             bufferlist *pbl, optional_yield y)
{
  return rgw_rados_notify(ref.pool.ioctx(), ref.obj.oid, bl, timeout_ms, pbl, y);
}

void RGWSI_RADOS::Obj::notify_ack(uint64_t notify_id,
                                 uint64_t cookie,
                                 bufferlist& bl)
{
  ref.pool.ioctx().notify_ack(ref.obj.oid, notify_id, cookie, bl);
}

uint64_t RGWSI_RADOS::Obj::get_last_version()
{
  return ref.pool.ioctx().get_last_version();
}

int RGWSI_RADOS::Pool::create()
{
  librados::Rados *rad = rados_svc->get_rados_handle();
  int r = rad->pool_create(pool.name.c_str());
  if (r < 0) {
    ldout(rados_svc->cct, 0) << "WARNING: pool_create returned " << r << dendl;
    return r;
  }
  librados::IoCtx io_ctx;
  r = rad->ioctx_create(pool.name.c_str(), io_ctx);
  if (r < 0) {
    ldout(rados_svc->cct, 0) << "WARNING: ioctx_create returned " << r << dendl;
    return r;
  }
  r = io_ctx.application_enable(pg_pool_t::APPLICATION_NAME_RGW, false);
  if (r < 0) {
    ldout(rados_svc->cct, 0) << "WARNING: application_enable returned " << r << dendl;
    return r;
  }
  return 0;
}

int RGWSI_RADOS::Pool::create(const vector<rgw_pool>& pools, vector<int> *retcodes)
{
  vector<librados::PoolAsyncCompletion *> completions;
  vector<int> rets;

  librados::Rados *rad = rados_svc->get_rados_handle();
  for (auto iter = pools.begin(); iter != pools.end(); ++iter) {
    librados::PoolAsyncCompletion *c = librados::Rados::pool_async_create_completion();
    completions.push_back(c);
    auto& pool = *iter;
    int ret = rad->pool_create_async(pool.name.c_str(), c);
    rets.push_back(ret);
  }

  vector<int>::iterator riter;
  vector<librados::PoolAsyncCompletion *>::iterator citer;

  bool error = false;
  ceph_assert(rets.size() == completions.size());
  for (riter = rets.begin(), citer = completions.begin(); riter != rets.end(); ++riter, ++citer) {
    int r = *riter;
    librados::PoolAsyncCompletion *c = *citer;
    if (r == 0) {
      c->wait();
      r = c->get_return_value();
      if (r < 0) {
        ldout(rados_svc->cct, 0) << "WARNING: async pool_create returned " << r << dendl;
        error = true;
      }
    }
    c->release();
    retcodes->push_back(r);
  }
  if (error) {
    return 0;
  }

  std::vector<librados::IoCtx> io_ctxs;
  retcodes->clear();
  for (auto pool : pools) {
    io_ctxs.emplace_back();
    int ret = rad->ioctx_create(pool.name.c_str(), io_ctxs.back());
    if (ret < 0) {
      ldout(rados_svc->cct, 0) << "WARNING: ioctx_create returned " << ret << dendl;
      error = true;
    }
    retcodes->push_back(ret);
  }
  if (error) {
    return 0;
  }

  completions.clear();
  for (auto &io_ctx : io_ctxs) {
    librados::PoolAsyncCompletion *c =
      librados::Rados::pool_async_create_completion();
    completions.push_back(c);
    int ret = io_ctx.application_enable_async(pg_pool_t::APPLICATION_NAME_RGW,
                                              false, c);
    ceph_assert(ret == 0);
  }

  retcodes->clear();
  for (auto c : completions) {
    c->wait();
    int ret = c->get_return_value();
    if (ret == -EOPNOTSUPP) {
      ret = 0;
    } else if (ret < 0) {
      ldout(rados_svc->cct, 0) << "WARNING: async application_enable returned " << ret
                    << dendl;
      error = true;
    }
    c->release();
    retcodes->push_back(ret);
  }
  return 0;
}

int RGWSI_RADOS::Pool::lookup()
{
  librados::Rados *rad = rados_svc->get_rados_handle();
  int ret = rad->pool_lookup(pool.name.c_str());
  if (ret < 0) {
    return ret;
  }

  return 0;
}

int RGWSI_RADOS::Pool::open(const OpenParams& params, const Span& parent_span)
{
  return rados_svc->open_pool_ctx(pool, state.ioctx, params, parent_span);
}

int RGWSI_RADOS::Pool::List::init(const string& marker, RGWAccessListFilter *filter)
{
  if (ctx.initialized) {
    return -EINVAL;
  }

  if (!pool) {
    return -EINVAL;
  }

  int r = pool->rados_svc->open_pool_ctx(pool->pool, ctx.ioctx);
  if (r < 0) {
    return r;
  }

  librados::ObjectCursor oc;
  if (!oc.from_str(marker)) {
    ldout(pool->rados_svc->cct, 10) << "failed to parse cursor: " << marker << dendl;
    return -EINVAL;
  }

  ctx.iter = ctx.ioctx.nobjects_begin(oc);
  ctx.filter = filter;
  ctx.initialized = true;

  return 0;
}

int RGWSI_RADOS::Pool::List::get_next(int max,
                                      std::vector<string> *oids,
                                      bool *is_truncated)
{
  if (!ctx.initialized) {
    return -EINVAL;
  }
  vector<rgw_bucket_dir_entry> objs;
  int r = pool->rados_svc->pool_iterate(ctx.ioctx, ctx.iter, max, objs, ctx.filter, is_truncated);
  if (r < 0) {
    if(r != -ENOENT) {
      ldout(pool->rados_svc->cct, 10) << "failed to list objects pool_iterate returned r=" << r << dendl;
    }
    return r;
  }

  for (auto& o : objs) {
    oids->push_back(o.key.name);
  }

  return oids->size();
}

RGWSI_RADOS::Obj RGWSI_RADOS::Handle::obj(const rgw_raw_obj& o)
{
  return RGWSI_RADOS::Obj(rados_svc, o);
}
int RGWSI_RADOS::Handle::watch_flush()
{
  librados::Rados *rad = rados_svc->get_rados_handle();
  return rad->watch_flush();
}

int RGWSI_RADOS::Handle::mon_command(std::string cmd,
                                     const bufferlist& inbl,
                                     bufferlist *outbl,
                                     std::string *outs)
{
  librados::Rados *rad = rados_svc->get_rados_handle();
  return rad->mon_command(cmd, inbl, outbl, outs);
}

int RGWSI_RADOS::Pool::List::get_marker(string *marker)
{
  if (!ctx.initialized) {
    return -EINVAL;
  }

  *marker = ctx.iter.get_cursor().to_str();
  return 0;
}

int RGWSI_RADOS::clog_warn(const string& msg)
{
  string cmd =
    "{"
      "\"prefix\": \"log\", "
      "\"level\": \"warn\", "
      "\"logtext\": [\"" + msg + "\"]"
    "}";

  bufferlist inbl;
  auto h = handle();
  return h.mon_command(cmd, inbl, nullptr, nullptr);
}

bool RGWSI_RADOS::check_secure_mon_conn() const
{
  AuthRegistry reg(cct);

  reg.refresh_config();

  std::vector<uint32_t> methods;
  std::vector<uint32_t> modes;

  reg.get_supported_methods(CEPH_ENTITY_TYPE_MON, &methods, &modes);
  ldout(cct, 20) << __func__ << "(): auth registy supported: methods=" << methods << " modes=" << modes << dendl;

  for (auto method : methods) {
    if (!reg.is_secure_method(method)) {
      ldout(cct, 20) << __func__ << "(): method " << method << " is insecure" << dendl;
      return false;
    }
  }

  for (auto mode : modes) {
    if (!reg.is_secure_mode(mode)) {
      ldout(cct, 20) << __func__ << "(): mode " << mode << " is insecure" << dendl;
      return false;
    }
  }

  return true;
}

