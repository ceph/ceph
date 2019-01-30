// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_sys_obj.h"
#include "svc_sys_obj_core.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

namespace bs = boost::system;

RGWSysObjectCtx RGWSI_SysObj::init_obj_ctx()
{
  return RGWSysObjectCtx(this);
}

RGWSI_SysObj::Obj RGWSI_SysObj::get_obj(RGWSysObjectCtx& obj_ctx, const rgw_raw_obj& obj)
{
  return Obj(core_svc, obj_ctx, obj);
}

void RGWSI_SysObj::Obj::invalidate()
{
  ctx.invalidate(obj);
}

RGWSI_SysObj::Obj::ROp::ROp(Obj& _source) : source(_source) {}

bs::error_code RGWSI_SysObj::Obj::ROp::stat(optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->stat(source.get_ctx(), state, obj,
		   attrs, raw_attrs,
                   lastmod, obj_size,
                   objv_tracker, y);
}

bs::error_code RGWSI_SysObj::Obj::ROp::read(int64_t ofs, int64_t end, bufferlist *bl,
                                                       optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->read(source.get_ctx(), state,
                   objv_tracker,
                   obj, bl, ofs, end,
                   attrs,
		   raw_attrs,
                   cache_info,
                   refresh_version, y);
}

bs::error_code RGWSI_SysObj::Obj::ROp::get_attr(const char *name, bufferlist *dest,
                                                           optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->get_attr(obj, name, dest, y);
}

bs::error_code RGWSI_SysObj::Obj::WOp::remove(optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->remove(source.get_ctx(),
                     objv_tracker,
                     obj, y);
}

bs::error_code RGWSI_SysObj::Obj::WOp::write(bufferlist& bl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write(obj, pmtime, attrs, exclusive,
                    bl, objv_tracker, mtime, y);
}

bs::error_code RGWSI_SysObj::Obj::WOp::write_data(bufferlist& bl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write_data(obj, bl, exclusive, objv_tracker, y);
}

bs::error_code RGWSI_SysObj::Obj::WOp::write_attrs(optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->set_attrs(obj, attrs, nullptr, objv_tracker, y);
}

bs::error_code RGWSI_SysObj::Obj::WOp::write_attr(const char *name, bufferlist& bl,
                                                             optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  boost::container::flat_map<string, bufferlist> m;
  m[name] = bl;

  return svc->set_attrs(obj, m, nullptr, objv_tracker, y);
}

bs::error_code RGWSI_SysObj::Pool::list_prefixed_objs(
  const std::string& prefix, std::function<void(std::string_view)> cb) {
  auto list = pool.list(RGWAccessListFilterPrefix(prefix));

  return list.for_each(cb, null_yield);
}

bs::error_code RGWSI_SysObj::Pool::list_prefixed_objs(const std::string& prefix,
						      std::vector<std::string>* oids) {
  auto list = pool.list(RGWAccessListFilterPrefix(prefix));

  return list.get_all(oids, null_yield);
}

boost::system::error_code RGWSI_SysObj::Pool::Op::get_next(int max, vector<string> *oids, bool *is_truncated)
{
  return ctx.get_next(max, oids, is_truncated, null_yield);
}

std::string RGWSI_SysObj::Pool::Op::get_marker()
{
  return ctx.get_marker();
}

bs::error_code RGWSI_SysObj::Obj::OmapOp::get_all(boost::container::flat_map<std::string, bufferlist> *m,
                                                             optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_all(obj, m, y);
}

bs::error_code RGWSI_SysObj::Obj::OmapOp::get_vals(const string& marker, uint64_t count,
                                                              boost::container::flat_map<std::string, bufferlist> *m,
                                                              bool *pmore, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_vals(obj, marker, count, m, pmore, y);
}

bs::error_code RGWSI_SysObj::Obj::OmapOp::set(const std::string& key, bufferlist& bl,
                                                         optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(obj, key, bl, must_exist, y);
}

bs::error_code RGWSI_SysObj::Obj::OmapOp::set(const boost::container::flat_map<std::string, bufferlist>& m,
                                                         optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(obj, m, must_exist, y);
}

bs::error_code RGWSI_SysObj::Obj::OmapOp::del(const std::string& key, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_del(obj, key, y);
}

bs::error_code RGWSI_SysObj::Obj::WNOp::notify(
  bufferlist& bl,
  std::optional<std::chrono::milliseconds> timeout,
  bufferlist *pbl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->notify(obj, bl, timeout, pbl, y);
}

RGWSI_Zone *RGWSI_SysObj::get_zone_svc()
{
  return core_svc->get_zone_svc();
}
