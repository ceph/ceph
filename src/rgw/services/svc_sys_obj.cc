// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_sys_obj.h"
#include "svc_sys_obj_core.h"
#include "svc_zone.h"

#include "rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

RGWSI_SysObj::Obj RGWSI_SysObj::get_obj(const rgw_raw_obj& obj)
{
  return Obj(core_svc, obj);
}

RGWSI_SysObj::Obj::ROp::ROp(Obj& _source) : source(_source) {
  state.emplace<RGWSI_SysObj_Core::GetObjState>();
}

int RGWSI_SysObj::Obj::ROp::stat(optional_yield y, const DoutPrefixProvider *dpp)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->stat(*state, obj, attrs, raw_attrs,
                   lastmod, obj_size, objv_tracker, y, dpp);
}

int RGWSI_SysObj::Obj::ROp::read(const DoutPrefixProvider *dpp,
                                 int64_t ofs, int64_t end, bufferlist *bl,
                                 optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->read(dpp, *state,
                   objv_tracker,
                   obj, bl, ofs, end,
                   lastmod, obj_size,
                   attrs,
		   raw_attrs,
                   cache_info,
                   refresh_version, y);
}

int RGWSI_SysObj::Obj::ROp::get_attr(const DoutPrefixProvider *dpp,
                                     const char *name, bufferlist *dest,
                                     optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->get_attr(dpp, obj, name, dest, y);
}

int RGWSI_SysObj::Obj::WOp::remove(const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->remove(dpp, objv_tracker, obj, y);
}

int RGWSI_SysObj::Obj::WOp::write(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write(dpp, obj, pmtime, attrs, exclusive,
                    bl, objv_tracker, mtime, y);
}

int RGWSI_SysObj::Obj::WOp::write_data(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write_data(dpp, obj, bl, exclusive, objv_tracker, y);
}

int RGWSI_SysObj::Obj::WOp::write_attrs(const DoutPrefixProvider *dpp, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->set_attrs(dpp, obj, attrs, nullptr, objv_tracker, exclusive, y);
}

int RGWSI_SysObj::Obj::WOp::write_attr(const DoutPrefixProvider *dpp, const char *name, bufferlist& bl,
                                       optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  map<string, bufferlist> m;
  m[name] = bl;

  return svc->set_attrs(dpp, obj, m, nullptr, objv_tracker, exclusive, y);
}

int RGWSI_SysObj::Pool::list_prefixed_objs(const DoutPrefixProvider *dpp, const string& prefix, std::function<void(const string&)> cb)
{
  return core_svc->pool_list_prefixed_objs(dpp, pool, prefix, cb);
}

int RGWSI_SysObj::Pool::Op::init(const DoutPrefixProvider *dpp, const string& marker, const string& prefix)
{
  return source.core_svc->pool_list_objects_init(dpp, source.pool, marker, prefix, &ctx);
}

int RGWSI_SysObj::Pool::Op::get_next(const DoutPrefixProvider *dpp, int max, vector<string> *oids, bool *is_truncated)
{
  return source.core_svc->pool_list_objects_next(dpp, ctx, max, oids, is_truncated);
}

int RGWSI_SysObj::Pool::Op::get_marker(string *marker)
{
  return source.core_svc->pool_list_objects_get_marker(ctx, marker);
}

int RGWSI_SysObj::Obj::OmapOp::get_all(const DoutPrefixProvider *dpp, std::map<string, bufferlist> *m,
                                       optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_all(dpp, obj, m, y);
}

int RGWSI_SysObj::Obj::OmapOp::get_vals(const DoutPrefixProvider *dpp, 
                                        const string& marker, uint64_t count,
                                        std::map<string, bufferlist> *m,
                                        bool *pmore, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_vals(dpp, obj, marker, count, m, pmore, y);
}

int RGWSI_SysObj::Obj::OmapOp::set(const DoutPrefixProvider *dpp, const std::string& key, bufferlist& bl,
                                   optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(dpp, obj, key, bl, must_exist, y);
}

int RGWSI_SysObj::Obj::OmapOp::set(const DoutPrefixProvider *dpp, const map<std::string, bufferlist>& m,
                                   optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(dpp, obj, m, must_exist, y);
}

int RGWSI_SysObj::Obj::OmapOp::del(const DoutPrefixProvider *dpp, const std::string& key, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_del(dpp, obj, key, y);
}

int RGWSI_SysObj::Obj::WNOp::notify(const DoutPrefixProvider *dpp, bufferlist& bl, uint64_t timeout_ms,
                                    bufferlist *pbl, optional_yield y)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->notify(dpp, obj, bl, timeout_ms, pbl, y);
}

RGWSI_Zone *RGWSI_SysObj::get_zone_svc()
{
  return core_svc->get_zone_svc();
}
