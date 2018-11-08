#include "svc_sys_obj.h"
#include "svc_sys_obj_core.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

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

int RGWSI_SysObj::Obj::ROp::stat()
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->stat(source.get_ctx(), state, obj, attrs,
                   lastmod, obj_size,
                   objv_tracker);
}

int RGWSI_SysObj::Obj::ROp::read(int64_t ofs, int64_t end, bufferlist *bl)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->read(source.get_ctx(), state,
                   objv_tracker,
                   obj, bl, ofs, end,
                   attrs,
                   cache_info,
                   refresh_version);
}

int RGWSI_SysObj::Obj::ROp::get_attr(const char *name, bufferlist *dest)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->get_attr(obj, name, dest);
}

int RGWSI_SysObj::Obj::WOp::remove()
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->remove(source.get_ctx(),
                     objv_tracker,
                     obj);
}

int RGWSI_SysObj::Obj::WOp::write(bufferlist& bl)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write(obj, pmtime, attrs, exclusive,
                    bl, objv_tracker, mtime);
}

int RGWSI_SysObj::Obj::WOp::write_data(bufferlist& bl)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->write_data(obj, bl, exclusive, objv_tracker);
}

int RGWSI_SysObj::Obj::WOp::write_attrs()
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->set_attrs(obj, attrs, nullptr, objv_tracker);
}

int RGWSI_SysObj::Obj::WOp::write_attr(const char *name, bufferlist& bl)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.get_obj();

  map<string, bufferlist> m;
  m[name] = bl;

  return svc->set_attrs(obj, m, nullptr, objv_tracker);
}

int RGWSI_SysObj::Pool::Op::list_prefixed_objs(const string& prefix, list<string> *result)
{
  bool is_truncated;

  auto rados_pool = source.rados_svc->pool(source.pool);

  auto op = rados_pool.op();

  RGWAccessListFilterPrefix filter(prefix);

  int r = op.init(string(), &filter);
  if (r < 0) {
    return r;
  }

  do {
    list<string> oids;
#define MAX_OBJS_DEFAULT 1000
    int r = op.get_next(MAX_OBJS_DEFAULT, &oids, &is_truncated);
    if (r < 0) {
      return r;
    }
    for (auto& val : oids) {
      if (val.size() > prefix.size()) {
        result->push_back(val.substr(prefix.size()));
      }
    }
  } while (is_truncated);

  return 0;
}

int RGWSI_SysObj::Obj::OmapOp::get_all(std::map<string, bufferlist> *m)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_all(obj, m);
}

int RGWSI_SysObj::Obj::OmapOp::get_vals(const string& marker,
                                        uint64_t count,
                                        std::map<string, bufferlist> *m,
                                        bool *pmore)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_get_vals(obj, marker, count, m, pmore);
}

int RGWSI_SysObj::Obj::OmapOp::set(const std::string& key, bufferlist& bl)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(obj, key, bl, must_exist);
}

int RGWSI_SysObj::Obj::OmapOp::set(const map<std::string, bufferlist>& m)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_set(obj, m, must_exist);
}

int RGWSI_SysObj::Obj::OmapOp::del(const std::string& key)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->omap_del(obj, key);
}

int RGWSI_SysObj::Obj::WNOp::notify(bufferlist& bl,
				    uint64_t timeout_ms,
				    bufferlist *pbl)
{
  RGWSI_SysObj_Core *svc = source.core_svc;
  rgw_raw_obj& obj = source.obj;

  return svc->notify(obj, bl, timeout_ms, pbl);
}

RGWSI_Zone *RGWSI_SysObj::get_zone_svc()
{
  return core_svc->get_zone_svc();
}
