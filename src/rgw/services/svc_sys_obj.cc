#include "svc_sys_obj.h"
#include "svc_rados.h"

int RGWS_SysObj::create_instance(const string& conf, RGWServiceInstanceRef *instance)
{
  instance->reset(new RGWSI_SysObj(this, cct));
  return 0;
}

std::map<string, RGWServiceInstance::dependency> RGWSI_SysObj::get_deps()
{
  RGWServiceInstance::dependency dep1 = { .name = "rados",
                                          .conf = "{}" };
  RGWServiceInstance::dependency dep2 = { .name = "zone",
                                          .conf = "{}" };
  map<string, RGWServiceInstance::dependency> deps;
  deps["rados_dep"] = dep1
  deps["zone_dep"] = dep2
  return deps;
}

int RGWSI_SysObj::load(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  rados_svc = static_pointer_cast<RGWSI_RADOS>(dep_refs["rados_dep"]);
  assert(rados_svc);

  zone_svc = static_pointer_cast<RGWSI_Zone>(dep_refs["zone_dep"]);
  assert(zone_svc);

  return 0;
}

int RGWSI_SysObj::get_rados_obj(RGWSI_Zone *zone_svc,
                                rgw_raw_obj& obj,
                                RGWSI_Rados::Obj *pobj)
{
  zone_svc->canonicalize_raw_obj(&obj);

  *pobj = rados_svc->obj(obj);
  int r = pobj->open();
  if (r < 0) {
    return r;
  }

  return 0;
}

int RGWSI_SysObj::get_system_obj_state_impl(RGWSysObjectCtx *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker)
{
  if (obj.empty()) {
    return -EINVAL;
  }

  RGWSysObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_system_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs) {
    return 0;
  }

  s->obj = obj;

  int r = raw_stat(obj, &s->size, &s->mtime, &s->epoch, &s->attrset, (s->prefetch_data ? &s->data : NULL), objv_tracker);
  if (r == -ENOENT) {
    s->exists = false;
    s->has_attrs = true;
    s->mtime = real_time();
    return 0;
  }
  if (r < 0)
    return r;

  s->exists = true;
  s->has_attrs = true;
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];

  if (s->obj_tag.length())
    ldout(cct, 20) << "get_system_obj_state: setting s->obj_tag to "
                   << s->obj_tag.c_str() << dendl;
  else
    ldout(cct, 20) << "get_system_obj_state: s->obj_tag was set empty" << dendl;

  return 0;
}

int RGWSI_SysObj::get_system_obj_state(RGWSysObjectCtx *rctx, rgw_raw_obj& obj, RGWSysObjState **state, RGWObjVersionTracker *objv_tracker)
{
  int ret;

  do {
    ret = get_system_obj_state_impl(rctx, obj, state, objv_tracker);
  } while (ret == -EAGAIN);

  return ret;
}

int RGWSI_SysObj::raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                           map<string, bufferlist> *attrs, bufferlist *first_chunk,
                           RGWObjVersionTracker *objv_tracker)
{
  RGWSI_Rados::Obj rados_obj;
  int r = get_rados_obj(obj, &rados_obj);
  if (r < 0) {
    return r;
  }

  map<string, bufferlist> unfiltered_attrset;
  uint64_t size = 0;
  struct timespec mtime_ts;

  librados::ObjectReadOperation op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }
  if (attrs) {
    op.getxattrs(&unfiltered_attrset, NULL);
  }
  if (psize || pmtime) {
    op.stat2(&size, &mtime_ts, NULL);
  }
  if (first_chunk) {
    op.read(0, cct->_conf->rgw_max_chunk_size, first_chunk, NULL);
  }
  bufferlist outbl;
  r = rados_obj.operate(&op, &outbl);

  if (epoch) {
    *epoch = rados_obj.get_last_version();
  }

  if (r < 0)
    return r;

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = ceph::real_clock::from_timespec(mtime_ts);
  if (attrs) {
    filter_attrset(unfiltered_attrset, RGW_ATTR_PREFIX, attrs);
  }

  return 0;
}

int RGWSI_SysObj::stat(RGWSysObjectCtx& obj_ctx,
                       RGWSI_SysObj::SystemObject::Read::GetObjState& state,
                       rgw_raw_obj& obj,
                       map<string, bufferlist> *attrs,
                       real_time *lastmod,
                       uint64_t *obj_size,
                       RGWObjVersionTracker *objv_tracker)
{
  RGWSysObjState *astate = NULL;

  int r = get_system_obj_state(&obj_ctx, obj, &astate, objv_tracker);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  if (attrs) {
    *attrs = astate->attrset;
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      map<string, bufferlist>::iterator iter;
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
  }

  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  return 0;
}

int RGWSI_SysObj::read_obj(RGWObjectCtx& obj_ctx,
                           Obj::Read::GetObjState& read_state,
                           RGWObjVersionTracker *objv_tracker,
                           rgw_raw_obj& obj,
                           bufferlist *bl, off_t ofs, off_t end,
                           map<string, bufferlist> *attrs,
                           boost::optional<obj_version>)
{
  uint64_t len;
  librados::ObjectReadOperation op;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(&op);
  }

  ldout(cct, 20) << "rados->read ofs=" << ofs << " len=" << len << dendl;
  op.read(ofs, len, bl, NULL);

  if (attrs) {
    op.getxattrs(attrs, NULL);
  }

  RGWSI_Rados::Obj rados_obj;
  int r = get_rados_obj(obj, &rados_obj);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }
  r = rados_obj.operate(&op, NULL);
  if (r < 0) {
    ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length=" << bl->length() << dendl;
    return r;
  }
  ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length=" << bl->length() << dendl;

  uint64_t op_ver = rados_obj.get_last_version();

  if (read_state.last_ver > 0 &&
      read_state.last_ver != op_ver) {
    ldout(cct, 5) << "raced with an object write, abort" << dendl;
    return -ECANCELED;
  }

  read_state.last_ver = op_ver;

  return bl.length();
}

/**
 * Get an attribute for a system object.
 * obj: the object to get attr
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWSI_SysObj::get_attr(rgw_raw_obj& obj, std::string_view name, bufferlist *dest)
{
  RGWSI_Rados::Obj rados_obj;
  int r = get_rados_obj(obj, &rados_obj);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  librados::ObjectReadOperation op;

  int rval;
  op.getxattr(name, dest, &rval);
  
  r = rados_obj.operate(&op, nullptr);
  if (r < 0)
    return r;

  return 0;
}

void RGWSI_SysObj::Obj::invalidate_state()
{
  ctx.invalidate(obj);
}

int RGWSI_SysObj::Obj::Read::GetObjState::get_rados_obj(RGWSI_RADOS *rados_svc,
                                                   RGWSI_Zone *zone_svc,
                                                   rgw_raw_obj& obj,
                                                   RGWSI_Rados::Obj **pobj)
{
  if (!has_rados_obj) {
    zone_svc->canonicalize_raw_obj(&obj);

    rados_obj = rados_svc->obj(obj);
    int r = rados_obj.open();
    if (r < 0) {
      return r;
    }
    has_rados_obj = true;
  }
  *pobj = &rados_obj;
  return 0;
}

int RGWSI_SysObj::Obj::Read::stat()
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.obj;

  return sysobj_svc->stat(source.ctx(), state, obj, stat_params.attrs,
                          stat_params.lastmod, stat_params.obj_size,
                          stat_params.objv_tracker);
}


int RGWSI_SysObj::Obj::Read::read(int64_t ofs, int64_t end, bufferlist *bl)
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->read(source.get_ctx(), state,
                   read_params.objv_tracker,
                   obj, bl, ofs, end,
                   read_params.attrs,
                   refresh_version);
}

int RGWSI_SysObj::Obj::Read::get_attr(std::string_view name, bufferlist *dest)
{
  RGWSI_SysObj *svc = source.sysobj_svc;
  rgw_raw_obj& obj = source.get_obj();

  return svc->get_attr(obj, name, dest);
}

