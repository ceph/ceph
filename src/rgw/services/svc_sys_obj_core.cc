// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "svc_sys_obj_core.h"
#include "rgw/rgw_tools.h"
#include "svc_zone.h"

#include "rgw/rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

using ceph::from_error_code;
using ceph::real_time;

int RGWSI_SysObj_Core_GetObjState::get_rados_obj(nr::RADOS* rados,
						 RGWSI_Zone* zone_svc,
                                                 const rgw_raw_obj& obj,
                                                 neo_obj_ref** pobj,
						 optional_yield y)
{
  if (!has_rados_obj) {
    if (obj.oid.empty()) {
      ldout(rados->cct(), 0) << "ERROR: obj.oid is empty" << dendl;
      return -EINVAL;
    }

    auto r = rgw_rados_acquire_obj(*rados, obj, y);
    if (!r) {
      return ceph::from_error_code(r.error());
    }
    rados_obj = std::move(*r);
    has_rados_obj = true;
  }
  *pobj = &rados_obj;
  return 0;
}

int RGWSI_SysObj_Core::get_rados_obj(RGWSI_Zone *zone_svc,
                                     const rgw_raw_obj& obj,
                                     neo_obj_ref* pobj,
				     optional_yield y)
{
  if (obj.oid.empty()) {
    ldout(rados->cct(), 0) << "ERROR: obj.oid is empty" << dendl;
    return -EINVAL;
  }
  auto r = rgw_rados_acquire_obj(*rados, obj, y);
  if (!r) {
    return ceph::from_error_code(r.error());
  }
  *pobj = std::move(*r);

  return 0;
}

int RGWSI_SysObj_Core::get_system_obj_state_impl(RGWSysObjectCtxBase *rctx,
                                                 const rgw_raw_obj& obj,
                                                 RGWSysObjState **state,
                                                 RGWObjVersionTracker *objv_tracker,
                                                 optional_yield y,
                                                 const DoutPrefixProvider *dpp)
{
  if (obj.empty()) {
    return -EINVAL;
  }

  RGWSysObjState *s = rctx->get_state(obj);
  ldpp_dout(dpp, 20) << "get_system_obj_state: rctx=" << (void *)rctx << " obj=" << obj << " state=" << (void *)s << " s->prefetch_data=" << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs) {
    return 0;
  }

  s->obj = obj;

  int r = raw_stat(obj, &s->size, &s->mtime, &s->epoch, &s->attrset,
                   (s->prefetch_data ? &s->data : nullptr), objv_tracker, y);
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

  if (s->obj_tag.length()) {
    ldpp_dout(dpp, 20) << "get_system_obj_state: setting s->obj_tag to " << s->obj_tag.c_str() << dendl;
  } else {
    ldpp_dout(dpp, 20) << "get_system_obj_state: s->obj_tag was set empty" << dendl;
  }

  return 0;
}

int RGWSI_SysObj_Core::get_system_obj_state(RGWSysObjectCtxBase *rctx,
                                            const rgw_raw_obj& obj,
                                            RGWSysObjState **state,
                                            RGWObjVersionTracker *objv_tracker,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp)
{
  int ret;

  do {
    ret = get_system_obj_state_impl(rctx, obj, state, objv_tracker, y, dpp);
  } while (ret == -EAGAIN);

  return ret;
}

int RGWSI_SysObj_Core::raw_stat(const rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
                                map<string, bufferlist> *attrs, bufferlist *first_chunk,
                                RGWObjVersionTracker *objv_tracker,
                                optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    return r;
  }

  uint64_t size = 0;
  real_time mtime;

  nr::ReadOp op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(op);
  }
  op.get_xattrs(attrs, nullptr);
  if (psize || pmtime) {
    op.stat(&size, &mtime, nullptr);
  }
  if (first_chunk) {
    op.read(0, cct->_conf->rgw_max_chunk_size, first_chunk, nullptr);
  }
  bufferlist outbl;
  bs::error_code ec;
  rados_obj.operate(std::move(op), &outbl, y[ec], epoch);
  r = from_error_code(ec);


  if (r < 0)
    return r;

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;

  return 0;
}

int RGWSI_SysObj_Core::stat(RGWSysObjectCtxBase& obj_ctx,
                            RGWSI_SysObj_Obj_GetObjState& _state,
                            const rgw_raw_obj& obj,
                            map<string, bufferlist> *attrs,
			    bool raw_attrs,
                            real_time *lastmod,
                            uint64_t *obj_size,
                            RGWObjVersionTracker *objv_tracker,
                            optional_yield y,
                            const DoutPrefixProvider *dpp)
{
  RGWSysObjState *astate = nullptr;

  int r = get_system_obj_state(&obj_ctx, obj, &astate, objv_tracker, y, dpp);
  if (r < 0)
    return r;

  if (!astate->exists) {
    return -ENOENT;
  }

  if (attrs) {
    if (raw_attrs) {
      *attrs = astate->attrset;
    } else {
      rgw_filter_attrset(astate->attrset, RGW_ATTR_PREFIX, attrs);
    }
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      map<string, bufferlist>::iterator iter;
      for (iter = attrs->begin(); iter != attrs->end(); ++iter) {
        ldpp_dout(dpp, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
  }

  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  return 0;
}

int RGWSI_SysObj_Core::read(RGWSysObjectCtxBase& obj_ctx,
                            RGWSI_SysObj_Obj_GetObjState& _read_state,
                            RGWObjVersionTracker *objv_tracker,
                            const rgw_raw_obj& obj,
                            bufferlist *bl, off_t ofs, off_t end,
                            map<string, bufferlist> *attrs,
			    bool raw_attrs,
                            rgw_cache_entry_info *cache_info,
                            boost::optional<obj_version>,
                            optional_yield y)
{
  auto& read_state = static_cast<GetObjState&>(_read_state);

  uint64_t len;
  nr::ReadOp op;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(op);
  }

  ldout(cct, 20) << "rados->read ofs=" << ofs << " len=" << len << dendl;
  op.read(ofs, len, bl, nullptr);

  map<string, bufferlist> unfiltered_attrset;

  if (attrs) {
    if (raw_attrs) {
      op.get_xattrs(attrs, nullptr);
    } else {
      op.get_xattrs(&unfiltered_attrset, nullptr);
    }
  }

  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }
  bs::error_code ec;
  uint64_t op_ver;
  rados_obj.operate(std::move(op), nullptr, y[ec], &op_ver);
  r = from_error_code(ec);
  if (r < 0) {
    ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length=" << bl->length() << dendl;
    return r;
  }
  ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length=" << bl->length() << dendl;


  if (read_state.last_ver > 0 &&
      read_state.last_ver != op_ver) {
    ldout(cct, 5) << "raced with an object write, abort" << dendl;
    return -ECANCELED;
  }

  if (attrs && !raw_attrs) {
    rgw_filter_attrset(unfiltered_attrset, RGW_ATTR_PREFIX, attrs);
  }

  read_state.last_ver = op_ver;

  return bl->length();
}

/**
 * Get an attribute for a system object.
 * obj: the object to get attr
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
int RGWSI_SysObj_Core::get_attr(const rgw_raw_obj& obj,
                                const char *name,
                                bufferlist *dest,
                                optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  nr::ReadOp op;

  bs::error_code ec;
  op.get_xattr(name, dest, &ec);

  rados_obj.operate(std::move(op), nullptr, y[ec]);
  if (ec)
    return from_error_code(ec);

  return 0;
}

int RGWSI_SysObj_Core::set_attrs(const rgw_raw_obj& obj,
                                 map<string, bufferlist>& attrs,
                                 map<string, bufferlist> *rmattrs,
                                 RGWObjVersionTracker *objv_tracker,
                                 optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  nr::WriteOp op;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  if (rmattrs) {
    for (auto iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const string& name = iter->first;
      op.rmxattr(name);
    }
  }

  for (auto iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name, bufferlist(bl));
  }

  if (!op.size())
    return 0;

  bufferlist bl;

  bs::error_code ec;
  rados_obj.operate(std::move(op), y[ec]);
  if (ec)
    return from_error_code(ec);

  if (objv_tracker) {
    objv_tracker->apply_write();
  }
  return 0;
}

int RGWSI_SysObj_Core::omap_get_vals(const rgw_raw_obj& obj,
                                     const string& marker,
                                     uint64_t count,
                                     std::map<string, bufferlist> *m,
                                     bool *pmore,
                                     optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  string start_after = marker;
  bool more;

  do {
    nr::ReadOp op;

    std::map<string, bufferlist> t;
    bs::error_code ec;
    op.get_omap_vals(start_after, {}, count, &t, &more, &ec);

    rados_obj.operate(std::move(op), nullptr, y[ec]);
    if (ec) {
      return from_error_code(ec);
    }
    if (t.empty()) {
      break;
    }
    count -= t.size();
    start_after = t.rbegin()->first;
    m->insert(t.begin(), t.end());
  } while (more && count > 0);

  if (pmore) {
    *pmore = more;
  }
  return 0;
}

int RGWSI_SysObj_Core::omap_get_all(const rgw_raw_obj& obj,
                                    std::map<string, bufferlist> *m,
                                    optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  static constexpr auto MAX_OMAP_GET_ENTRIES = 1024u;
  const int count = MAX_OMAP_GET_ENTRIES;
  string start_after;
  bool more;

  do {
    nr::ReadOp op;

    std::map<string, bufferlist> t;
    bs::error_code ec;
    op.get_omap_vals(start_after, {}, count, &t, &more, &ec);

    rados_obj.operate(std::move(op), nullptr, y[ec]);
    if (ec) {
      return from_error_code(ec);
    }
    if (t.empty()) {
      break;
    }
    start_after = t.rbegin()->first;
    m->insert(t.begin(), t.end());
  } while (more);
  return 0;
}

int RGWSI_SysObj_Core::omap_set(const rgw_raw_obj& obj, const std::string& key,
                                bufferlist& bl, bool must_exist,
                                optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  ldout(cct, 15) << "omap_set obj=" << obj << " key=" << key << dendl;

  map<string, bufferlist> m;
  m[key] = bl;
  nr::WriteOp op;
  if (must_exist)
    op.assert_exists();
  op.set_omap(m);
  bs::error_code ec;
  rados_obj.operate(std::move(op), y[ec]);
  return from_error_code(ec);
}

int RGWSI_SysObj_Core::omap_set(const rgw_raw_obj& obj,
                                const std::map<std::string, bufferlist>& m,
                                bool must_exist, optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  nr::WriteOp op;
  if (must_exist)
    op.assert_exists();
  op.set_omap(m);
  bs::error_code ec;
  rados_obj.operate(std::move(op), y[ec]);
  return from_error_code(ec);
}

int RGWSI_SysObj_Core::omap_del(const rgw_raw_obj& obj, const std::string& key,
                                optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  set<string> k;
  k.insert(key);

  nr::WriteOp op;

  op.rm_omap_keys(k);

  bs::error_code ec;
  rados_obj.operate(std::move(op), y[ec]);
  return from_error_code(ec);
}

int RGWSI_SysObj_Core::notify(const rgw_raw_obj& obj, bufferlist& bl,
                              uint64_t timeout_ms, bufferlist *pbl,
                              optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  bs::error_code ec;
  auto rbl = rados_obj.notify(bufferlist(bl), timeout_ms * 1ms, y[ec]);
  if (pbl)
    *pbl = std::move(rbl);

  return from_error_code(ec);
}

int RGWSI_SysObj_Core::remove(RGWSysObjectCtxBase& obj_ctx,
                              RGWObjVersionTracker *objv_tracker,
                              const rgw_raw_obj& obj,
                              optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  nr::WriteOp op;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  op.remove();
  bs::error_code ec;
  rados_obj.operate(std::move(op), y[ec]);

  return from_error_code(ec);
}

int RGWSI_SysObj_Core::write(const rgw_raw_obj& obj,
                             real_time *pmtime,
                             map<std::string, bufferlist>& attrs,
                             bool exclusive,
                             const bufferlist& data,
                             RGWObjVersionTracker *objv_tracker,
                             real_time set_mtime,
                             optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  nr::WriteOp op;

  if (exclusive) {
    op.create(true); // exclusive create
  } else {
    op.remove();
    op.set_failok();
    op.create(false);
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  if (real_clock::is_zero(set_mtime)) {
    set_mtime = real_clock::now();
  }

  op.set_mtime(set_mtime);
  op.write_full(bufferlist(data));

  bufferlist acl_bl;

  for (auto iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const string& name = iter->first;
    bufferlist& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name, bufferlist(bl));
  }

  bs::error_code ec;
  rados_obj.operate(std::move(op), y[ec]);
  if (ec) {
    return from_error_code(ec);
  }

  if (objv_tracker) {
    objv_tracker->apply_write();
  }

  if (pmtime) {
    *pmtime = set_mtime;
  }

  return 0;
}


int RGWSI_SysObj_Core::write_data(const rgw_raw_obj& obj,
                                  const bufferlist& bl,
                                  bool exclusive,
                                  RGWObjVersionTracker *objv_tracker,
                                  optional_yield y)
{
  neo_obj_ref rados_obj;
  int r = get_rados_obj(zone_svc, obj, &rados_obj, y);
  if (r < 0) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned " << r << dendl;
    return r;
  }

  nr::WriteOp op;

  if (exclusive) {
    op.create(true);
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }
  op.write_full(bufferlist(bl));
  bs::error_code ec;
  rados_obj.operate(std::move(op), y[ec]);
  if (ec)
    return from_error_code(ec);

  if (objv_tracker) {
    objv_tracker->apply_write();
  }
  return 0;
}

int RGWSI_SysObj_Core::pool_list_prefixed_objs(const rgw_pool& pool,
					       const string& prefix,
                                               std::function<void(const string&)> cb,
					       optional_yield y)
{
  bool is_truncated;

  auto rados_pool = rgw_rados_acquire_pool(*rados, pool, false, y);
  if (!rados_pool) {
    return from_error_code(rados_pool.error());
  }

  auto filter = RGWAccessListFilterPrefix(prefix);

  auto cursor = nr::Cursor::begin();
  do {
    vector<string> oids;
    static constexpr auto MAX_OBJS_DEFAULT = 1000u;
    auto ec = rgw_rados_list_pool(*rados, *rados_pool, MAX_OBJS_DEFAULT,
				 filter, cursor, &oids,
				 &is_truncated, y);
    if (ec) {
      return from_error_code(ec);
    }
    for (auto& val : oids) {
      if (val.size() > prefix.size()) {
        cb(val.substr(prefix.size()));
      }
    }
  } while (is_truncated);

  return 0;
}

int RGWSI_SysObj_Core::pool_list_objects_init(const rgw_pool& pool,
                                              const string& marker,
                                              const string& prefix,
                                              RGWSI_SysObj::Pool::ListCtx *_ctx,
					      optional_yield y)
{
  _ctx->impl.emplace<PoolListImplInfo>(rados, prefix);

  auto& ctx = static_cast<PoolListImplInfo&>(*_ctx->impl);

  auto rados_pool = rgw_rados_acquire_pool(*rados, pool, false, y);
  if (!rados_pool) {
    ldout(cct, 10) << "failed to list objects pool_iterate_begin() returned ec="
		   << rados_pool.error().message() << dendl;
    return from_error_code(rados_pool.error());
  }

  ctx.pool = std::move(*rados_pool);
  auto cursor = nr::Cursor::from_str(marker);
  if (!cursor) {
    ldout(cct, 10) << "failed to list objects pool_iterate_begin() returned r="
		   << -EINVAL << dendl;
    return -EINVAL;
  }

  return 0;
}

int RGWSI_SysObj_Core::pool_list_objects_next(RGWSI_SysObj::Pool::ListCtx& _ctx,
                                              int max,
                                              vector<string> *oids,
                                              bool *is_truncated,
					      optional_yield y)
{
  if (!_ctx.impl) {
    return -EINVAL;
  }
  auto& ctx = static_cast<PoolListImplInfo&>(*_ctx.impl);

  auto ec = rgw_rados_list_pool(*rados, ctx.pool, max,
				ctx.filter, ctx.cursor, oids,
				is_truncated, y);


  if (ec) {
    if(ec != bs::errc::no_such_file_or_directory)
      ldout(cct, 10) << "failed to list objects pool_iterate returned ec="
		     << ec.message() << dendl;
    return from_error_code(ec);
  }

  return oids->size();
}

int RGWSI_SysObj_Core::pool_list_objects_get_marker(RGWSI_SysObj::Pool::ListCtx& _ctx,
                                                    string *marker)
{
  if (!_ctx.impl) {
    return -EINVAL;
  }

  auto& ctx = static_cast<PoolListImplInfo&>(*_ctx.impl);
  if (marker)
    *marker = ctx.cursor.to_str();
  return 0;
}
