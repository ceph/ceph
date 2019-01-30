// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/expected.h"

#include "svc_sys_obj_core.h"
#include "svc_rados.h"
#include "svc_zone.h"

#include "rgw/rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

boost::system::error_code RGWSI_SysObj_Obj_GetObjState::get_rados_obj(
  RGWSI_RADOS *rados_svc, RGWSI_Zone *zone_svc,
  const rgw_raw_obj& obj, RGWSI_RADOS::Obj **pobj,
  optional_yield y)
{
  if (!rados_obj) {
    if (obj.oid.empty()) {
      ldout(rados_svc->ctx(), 0) << "ERROR: obj.oid is empty" << dendl;
      return ceph::to_error_code(EINVAL);
    }

    auto r = rados_svc->obj(obj, y);
    if (!r)
      return r.error();
    rados_obj.emplace(std::move(*r));
  }
  *pobj = &rados_obj.get();
  return {};
}

tl::expected<RGWSI_RADOS::Obj, boost::system::error_code>
RGWSI_SysObj_Core::get_rados_obj(RGWSI_Zone *zone_svc, const rgw_raw_obj& obj,
                                 optional_yield y)
{
  if (obj.oid.empty()) {
    ldout(rados_svc->ctx(), 0) << "ERROR: obj.oid is empty" << dendl;
    return tl::unexpected(ceph::to_error_code(EINVAL));
  }

  return TRY(rados_svc->obj(obj, y));
}

boost::system::error_code
RGWSI_SysObj_Core::get_system_obj_state_impl(RGWSysObjectCtxBase *rctx,
                                             const rgw_raw_obj& obj,
                                             RGWSysObjState **state,
                                             RGWObjVersionTracker *objv_tracker,
                                             optional_yield y)
{
  if (obj.empty()) {
    return ceph::to_error_code(EINVAL);
  }

  RGWSysObjState *s = rctx->get_state(obj);
  ldout(cct, 20) << "get_system_obj_state: rctx=" << (void *)rctx << " obj="
                 << obj << " state=" << (void *)s << " s->prefetch_data="
                 << s->prefetch_data << dendl;
  *state = s;
  if (s->has_attrs) {
    return {};
  }

  s->obj = obj;

  auto r = raw_stat(obj, &s->size, &s->mtime, &s->epoch, &s->attrset,
                   (s->prefetch_data ? &s->data : nullptr), objv_tracker, y);
  if (r == boost::system::errc::no_such_file_or_directory) {
    s->exists = false;
    s->has_attrs = true;
    s->mtime = real_time();
    return {};
  }
  if (r)
    return r;

  s->exists = true;
  s->has_attrs = true;
  s->obj_tag = s->attrset[RGW_ATTR_ID_TAG];

  if (s->obj_tag.length())
    ldout(cct, 20) << "get_system_obj_state: setting s->obj_tag to "
                   << s->obj_tag.c_str() << dendl;
  else
    ldout(cct, 20) << "get_system_obj_state: s->obj_tag was set empty" << dendl;

  return {};
}

boost::system::error_code
RGWSI_SysObj_Core::get_system_obj_state(RGWSysObjectCtxBase *rctx,
                                        const rgw_raw_obj& obj,
                                        RGWSysObjState **state,
                                        RGWObjVersionTracker *objv_tracker,
                                        optional_yield y)
{
  boost::system::error_code ret;

  do {
    ret = get_system_obj_state_impl(rctx, obj, state, objv_tracker, y);
  } while (ret == boost::system::errc::resource_unavailable_try_again);

  return ret;
}

boost::system::error_code RGWSI_SysObj_Core::
raw_stat(const rgw_raw_obj& obj, uint64_t *psize,
         real_time *pmtime, uint64_t *epoch,
         boost::container::flat_map<std::string,ceph::buffer::list> *attrs,
         ceph::buffer::list *first_chunk,
         RGWObjVersionTracker *objv_tracker,
         optional_yield y)
{
  auto robj = get_rados_obj(zone_svc, obj, y);
  if (!robj)
    return robj.error();
  auto rados_obj = std::move(*robj);

  uint64_t size = 0;
  ceph::real_time mtime;

  RADOS::ReadOp op;
  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(op);
  }
  op.get_xattrs(attrs);
  if (psize || pmtime) {
    op.stat(&size, &mtime);
  }
  if (first_chunk) {
    op.read(0, cct->_conf->rgw_max_chunk_size, first_chunk);
  }
  auto r = rados_obj.operate(std::move(op), nullptr, y, epoch);

  if (r)
    return r;

  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;

  return {};
}

boost::system::error_code RGWSI_SysObj_Core::
stat(RGWSysObjectCtxBase& obj_ctx,
     GetObjState& state,
     const rgw_raw_obj& obj,
     boost::container::flat_map<std::string, ceph::bufferlist>* attrs,
     bool raw_attrs,
     real_time *lastmod,
     uint64_t *obj_size,
     RGWObjVersionTracker *objv_tracker,
     optional_yield y)
{
  RGWSysObjState *astate = nullptr;

  auto r = get_system_obj_state(&obj_ctx, obj, &astate, objv_tracker, y);
  if (r)
    return r;

  if (!astate->exists) {
    return ceph::to_error_code(ENOENT);
  }

  if (attrs) {
    if (raw_attrs) {
      *attrs = astate->attrset;
    } else {
      rgw_filter_attrset(astate->attrset, RGW_ATTR_PREFIX, attrs);
    }
    if (cct->_conf->subsys.should_gather<ceph_subsys_rgw, 20>()) {
      for (auto iter = attrs->begin(); iter != attrs->end(); ++iter) {
        ldout(cct, 20) << "Read xattr: " << iter->first << dendl;
      }
    }
  }

  if (obj_size)
    *obj_size = astate->size;
  if (lastmod)
    *lastmod = astate->mtime;

  return {};
}

boost::system::error_code
RGWSI_SysObj_Core::read(RGWSysObjectCtxBase& obj_ctx,
                        GetObjState& read_state,
                        RGWObjVersionTracker *objv_tracker,
                        const rgw_raw_obj& obj,
                        bufferlist *bl, off_t ofs, off_t end,
                        boost::container::flat_map<std::string, ceph::buffer::list> *attrs,
                        bool raw_attrs,
                        rgw_cache_entry_info *cache_info,
                        boost::optional<obj_version>,
                        optional_yield y)
{
  uint64_t len;
  RADOS::ReadOp op;

  if (end < 0)
    len = 0;
  else
    len = end - ofs + 1;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_read(op);
  }

  ldout(cct, 20) << "rados->read ofs=" << ofs << " len=" << len << dendl;
  op.read(ofs, len, bl);

  boost::container::flat_map<std::string, ceph::buffer::list> unfiltered_attrset;

  if (attrs) {
    if (raw_attrs) {
      op.get_xattrs(attrs);
    } else {
      op.get_xattrs(&unfiltered_attrset);
    }
  }

  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }
  version_t op_ver;
  auto r = rados_obj->operate(std::move(op), nullptr, y, &op_ver);
  if (r) {
    ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length="
                   << bl->length() << dendl;
    return r;
  }
  ldout(cct, 20) << "rados_obj.operate() r=" << r << " bl.length="
                 << bl->length() << dendl;


  if (read_state.last_ver > 0 &&
      read_state.last_ver != op_ver) {
    ldout(cct, 5) << "raced with an object write, abort" << dendl;
    return ceph::to_error_code(ECANCELED);
  }

  if (attrs && !raw_attrs) {
    rgw_filter_attrset(unfiltered_attrset, RGW_ATTR_PREFIX, attrs);
  }

  read_state.last_ver = op_ver;

  return {};
}

/**
 * Get an attribute for a system object.
 * obj: the object to get attr
 * name: name of the attr to retrieve
 * dest: bufferlist to store the result in
 * Returns: 0 on success, -ERR# otherwise.
 */
boost::system::error_code
RGWSI_SysObj_Core::get_attr(const rgw_raw_obj& obj,
                            const char *name,
                            bufferlist *dest,
                            optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  RADOS::ReadOp op;

  boost::system::error_code ecv;
  op.get_xattr(name, dest, &ecv);

  auto r = rados_obj->operate(std::move(op), nullptr, y);
  return !r ? ecv : r;
}

boost::system::error_code
RGWSI_SysObj_Core::set_attrs(const rgw_raw_obj& obj,
                             boost::container::flat_map<std::string, ceph::buffer::list>& attrs,
                             boost::container::flat_map<std::string, ceph::buffer::list> *rmattrs,
                             RGWObjVersionTracker *objv_tracker,
                             optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  RADOS::WriteOp op;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  if (rmattrs) {
    for (auto iter = rmattrs->begin(); iter != rmattrs->end(); ++iter) {
      const auto& name = iter->first;
      op.rmxattr(name);
    }
  }

  for (auto iter = attrs.begin(); iter != attrs.end(); ++iter) {
    const auto& name = iter->first;
    auto& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name, std::move(bl));
  }

  if (!op.size())
    return {};


  return rados_obj->operate(std::move(op), y);
}

boost::system::error_code
RGWSI_SysObj_Core::omap_get_vals(const rgw_raw_obj& obj,
                                 const string& marker,
                                 uint64_t count,
                                 boost::container::flat_map<std::string, ceph::buffer::list> *m,
                                 bool *pmore,
                                 optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }


  string start_after = marker;
  bool more;

  do {
    RADOS::ReadOp op;

    boost::container::flat_map<std::string, ceph::buffer::list> t;
    boost::system::error_code ec;
    op.get_omap_vals(start_after, nullopt, count, &t, &more, &ec);
    auto r = rados_obj->operate(std::move(op), nullptr, y);
    if (r) {
      return r;
    }
    if (ec)
      return ec;

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
  return {};
}

boost::system::error_code
RGWSI_SysObj_Core::omap_get_all(const rgw_raw_obj& obj,
                                boost::container::flat_map<std::string, ceph::buffer::list> *m,
                                optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  static constexpr auto MAX_OMAP_GET_ENTRIES = 1024;
  const int count = MAX_OMAP_GET_ENTRIES;
  string start_after;
  bool more;

  do {
    RADOS::ReadOp op;

    boost::container::flat_map<std::string, ceph::buffer::list> t;
    boost::system::error_code ec;
    op.get_omap_vals(start_after, std::nullopt, count, &t, &more, &ec);

    auto r = rados_obj->operate(std::move(op), nullptr, y);
    if (r) {
      return r;
    }
    if (ec) {
      return ec;
    }
    if (t.empty()) {
      break;
    }
    start_after = t.rbegin()->first;
    m->insert(t.begin(), t.end());
  } while (more);
  return {};
}

boost::system::error_code
RGWSI_SysObj_Core::omap_set(const rgw_raw_obj& obj, const std::string& key,
                            bufferlist& bl, bool must_exist,
                            optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  ldout(cct, 15) << "omap_set obj=" << obj << " key=" << key << dendl;

  RADOS::WriteOp op;
  if (must_exist)
    op.assert_exists();
  op.set_omap({{ key, bl }});
  return rados_obj->operate(std::move(op), y);
}

boost::system::error_code
RGWSI_SysObj_Core::omap_set(const rgw_raw_obj& obj,
                            const boost::container::flat_map<std::string, ceph::buffer::list>& m,
                            bool must_exist, optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  RADOS::WriteOp op;
  if (must_exist)
    op.assert_exists();
  op.set_omap(m);
  return rados_obj->operate(std::move(op), y);
}

boost::system::error_code
RGWSI_SysObj_Core::omap_del(const rgw_raw_obj& obj, const std::string& key,
                            optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  RADOS::WriteOp op;
  op.rm_omap_keys({key});

  return rados_obj->operate(std::move(op), y);
}

boost::system::error_code
RGWSI_SysObj_Core::notify(const rgw_raw_obj& obj, bufferlist& bl,
                          std::optional<std::chrono::milliseconds> timeout,
                          bufferlist *pbl, optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  return rados_obj->notify(std::move(bl), timeout, pbl, y);
}

boost::system::error_code
RGWSI_SysObj_Core::remove(RGWSysObjectCtxBase& obj_ctx,
                          RGWObjVersionTracker *objv_tracker,
                          const rgw_raw_obj& obj,
                          optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  RADOS::WriteOp op;

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }

  op.remove();
  return rados_obj->operate(std::move(op), y);
}

boost::system::error_code
RGWSI_SysObj_Core::write(const rgw_raw_obj& obj,
                         real_time *pmtime,
                         boost::container::flat_map<std::string, ceph::buffer::list>& attrs,
                         bool exclusive,
                         const bufferlist& data,
                         RGWObjVersionTracker *objv_tracker,
                         real_time set_mtime,
                         optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  RADOS::WriteOp op;

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
    const auto& name = iter->first;
    auto& bl = iter->second;

    if (!bl.length())
      continue;

    op.setxattr(name, std::move(bl));
  }

  auto r = rados_obj->operate(std::move(op), y);
  if (r) {
    return r;
  }

  if (objv_tracker) {
    objv_tracker->apply_write();
  }

  if (pmtime) {
    *pmtime = set_mtime;
  }

  return {};
}


boost::system::error_code
RGWSI_SysObj_Core::write_data(const rgw_raw_obj& obj,
                              const bufferlist& bl,
                              bool exclusive,
                              RGWObjVersionTracker *objv_tracker,
                              optional_yield y)
{
  auto rados_obj = get_rados_obj(zone_svc, obj, y);
  if (!rados_obj) {
    ldout(cct, 20) << "get_rados_obj() on obj=" << obj << " returned "
                   << rados_obj.error() << dendl;
    return rados_obj.error();
  }

  RADOS::WriteOp op;

  if (exclusive) {
    op.create(true);
  }

  if (objv_tracker) {
    objv_tracker->prepare_op_for_write(op);
  }
  op.write_full(bufferlist(bl));
  auto r = rados_obj->operate(std::move(op), y);
  if (r)
    return r;

  if (objv_tracker) {
    objv_tracker->apply_write();
  }
  return {};
}
