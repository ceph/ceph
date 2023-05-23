// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "auth/AuthRegistry.h"

#include "common/errno.h"
#include "librados/AioCompletionImpl.h"
#include "librados/librados_asio.h"

#include "include/stringify.h"

#include "rgw_tools.h"
#include "rgw_acl_s3.h"
#include "rgw_aio_throttle.h"
#include "rgw_compression.h"
#include "common/BackTrace.h"

#define dout_subsys ceph_subsys_rgw

#define READ_CHUNK_LEN (512 * 1024)

using namespace std;

int rgw_init_ioctx(const DoutPrefixProvider *dpp,
                   librados::Rados *rados, const rgw_pool& pool,
                   librados::IoCtx& ioctx, bool create,
                   bool mostly_omap,
                   bool bulk)
{
  int r = rados->ioctx_create(pool.name.c_str(), ioctx);
  if (r == -ENOENT && create) {
    r = rados->pool_create(pool.name.c_str());
    if (r == -ERANGE) {
      ldpp_dout(dpp, 0)
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

    if (mostly_omap) {
      // set pg_autoscale_bias
      bufferlist inbl;
      float bias = g_conf().get_val<double>("rgw_rados_pool_autoscale_bias");
      int r = rados->mon_command(
	"{\"prefix\": \"osd pool set\", \"pool\": \"" +
	pool.name + "\", \"var\": \"pg_autoscale_bias\", \"val\": \"" +
	stringify(bias) + "\"}",
	inbl, NULL, NULL);
      if (r < 0) {
	ldpp_dout(dpp, 10) << __func__ << " warning: failed to set pg_autoscale_bias on "
		 << pool.name << dendl;
      }
      // set recovery_priority
      int p = g_conf().get_val<uint64_t>("rgw_rados_pool_recovery_priority");
      r = rados->mon_command(
	"{\"prefix\": \"osd pool set\", \"pool\": \"" +
	pool.name + "\", \"var\": \"recovery_priority\": \"" +
	stringify(p) + "\"}",
	inbl, NULL, NULL);
      if (r < 0) {
	ldpp_dout(dpp, 10) << __func__ << " warning: failed to set recovery_priority on "
		 << pool.name << dendl;
      }
    }
    if (bulk) {
      // set bulk
      bufferlist inbl;
      int r = rados->mon_command(
        "{\"prefix\": \"osd pool set\", \"pool\": \"" +
        pool.name + "\", \"var\": \"bulk\", \"val\": \"true\"}",
        inbl, NULL, NULL);
      if (r < 0) {
        ldpp_dout(dpp, 10) << __func__ << " warning: failed to set 'bulk' on "
                 << pool.name << dendl;
      }
    }
  } else if (r < 0) {
    return r;
  }
  if (!pool.ns.empty()) {
    ioctx.set_namespace(pool.ns);
  }
  return 0;
}

int rgw_get_rados_ref(const DoutPrefixProvider* dpp, librados::Rados* rados,
		      rgw_raw_obj obj, rgw_rados_ref* ref)
{
  ref->obj = std::move(obj);

  int r = rgw_init_ioctx(dpp, rados, ref->obj.pool,
			 ref->ioctx, true, false);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: creating ioctx (pool=" << ref->obj.pool
        << "); r=" << r << dendl;
    return r;
  }

  ref->ioctx.locator_set_key(ref->obj.loc);
  return 0;
}


map<string, bufferlist>* no_change_attrs() {
  static map<string, bufferlist> no_change;
  return &no_change;
}

int rgw_put_system_obj(const DoutPrefixProvider *dpp, RGWSI_SysObj* svc_sysobj,
                       const rgw_pool& pool, const string& oid, bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, real_time set_mtime, optional_yield y, const map<string, bufferlist> *pattrs)
{
  map<string,bufferlist> no_attrs;
  if (!pattrs) {
    pattrs = &no_attrs;
  }

  rgw_raw_obj obj(pool, oid);

  auto sysobj = svc_sysobj->get_obj(obj);
  int ret;

  if (pattrs != no_change_attrs()) {
    ret = sysobj.wop()
      .set_objv_tracker(objv_tracker)
      .set_exclusive(exclusive)
      .set_mtime(set_mtime)
      .set_attrs(*pattrs)
      .write(dpp, data, y);
  } else {
    ret = sysobj.wop()
      .set_objv_tracker(objv_tracker)
      .set_exclusive(exclusive)
      .set_mtime(set_mtime)
      .write_data(dpp, data, y);
  }

  return ret;
}

int rgw_stat_system_obj(const DoutPrefixProvider *dpp, RGWSI_SysObj* svc_sysobj,
                        const rgw_pool& pool, const std::string& key,
                        RGWObjVersionTracker *objv_tracker,
			real_time *pmtime, optional_yield y,
			std::map<std::string, bufferlist> *pattrs)
{
  rgw_raw_obj obj(pool, key);
  auto sysobj = svc_sysobj->get_obj(obj);
  return sysobj.rop()
               .set_attrs(pattrs)
               .set_last_mod(pmtime)
               .stat(y, dpp);
}


int rgw_get_system_obj(RGWSI_SysObj* svc_sysobj, const rgw_pool& pool, const string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, real_time *pmtime, optional_yield y,
                       const DoutPrefixProvider *dpp, map<string, bufferlist> *pattrs,
                       rgw_cache_entry_info *cache_info,
		       boost::optional<obj_version> refresh_version, bool raw_attrs)
{
  const rgw_raw_obj obj(pool, key);
  auto sysobj = svc_sysobj->get_obj(obj);
  auto rop = sysobj.rop();
  return rop.set_attrs(pattrs)
            .set_last_mod(pmtime)
            .set_objv_tracker(objv_tracker)
            .set_raw_attrs(raw_attrs)
            .set_cache_info(cache_info)
            .set_refresh_version(refresh_version)
            .read(dpp, &bl, y);
}

int rgw_delete_system_obj(const DoutPrefixProvider *dpp, 
                          RGWSI_SysObj *sysobj_svc, const rgw_pool& pool, const string& oid,
                          RGWObjVersionTracker *objv_tracker, optional_yield y)
{
  auto sysobj = sysobj_svc->get_obj(rgw_raw_obj{pool, oid});
  rgw_raw_obj obj(pool, oid);
  return sysobj.wop()
               .set_objv_tracker(objv_tracker)
               .remove(dpp, y);
}

int rgw_rados_operate(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation *op, bufferlist* pbl,
                      optional_yield y, int flags, const jspan_context* trace_info)
{
  // given a yield_context, call async_operate() to yield the coroutine instead
  // of blocking
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    auto bl = librados::async_operate(
      context, ioctx, oid, op, flags, yield[ec]);
    if (pbl) {
      *pbl = std::move(bl);
    }
    return -ec.value();
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    ldpp_dout(dpp, 20) << "WARNING: blocking librados call" << dendl;
#ifdef _BACKTRACE_LOGGING
    ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl;
#endif
  }
  return ioctx.operate(oid, op, nullptr, flags);
}

int rgw_rados_operate(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation *op, optional_yield y,
		      int flags, const jspan_context* trace_info)
{
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    librados::async_operate(context, ioctx, oid, op, flags, yield[ec], trace_info);
    return -ec.value();
  }
  if (is_asio_thread) {
    ldpp_dout(dpp, 20) << "WARNING: blocking librados call" << dendl;
#ifdef _BACKTRACE_LOGGING
    ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl;
#endif
  }
  return ioctx.operate(oid, op, flags, trace_info);
}

int rgw_rados_notify(const DoutPrefixProvider *dpp, librados::IoCtx& ioctx, const std::string& oid,
                     bufferlist& bl, uint64_t timeout_ms, bufferlist* pbl,
                     optional_yield y)
{
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    auto reply = librados::async_notify(context, ioctx, oid,
                                        bl, timeout_ms, yield[ec]);
    if (pbl) {
      *pbl = std::move(reply);
    }
    return -ec.value();
  }
  if (is_asio_thread) {
    ldpp_dout(dpp, 20) << "WARNING: blocking librados call" << dendl;
#ifdef _BACKTRACE_LOGGING
    ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl;
#endif
  }
  return ioctx.notify2(oid, bl, timeout_ms, pbl);
}

void rgw_filter_attrset(map<string, bufferlist>& unfiltered_attrset, const string& check_prefix,
                        map<string, bufferlist> *attrset)
{
  attrset->clear();
  map<string, bufferlist>::iterator iter;
  for (iter = unfiltered_attrset.lower_bound(check_prefix);
       iter != unfiltered_attrset.end(); ++iter) {
    if (!boost::algorithm::starts_with(iter->first, check_prefix))
      break;
    (*attrset)[iter->first] = iter->second;
  }
}

void rgw_complete_aio_completion(librados::AioCompletion* c, int r) {
  auto pc = c->pc;
  librados::CB_AioCompleteAndSafe cb(pc);
  cb(r);
}

bool rgw_check_secure_mon_conn(const DoutPrefixProvider *dpp)
{
  AuthRegistry reg(dpp->get_cct());

  reg.refresh_config();

  std::vector<uint32_t> methods;
  std::vector<uint32_t> modes;

  reg.get_supported_methods(CEPH_ENTITY_TYPE_MON, &methods, &modes);
  ldpp_dout(dpp, 20) << __func__ << "(): auth registy supported: methods=" << methods << " modes=" << modes << dendl;

  for (auto method : methods) {
    if (!reg.is_secure_method(method)) {
      ldpp_dout(dpp, 20) << __func__ << "(): method " << method << " is insecure" << dendl;
      return false;
    }
  }

  for (auto mode : modes) {
    if (!reg.is_secure_mode(mode)) {
      ldpp_dout(dpp, 20) << __func__ << "(): mode " << mode << " is insecure" << dendl;
      return false;
    }
  }

  return true;
}

int rgw_clog_warn(librados::Rados* h, const string& msg)
{
  string cmd =
    "{"
      "\"prefix\": \"log\", "
      "\"level\": \"warn\", "
      "\"logtext\": [\"" + msg + "\"]"
    "}";

  bufferlist inbl;
  return h->mon_command(cmd, inbl, nullptr, nullptr);
}

int rgw_list_pool(const DoutPrefixProvider *dpp,
		  librados::IoCtx& ioctx,
		  uint32_t max,
		  const rgw::AccessListFilter& filter,
		  std::string& marker,
		  std::vector<string> *oids,
		  bool *is_truncated)
{
  librados::ObjectCursor oc;
  if (!oc.from_str(marker)) {
    ldpp_dout(dpp, 10) << "failed to parse cursor: " << marker << dendl;
    return -EINVAL;
  }

  auto iter = ioctx.nobjects_begin(oc);
  /// Pool_iterate
  if (iter == ioctx.nobjects_end())
    return -ENOENT;

  for (; oids->size() < max && iter != ioctx.nobjects_end(); ++iter) {
    string oid = iter->get_oid();
    ldpp_dout(dpp, 20) << "RGWRados::pool_iterate: got " << oid << dendl;

    // fill it in with initial values; we may correct later
    if (filter && !filter(oid, oid))
      continue;

    oids->push_back(oid);
  }

  marker = iter.get_cursor().to_str();
  if (is_truncated)
    *is_truncated = (iter != ioctx.nobjects_end());

  return oids->size();
}
