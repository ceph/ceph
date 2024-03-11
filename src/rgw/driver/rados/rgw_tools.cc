// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"
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

map<string, bufferlist>* no_change_attrs() {
  static map<string, bufferlist> no_change;
  return &no_change;
}

int rgw_put_system_obj(const DoutPrefixProvider *dpp, RGWSI_SysObj* svc_sysobj,
                       const rgw_pool& pool, const string& oid, bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, real_time set_mtime, optional_yield y, map<string, bufferlist> *pattrs)
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
                      optional_yield y, int flags)
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
		      int flags)
{
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    librados::async_operate(context, ioctx, oid, op, flags, yield[ec]);
    return -ec.value();
  }
  if (is_asio_thread) {
    ldpp_dout(dpp, 20) << "WARNING: blocking librados call" << dendl;
#ifdef _BACKTRACE_LOGGING
    ldpp_dout(dpp, 20) << "BACKTRACE: " << __func__ << ": " << ClibBackTrace(0) << dendl;
#endif
  }
  return ioctx.operate(oid, op, flags);
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

RGWDataAccess::RGWDataAccess(rgw::sal::Driver* _driver) : driver(_driver)
{
}


int RGWDataAccess::Bucket::finish_init()
{
  auto iter = attrs.find(RGW_ATTR_ACL);
  if (iter == attrs.end()) {
    return 0;
  }

  bufferlist::const_iterator bliter = iter->second.begin();
  try {
    policy.decode(bliter);
  } catch (buffer::error& err) {
    return -EIO;
  }

  return 0;
}

int RGWDataAccess::Bucket::init(const DoutPrefixProvider *dpp, optional_yield y)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = sd->driver->get_bucket(dpp, nullptr, tenant, name, &bucket, y);
  if (ret < 0) {
    return ret;
  }

  bucket_info = bucket->get_info();
  mtime = bucket->get_modification_time();
  attrs = bucket->get_attrs();

  return finish_init();
}

int RGWDataAccess::Bucket::init(const RGWBucketInfo& _bucket_info,
				const map<string, bufferlist>& _attrs)
{
  bucket_info = _bucket_info;
  attrs = _attrs;

  return finish_init();
}

int RGWDataAccess::Bucket::get_object(const rgw_obj_key& key,
				      ObjectRef *obj) {
  obj->reset(new Object(sd, shared_from_this(), key));
  return 0;
}

int RGWDataAccess::Object::put(bufferlist& data,
			       map<string, bufferlist>& attrs,
                               const DoutPrefixProvider *dpp,
                               optional_yield y)
{
  rgw::sal::Driver* driver = sd->driver;
  CephContext *cct = driver->ctx();

  string tag;
  append_rand_alpha(cct, tag, tag, 32);

  RGWBucketInfo& bucket_info = bucket->bucket_info;

  rgw::BlockingAioThrottle aio(driver->ctx()->_conf->rgw_put_obj_min_window_size);

  std::unique_ptr<rgw::sal::Bucket> b;
  driver->get_bucket(NULL, bucket_info, &b);
  std::unique_ptr<rgw::sal::Object> obj = b->get_object(key);

  auto& owner = bucket->policy.get_owner();

  string req_id = driver->zone_unique_id(driver->get_new_req_id());

  std::unique_ptr<rgw::sal::Writer> processor;
  processor = driver->get_atomic_writer(dpp, y, obj.get(),
				       owner.get_id(),
				       nullptr, olh_epoch, req_id);

  int ret = processor->prepare(y);
  if (ret < 0)
    return ret;

  rgw::sal::DataProcessor *filter = processor.get();

  CompressorRef plugin;
  boost::optional<RGWPutObj_Compress> compressor;

  const auto& compression_type = driver->get_compression_type(bucket_info.placement_rule);
  if (compression_type != "none") {
    plugin = Compressor::create(driver->ctx(), compression_type);
    if (!plugin) {
      ldpp_dout(dpp, 1) << "Cannot load plugin for compression type "
        << compression_type << dendl;
    } else {
      compressor.emplace(driver->ctx(), plugin, filter);
      filter = &*compressor;
    }
  }

  off_t ofs = 0;
  auto obj_size = data.length();

  RGWMD5Etag etag_calc;

  do {
    size_t read_len = std::min(data.length(), (unsigned int)cct->_conf->rgw_max_chunk_size);

    bufferlist bl;

    data.splice(0, read_len, &bl);
    etag_calc.update(bl);

    ret = filter->process(std::move(bl), ofs);
    if (ret < 0)
      return ret;

    ofs += read_len;
  } while (data.length() > 0);

  ret = filter->process({}, ofs);
  if (ret < 0) {
    return ret;
  }
  bool has_etag_attr = false;
  auto iter = attrs.find(RGW_ATTR_ETAG);
  if (iter != attrs.end()) {
    bufferlist& bl = iter->second;
    etag = bl.to_str();
    has_etag_attr = true;
  }

  if (!aclbl) {
    RGWAccessControlPolicy_S3 policy(cct);

    policy.create_canned(bucket->policy.get_owner(), bucket->policy.get_owner(), string()); /* default private policy */

    policy.encode(aclbl.emplace());
  }

  if (etag.empty()) {
    etag_calc.finish(&etag);
  }

  if (!has_etag_attr) {
    bufferlist etagbl;
    etagbl.append(etag);
    attrs[RGW_ATTR_ETAG] = etagbl;
  }
  attrs[RGW_ATTR_ACL] = *aclbl;

  string *puser_data = nullptr;
  if (user_data) {
    puser_data = &(*user_data);
  }

  return processor->complete(obj_size, etag,
			    &mtime, mtime,
			    attrs, delete_at,
                            nullptr, nullptr,
                            puser_data,
                            nullptr, nullptr, y,
                            rgw::sal::FLAG_LOG_OP);
}

void RGWDataAccess::Object::set_policy(const RGWAccessControlPolicy& policy)
{
  policy.encode(aclbl.emplace());
}

void rgw_complete_aio_completion(librados::AioCompletion* c, int r) {
  auto pc = c->pc;
  librados::CB_AioCompleteAndSafe cb(pc);
  cb(r);
}
