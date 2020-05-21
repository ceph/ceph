// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/errno.h"
#include "common/safe_io.h"
#include "librados/librados_asio.h"
#include "common/async/yield_context.h"

#include "include/types.h"
#include "include/stringify.h"

#include "rgw_common.h"
#include "rgw_rados.h"
#include "rgw_tools.h"
#include "rgw_acl_s3.h"
#include "rgw_op.h"
#include "rgw_putobj_processor.h"
#include "rgw_aio_throttle.h"
#include "rgw_compression.h"
#include "rgw_zone.h"
#include "osd/osd_types.h"

#include "services/svc_sys_obj.h"
#include "services/svc_zone_utils.h"

#define dout_subsys ceph_subsys_rgw
#define dout_context g_ceph_context

#define READ_CHUNK_LEN (512 * 1024)

static std::map<std::string, std::string>* ext_mime_map;

int rgw_init_ioctx(librados::Rados *rados, const rgw_pool& pool,
                   librados::IoCtx& ioctx, bool create,
		   bool mostly_omap)
{
  int r = rados->ioctx_create(pool.name.c_str(), ioctx);
  if (r == -ENOENT && create) {
    r = rados->pool_create(pool.name.c_str());
    if (r == -ERANGE) {
      dout(0)
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
	pool.name + "\", \"var\": \"pg_autoscale_bias\": \"" +
	stringify(bias) + "\"}",
	inbl, NULL, NULL);
      if (r < 0) {
	dout(10) << __func__ << " warning: failed to set pg_autoscale_bias on "
		 << pool.name << dendl;
      }
      // set pg_num_min
      int min = g_conf().get_val<uint64_t>("rgw_rados_pool_pg_num_min");
      r = rados->mon_command(
	"{\"prefix\": \"osd pool set\", \"pool\": \"" +
	pool.name + "\", \"var\": \"pg_num_min\": \"" +
	stringify(min) + "\"}",
	inbl, NULL, NULL);
     if (r < 0) {
       dout(10) << __func__ << " warning: failed to set pg_num_min on "
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

int rgw_put_system_obj(RGWRados *rgwstore, const rgw_pool& pool, const string& oid, bufferlist& data, bool exclusive,
                       RGWObjVersionTracker *objv_tracker, real_time set_mtime, map<string, bufferlist> *pattrs)
{
  map<string,bufferlist> no_attrs;
  if (!pattrs) {
    pattrs = &no_attrs;
  }

  rgw_raw_obj obj(pool, oid);

  auto obj_ctx = rgwstore->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(obj);
  int ret = sysobj.wop()
                  .set_objv_tracker(objv_tracker)
                  .set_exclusive(exclusive)
                  .set_mtime(set_mtime)
                  .set_attrs(*pattrs)
                  .write(data);

  if (ret == -ENOENT) {
    ret = rgwstore->create_pool(pool);
    if (ret >= 0) {
      ret = sysobj.wop()
                  .set_objv_tracker(objv_tracker)
                  .set_exclusive(exclusive)
                  .set_mtime(set_mtime)
                  .set_attrs(*pattrs)
                  .write(data);
    }
  }

  return ret;
}

int rgw_get_system_obj(RGWRados *rgwstore, RGWSysObjectCtx& obj_ctx, const rgw_pool& pool, const string& key, bufferlist& bl,
                       RGWObjVersionTracker *objv_tracker, real_time *pmtime, map<string, bufferlist> *pattrs,
                       rgw_cache_entry_info *cache_info, boost::optional<obj_version> refresh_version)
{
  bufferlist::iterator iter;
  int request_len = READ_CHUNK_LEN;
  rgw_raw_obj obj(pool, key);

  obj_version original_readv;
  if (objv_tracker && !objv_tracker->read_version.empty()) {
    original_readv = objv_tracker->read_version;
  }

  do {
    auto sysobj = obj_ctx.get_obj(obj);
    auto rop = sysobj.rop();

    int ret = rop.set_attrs(pattrs)
                 .set_last_mod(pmtime)
                 .set_objv_tracker(objv_tracker)
                 .stat();
    if (ret < 0)
      return ret;

    ret = rop.set_cache_info(cache_info)
             .set_refresh_version(refresh_version)
             .read(&bl);
    if (ret == -ECANCELED) {
      /* raced, restart */
      if (!original_readv.empty()) {
        /* we were asked to read a specific obj_version, failed */
        return ret;
      }
      if (objv_tracker) {
        objv_tracker->read_version.clear();
      }
      sysobj.invalidate();
      continue;
    }
    if (ret < 0)
      return ret;

    if (ret < request_len)
      break;
    bl.clear();
    request_len *= 2;
  } while (true);

  return 0;
}

int rgw_delete_system_obj(RGWRados *rgwstore, const rgw_pool& pool, const string& oid,
                          RGWObjVersionTracker *objv_tracker)
{
  auto obj_ctx = rgwstore->svc.sysobj->init_obj_ctx();
  auto sysobj = obj_ctx.get_obj(rgw_raw_obj{pool, oid});
  rgw_raw_obj obj(pool, oid);
  return sysobj.wop()
               .set_objv_tracker(objv_tracker)
               .remove();
}

thread_local bool is_asio_thread = false;

int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation *op, bufferlist* pbl,
                      int flags, optional_yield y)
{
#ifdef HAVE_BOOST_CONTEXT
  // given a yield_context, call async_operate() to yield the coroutine instead
  // of blocking
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    auto bl = librados::async_operate(context, ioctx, oid, op, flags, yield[ec]);
    if (pbl) {
      *pbl = std::move(bl);
    }
    return -ec.value();
  }
  // work on asio threads should be asynchronous, so warn when they block
  if (is_asio_thread) {
    dout(20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  return ioctx.operate(oid, op, nullptr, flags);
}

int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectReadOperation *op, bufferlist* pbl,
                      optional_yield y)
{
  return rgw_rados_operate(ioctx, oid, op, pbl, 0, y);
}

int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation *op, int flags, optional_yield y)
{
#ifdef HAVE_BOOST_CONTEXT
  if (y) {
    auto& context = y.get_io_context();
    auto& yield = y.get_yield_context();
    boost::system::error_code ec;
    librados::async_operate(context, ioctx, oid, op, flags, yield[ec]);
    return -ec.value();
  }
  if (is_asio_thread) {
    dout(20) << "WARNING: blocking librados call" << dendl;
  }
#endif
  return ioctx.operate(oid, op, flags);
}

int rgw_rados_operate(librados::IoCtx& ioctx, const std::string& oid,
                      librados::ObjectWriteOperation *op, optional_yield y)
{
  return rgw_rados_operate(ioctx, oid, op, 0, y);
}

void parse_mime_map_line(const char *start, const char *end)
{
  char line[end - start + 1];
  strncpy(line, start, end - start);
  line[end - start] = '\0';
  char *l = line;
#define DELIMS " \t\n\r"

  while (isspace(*l))
    l++;

  char *mime = strsep(&l, DELIMS);
  if (!mime)
    return;

  char *ext;
  do {
    ext = strsep(&l, DELIMS);
    if (ext && *ext) {
      (*ext_mime_map)[ext] = mime;
    }
  } while (ext);
}


void parse_mime_map(const char *buf)
{
  const char *start = buf, *end = buf;
  while (*end) {
    while (*end && *end != '\n') {
      end++;
    }
    parse_mime_map_line(start, end);
    end++;
    start = end;
  }
}

static int ext_mime_map_init(CephContext *cct, const char *ext_map)
{
  int fd = open(ext_map, O_RDONLY);
  char *buf = NULL;
  int ret;
  if (fd < 0) {
    ret = -errno;
    ldout(cct, 0) << __func__ << " failed to open file=" << ext_map
                  << " : " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  struct stat st;
  ret = fstat(fd, &st);
  if (ret < 0) {
    ret = -errno;
    ldout(cct, 0) << __func__ << " failed to stat file=" << ext_map
                  << " : " << cpp_strerror(-ret) << dendl;
    goto done;
  }

  buf = (char *)malloc(st.st_size + 1);
  if (!buf) {
    ret = -ENOMEM;
    ldout(cct, 0) << __func__ << " failed to allocate buf" << dendl;
    goto done;
  }

  ret = safe_read(fd, buf, st.st_size + 1);
  if (ret != st.st_size) {
    // huh? file size has changed?
    ldout(cct, 0) << __func__ << " raced! will retry.." << dendl;
    free(buf);
    close(fd);
    return ext_mime_map_init(cct, ext_map);
  }
  buf[st.st_size] = '\0';

  parse_mime_map(buf);
  ret = 0;
done:
  free(buf);
  close(fd);
  return ret;
}

const char *rgw_find_mime_by_ext(string& ext)
{
  map<string, string>::iterator iter = ext_mime_map->find(ext);
  if (iter == ext_mime_map->end())
    return NULL;

  return iter->second.c_str();
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

RGWDataAccess::RGWDataAccess(RGWRados *_store) : store(_store)
{
  sysobj_ctx = std::make_unique<RGWSysObjectCtx>(store->svc.sysobj->init_obj_ctx());
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

int RGWDataAccess::Bucket::init()
{
  int ret = sd->store->get_bucket_info(*sd->sysobj_ctx,
				       tenant, name,
				       bucket_info,
				       &mtime,
				       &attrs);
  if (ret < 0) {
    return ret;
  }

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
			       map<string, bufferlist>& attrs)
{
  RGWRados *store = sd->store;
  CephContext *cct = store->ctx();

  string tag;
  append_rand_alpha(cct, tag, tag, 32);

  RGWBucketInfo& bucket_info = bucket->bucket_info;

  using namespace rgw::putobj;
  rgw::AioThrottle aio(store->ctx()->_conf->rgw_put_obj_min_window_size);

  RGWObjectCtx obj_ctx(store);
  rgw_obj obj(bucket_info.bucket, key);

  auto& owner = bucket->policy.get_owner();

  string req_id = store->svc.zone_utils->unique_id(store->get_new_req_id());

  AtomicObjectProcessor processor(&aio, store, bucket_info,
                                  nullptr,
                                  owner.get_id(),
                                  obj_ctx, obj, olh_epoch, req_id);

  int ret = processor.prepare();
  if (ret < 0)
    return ret;

  using namespace rgw::putobj;

  DataProcessor *filter = &processor;

  CompressorRef plugin;
  boost::optional<RGWPutObj_Compress> compressor;

  const auto& compression_type = store->svc.zone->get_zone_params().get_compression_type(bucket_info.placement_rule);
  if (compression_type != "none") {
    plugin = Compressor::create(store->ctx(), compression_type);
    if (!plugin) {
      ldout(store->ctx(), 1) << "Cannot load plugin for compression type "
        << compression_type << dendl;
    } else {
      compressor.emplace(store->ctx(), plugin, filter);
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

  return processor.complete(obj_size, etag,
			    &mtime, mtime,
			    attrs, delete_at,
                            nullptr, nullptr,
                            puser_data,
                            nullptr, nullptr);
}

void RGWDataAccess::Object::set_policy(const RGWAccessControlPolicy& policy)
{
  policy.encode(aclbl.emplace());
}

int rgw_tools_init(CephContext *cct)
{
  ext_mime_map = new std::map<std::string, std::string>;
  ext_mime_map_init(cct, cct->_conf->rgw_mime_types_file.c_str());
  // ignore errors; missing mime.types is not fatal
  return 0;
}

void rgw_tools_cleanup()
{
  delete ext_mime_map;
  ext_mime_map = nullptr;
}
