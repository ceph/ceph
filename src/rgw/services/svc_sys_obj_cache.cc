#include "svc_sys_obj_cache.h"
#include "svc_zone.h"
#include "svc_notify.h"

#define dout_subsys ceph_subsys_rgw

class RGWSI_SysObj_Cache_CB : public RGWSI_Notify::CB
{
  RGWSI_SysObj_Cache *svc;
public:
  RGWSI_SysObj_Cache_CB(RGWSI_SysObj_Cache *_svc) : svc(_svc) {}
  int watch_cb(uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl) {
    return svc->watch_cb(notify_id, cookie, notifier_id, bl);
  }

  void set_enabled(bool status) {
    svc->set_enabled(status);
  }
};

std::map<string, RGWServiceInstance::dependency> RGWSI_SysObj_Cache::get_deps()
{
  map<string, RGWServiceInstance::dependency> deps = RGWSI_SysObj_Core::get_deps();

  deps["cache_notify_dep"] = { .name = "notify",
                               .conf = "{}" };
  return deps;
}

int RGWSI_SysObj_Cache::load(const string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs)
{
  int r = RGWSI_SysObj_Core::load(conf, dep_refs);
  if (r < 0) {
    return r;
  }

  notify_svc = static_pointer_cast<RGWSI_Notify>(dep_refs["cache_notify_dep"]);
  assert(notify_svc);

  cb.reset(new RGWSI_SysObj_Cache_CB(this));

  notify_svc->register_watch_cb(cb.get());

  return 0;
}

static string normal_name(rgw_pool& pool, const std::string& oid) {
  std::string buf;
  buf.reserve(pool.name.size() + pool.ns.size() + oid.size() + 2);
  buf.append(pool.name).append("+").append(pool.ns).append("+").append(oid);
  return buf;
}

void RGWSI_SysObj_Cache::normalize_pool_and_obj(rgw_pool& src_pool, const string& src_obj, rgw_pool& dst_pool, string& dst_obj)
{
  if (src_obj.size()) {
    dst_pool = src_pool;
    dst_obj = src_obj;
  } else {
    dst_pool = zone_svc->get_zone_params().domain_root;
    dst_obj = src_pool.name;
  }
}


int RGWSI_SysObj_Cache::remove(RGWSysObjectCtxBase& obj_ctx,
                               RGWObjVersionTracker *objv_tracker,
                               rgw_raw_obj& obj)

{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  string name = normal_name(pool, oid);
  cache.remove(name);

  ObjectCacheInfo info;
  int r = distribute_cache(name, obj, info, REMOVE_OBJ);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to distribute cache: r=" << r << dendl;
  }

  return RGWSI_SysObj_Core::remove(obj_ctx, objv_tracker, obj);
}

int RGWSI_SysObj_Cache::read(RGWSysObjectCtxBase& obj_ctx,
                             GetObjState& read_state,
                             RGWObjVersionTracker *objv_tracker,
                             rgw_raw_obj& obj,
                             bufferlist *obl, off_t ofs, off_t end,
                             map<string, bufferlist> *attrs,
                             rgw_cache_entry_info *cache_info,
                             boost::optional<obj_version> refresh_version)
{
  rgw_pool pool;
  string oid;
  if (ofs != 0) {
    return RGWSI_SysObj_Core::read(obj_ctx, read_state, objv_tracker,
                          obj, obl, ofs, end, attrs,
                          cache_info, refresh_version);
  }

  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  string name = normal_name(pool, oid);

  ObjectCacheInfo info;

  uint32_t flags = CACHE_FLAG_DATA;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  if (attrs)
    flags |= CACHE_FLAG_XATTRS;

  if ((cache.get(name, info, flags, cache_info) == 0) &&
      (!refresh_version || !info.version.compare(&(*refresh_version)))) {
    if (info.status < 0)
      return info.status;

    bufferlist& bl = info.data;

    bufferlist::iterator i = bl.begin();

    obl->clear();

    i.copy_all(*obl);
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    if (attrs)
      *attrs = info.xattrs;
    return obl->length();
  }
  int r = RGWSI_SysObj_Core::read(obj_ctx, read_state, objv_tracker,
                         obj, obl, ofs, end,
                         attrs, cache_info,
                         refresh_version);
  if (r < 0) {
    if (r == -ENOENT) { // only update ENOENT, we'd rather retry other errors
      info.status = r;
      cache.put(name, info, cache_info);
    }
    return r;
  }

  if (obl->length() == end + 1) {
    /* in this case, most likely object contains more data, we can't cache it */
    return r;
  }

  bufferptr p(r);
  bufferlist& bl = info.data;
  bl.clear();
  bufferlist::iterator o = obl->begin();
  o.copy_all(bl);
  info.status = 0;
  info.flags = flags;
  if (objv_tracker) {
    info.version = objv_tracker->read_version;
  }
  if (attrs) {
    info.xattrs = *attrs;
  }
  cache.put(name, info, cache_info);
  return r;
}

int RGWSI_SysObj_Cache::set_attrs(rgw_raw_obj& obj, 
                                  map<string, bufferlist>& attrs,
                                  map<string, bufferlist> *rmattrs,
                                  RGWObjVersionTracker *objv_tracker) 
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  info.xattrs = attrs;
  if (rmattrs) {
    info.rm_xattrs = *rmattrs;
  }
  info.status = 0;
  info.flags = CACHE_FLAG_MODIFY_XATTRS;
  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  int ret = RGWSI_SysObj_Core::set_attrs(obj, attrs, rmattrs, objv_tracker);
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    cache.put(name, info, NULL);
    int r = distribute_cache(name, obj, info, UPDATE_OBJ);
    if (r < 0)
      ldout(cct, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.remove(name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::write(rgw_raw_obj& obj,
                             real_time *pmtime,
                             map<std::string, bufferlist>& attrs,
                             bool exclusive,
                             const bufferlist& data,
                             RGWObjVersionTracker *objv_tracker,
                             real_time set_mtime)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  info.xattrs = attrs;
  info.status = 0;
  info.data = data;
  info.flags = CACHE_FLAG_XATTRS | CACHE_FLAG_DATA | CACHE_FLAG_META;
  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  ceph::real_time result_mtime;
  int ret = RGWSI_SysObj_Core::write(obj, &result_mtime, attrs,
                            exclusive, data,
                            objv_tracker, set_mtime);
  if (pmtime) {
    *pmtime = result_mtime;
  }
  info.meta.mtime = result_mtime;
  info.meta.size = data.length();
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    cache.put(name, info, NULL);
    // Only distribute the cache information if we did not just create
    // the object with the exclusive flag. Note: PUT_OBJ_EXCL implies
    // PUT_OBJ_CREATE. Generally speaking, when successfully creating
    // a system object with the exclusive flag it is not necessary to
    // call distribute_cache, as a) it's unclear whether other RGWs
    // will need that system object in the near-term and b) it
    // generates additional network traffic.
    if (!exclusive) {
      int r = distribute_cache(name, obj, info, UPDATE_OBJ);
      if (r < 0)
	ldout(cct, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
    }
  } else {
    cache.remove(name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::write_data(rgw_raw_obj& obj,
                                   const bufferlist& data,
                                   bool exclusive,
                                   RGWObjVersionTracker *objv_tracker)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  ObjectCacheInfo info;
  info.data = data;
  info.meta.size = data.length();
  info.status = 0;
  info.flags = CACHE_FLAG_DATA;

  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  int ret = RGWSI_SysObj_Core::write_data(obj, data, exclusive, objv_tracker);
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    cache.put(name, info, NULL);
    int r = distribute_cache(name, obj, info, UPDATE_OBJ);
    if (r < 0)
      ldout(cct, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.remove(name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *pepoch,
                                 map<string, bufferlist> *attrs, bufferlist *first_chunk,
                                 RGWObjVersionTracker *objv_tracker)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  string name = normal_name(pool, oid);

  uint64_t size;
  real_time mtime;
  uint64_t epoch;

  ObjectCacheInfo info;
  uint32_t flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  int r = cache.get(name, info, flags, NULL);
  if (r == 0) {
    if (info.status < 0)
      return info.status;

    size = info.meta.size;
    mtime = info.meta.mtime;
    epoch = info.epoch;
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    goto done;
  }
  r = RGWSI_SysObj_Core::raw_stat(obj, &size, &mtime, &epoch, &info.xattrs, first_chunk, objv_tracker);
  if (r < 0) {
    if (r == -ENOENT) {
      info.status = r;
      cache.put(name, info, NULL);
    }
    return r;
  }
  info.status = 0;
  info.epoch = epoch;
  info.meta.mtime = mtime;
  info.meta.size = size;
  info.flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker) {
    info.flags |= CACHE_FLAG_OBJV;
    info.version = objv_tracker->read_version;
  }
  cache.put(name, info, NULL);
done:
  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  if (pepoch)
    *pepoch = epoch;
  if (attrs)
    *attrs = info.xattrs;
  return 0;
}

int RGWSI_SysObj_Cache::distribute_cache(const string& normal_name, rgw_raw_obj& obj, ObjectCacheInfo& obj_info, int op)
{
  RGWCacheNotifyInfo info;

  info.op = op;

  info.obj_info = obj_info;
  info.obj = obj;
  bufferlist bl;
  encode(info, bl);
  return notify_svc->distribute(normal_name, bl);
}

int RGWSI_SysObj_Cache::watch_cb(uint64_t notify_id,
                                 uint64_t cookie,
                                 uint64_t notifier_id,
                                 bufferlist& bl)
{
  RGWCacheNotifyInfo info;

  try {
    auto iter = bl.cbegin();
    decode(info, iter);
  } catch (buffer::end_of_buffer& err) {
    ldout(cct, 0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  } catch (buffer::error& err) {
    ldout(cct, 0) << "ERROR: buffer::error" << dendl;
    return -EIO;
  }

  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(info.obj.pool, info.obj.oid, pool, oid);
  string name = normal_name(pool, oid);
  
  switch (info.op) {
  case UPDATE_OBJ:
    cache.put(name, info.obj_info, NULL);
    break;
  case REMOVE_OBJ:
    cache.remove(name);
    break;
  default:
    ldout(cct, 0) << "WARNING: got unknown notification op: " << info.op << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWSI_SysObj_Cache::set_enabled(bool status)
{
  cache.set_enabled(status);
}

bool RGWSI_SysObj_Cache::chain_cache_entry(std::initializer_list<rgw_cache_entry_info *> cache_info_entries,
                                           RGWChainedCache::Entry *chained_entry)
{
  return cache.chain_cache_entry(cache_info_entries, chained_entry);
}

void RGWSI_SysObj_Cache::register_chained_cache(RGWChainedCache *cc)
{
  cache.chain_cache(cc);
}

static void cache_list_dump_helper(Formatter* f,
                                   const std::string& name,
                                   const ceph::real_time mtime,
                                   const std::uint64_t size)
{
  f->dump_string("name", name);
  f->dump_string("mtime", ceph::to_iso_8601(mtime));
  f->dump_unsigned("size", size);
}

void RGWSI_SysObj_Cache::call_list(const std::optional<std::string>& filter, Formatter* f)
{
  cache.for_each(
    [this, &filter, f] (const string& name, const ObjectCacheEntry& entry) {
      if (!filter || name.find(*filter) != name.npos) {
	cache_list_dump_helper(f, name, entry.info.meta.mtime,
                               entry.info.meta.size);
      }
    });
}

int RGWSI_SysObj_Cache::call_inspect(const std::string& target, Formatter* f)
{
  if (const auto entry = cache.get(target)) {
    f->open_object_section("cache_entry");
    f->dump_string("name", target.c_str());
    entry->dump(f);
    f->close_section();
    return true;
  } else {
    return false;
  }
}

int RGWSI_SysObj_Cache::call_erase(const std::string& target)
{
  return cache.remove(target);
}

int RGWSI_SysObj_Cache::call_zap()
{
  cache.invalidate_all();
  return 0;
}
