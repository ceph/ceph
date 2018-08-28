#include "svc_sys_obj_cache.h"


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
  distribute_cache(name, obj, info, REMOVE_OBJ);

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

    i.copy_all(obl);
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
                                  RGWObjVersionTracker *objv_tracker) 
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  info.xattrs = attrs;
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
      mydout(0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.remove(name);
  }

  return ret;
}

int RGWSI_SysObjCache::write(rgw_raw_obj& obj,
                             real_time *pmtime,
                             map<std::string, bufferlist>& attrs,
                             bool exclusive,
                             const bufferlist& data,
                             RGWObjVersionTracker *objv_tracker,
                             real_time set_mtime) override;
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
	mydout(0) << "ERROR: failed to distribute cache for " << obj << dendl;
    }
  } else {
    cache.remove(name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::write_data(rgw_raw_obj& obj,
                                   const bufferlist& bl,
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
  int ret = RGWSI_SysObj_Core::write_data(obj, data, ofs, exclusive, objv_tracker);
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    cache.put(name, info, NULL);
    int r = distribute_cache(name, obj, info, UPDATE_OBJ);
    if (r < 0)
      mydout(0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.remove(name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
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
  return T::distribute(normal_name, bl);
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
    mydout(0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  } catch (buffer::error& err) {
    mydout(0) << "ERROR: buffer::error" << dendl;
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
    mydout(0) << "WARNING: got unknown notification op: " << info.op << dendl;
    return -EINVAL;
  }

  return 0;
}

int RGWSI_SysObj_Cache::call_list(const std::optional<std::string>& filter, Formatter* f)
{
  cache.for_each(
    [this, &filter, f] (const string& name, const ObjectCacheEntry& entry) {
      if (!filter || name.find(*filter) != name.npos) {
	T::cache_list_dump_helper(f, name, entry.info.meta.mtime,
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
}
