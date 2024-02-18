
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/admin_socket.h"

#include "svc_sys_obj_cache.h"
#include "svc_zone.h"
#include "svc_notify.h"

#include "rgw_zone.h"
#include "rgw_tools.h"

#define dout_subsys ceph_subsys_rgw

using namespace std;

class RGWSI_SysObj_Cache_CB : public RGWSI_Notify::CB
{
  RGWSI_SysObj_Cache *svc;
public:
  RGWSI_SysObj_Cache_CB(RGWSI_SysObj_Cache *_svc) : svc(_svc) {}
  int watch_cb(const DoutPrefixProvider *dpp,
               uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl) {
    return svc->watch_cb(dpp, notify_id, cookie, notifier_id, bl);
  }

  void set_enabled(bool status) {
    svc->set_enabled(status);
  }
};

int RGWSI_SysObj_Cache::do_start(optional_yield y, const DoutPrefixProvider *dpp)
{
  int r = asocket.start();
  if (r < 0) {
    return r;
  }

  r = RGWSI_SysObj_Core::do_start(y, dpp);
  if (r < 0) {
    return r;
  }

  r = notify_svc->start(y, dpp);
  if (r < 0) {
    return r;
  }

  assert(notify_svc->is_started());

  cb.reset(new RGWSI_SysObj_Cache_CB(this));

  notify_svc->register_watch_cb(cb.get());

  return 0;
}

void RGWSI_SysObj_Cache::shutdown()
{
  asocket.shutdown();
  RGWSI_SysObj_Core::shutdown();
}

static string normal_name(rgw_pool& pool, const std::string& oid) {
  std::string buf;
  buf.reserve(pool.name.size() + pool.ns.size() + oid.size() + 2);
  buf.append(pool.name).append("+").append(pool.ns).append("+").append(oid);
  return buf;
}

void RGWSI_SysObj_Cache::normalize_pool_and_obj(const rgw_pool& src_pool, const string& src_obj, rgw_pool& dst_pool, string& dst_obj)
{
  if (src_obj.size()) {
    dst_pool = src_pool;
    dst_obj = src_obj;
  } else {
    dst_pool = zone_svc->get_zone_params().domain_root;
    dst_obj = src_pool.name;
  }
}


int RGWSI_SysObj_Cache::remove(const DoutPrefixProvider *dpp, 
                               RGWObjVersionTracker *objv_tracker,
                               const rgw_raw_obj& obj,
                               optional_yield y)

{
  int r = RGWSI_SysObj_Core::remove(dpp, objv_tracker, obj, y);
  if (r < 0) {
    return r;
  }

  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  string name = normal_name(pool, oid);
  cache.invalidate_remove(dpp, name);

  ObjectCacheInfo info;
  r = distribute_cache(dpp, name, obj, info, INVALIDATE_OBJ, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: " << __func__ << "(): failed to distribute cache: r=" << r << dendl;
  } // not fatal

  return 0;
}

int RGWSI_SysObj_Cache::read(const DoutPrefixProvider *dpp,
                             RGWSI_SysObj_Obj_GetObjState& read_state,
                             RGWObjVersionTracker *objv_tracker,
                             const rgw_raw_obj& obj,
                             bufferlist *obl, off_t ofs, off_t end,
                             ceph::real_time* pmtime, uint64_t* psize,
                             map<string, bufferlist> *attrs,
			     bool raw_attrs,
                             rgw_cache_entry_info *cache_info,
                             boost::optional<obj_version> refresh_version,
                             optional_yield y)
{
  rgw_pool pool;
  string oid;
  if (ofs != 0) {
    return RGWSI_SysObj_Core::read(dpp, read_state, objv_tracker, obj, obl,
                                   ofs, end, pmtime, psize, attrs, raw_attrs,
                                   cache_info, refresh_version, y);
  }

  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  string name = normal_name(pool, oid);

  ObjectCacheInfo info;

  uint32_t flags = (end != 0 ? CACHE_FLAG_DATA : 0);
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  if (pmtime || psize)
    flags |= CACHE_FLAG_META;
  if (attrs)
    flags |= CACHE_FLAG_XATTRS;
  
  int r = cache.get(dpp, name, info, flags, cache_info);
  if (r == 0 &&
      (!refresh_version || !info.version.compare(&(*refresh_version)))) {
    if (info.status < 0)
      return info.status;

    bufferlist& bl = info.data;

    bufferlist::iterator i = bl.begin();

    obl->clear();

    i.copy_all(*obl);
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    if (pmtime) {
      *pmtime = info.meta.mtime;
    }
    if (psize) {
      *psize = info.meta.size;
    }
    if (attrs) {
      if (raw_attrs) {
	*attrs = info.xattrs;
      } else {
	rgw_filter_attrset(info.xattrs, RGW_ATTR_PREFIX, attrs);
      }
    }
    return obl->length();
  }
  if(r == -ENODATA)
    return -ENOENT;

  // if we only ask for one of mtime or size, ask for the other too so we can
  // satisfy CACHE_FLAG_META
  uint64_t size = 0;
  real_time mtime;
  if (pmtime) {
    if (!psize) {
      psize = &size;
    }
  } else if (psize) {
    if (!pmtime) {
      pmtime = &mtime;
    }
  }

  map<string, bufferlist> unfiltered_attrset;
  r = RGWSI_SysObj_Core::read(dpp, read_state, objv_tracker,
                         obj, obl, ofs, end, pmtime, psize,
			 (attrs ? &unfiltered_attrset : nullptr),
			 true, /* cache unfiltered attrs */
			 cache_info,
                         refresh_version, y);
  if (r < 0) {
    if (r == -ENOENT) { // only update ENOENT, we'd rather retry other errors
      info.status = r;
      cache.put(dpp, name, info, cache_info);
    }
    return r;
  }

  if (obl->length() == end + 1) {
    /* in this case, most likely object contains more data, we can't cache it */
    flags &= ~CACHE_FLAG_DATA;
  } else {
    bufferptr p(r);
    bufferlist& bl = info.data;
    bl.clear();
    bufferlist::iterator o = obl->begin();
    o.copy_all(bl);
  }

  info.status = 0;
  info.flags = flags;
  if (objv_tracker) {
    info.version = objv_tracker->read_version;
  }
  if (pmtime) {
    info.meta.mtime = *pmtime;
  }
  if (psize) {
    info.meta.size = *psize;
  }
  if (attrs) {
    info.xattrs = std::move(unfiltered_attrset);
    if (raw_attrs) {
      *attrs = info.xattrs;
    } else {
      rgw_filter_attrset(info.xattrs, RGW_ATTR_PREFIX, attrs);
    }
  }
  cache.put(dpp, name, info, cache_info);
  return r;
}

int RGWSI_SysObj_Cache::get_attr(const DoutPrefixProvider *dpp,
                                 const rgw_raw_obj& obj,
                                 const char *attr_name,
                                 bufferlist *dest,
                                 optional_yield y)
{
  rgw_pool pool;
  string oid;

  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  string name = normal_name(pool, oid);

  ObjectCacheInfo info;

  uint32_t flags = CACHE_FLAG_XATTRS;

  int r = cache.get(dpp, name, info, flags, nullptr);
  if (r == 0) {
    if (info.status < 0)
      return info.status;

    auto iter = info.xattrs.find(attr_name);
    if (iter == info.xattrs.end()) {
      return -ENODATA;
    }

    *dest = iter->second;
    return dest->length();
  } else if (r == -ENODATA) {
    return -ENOENT;
  }
  /* don't try to cache this one */
  return RGWSI_SysObj_Core::get_attr(dpp, obj, attr_name, dest, y);
}

int RGWSI_SysObj_Cache::set_attrs(const DoutPrefixProvider *dpp, 
                                  const rgw_raw_obj& obj, 
                                  map<string, bufferlist>& attrs,
                                  map<string, bufferlist> *rmattrs,
                                  RGWObjVersionTracker *objv_tracker,
                                  bool exclusive, optional_yield y)
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
  int ret = RGWSI_SysObj_Core::set_attrs(dpp, obj, attrs, rmattrs, objv_tracker, exclusive, y);
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    if (objv_tracker && objv_tracker->read_version.ver) {
      info.version = objv_tracker->read_version;
      info.flags |= CACHE_FLAG_OBJV;
    }
    cache.put(dpp, name, info, NULL);
    int r = distribute_cache(dpp, name, obj, info, UPDATE_OBJ, y);
    if (r < 0)
      ldpp_dout(dpp, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.invalidate_remove(dpp, name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::write(const DoutPrefixProvider *dpp, 
                             const rgw_raw_obj& obj,
                             real_time *pmtime,
                             map<std::string, bufferlist>& attrs,
                             bool exclusive,
                             const bufferlist& data,
                             RGWObjVersionTracker *objv_tracker,
                             real_time set_mtime,
                             optional_yield y)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  info.xattrs = attrs;
  info.status = 0;
  info.data = data;
  info.flags = CACHE_FLAG_XATTRS | CACHE_FLAG_DATA | CACHE_FLAG_META;
  ceph::real_time result_mtime;
  int ret = RGWSI_SysObj_Core::write(dpp, obj, &result_mtime, attrs,
                                     exclusive, data,
                                     objv_tracker, set_mtime, y);
  if (pmtime) {
    *pmtime = result_mtime;
  }
  if (objv_tracker && objv_tracker->read_version.ver) {
    info.version = objv_tracker->read_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  info.meta.mtime = result_mtime;
  info.meta.size = data.length();
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    cache.put(dpp, name, info, NULL);
    int r = distribute_cache(dpp, name, obj, info, UPDATE_OBJ, y);
    if (r < 0)
      ldpp_dout(dpp, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.invalidate_remove(dpp, name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::write_data(const DoutPrefixProvider *dpp, 
                                   const rgw_raw_obj& obj,
                                   const bufferlist& data,
                                   bool exclusive,
                                   RGWObjVersionTracker *objv_tracker,
                                   optional_yield y)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  ObjectCacheInfo info;
  info.data = data;
  info.meta.size = data.length();
  info.status = 0;
  info.flags = CACHE_FLAG_DATA;

  int ret = RGWSI_SysObj_Core::write_data(dpp, obj, data, exclusive, objv_tracker, y);
  string name = normal_name(pool, oid);
  if (ret >= 0) {
    if (objv_tracker && objv_tracker->read_version.ver) {
      info.version = objv_tracker->read_version;
      info.flags |= CACHE_FLAG_OBJV;
    }
    cache.put(dpp, name, info, NULL);
    int r = distribute_cache(dpp, name, obj, info, UPDATE_OBJ, y);
    if (r < 0)
      ldpp_dout(dpp, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.invalidate_remove(dpp, name);
  }

  return ret;
}

int RGWSI_SysObj_Cache::raw_stat(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj,
                                 uint64_t *psize, real_time *pmtime,
                                 map<string, bufferlist> *attrs,
                                 RGWObjVersionTracker *objv_tracker,
                                 optional_yield y)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  string name = normal_name(pool, oid);

  uint64_t size;
  real_time mtime;

  ObjectCacheInfo info;
  uint32_t flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  int r = cache.get(dpp, name, info, flags, NULL);
  if (r == 0) {
    if (info.status < 0)
      return info.status;

    size = info.meta.size;
    mtime = info.meta.mtime;
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    goto done;
  }
  if (r == -ENODATA) {
    return -ENOENT;
  }
  r = RGWSI_SysObj_Core::raw_stat(dpp, obj, &size, &mtime, &info.xattrs,
                                  objv_tracker, y);
  if (r < 0) {
    if (r == -ENOENT) {
      info.status = r;
      cache.put(dpp, name, info, NULL);
    }
    return r;
  }
  info.status = 0;
  info.meta.mtime = mtime;
  info.meta.size = size;
  info.flags = CACHE_FLAG_META | CACHE_FLAG_XATTRS;
  if (objv_tracker) {
    info.flags |= CACHE_FLAG_OBJV;
    info.version = objv_tracker->read_version;
  }
  cache.put(dpp, name, info, NULL);
done:
  if (psize)
    *psize = size;
  if (pmtime)
    *pmtime = mtime;
  if (attrs)
    *attrs = info.xattrs;
  return 0;
}

int RGWSI_SysObj_Cache::distribute_cache(const DoutPrefixProvider *dpp, 
                                         const string& normal_name,
                                         const rgw_raw_obj& obj,
                                         ObjectCacheInfo& obj_info, int op,
                                         optional_yield y)
{
  RGWCacheNotifyInfo info;
  info.op = op;
  info.obj_info = obj_info;
  info.obj = obj;
  return notify_svc->distribute(dpp, normal_name, info, y);
}

int RGWSI_SysObj_Cache::watch_cb(const DoutPrefixProvider *dpp,
                                 uint64_t notify_id,
                                 uint64_t cookie,
                                 uint64_t notifier_id,
                                 bufferlist& bl)
{
  RGWCacheNotifyInfo info;

  try {
    auto iter = bl.cbegin();
    decode(info, iter);
  } catch (buffer::end_of_buffer& err) {
    ldpp_dout(dpp, 0) << "ERROR: got bad notification" << dendl;
    return -EIO;
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: buffer::error" << dendl;
    return -EIO;
  }

  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(info.obj.pool, info.obj.oid, pool, oid);
  string name = normal_name(pool, oid);
  
  switch (info.op) {
  case UPDATE_OBJ:
    cache.put(dpp, name, info.obj_info, NULL);
    break;
  case INVALIDATE_OBJ:
    cache.invalidate_remove(dpp, name);
    break;
  default:
    ldpp_dout(dpp, 0) << "WARNING: got unknown notification op: " << info.op << dendl;
    return -EINVAL;
  }

  return 0;
}

void RGWSI_SysObj_Cache::set_enabled(bool status)
{
  cache.set_enabled(status);
}

bool RGWSI_SysObj_Cache::chain_cache_entry(const DoutPrefixProvider *dpp,
                                           std::initializer_list<rgw_cache_entry_info *> cache_info_entries,
                                           RGWChainedCache::Entry *chained_entry)
{
  return cache.chain_cache_entry(dpp, cache_info_entries, chained_entry);
}

void RGWSI_SysObj_Cache::register_chained_cache(RGWChainedCache *cc)
{
  cache.chain_cache(cc);
}

void RGWSI_SysObj_Cache::unregister_chained_cache(RGWChainedCache *cc)
{
  cache.unchain_cache(cc);
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

class RGWSI_SysObj_Cache_ASocketHook : public AdminSocketHook {
  RGWSI_SysObj_Cache *svc;

  static constexpr std::string_view admin_commands[][2] = {
    { "cache list name=filter,type=CephString,req=false",
      "cache list [filter_str]: list object cache, possibly matching substrings" },
    { "cache inspect name=target,type=CephString,req=true",
      "cache inspect target: print cache element" },
    { "cache erase name=target,type=CephString,req=true",
      "cache erase target: erase element from cache" },
    { "cache zap",
      "cache zap: erase all elements from cache" }
  };

public:
    RGWSI_SysObj_Cache_ASocketHook(RGWSI_SysObj_Cache *_svc) : svc(_svc) {}

    int start();
    void shutdown();

    int call(std::string_view command, const cmdmap_t& cmdmap,
	     const bufferlist&,
	     Formatter *f,
	     std::ostream& ss,
	     bufferlist& out) override;
};

int RGWSI_SysObj_Cache_ASocketHook::start()
{
  auto admin_socket = svc->ctx()->get_admin_socket();
  for (auto cmd : admin_commands) {
    int r = admin_socket->register_command(cmd[0], this, cmd[1]);
    if (r < 0) {
      ldout(svc->ctx(), 0) << "ERROR: fail to register admin socket command (r=" << r
                           << ")" << dendl;
      return r;
    }
  }
  return 0;
}

void RGWSI_SysObj_Cache_ASocketHook::shutdown()
{
  auto admin_socket = svc->ctx()->get_admin_socket();
  admin_socket->unregister_commands(this);
}

int RGWSI_SysObj_Cache_ASocketHook::call(
  std::string_view command, const cmdmap_t& cmdmap,
  const bufferlist&,
  Formatter *f,
  std::ostream& ss,
  bufferlist& out)
{
  if (command == "cache list"sv) {
    std::optional<std::string> filter;
    if (auto i = cmdmap.find("filter"); i != cmdmap.cend()) {
      filter = boost::get<std::string>(i->second);
    }
    f->open_array_section("cache_entries");
    svc->asocket.call_list(filter, f);
    f->close_section();
    return 0;
  } else if (command == "cache inspect"sv) {
    const auto& target = boost::get<std::string>(cmdmap.at("target"));
    if (svc->asocket.call_inspect(target, f)) {
      return 0;
    } else {
      ss << "Unable to find entry "s + target + ".\n";
      return -ENOENT;
    }
  } else if (command == "cache erase"sv) {
    const auto& target = boost::get<std::string>(cmdmap.at("target"));
    if (svc->asocket.call_erase(target)) {
      return 0;
    } else {
      ss << "Unable to find entry "s + target + ".\n";
      return -ENOENT;
    }
  } else if (command == "cache zap"sv) {
    svc->asocket.call_zap();
    return 0;
  }
  return -ENOSYS;
}

RGWSI_SysObj_Cache::ASocketHandler::ASocketHandler(const DoutPrefixProvider *_dpp, RGWSI_SysObj_Cache *_svc) : dpp(_dpp), svc(_svc)
{
  hook.reset(new RGWSI_SysObj_Cache_ASocketHook(_svc));
}

RGWSI_SysObj_Cache::ASocketHandler::~ASocketHandler()
{
}

int RGWSI_SysObj_Cache::ASocketHandler::start()
{
  return hook->start();
}

void RGWSI_SysObj_Cache::ASocketHandler::shutdown()
{
  return hook->shutdown();
}

void RGWSI_SysObj_Cache::ASocketHandler::call_list(const std::optional<std::string>& filter, Formatter* f)
{
  svc->cache.for_each(
    [&filter, f] (const string& name, const ObjectCacheEntry& entry) {
      if (!filter || name.find(*filter) != name.npos) {
	cache_list_dump_helper(f, name, entry.info.meta.mtime,
                               entry.info.meta.size);
      }
    });
}

int RGWSI_SysObj_Cache::ASocketHandler::call_inspect(const std::string& target, Formatter* f)
{
  if (const auto entry = svc->cache.get(dpp, target)) {
    f->open_object_section("cache_entry");
    f->dump_string("name", target.c_str());
    entry->dump(f);
    f->close_section();
    return true;
  } else {
    return false;
  }
}

int RGWSI_SysObj_Cache::ASocketHandler::call_erase(const std::string& target)
{
  return svc->cache.invalidate_remove(dpp, target);
}

int RGWSI_SysObj_Cache::ASocketHandler::call_zap()
{
  svc->cache.invalidate_all();
  return 0;
}
