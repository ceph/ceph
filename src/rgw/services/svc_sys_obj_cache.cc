// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/admin_socket.h"

#include "svc_sys_obj_cache.h"
#include "svc_zone.h"
#include "svc_notify.h"

#include "rgw/rgw_zone.h"
#include "rgw/rgw_tools.h"

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

boost::system::error_code RGWSI_SysObj_Cache::do_start()
{
  auto r = RGWSI_SysObj_Core::do_start();
  if (r) {
    return r;
  }

  r = notify_svc->start();
  if (r) {
    return r;
  }

  assert(notify_svc->is_started());

  cb = std::make_shared<RGWSI_SysObj_Cache_CB>(this);

  notify_svc->register_watch_cb(cb.get());

  return {};
}

void RGWSI_SysObj_Cache::shutdown()
{
  asocket.shutdown();
  RGWSI_SysObj_Core::shutdown();
}

static std::string normal_name(rgw_pool& pool, const std::string& oid) {
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


boost::system::error_code
RGWSI_SysObj_Cache::remove(RGWSysObjectCtxBase& obj_ctx,
                           RGWObjVersionTracker *objv_tracker,
                           const rgw_raw_obj& obj,
                           optional_yield y)

{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);

  string name = normal_name(pool, oid);
  cache.remove(name);

  ObjectCacheInfo info;
  auto r = distribute_cache(name, obj, info, REMOVE_OBJ, y);
  if (r) {
    ldout(cct, 0) << "ERROR: " << __func__ << "(): failed to distribute cache: r=" << r << dendl;
  }

  return RGWSI_SysObj_Core::remove(obj_ctx, objv_tracker, obj, y);
}

boost::system::error_code
RGWSI_SysObj_Cache::read(RGWSysObjectCtxBase& obj_ctx,
                         GetObjState& read_state,
                         RGWObjVersionTracker *objv_tracker,
                         const rgw_raw_obj& obj,
                         bufferlist *obl, off_t ofs, off_t end,
                         boost::container::flat_map<std::string, ceph::buffer::list> *attrs,
                         bool raw_attrs,
                         rgw_cache_entry_info *cache_info,
                         boost::optional<obj_version> refresh_version,
                         optional_yield y)
{
  rgw_pool pool;
  string oid;
  if (ofs != 0) {
    return RGWSI_SysObj_Core::read(obj_ctx, read_state, objv_tracker,
                                   obj, obl, ofs, end, attrs, raw_attrs,
                                   cache_info, refresh_version, y);
  }

  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  string name = normal_name(pool, oid);

  ObjectCacheInfo info;

  uint32_t flags = (end != 0 ? CACHE_FLAG_DATA : 0);
  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  if (attrs)
    flags |= CACHE_FLAG_XATTRS;

  if ((cache.get(name, info, flags, cache_info) == 0) &&
      (!refresh_version || !info.version.compare(&(*refresh_version)))) {
    if (info.status < 0)
      return ceph::to_error_code(info.status);

    bufferlist& bl = info.data;

    bufferlist::iterator i = bl.begin();

    obl->clear();

    i.copy_all(*obl);
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    if (attrs) {
      if (raw_attrs) {
        attrs->clear();
        std::copy(info.xattrs.cbegin(), info.xattrs.cend(),
                  std::inserter(*attrs, attrs->end()));
      } else {
	rgw_filter_attrset(info.xattrs, RGW_ATTR_PREFIX, attrs);
      }
    }
    return {};
  }

  boost::container::flat_map<std::string, ceph::buffer::list> unfiltered_attrset;
  auto r = RGWSI_SysObj_Core::read(obj_ctx, read_state, objv_tracker,
                                   obj, obl, ofs, end,
                                   (attrs ? &unfiltered_attrset : nullptr),
                                   true, /* cache unfiltered attrs */
                                   cache_info,
                                   refresh_version, y);
  if (r) {
    if (r == boost::system::errc::no_such_file_or_directory) { // only update ENOENT, we'd rather retry other errors
      info.status = ceph::from_error_code(r);
      cache.put(name, info, cache_info);
    }
    return r;
  }

  if (obl->length() == end + 1) {
    /* in this case, most likely object contains more data, we can't cache it */
    flags &= ~CACHE_FLAG_DATA;
  } else {
    bufferptr p(obl->length());
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
  if (attrs) {
    info.xattrs.clear();
    std::move(unfiltered_attrset.begin(), unfiltered_attrset.end(),
              std::inserter(info.xattrs, info.xattrs.end()));
    if (raw_attrs) {
      attrs->clear();
      std::copy(info.xattrs.begin(), info.xattrs.end(),
                std::inserter(*attrs, attrs->end()));
    } else {
      rgw_filter_attrset(info.xattrs, RGW_ATTR_PREFIX, attrs);
    }
  }
  cache.put(name, info, cache_info);
  return r;
}

boost::system::error_code
RGWSI_SysObj_Cache::get_attr(const rgw_raw_obj& obj,
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

  if (cache.get(name, info, flags, nullptr) == 0) {
    if (info.status < 0)
      return ceph::to_error_code(info.status);

    auto iter = info.xattrs.find(attr_name);
    if (iter == info.xattrs.end()) {
      return ceph::to_error_code(-ENODATA);
    }

    *dest = iter->second;
    return {};
  }
  /* don't try to cache this one */
  return RGWSI_SysObj_Core::get_attr(obj, attr_name, dest, y);
}

boost::system::error_code
RGWSI_SysObj_Cache::set_attrs(const rgw_raw_obj& obj,
                              boost::container::flat_map<std::string, ceph::buffer::list>& attrs,
                              boost::container::flat_map<std::string, ceph::buffer::list> *rmattrs,
                              RGWObjVersionTracker *objv_tracker,
                              optional_yield y)
{
  rgw_pool pool;
  string oid;
  normalize_pool_and_obj(obj.pool, obj.oid, pool, oid);
  ObjectCacheInfo info;
  info.xattrs.clear();
  std::copy(attrs.begin(), attrs.end(),
            std::inserter(info.xattrs, info.xattrs.end()));
  if (rmattrs) {
    info.rm_xattrs.clear();
    std::copy(rmattrs->begin(), rmattrs->end(),
              std::inserter(info.rm_xattrs, info.rm_xattrs.end()));
  }
  info.status = 0;
  info.flags = CACHE_FLAG_MODIFY_XATTRS;
  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  auto ret = RGWSI_SysObj_Core::set_attrs(obj, attrs, rmattrs, objv_tracker, y);
  string name = normal_name(pool, oid);
  if (!ret) {
    cache.put(name, info, NULL);
    auto r = distribute_cache(name, obj, info, UPDATE_OBJ, y);
    if (r)
      ldout(cct, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.remove(name);
  }

  return ret;
}

boost::system::error_code
RGWSI_SysObj_Cache::write(const rgw_raw_obj& obj,
                          real_time *pmtime,
                          boost::container::flat_map<std::string, ceph::buffer::list>& attrs,
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
  info.xattrs.clear();
  std::copy(attrs.begin(), attrs.end(),
            std::inserter(info.xattrs, info.xattrs.end()));
  info.status = 0;
  info.data = data;
  info.flags = CACHE_FLAG_XATTRS | CACHE_FLAG_DATA | CACHE_FLAG_META;
  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  ceph::real_time result_mtime;
  auto ret = RGWSI_SysObj_Core::write(obj, &result_mtime, attrs,
                                      exclusive, data,
                                      objv_tracker, set_mtime, y);
  if (pmtime) {
    *pmtime = result_mtime;
  }
  info.meta.mtime = result_mtime;
  info.meta.size = data.length();
  string name = normal_name(pool, oid);
  if (!ret) {
    cache.put(name, info, NULL);
    // Only distribute the cache information if we did not just create
    // the object with the exclusive flag. Note: PUT_OBJ_EXCL implies
    // PUT_OBJ_CREATE. Generally speaking, when successfully creating
    // a system object with the exclusive flag it is not necessary to
    // call distribute_cache, as a) it's unclear whether other RGWs
    // will need that system object in the near-term and b) it
    // generates additional network traffic.
    if (!exclusive) {
      auto r = distribute_cache(name, obj, info, UPDATE_OBJ, y);
      if (r)
	ldout(cct, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
    }
  } else {
    cache.remove(name);
  }

  return ret;
}

boost::system::error_code
RGWSI_SysObj_Cache::write_data(const rgw_raw_obj& obj,
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

  if (objv_tracker) {
    info.version = objv_tracker->write_version;
    info.flags |= CACHE_FLAG_OBJV;
  }
  auto ret = RGWSI_SysObj_Core::write_data(obj, data, exclusive, objv_tracker, y);
  std::string name = normal_name(pool, oid);
  if (!ret) {
    cache.put(name, info, NULL);
    auto r = distribute_cache(name, obj, info, UPDATE_OBJ, y);
    if (r)
      ldout(cct, 0) << "ERROR: failed to distribute cache for " << obj << dendl;
  } else {
    cache.remove(name);
  }

  return ret;
}

boost::system::error_code
RGWSI_SysObj_Cache::raw_stat(const rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *pepoch,
                             boost::container::flat_map<std::string, ceph::buffer::list> *attrs,
                             bufferlist *first_chunk,
                             RGWObjVersionTracker *objv_tracker,
                             optional_yield y)
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
  // TODO (Moved up here so the goto could compile)
  boost::container::flat_map<std::string, ceph::buffer::list> tmp;

  if (objv_tracker)
    flags |= CACHE_FLAG_OBJV;
  auto r = ceph::to_error_code(cache.get(name, info, flags, NULL));
  if (!r) {
    if (info.status < 0)
      return ceph::to_error_code(info.status);

    size = info.meta.size;
    mtime = info.meta.mtime;
    epoch = info.epoch;
    if (objv_tracker)
      objv_tracker->read_version = info.version;
    goto done;
  }
  // TODO: This actually does cost us (the std::move and std::copies
  // just do what move assignment and copy assignment would have done
  // anyway), so come back and fix it when we update ObjCacheInfo to
  // not use std::map
  r = RGWSI_SysObj_Core::raw_stat(obj, &size, &mtime, &epoch, &tmp,
                                  first_chunk, objv_tracker, y);
  info.xattrs.clear();
  std::copy(tmp.begin(), tmp.end(),
            std::inserter(info.xattrs, info.xattrs.end()));

  if (r) {
    if (r == boost::system::errc::no_such_file_or_directory) {
      info.status = ceph::from_error_code(r);
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
  if (attrs) {
    attrs->clear();
    std::copy(info.xattrs.begin(), info.xattrs.end(),
              std::inserter(*attrs, attrs->end()));
  }
  return {};
}

boost::system::error_code
RGWSI_SysObj_Cache::distribute_cache(const string& normal_name,
                                     const rgw_raw_obj& obj,
                                     ObjectCacheInfo& obj_info, int op,
                                     optional_yield y)
{
  RGWCacheNotifyInfo info;
  info.op = op;
  info.obj_info = obj_info;
  info.obj = obj;
  bufferlist bl;
  encode(info, bl);
  return notify_svc->distribute(normal_name, bl, y);
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

  static constexpr const char* admin_commands[4][3] = {
    { "cache list",
      "cache list name=filter,type=CephString,req=false",
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

RGWSI_SysObj_Cache::ASocketHandler::ASocketHandler(RGWSI_SysObj_Cache *_svc) : svc(_svc)
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
  if (const auto entry = svc->cache.get(target)) {
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
  return svc->cache.remove(target);
}

int RGWSI_SysObj_Cache::ASocketHandler::call_zap()
{
  svc->cache.invalidate_all();
  return 0;
}
