// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/RWLock.h"
#include "rgw/rgw_service.h"
#include "rgw/rgw_cache.h"

#include "svc_sys_obj_core.h"

class RGWSI_SysObj_Cache_CB;
class RGWSI_SysObj_Cache_ASocketHook;

class RGWSI_SysObj_Cache : public RGWSI_SysObj_Core
{
  friend class RGWSI_SysObj_Cache_CB;
  friend class RGWServices_Def;
  friend class ASocketHandler;

  RGWSI_SysObj_Core *sysobj_core_svc{nullptr};
  ObjectCache cache;

  std::shared_ptr<RGWSI_SysObj_Cache_CB> cb;

protected:
  static std::string normal_name(rgw_pool& pool, const std::string& oid);
  void normalize_pool_and_obj(const rgw_pool& src_pool, const std::string& src_obj, rgw_pool& dst_pool, std::string& dst_obj);

  void init(RGWSI_SysObj_Core *_sysobj_core_svc,
            RGWSI_Zone *_zone_svc) {
    sysobj_core_svc = _sysobj_core_svc;
    init_core(_zone_svc);
  }

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;
  void shutdown() override;

  int raw_stat(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
               std::map<std::string, bufferlist> *attrs, bufferlist *first_chunk,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y) override;

  int read(const DoutPrefixProvider *dpp,
           RGWSysObjectCtxBase& obj_ctx,
           RGWSI_SysObj_Obj_GetObjState& read_state,
           RGWObjVersionTracker *objv_tracker,
           const rgw_raw_obj& obj,
           bufferlist *bl, off_t ofs, off_t end,
           std::map<std::string, bufferlist> *attrs,
	   bool raw_attrs,
           rgw_cache_entry_info *cache_info,
           boost::optional<obj_version>,
           optional_yield y) override;

  int get_attr(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, const char *name, bufferlist *dest,
               optional_yield y) override;

  int set_attrs(const DoutPrefixProvider *dpp, 
                const rgw_raw_obj& obj, 
                std::map<std::string, bufferlist>& attrs,
                std::map<std::string, bufferlist> *rmattrs,
                RGWObjVersionTracker *objv_tracker,
                optional_yield y);

  int remove(const DoutPrefixProvider *dpp, 
             RGWSysObjectCtxBase& obj_ctx,
             RGWObjVersionTracker *objv_tracker,
             const rgw_raw_obj& obj,
             optional_yield y) override;

  int write(const DoutPrefixProvider *dpp, 
            const rgw_raw_obj& obj,
            real_time *pmtime,
            std::map<std::string, bufferlist>& attrs,
            bool exclusive,
            const bufferlist& data,
            RGWObjVersionTracker *objv_tracker,
            real_time set_mtime,
            optional_yield y) override;

  int write_data(const DoutPrefixProvider *dpp, 
                 const rgw_raw_obj& obj,
                 const bufferlist& bl,
                 bool exclusive,
                 RGWObjVersionTracker *objv_tracker,
                 optional_yield y);

  virtual int distribute_cache(const DoutPrefixProvider *dpp, const std::string& normal_name, const rgw_raw_obj& obj,
                               ObjectCacheInfo& obj_info, int op,
                               optional_yield y) = 0;
  void set_enabled(bool status);

  /* uncached interfaces */

  int omap_get_all(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, std::map<std::string, bufferlist> *m,
                   optional_yield y) override {
    return sysobj_core_svc->omap_get_all(dpp, obj, m, y);
  }

  int omap_get_vals(const DoutPrefixProvider *dpp, 
                    const rgw_raw_obj& obj,
                    const std::string& marker,
                    uint64_t count,
                    std::map<std::string, bufferlist> *m,
                    bool *pmore,
                    optional_yield y) override {
    return sysobj_core_svc->omap_get_vals(dpp, obj, marker, count, m, pmore, y);
  }

  int omap_set(const DoutPrefixProvider *dpp, 
               const rgw_raw_obj& obj, const std::string& key,
               bufferlist& bl, bool must_exist,
               optional_yield y) override {
    return sysobj_core_svc->omap_set(dpp, obj, key, bl, must_exist, y);
  }

  int omap_set(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj,
               const std::map<std::string, bufferlist>& m, bool must_exist,
               optional_yield y) override {
    return sysobj_core_svc->omap_set(dpp, obj, m, must_exist, y);
  }

  int omap_del(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj, const std::string& key,
               optional_yield y) override {
    return sysobj_core_svc->omap_del(dpp, obj, key, y);
  }

  int notify(const DoutPrefixProvider *dpp, 
                     const rgw_raw_obj& obj, bufferlist& bl,
                     uint64_t timeout_ms, bufferlist *pbl,
                     optional_yield y) override {
    return sysobj_core_svc->notify(dpp, obj, bl, timeout_ms, pbl, y);
  }

  int pool_list_prefixed_objs(const DoutPrefixProvider *dpp,
                                      const rgw_pool& pool,
                                      const std::string& prefix,
                                      std::function<void(const std::string&)> cb) override {
    return sysobj_core_svc->pool_list_prefixed_objs(dpp, pool, prefix, cb);
  }

  int pool_list_objects_init(const DoutPrefixProvider *dpp,
                                     const rgw_pool& pool,
                                     const std::string& marker,
                                     const std::string& prefix,
                                     RGWSI_SysObj::Pool::ListCtx *ctx) override {
    return sysobj_core_svc->pool_list_objects_init(dpp, pool, marker, prefix, ctx);
  }

  int pool_list_objects_next(const DoutPrefixProvider *dpp,
                                     RGWSI_SysObj::Pool::ListCtx& ctx,
                                     int max,
                                     std::vector<std::string> *oids,
                                     bool *is_truncated) override {
    return sysobj_core_svc->pool_list_objects_next(dpp, ctx, max, oids, is_truncated);
  }

  int pool_list_objects_get_marker(RGWSI_SysObj::Pool::ListCtx& _ctx,
                                           std::string *marker) override {
    return sysobj_core_svc->pool_list_objects_get_marker(_ctx, marker);
  }

  int stat(RGWSysObjectCtxBase& obj_ctx,
                   RGWSI_SysObj_Obj_GetObjState& state,
                   const rgw_raw_obj& obj,
                   std::map<std::string, bufferlist> *attrs,
                   bool raw_attrs,
                   real_time *lastmod,
                   uint64_t *obj_size,
                   RGWObjVersionTracker *objv_tracker,
                   optional_yield y,
                   const DoutPrefixProvider *dpp) override {
    return sysobj_core_svc->stat(obj_ctx, state, obj, attrs, raw_attrs,
                                 lastmod, obj_size, objv_tracker, y, dpp);
  }

public:
  RGWSI_SysObj_Cache(const DoutPrefixProvider *dpp, CephContext *cct) : RGWSI_SysObj_Core(cct), asocket(dpp, this) {
    cache.set_ctx(cct);
  }

  bool chain_cache_entry(const DoutPrefixProvider *dpp,
                         std::initializer_list<rgw_cache_entry_info *> cache_info_entries,
                         RGWChainedCache::Entry *chained_entry);
  void register_chained_cache(RGWChainedCache *cc);
  void unregister_chained_cache(RGWChainedCache *cc);

  RGWSI_Zone *get_zone_svc() {
    return sysobj_core_svc->get_zone_svc();
  }

  class ASocketHandler {
    const DoutPrefixProvider *dpp;
    RGWSI_SysObj_Cache *svc;

    std::unique_ptr<RGWSI_SysObj_Cache_ASocketHook> hook;

  public:
    ASocketHandler(const DoutPrefixProvider *dpp, RGWSI_SysObj_Cache *_svc);
    ~ASocketHandler();

    int start();
    void shutdown();

    // `call_list` must iterate over all cache entries and call
    // `cache_list_dump_helper` with the supplied Formatter on any that
    // include `filter` as a substd::string.
    //
    void call_list(const std::optional<std::string>& filter, Formatter* f);

    // `call_inspect` must look up the requested target and, if found,
    // dump it to the supplied Formatter and return true. If not found,
    // it must return false.
    //
    int call_inspect(const std::string& target, Formatter* f);

    // `call_erase` must erase the requested target and return true. If
    // the requested target does not exist, it should return false.
    int call_erase(const std::string& target);

    // `call_zap` must erase the cache.
    int call_zap();
  } asocket;
};

template <class T>
class RGWChainedCacheImpl : public RGWChainedCache {
  RGWSI_SysObj_Cache *svc{nullptr};
  ceph::timespan expiry;
  RWLock lock;

  std::unordered_map<std::string, std::pair<T, ceph::coarse_mono_time>> entries;

public:
  RGWChainedCacheImpl() : lock("RGWChainedCacheImpl::lock") {}
  ~RGWChainedCacheImpl() {
    if (!svc) {
      return;
    }
    svc->unregister_chained_cache(this);
  }

  void unregistered() override {
    svc = nullptr;
  }

  void init(RGWSI_SysObj_Cache *_svc) {
    if (!_svc) {
      return;
    }
    svc = _svc;
    svc->register_chained_cache(this);
    expiry = std::chrono::seconds(svc->ctx()->_conf.get_val<uint64_t>(
				    "rgw_cache_expiry_interval"));
  }

  boost::optional<T> find(const std::string& key) {
    std::shared_lock rl{lock};
    auto iter = entries.find(key);
    if (iter == entries.end()) {
      return boost::none;
    }
    if (expiry.count() &&
	(ceph::coarse_mono_clock::now() - iter->second.second) > expiry) {
      return boost::none;
    }

    return iter->second.first;
  }

  bool put(const DoutPrefixProvider *dpp, RGWSI_SysObj_Cache *svc, const std::string& key, T *entry,
	   std::initializer_list<rgw_cache_entry_info *> cache_info_entries) {
    if (!svc) {
      return false;
    }

    Entry chain_entry(this, key, entry);

    /* we need the svc cache to call us under its lock to maintain lock ordering */
    return svc->chain_cache_entry(dpp, cache_info_entries, &chain_entry);
  }

  void chain_cb(const std::string& key, void *data) override {
    T *entry = static_cast<T *>(data);
    std::unique_lock wl{lock};
    entries[key].first = *entry;
    if (expiry.count() > 0) {
      entries[key].second = ceph::coarse_mono_clock::now();
    }
  }

  void invalidate(const std::string& key) override {
    std::unique_lock wl{lock};
    entries.erase(key);
  }

  void invalidate_all() override {
    std::unique_lock wl{lock};
    entries.clear();
  }
}; /* RGWChainedCacheImpl */
