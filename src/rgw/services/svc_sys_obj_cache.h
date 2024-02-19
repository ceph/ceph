// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "common/RWLock.h"
#include "rgw_service.h"
#include "rgw_cache.h"

#include "svc_sys_obj_core.h"

class RGWSI_Notify;

class RGWSI_SysObj_Cache_CB;
class RGWSI_SysObj_Cache_ASocketHook;

class RGWSI_SysObj_Cache : public RGWSI_SysObj_Core
{
  friend class RGWSI_SysObj_Cache_CB;
  friend RGWServices_Def;
  friend class ASocketHandler;

  RGWSI_Notify *notify_svc{nullptr};
  ObjectCache cache;

  std::shared_ptr<RGWSI_SysObj_Cache_CB> cb;

  void normalize_pool_and_obj(const rgw_pool& src_pool, const std::string& src_obj, rgw_pool& dst_pool, std::string& dst_obj);
protected:
  void init(librados::Rados* rados_,
            RGWSI_Zone *_zone_svc,
            RGWSI_Notify *_notify_svc) {
    core_init(rados_, _zone_svc);
    notify_svc = _notify_svc;
  }

  int do_start(optional_yield, const DoutPrefixProvider *dpp) override;
  void shutdown() override;

  int raw_stat(const DoutPrefixProvider *dpp, const rgw_raw_obj& obj,
               uint64_t *psize, real_time *pmtime,
               std::map<std::string, bufferlist> *attrs,
               RGWObjVersionTracker *objv_tracker,
               optional_yield y) override;

  int read(const DoutPrefixProvider *dpp,
           RGWSI_SysObj_Obj_GetObjState& read_state,
           RGWObjVersionTracker *objv_tracker,
           const rgw_raw_obj& obj,
           bufferlist *bl, off_t ofs, off_t end,
           ceph::real_time* pmtime, uint64_t* psize,
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
                bool exclusive, optional_yield y) override;

  int remove(const DoutPrefixProvider *dpp, 
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
                 optional_yield y) override;

  int distribute_cache(const DoutPrefixProvider *dpp, const std::string& normal_name, const rgw_raw_obj& obj,
                       ObjectCacheInfo& obj_info, int op,
                       optional_yield y);

  int watch_cb(const DoutPrefixProvider *dpp,
               uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl);

  void set_enabled(bool status);

public:
  RGWSI_SysObj_Cache(const DoutPrefixProvider *dpp, CephContext *cct) : RGWSI_SysObj_Core(cct), asocket(dpp, this) {
    cache.set_ctx(cct);
  }

  bool chain_cache_entry(const DoutPrefixProvider *dpp,
                         std::initializer_list<rgw_cache_entry_info *> cache_info_entries,
                         RGWChainedCache::Entry *chained_entry);
  void register_chained_cache(RGWChainedCache *cc);
  void unregister_chained_cache(RGWChainedCache *cc);

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
