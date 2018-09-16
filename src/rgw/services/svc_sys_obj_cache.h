
#ifndef CEPH_RGW_SERVICES_SYS_OBJ_CACHE_H
#define CEPH_RGW_SERVICES_SYS_OBJ_CACHE_H


#include "rgw/rgw_service.h"
#include "rgw/rgw_cache.h"

#include "svc_sys_obj_core.h"

class RGWSI_Notify;

class RGWSI_SysObj_Cache_CB;

class RGWS_SysObj_Cache : public RGWService
{
public:
  RGWS_SysObj_Cache(CephContext *cct) : RGWService(cct, "sysobj_cache") {}

  int create_instance(const std::string& conf, RGWServiceInstanceRef *instance) override;
};

class RGWSI_SysObj_Cache : public RGWSI_SysObj_Core
{
  friend class RGWSI_SysObj_Cache_CB;

  std::shared_ptr<RGWSI_Notify> notify_svc;
  ObjectCache cache;

  std::shared_ptr<RGWSI_SysObj_Cache_CB> cb;

  void normalize_pool_and_obj(rgw_pool& src_pool, const string& src_obj, rgw_pool& dst_pool, string& dst_obj);
protected:
  std::map<std::string, RGWServiceInstance::dependency> get_deps() override;
  int load(const std::string& conf, std::map<std::string, RGWServiceInstanceRef>& dep_refs) override;

  int raw_stat(rgw_raw_obj& obj, uint64_t *psize, real_time *pmtime, uint64_t *epoch,
               map<string, bufferlist> *attrs, bufferlist *first_chunk,
               RGWObjVersionTracker *objv_tracker) override;

  int read(RGWSysObjectCtxBase& obj_ctx,
           GetObjState& read_state,
           RGWObjVersionTracker *objv_tracker,
           rgw_raw_obj& obj,
           bufferlist *bl, off_t ofs, off_t end,
           map<string, bufferlist> *attrs,
           rgw_cache_entry_info *cache_info,
           boost::optional<obj_version>) override;

  int get_attr(rgw_raw_obj& obj, const char *name, bufferlist *dest) override;

  int set_attrs(rgw_raw_obj& obj, 
                map<string, bufferlist>& attrs,
                map<string, bufferlist> *rmattrs,
                RGWObjVersionTracker *objv_tracker);

  int remove(RGWSysObjectCtxBase& obj_ctx,
             RGWObjVersionTracker *objv_tracker,
             rgw_raw_obj& obj) override;

  int write(rgw_raw_obj& obj,
            real_time *pmtime,
            map<std::string, bufferlist>& attrs,
            bool exclusive,
            const bufferlist& data,
            RGWObjVersionTracker *objv_tracker,
            real_time set_mtime) override;

  int write_data(rgw_raw_obj& obj,
                 const bufferlist& bl,
                 bool exclusive,
                 RGWObjVersionTracker *objv_tracker);

  int distribute_cache(const string& normal_name, rgw_raw_obj& obj, ObjectCacheInfo& obj_info, int op);

  int watch_cb(uint64_t notify_id,
               uint64_t cookie,
               uint64_t notifier_id,
               bufferlist& bl);

  void set_enabled(bool status);

public:
  RGWSI_SysObj_Cache(RGWService *svc, CephContext *cct) : RGWSI_SysObj_Core(svc, cct) {
    cache.set_ctx(cct);
  }

  bool chain_cache_entry(std::initializer_list<rgw_cache_entry_info *> cache_info_entries,
                         RGWChainedCache::Entry *chained_entry);
  void register_chained_cache(RGWChainedCache *cc);

  void call_list(const std::optional<std::string>& filter, Formatter* f);
  int call_inspect(const std::string& target, Formatter* f);
  int call_erase(const std::string& target);
  int call_zap();
};

template <class T>
class RGWChainedCacheImpl : public RGWChainedCache {
  RGWSI_SysObj_Cache *svc{nullptr};
  ceph::timespan expiry;
  RWLock lock;

  std::unordered_map<std::string, std::pair<T, ceph::coarse_mono_time>> entries;

public:
  RGWChainedCacheImpl() : lock("RGWChainedCacheImpl::lock") {}

  void init(RGWSI_SysObj_Cache *svc) {
    if (!svc) {
      return;
    }
    svc->register_chained_cache(this);
    expiry = std::chrono::seconds(svc->ctx()->_conf.get_val<uint64_t>(
				    "rgw_cache_expiry_interval"));
  }

  boost::optional<T> find(const string& key) {
    RWLock::RLocker rl(lock);
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

  bool put(RGWSI_SysObj_Cache *svc, const string& key, T *entry,
	   std::initializer_list<rgw_cache_entry_info *> cache_info_entries) {
    Entry chain_entry(this, key, entry);

    /* we need the svc cache to call us under its lock to maintain lock ordering */
    return svc->chain_cache_entry(cache_info_entries, &chain_entry);
  }

  void chain_cb(const string& key, void *data) override {
    T *entry = static_cast<T *>(data);
    RWLock::WLocker wl(lock);
    entries[key].first = *entry;
    if (expiry.count() > 0) {
      entries[key].second = ceph::coarse_mono_clock::now();
    }
  }

  void invalidate(const string& key) override {
    RWLock::WLocker wl(lock);
    entries.erase(key);
  }

  void invalidate_all() override {
    RWLock::WLocker wl(lock);
    entries.clear();
  }
}; /* RGWChainedCacheImpl */

#endif
