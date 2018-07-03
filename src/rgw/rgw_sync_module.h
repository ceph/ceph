#ifndef CEPH_RGW_SYNC_MODULE_H
#define CEPH_RGW_SYNC_MODULE_H

#include "rgw_common.h"
#include "rgw_coroutine.h"

class RGWBucketInfo;
class RGWRemoteDataLog;
struct RGWDataSyncEnv;
struct rgw_bucket_entry_owner;
struct rgw_obj_key;


class RGWDataSyncModule {
public:
  RGWDataSyncModule() {}
  virtual ~RGWDataSyncModule() {}

  virtual void init(RGWDataSyncEnv *sync_env, uint64_t instance_id) {}

  virtual RGWCoroutine *init_sync(RGWDataSyncEnv *sync_env) {
    return nullptr;
  }

  virtual RGWCoroutine *start_sync(RGWDataSyncEnv *sync_env) {
    return nullptr;
  }
  virtual RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, std::optional<uint64_t> versioned_epoch, rgw_zone_set *zones_trace) = 0;
  virtual RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                      bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) = 0;
  virtual RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                             rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) = 0;
};

class RGWRESTMgr;

class RGWSyncModuleInstance {
public:
  RGWSyncModuleInstance() {}
  virtual ~RGWSyncModuleInstance() {}
  virtual RGWDataSyncModule *get_data_handler() = 0;
  virtual RGWRESTMgr *get_rest_filter(int dialect, RGWRESTMgr *orig) {
    return orig;
  }
};

typedef std::shared_ptr<RGWSyncModuleInstance> RGWSyncModuleInstanceRef;

class JSONFormattable;

class RGWSyncModule {

public:
  RGWSyncModule() {}
  virtual ~RGWSyncModule() {}

  virtual bool supports_data_export() = 0;
  virtual int create_instance(CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) = 0;
};

typedef std::shared_ptr<RGWSyncModule> RGWSyncModuleRef;


class RGWSyncModulesManager {
  Mutex lock;

  map<string, RGWSyncModuleRef> modules;
public:
  RGWSyncModulesManager() : lock("RGWSyncModulesManager") {}

  void register_module(const string& name, RGWSyncModuleRef& module, bool is_default = false) {
    Mutex::Locker l(lock);
    modules[name] = module;
    if (is_default) {
      modules[string()] = module;
    }
  }

  bool get_module(const string& name, RGWSyncModuleRef *module) {
    Mutex::Locker l(lock);
    auto iter = modules.find(name);
    if (iter == modules.end()) {
      return false;
    }
    if (module != nullptr) {
      *module = iter->second;
    }
    return true;
  }


  int supports_data_export(const string& name) {
    RGWSyncModuleRef module;
    if (!get_module(name, &module)) {
      return -ENOENT;
    }

    return module.get()->supports_data_export();
  }

  int create_instance(CephContext *cct, const string& name, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) {
    RGWSyncModuleRef module;
    if (!get_module(name, &module)) {
      return -ENOENT;
    }

    return module.get()->create_instance(cct, config, instance);
  }

  vector<string> get_registered_module_names() const {
    vector<string> names;
    for (auto& i: modules) {
      if (!i.first.empty()) {
        names.push_back(i.first);
      }
    }
    return names;
  }
};

class RGWStatRemoteObjCBCR : public RGWCoroutine {
protected:
  RGWDataSyncEnv *sync_env;

  RGWBucketInfo bucket_info;
  rgw_obj_key key;

  ceph::real_time mtime;
  uint64_t size = 0;
  string etag;
  map<string, bufferlist> attrs;
  map<string, string> headers;
public:
  RGWStatRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                       RGWBucketInfo& _bucket_info, rgw_obj_key& _key);
  ~RGWStatRemoteObjCBCR() override {}

  void set_result(ceph::real_time& _mtime,
                  uint64_t _size,
                  const string& _etag,
                  map<string, bufferlist>&& _attrs,
                  map<string, string>&& _headers) {
    mtime = _mtime;
    size = _size;
    etag = _etag;
    attrs = std::move(_attrs);
    headers = std::move(_headers);
  }
};

class RGWCallStatRemoteObjCR : public RGWCoroutine {
  ceph::real_time mtime;
  uint64_t size{0};
  string etag;
  map<string, bufferlist> attrs;
  map<string, string> headers;

protected:
  RGWDataSyncEnv *sync_env;

  RGWBucketInfo bucket_info;
  rgw_obj_key key;

public:
  RGWCallStatRemoteObjCR(RGWDataSyncEnv *_sync_env,
                     RGWBucketInfo& _bucket_info, rgw_obj_key& _key);

  ~RGWCallStatRemoteObjCR() override {}

  int operate() override;

  virtual RGWStatRemoteObjCBCR *allocate_callback() {
    return nullptr;
  }
};

void rgw_register_sync_modules(RGWSyncModulesManager *modules_manager);

#endif
