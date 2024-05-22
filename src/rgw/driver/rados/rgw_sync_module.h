// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#include "rgw_common.h"
#include "rgw_coroutine.h"

class RGWBucketInfo;
class RGWRemoteDataLog;
struct RGWDataSyncCtx;
struct RGWDataSyncEnv;
struct rgw_bucket_entry_owner;
struct rgw_obj_key;
struct rgw_bucket_sync_pipe;


class RGWDataSyncModule {
public:
  RGWDataSyncModule() {}
  virtual ~RGWDataSyncModule() {}

  virtual void init(RGWDataSyncCtx *sync_env, uint64_t instance_id) {}

  virtual RGWCoroutine *init_sync(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc) {
    return nullptr;
  }

  virtual RGWCoroutine *start_sync(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc) {
    return nullptr;
  }
  virtual RGWCoroutine *sync_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc,
                                    rgw_bucket_sync_pipe& sync_pipe, rgw_obj_key& key,
                                    std::optional<uint64_t> versioned_epoch,
                                    const rgw_zone_set_entry& my_trace_entry,
                                    rgw_zone_set *zones_trace) = 0;
  virtual RGWCoroutine *remove_object(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& bucket_info, rgw_obj_key& key, real_time& mtime,
                                      bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) = 0;
  virtual RGWCoroutine *create_delete_marker(const DoutPrefixProvider *dpp, RGWDataSyncCtx *sc, rgw_bucket_sync_pipe& bucket_info, rgw_obj_key& key, real_time& mtime,
                                             rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) = 0;
};

class RGWRESTMgr;
class RGWMetadataHandler;
class RGWBucketInstanceMetadataHandlerBase;

class RGWSyncModuleInstance {
public:
  RGWSyncModuleInstance() {}
  virtual ~RGWSyncModuleInstance() {}
  virtual RGWDataSyncModule *get_data_handler() = 0;
  virtual RGWRESTMgr *get_rest_filter(int dialect, RGWRESTMgr *orig) {
    return orig;
  }
  virtual bool supports_user_writes() {
    return false;
  }
  virtual RGWMetadataHandler *alloc_bucket_meta_handler();
  virtual RGWBucketInstanceMetadataHandlerBase *alloc_bucket_instance_meta_handler(rgw::sal::Driver* driver);

  // indication whether the sync module start with full sync (default behavior)
  // incremental sync would follow anyway
  virtual bool should_full_sync() const {
      return true;
  }
};

typedef std::shared_ptr<RGWSyncModuleInstance> RGWSyncModuleInstanceRef;

class JSONFormattable;

class RGWSyncModule {

public:
  RGWSyncModule() {}
  virtual ~RGWSyncModule() {}

  virtual bool supports_writes() {
    return false;
  }
  virtual bool supports_data_export() = 0;
  virtual int create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) = 0;
};

typedef std::shared_ptr<RGWSyncModule> RGWSyncModuleRef;


class RGWSyncModulesManager {
  ceph::mutex lock = ceph::make_mutex("RGWSyncModulesManager");

  std::map<std::string, RGWSyncModuleRef> modules;
public:
  RGWSyncModulesManager() = default;

  void register_module(const std::string& name, RGWSyncModuleRef& module, bool is_default = false) {
    std::lock_guard l{lock};
    modules[name] = module;
    if (is_default) {
      modules[std::string()] = module;
    }
  }

  bool get_module(const std::string& name, RGWSyncModuleRef *module) {
    std::lock_guard l{lock};
    auto iter = modules.find(name);
    if (iter == modules.end()) {
      return false;
    }
    if (module != nullptr) {
      *module = iter->second;
    }
    return true;
  }


  bool supports_data_export(const std::string& name) {
    RGWSyncModuleRef module;
    if (!get_module(name, &module)) {
      return false;
    }

    return module->supports_data_export();
  }

  int create_instance(const DoutPrefixProvider *dpp, CephContext *cct, const std::string& name, const JSONFormattable& config, RGWSyncModuleInstanceRef *instance) {
    RGWSyncModuleRef module;
    if (!get_module(name, &module)) {
      return -ENOENT;
    }

    return module.get()->create_instance(dpp, cct, config, instance);
  }

  std::vector<std::string> get_registered_module_names() const {
    std::vector<std::string> names;
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
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_bucket src_bucket;
  rgw_obj_key key;

  ceph::real_time mtime;
  uint64_t size = 0;
  std::string etag;
  std::map<std::string, bufferlist> attrs;
  std::map<std::string, std::string> headers;
public:
  RGWStatRemoteObjCBCR(RGWDataSyncCtx *_sc,
                       rgw_bucket& _src_bucket, rgw_obj_key& _key);
  ~RGWStatRemoteObjCBCR() override {}

  void set_result(ceph::real_time& _mtime,
                  uint64_t _size,
                  const std::string& _etag,
                  std::map<std::string, bufferlist>&& _attrs,
                  std::map<std::string, std::string>&& _headers) {
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
  std::string etag;
  std::map<std::string, bufferlist> attrs;
  std::map<std::string, std::string> headers;

protected:
  RGWDataSyncCtx *sc;
  RGWDataSyncEnv *sync_env;

  rgw_bucket src_bucket;
  rgw_obj_key key;

public:
  RGWCallStatRemoteObjCR(RGWDataSyncCtx *_sc,
                     rgw_bucket& _src_bucket, rgw_obj_key& _key);

  ~RGWCallStatRemoteObjCR() override {}

  int operate(const DoutPrefixProvider *dpp) override;

  virtual RGWStatRemoteObjCBCR *allocate_callback() {
    return nullptr;
  }
};

void rgw_register_sync_modules(RGWSyncModulesManager *modules_manager);
