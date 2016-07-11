#ifndef CEPH_RGW_SYNC_MODULE_H
#define CEPH_RGW_SYNC_MODULE_H

#include "rgw_common.h"

class RGWCoroutine;
class RGWBucketInfo;
class RGWRemoteDataLog;
struct RGWDataSyncEnv;
struct rgw_bucket_entry_owner;
struct rgw_obj_key;

class RGWDataSyncModule {
public:
  RGWDataSyncModule() {}
  virtual ~RGWDataSyncModule() {}

  virtual RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) = 0;
  virtual RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                      bool versioned, uint64_t versioned_epoch) = 0;
  virtual RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                             rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) = 0;
};

class RGWSyncModuleInstance {
public:
  RGWSyncModuleInstance() {}
  virtual ~RGWSyncModuleInstance() {}
  virtual RGWDataSyncModule *get_data_handler() = 0;
};

typedef std::shared_ptr<RGWSyncModuleInstance> RGWSyncModuleInstanceRef;

class RGWSyncModule {

public:
  RGWSyncModule() {}
  virtual ~RGWSyncModule() {}

  virtual int create_instance(map<string, string>& config, RGWSyncModuleInstanceRef *instance) = 0;
};

typedef std::shared_ptr<RGWSyncModule> RGWSyncModuleRef;


class RGWSyncModulesManager {
  Mutex lock;

  map<string, RGWSyncModuleRef> modules;
public:
  RGWSyncModulesManager() : lock("RGWSyncModulesManager") {}

  void register_module(const string& name, RGWSyncModuleRef& module) {
    Mutex::Locker l(lock);
    modules[name] = module;
  }

  bool get_module(const string& name, RGWSyncModuleRef *module) {
    Mutex::Locker l(lock);
    auto iter = modules.find(name);
    if (iter == modules.end()) {
      return false;
    }
    *module = iter->second;
    return true;
  }


  int create_instance(const string& name, map<string, string>& config, RGWSyncModuleInstanceRef *instance) {
    RGWSyncModuleRef module;
    if (!get_module(name, &module)) {
      return -ENOENT;
    }

    return module.get()->create_instance(config, instance);
  }
};

#endif
