#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"

#define dout_subsys ceph_subsys_rgw

class RGWLogStatRemoteObjCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;

  RGWBucketInfo bucket_info;
  rgw_obj_key key;

  ceph::real_time mtime;
  uint64_t size;
  map<string, bufferlist> attrs;


public:
  RGWLogStatRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key) : RGWCoroutine(_sync_env->cct),
                                                                          sync_env(_sync_env),
                                                                          bucket_info(_bucket_info), key(_key) {
  }

  ~RGWLogStatRemoteObjCR() {}

  int operate() {
    reenter(this) {
      yield {
        call(new RGWStatRemoteObjCR(sync_env->async_rados, sync_env->store,
                      sync_env->source_zone,
                      bucket_info, key, &mtime, &size, &attrs));
      }
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "RGWStatRemoteObjCR() returned " << retcode << dendl;
        return set_cr_error(retcode);
      }
      ldout(sync_env->cct, 0) << "stat of remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                              << " attrs=" << attrs << dendl;
      return set_cr_done();
    }
    return 0;
  }
};

class RGWLogDataSyncModule : public RGWDataSyncModule {
  string prefix;
public:
  RGWLogDataSyncModule(const string& _prefix) : prefix(_prefix) {}

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_LOG: sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWLogStatRemoteObjCR(sync_env, bucket_info, key);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_LOG: rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_LOG: create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWLogSyncModuleInstance : public RGWSyncModuleInstance {
  RGWLogDataSyncModule data_handler;
public:
  RGWLogSyncModuleInstance(const string& prefix) : data_handler(prefix) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

class RGWLogSyncModule : public RGWSyncModule {
public:
  RGWLogSyncModule() {}
  bool supports_data_export() override { return false; }
  int create_instance(map<string, string>& config, RGWSyncModuleInstanceRef *instance) override {
    string prefix;
    auto i = config.find("prefix");
    if (i != config.end()) {
      prefix = i->second;
    }
    instance->reset(new RGWLogSyncModuleInstance(prefix));
    return 0;
  }
};



void rgw_register_sync_modules(RGWSyncModulesManager *modules_manager)
{
  RGWSyncModuleRef default_module(new RGWDefaultSyncModule());
  modules_manager->register_module("rgw", default_module, true);

  RGWSyncModuleRef log_module(new RGWLogSyncModule());
  modules_manager->register_module("log", log_module);
}
