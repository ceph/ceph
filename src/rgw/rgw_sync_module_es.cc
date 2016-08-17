#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_sync_module_es.h"
#include "rgw_rest_conn.h"

#define dout_subsys ceph_subsys_rgw

struct ElasticConfig {
  string id;
  RGWRESTConn *conn{nullptr};
};

class RGWElasticHandleRemoteObjCBCR : public RGWStatRemoteObjCBCR {
  const ElasticConfig& conf;
public:
  RGWElasticHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                          const ElasticConfig& _conf) : RGWStatRemoteObjCBCR(sync_env, bucket_info, key), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": stat of remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size << " mtime=" << mtime
                              << " attrs=" << attrs << dendl;
      return set_cr_done();
    }
    return 0;
  }

};

class RGWElasticHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  const ElasticConfig& conf;
public:
  RGWElasticHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                        RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                        const ElasticConfig& _conf) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                           conf(_conf) {
  }

  ~RGWElasticHandleRemoteObjCR() {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWElasticHandleRemoteObjCBCR(sync_env, bucket_info, key, conf);
  }
};

class RGWElasticDataSyncModule : public RGWDataSyncModule {
  ElasticConfig conf;
public:
  RGWElasticDataSyncModule(CephContext *cct, const string& elastic_endpoint) {
    conf.id = string("elastic:") + elastic_endpoint;
    conf.conn = new RGWRESTConn(cct, nullptr, conf.id, { elastic_endpoint });
  }
  ~RGWElasticDataSyncModule() {
    delete conf.conn;
  }

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWElasticHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWElasticSyncModuleInstance : public RGWSyncModuleInstance {
  RGWElasticDataSyncModule data_handler;
public:
  RGWElasticSyncModuleInstance(CephContext *cct, const string& endpoint) : data_handler(cct, endpoint) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWElasticSyncModule::create_instance(CephContext *cct, map<string, string>& config, RGWSyncModuleInstanceRef *instance) {
  string endpoint;
  auto i = config.find("endpoint");
  if (i != config.end()) {
    endpoint = i->second;
  }
  instance->reset(new RGWElasticSyncModuleInstance(cct, endpoint));
  return 0;
}

