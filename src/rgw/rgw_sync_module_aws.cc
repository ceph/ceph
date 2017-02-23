#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_boost_asio_yield.h"
#include "rgw_sync_module_aws.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"

#define dout_subsys ceph_subsys_rgw

static string aws_object_name(const RGWBucketInfo& bucket_info, const rgw_obj_key&key, bool user_buckets=false){
  string obj_name;
  if(!user_buckets){
    obj_name = bucket_info.owner.tenant + bucket_info.owner.id + "/" + bucket_info.bucket.name + "/" + key.name;
      } else {
    // for future when every tenant gets their own bucket
    obj_name = bucket_info.bucket.name + "/" + key.name;
  }
  return obj_name;
}

struct AWSConfig {
  string id;
  RGWRESTConn *conn{nullptr};
};

// maybe use Fetch Remote Obj instead?
class RGWAWSHandleRemoteObjCBCR: public RGWStatRemoteObjCBCR {
  const AWSConfig& conf;
  bufferlist res;
public:
  RGWAWSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                            RGWBucketInfo& _bucket_info,
                            rgw_obj_key& _key,
                            const AWSConfig& _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                         conf(_conf) {}
  int operate () override {
    auto store = sync_env->store;
    RGWRESTConn *conn = store->rest_master_conn;

    if (conn == nullptr)
      return -EIO;

    reenter(this) {

      ldout(sync_env->cct, 0) << "AWS: download begin: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size
                              << " mtime=" << mtime << " attrs=" << attrs
                              << dendl;

      yield {
        // and here be dragons!
        // ultimately we should be using a form of  fetch obj that doesn't write to rados maybe?

        string obj_path = bucket_info.bucket.name + "/" + key.name;
        ldout(store->ctx(),0) << "abhi: path=" << obj_path << dendl;

        // Don't try this at home, very hacky, probably need a proper aws client
        // written so RESTResourceCR expects a JSON as a result, we workaround
        // by providing a list which is a native json type

        // And we should do a part by part get and initiate mp on the aws side
        call(new RGWReadRESTResourceCR<bufferlist>(sync_env->cct,
                                                   conn,
                                                   sync_env->http_manager,
                                                   obj_path,
                                                   nullptr,
                                                   &res,
                                                   true,
                                                   &res));

      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      ldout(sync_env->cct,0) << "abhi: download complete, printing object"<< dendl;
      yield {
        // TODO: actually create buckets if they dont' exist, we currently just
        // create object, assuming buckets are already there
        string path=aws_object_name(bucket_info, key);
        ldout(sync_env->cct,0) << "abhi sending request to " << conf.conn << " path" << path << dendl;
        call(new RGWPutRawRESTResourceCR<int> (sync_env->cct, conf.conn,
                                                        sync_env->http_manager,
                                                        path, nullptr,
                                                        res, nullptr));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }



      // };

      return set_cr_done();
    }

    return 0;
  }
};

class RGWAWSHandleRemoteObjCR : public RGWCallStatRemoteObjCR {
  const AWSConfig& conf;
public:
  RGWAWSHandleRemoteObjCR(RGWDataSyncEnv *_sync_env,
                              RGWBucketInfo& _bucket_info, rgw_obj_key& _key,
                              const AWSConfig& _conf) : RGWCallStatRemoteObjCR(_sync_env, _bucket_info, _key),
                                                            conf(_conf) {
  }

  ~RGWAWSHandleRemoteObjCR() {}

  RGWStatRemoteObjCBCR *allocate_callback() override {
    return new RGWAWSHandleRemoteObjCBCR(sync_env, bucket_info, key, conf);
  }
};

class RGWAWSRemoveRemoteObjCBCR : public RGWCoroutine {
  RGWDataSyncEnv *sync_env;
  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  ceph::real_time mtime;
  const AWSConfig& conf;
public:
  RGWAWSRemoveRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                          RGWBucketInfo& _bucket_info, rgw_obj_key& _key, const ceph::real_time& _mtime,
                          const AWSConfig& _conf) : RGWCoroutine(_sync_env->cct), sync_env(_sync_env),
                                                        bucket_info(_bucket_info), key(_key),
                                                        mtime(_mtime), conf(_conf) {}
  int operate() override {
    reenter(this) {
      ldout(sync_env->cct, 0) << ": remove remote obj: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << dendl;
      yield {
        string path = aws_object_name(bucket_info, key);
        ldout(sync_env->cct, 0) << "abhi: removing aws object at" << path << dendl;
        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf.conn,
                                         sync_env->http_manager,
                                         path, nullptr /* params */));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }
      return set_cr_done();
    }
    return 0;
  }

};


class RGWAWSDataSyncModule: public RGWDataSyncModule {
  AWSConfig conf;
public:
  RGWAWSDataSyncModule(CephContext *cct, const string& s3_endpoint, const string& access_key, const string& secret){
    conf.id = string("s3:") + s3_endpoint;
    conf.conn = new RGWRESTConn(cct,
                                conf.id,
                                { s3_endpoint },
                                RGWAccessKey(access_key,secret));
  }

  ~RGWAWSDataSyncModule() {
    delete conf.conn;
  }
    RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) << conf.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) <<"rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch) override {
    ldout(sync_env->cct, 0) <<"AWS Not implemented: create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWAWSSyncModuleInstance : public RGWSyncModuleInstance {
  RGWAWSDataSyncModule data_handler;
public:
  RGWAWSSyncModuleInstance(CephContext *cct, const string& s3_endpoint, const string& access_key, const string& secret) : data_handler(cct, s3_endpoint, access_key, secret) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWAWSSyncModule::create_instance(CephContext *cct, map<string, string>& config,  RGWSyncModuleInstanceRef *instance){
  string s3_endpoint, access_key, secret;
  auto i = config.find("s3_endpoint");
  if (i != config.end())
    s3_endpoint = i->second;

  i = config.find("access_key");
  if (i != config.end())
    access_key = i->second;

  i = config.find("secret");
  if (i != config.end())
    secret = i->second;

    // maybe we should just pass dictionaries around?
  instance->reset(new RGWAWSSyncModuleInstance(cct, s3_endpoint, access_key, secret));
  return 0;
}
