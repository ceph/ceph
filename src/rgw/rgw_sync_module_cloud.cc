
#include <algorithm>
#include <cstddef>

#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_cloud_cloud.h"
#include "rgw_cloud_access.h"
#include "rgw_rest_client.h"
#include "rgw_rest_conn.h"

#include <boost/asio/yield.hpp>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

class RGWGetRemoteObjCB: public RGWGetDataCB {
  bufferlist data;
  uint64_t len;
public:
  RGWGetRemoteObjCB():len(0) {}
  ~RGWGetRemoteObjCB() { data.clear(); }
  
  bufferlist& get_data() { return data; }
  uint64_t get_len() { return len; } 
  
  int handle_data(bufferlist& _bl, off_t _ofs, off_t _len) {
    bufferptr bp(_bl.c_str(), _len);
    data.push_back(bp);
    len += _len;
    return 0;
  }
};

class RGWCloudSyncObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  std::string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;

  ceph::real_time *pmtime;
  uint64_t *psize;
  RGWCloudAccess* cloud_access;
protected:
  int _send_request();
public:
  RGWCloudSyncObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                         const std::string& _source_zone,
                         RGWBucketInfo& _bucket_info,
                         const rgw_obj_key& _key,
                         RGWCloudAccess* _cloud_access) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                      source_zone(_source_zone),
                                                      bucket_info(_bucket_info),
                                                      key(_key),
                                                      cloud_access(_cloud_access){}
};

int RGWCloudSyncObj::_send_request()
{
  std::string user_id;
  rgw_obj src_obj(bucket_info.bucket, key);

  RGWRESTStreamRWRequest *in_stream_req;
 
  RGWRESTConn *conn;
  if (source_zone.empty()) {
    if (bucket_info.zonegroup.empty()) {
      /* source is in the master zonegroup */
      conn = store->rest_master_conn;
    } else {
      map<std::string, RGWRESTConn *>::iterator iter = store->zonegroup_conn_map.find(bucket_info.zonegroup);
      if (iter == store->zonegroup_conn_map.end()) {
        ldout(store->ctx(), 0) << "RGWCloudSyncObj:could not find zonegroup connection to zonegroup: " << source_zone << dendl;
        return -ENOENT;
      }
      conn = iter->second;
    }
  } else {
    map<std::string, RGWRESTConn *>::iterator iter = store->zone_conn_map.find(source_zone);
    if (iter == store->zone_conn_map.end()) {
      ldout(store->ctx(), 0) << "RGWCloudSyncObj:could not find zone connection to zone: " << source_zone << dendl;
      return -ENOENT;
    }
    conn = iter->second;
  }

  std::string etag;
  map<std::string, std::string> req_headers;
  real_time set_mtime;

  //obj_time_weight dest_mtime_weight;
  uint32_t mod_zone_id = 0; 
  uint64_t mod_pg_ver = 0;
  
  RGWGetRemoteObjCB cb;
  int ret = conn->get_obj(user_id, NULL, src_obj, NULL, NULL,
                      mod_zone_id, mod_pg_ver,
                      false /* prepend_meta */, true /* GET */, false /* rgwx-stat */,
                      true /* sync manifest */, &cb, &in_stream_req);
  if (ret < 0) {
    ldout(store->ctx(),0) << "cloud error get_obj ret:" << ret << " "
                          << bucket_info.bucket.name << "file:" << key.name << dendl;
    return ret;
  }

  ret = conn->complete_request(in_stream_req, etag, &set_mtime, nullptr, req_headers);
  if (ret < 0) {
    ldout(store->ctx(),0) << "cloud error complete_request ret:" << ret << " "
                          << bucket_info.bucket.name << "file:" << key.name << dendl;
    return ret;
  }
  
  ldout(store->ctx(), 0) <<"cloud get obj len="<< cb.get_len() << dendl;
  ret = cloud_access->put_obj(bucket_info.bucket.name, key.name, &(cb.get_data()), cb.get_len());
  if (ret < 0) {
    ldout(store->ctx(),0) << "cloud error put_obj ret:" << ret << " "
                          << bucket_info.bucket.name << " file:" << key.name << dendl;
  }else
    ldout(store->ctx(),0) << "cloud success ret:"<<ret<<" bucket:" 
                          << bucket_info.bucket.name << " file:" << key.name << dendl;
  return ret;
}

class RGWCloudSyncObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  std::string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;

  RGWCloudSyncObj *req;
  RGWCloudAccess* cloud_access;
public:
  RGWCloudSyncObjCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                     const std::string& _source_zone,
                     RGWBucketInfo& _bucket_info,
                     const rgw_obj_key& _key, RGWCloudAccess* _cloud_access) 
                       : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
                         async_rados(_async_rados), store(_store),
                         source_zone(_source_zone),
                         bucket_info(_bucket_info),
                         key(_key),
                         req(nullptr),
                         cloud_access(_cloud_access) 
  { }

  ~RGWCloudSyncObjCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWCloudSyncObj(this, stack->create_completion_notifier(), store, source_zone, bucket_info, key, cloud_access);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWCloudSyncCR : public RGWCoroutine {

  RGWDataSyncEnv *sync_env;

  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  std::shared_ptr<RGWCloudAccess> cloud_access;

public:
  RGWCloudSyncCR(RGWDataSyncEnv *_sync_env,
                  RGWBucketInfo& _bucket_info, rgw_obj_key& _key, std::shared_ptr<RGWCloudAccess>& _cloud_access) 
                  :RGWCoroutine(_sync_env->cct), sync_env(_sync_env), bucket_info(_bucket_info), key(_key), cloud_access(_cloud_access)
  { }

  virtual ~RGWCloudSyncCR() {}

  int operate() override {
    reenter(this) {
      yield {
        call(new RGWCloudSyncObjCR(sync_env->async_rados, sync_env->store,
                                    sync_env->source_zone,
                                    bucket_info, key, cloud_access.get()));
      }
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "RGWCloudSyncCR() returned " << retcode << dendl;
        return set_cr_error(retcode);
      }
      ldout(sync_env->cct, 20) << "info of remote obj: z=" << sync_env->source_zone
        << " b=" << bucket_info.bucket << " k=" << key << dendl;
      return set_cr_done();
    }
    return 0;
  }

};

class RGWCloudRemove : public RGWAsyncRadosRequest {
  CephContext *cct;
  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  RGWCloudAccess *cloud_access;
protected:
  int _send_request(){
    int ret = cloud_access->remove_obj(bucket_info.bucket.name, key.name);
    return ret;
  }
public:
  RGWCloudRemove(RGWCoroutine *caller, RGWAioCompletionNotifier *cn,
                       RGWBucketInfo& _bucket_info,
                       const rgw_obj_key& _key,
                       CephContext *_cct,
                       RGWCloudAccess *_cloud_access) 
                         : RGWAsyncRadosRequest(caller, cn),
                           cct(_cct),
                           bucket_info(_bucket_info),
                           key(_key),
                           cloud_access(_cloud_access)
  { }
};

class RGWCloudRemoveCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWBucketInfo bucket_info;

  rgw_obj_key key;

  RGWDataSyncEnv *sync_env;
  RGWCloudRemove *req;
  std::shared_ptr<RGWCloudAccess> cloud_access;
public:
  RGWCloudRemoveCR(RGWDataSyncEnv *_sync_env,
                    RGWBucketInfo& _bucket_info,
                    const rgw_obj_key& _key,
                    std::shared_ptr<RGWCloudAccess>& _cloud_access) 
                      : RGWSimpleCoroutine(_sync_env->store->ctx()), cct(_sync_env->store->ctx()),
                        bucket_info(_bucket_info),
                        key(_key),
                        sync_env(_sync_env),
                        req(NULL),
                        cloud_access(_cloud_access) {

  }
  ~RGWCloudRemoveCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWCloudRemove(this, stack->create_completion_notifier(), bucket_info, key, cct, cloud_access.get());
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWCloudDataSyncModule : public RGWDataSyncModule {
  std::string prefix;
  map<std::string, std::string, ltstr_nocase>& config;
  set<std::string> bucket_filter;
  RGWCloudInfo cloud_info;
  
  bool check_bucket_filter(std::string& bucket_name) {
    if (bucket_filter.empty()) {
      return true;
    }
      
    auto iter = bucket_filter.find(bucket_name);
    return iter != bucket_filter.end() ? true : false;
  }
  
public:
  RGWCloudDataSyncModule(const std::string& _prefix, map<std::string, std::string, ltstr_nocase>& _config) 
                          : prefix(_prefix), config(_config) {
    auto iter = config.find("src_bucket");
    if (iter == config.end())
      return;

    if(iter->second =="*" || iter->second.empty()) {
      return;
    }
    bucket_filter.insert(iter->second);
  }
  
  ~RGWCloudDataSyncModule() { }
  
  std::shared_ptr<RGWCloudAccess> create_cloud_access() {
    int ret = init_cloud_info();
    if (ret < 0) {
      return nullptr;
    }
    
    if (cloud_info.access_type == "ufile") {
      if (cloud_info.bucket_host.empty()) {
        dout(0) << "ERROR: bucket host of ufile is unknown." << dendl;
        return nullptr;
      }
      if (cloud_info.bucket_region.empty()) {
        dout(0) << "ERROR: bucket region of ufile is unknown." << dendl;
        return nullptr;
      }
      return std::make_shared<RGWUfileAccess>(cloud_info);
    }
    else {
      dout(0) << "ERROR: cloud sync target interface type is invalid." << dendl;
      return nullptr;
    }
  }
  
  int init_cloud_info() {
    auto iter = config.find("cloud_type");
    if (iter == config.end() ) {
      dout(0) << "ERROR: cloud sync target interface type is unknown." << dendl;
      return -EINVAL;
    }
    cloud_info.access_type = iter->second;
    transform(cloud_info.access_type.begin(), cloud_info.access_type.end(), cloud_info.access_type.begin(), ::tolower);  
  
    iter = config.find("domain_name");
    if (iter == config.end()) {
      dout(0) << "ERROR: the domain name of cloud sync target is unknown." << dendl;
      return -EINVAL;
    }
    
    cloud_info.domain_name = iter->second;
    
    iter = config.find("public_key");
    if (iter == config.end()) {
      dout(0) << "ERROR: the public key of cloud sync target is unknown." << dendl;
      return -EINVAL;
    }
    cloud_info.public_key = iter->second;
    
    iter = config.find("private_key");
    if (iter == config.end()) {
      dout(0) << "ERROR: the private key of cloud sync target is unknown." << dendl;
      return -EINVAL;
    }
    cloud_info.private_key = iter->second;
  
    iter = config.find("prefix_bucket");
    if (iter != config.end()) {
        cloud_info.bucket_prefix = iter->second;
    }
    
    iter = config.find("dest_bucket");
    if (iter != config.end()) {
      if (iter->second != "*") {
        cloud_info.dest_bucket = iter->second;
      }
    }
    
    iter = config.find("bucket_host");
    if (iter != config.end()) {
      cloud_info.bucket_host = iter->second;
    }
    
    iter = config.find("bucket_region");
    if (iter != config.end()) {
      cloud_info.bucket_region = iter->second;
    }
    
    return 0;
  }

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_CUSTOM: sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    std::shared_ptr<RGWCloudAccess> cloud_access = create_cloud_access();
    if (cloud_access != nullptr && check_bucket_filter(bucket_info.bucket.name)) {
      cloud_access->set_ceph_context(sync_env->cct);
      return new RGWCloudSyncCR(sync_env, bucket_info, key, cloud_access);
    }
    return nullptr;
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_CUSTOM: rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    std::shared_ptr<RGWCloudAccess> cloud_access = create_cloud_access();
    if (cloud_access != nullptr && check_bucket_filter(bucket_info.bucket.name)) {
      cloud_access->set_ceph_context(sync_env->cct);
      return new RGWCloudRemoveCR(sync_env, bucket_info, key, cloud_access);
    }
    return nullptr;
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_CUSTOM: create_delete_marker: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime
                            << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return NULL;
  }
};

class RGWCloudSyncModuleInstance : public RGWSyncModuleInstance {
  RGWCloudDataSyncModule data_handler;
public:
  RGWCloudSyncModuleInstance(const std::string& _prefix, map<std::string, std::string, ltstr_nocase>& _config) : data_handler(_prefix, _config) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWCloudSyncModule::create_instance(CephContext *cct, map<std::string, std::string, ltstr_nocase>& config, RGWSyncModuleInstanceRef *instance) {
  std::string prefix;
  auto i = config.find("prefix");
  if (i != config.end()) {
    prefix = i->second;
  }
  
  instance->reset(new RGWCloudSyncModuleInstance(prefix, config));
  return 0;
}

