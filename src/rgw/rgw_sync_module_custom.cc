
#include <algorithm>
#include <cstddef>

#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_cr_rados.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_custom.h"
#include "rgw_custom_access.h"
#include "rgw_rest_client.h"
#include "rgw_rest_conn.h"
#include "rgw_custom_access.h"

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
    //dout(0) << "nike _bl.len=" << _bl.length() << " _ofs=" << _ofs << " _len=" << _len << " len="<<len<<dendl;
    return 0;
  }
};

class RGWCustomSyncObj : public RGWAsyncRadosRequest {
  RGWRados *store;
  string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;

  ceph::real_time *pmtime;
  uint64_t *psize;
  RGWCustomAccess* custom_access;
protected:
  int _send_request();
public:
  RGWCustomSyncObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWRados *_store,
                         const string& _source_zone,
                         RGWBucketInfo& _bucket_info,
                         const rgw_obj_key& _key,
                         RGWCustomAccess* _custom_access) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                      source_zone(_source_zone),
                                                      bucket_info(_bucket_info),
                                                      key(_key),
                                                      custom_access(_custom_access){}
};

int RGWCustomSyncObj::_send_request()
{
  string user_id;
  rgw_obj src_obj(bucket_info.bucket, key);

  RGWRESTStreamRWRequest *in_stream_req;
 
  RGWRESTConn *conn;
  if (source_zone.empty()) {
    if (bucket_info.zonegroup.empty()) {
      /* source is in the master zonegroup */
      conn = store->rest_master_conn;
    } else {
      map<string, RGWRESTConn *>::iterator iter = store->zonegroup_conn_map.find(bucket_info.zonegroup);
      if (iter == store->zonegroup_conn_map.end()) {
        ldout(store->ctx(), 0) << "RGWCustomSyncObj:could not find zonegroup connection to zonegroup: " << source_zone << dendl;
        return -ENOENT;
      }
      conn = iter->second;
    }
  } else {
    map<string, RGWRESTConn *>::iterator iter = store->zone_conn_map.find(source_zone);
    if (iter == store->zone_conn_map.end()) {
      ldout(store->ctx(), 0) << "RGWCustomSyncObj:could not find zone connection to zone: " << source_zone << dendl;
      return -ENOENT;
    }
    conn = iter->second;
  }

  string etag;
  map<string, string> req_headers;
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
    ldout(store->ctx(),0) << "custom error get_obj ret:" << ret << bucket_info.bucket.name << "file:" << key.name << dendl;
    return ret;
  }

  ret = conn->complete_request(in_stream_req, etag, &set_mtime, nullptr, req_headers);
  if (ret < 0) {
    ldout(store->ctx(),0) << "custom error complete_request ret:" << ret << bucket_info.bucket.name << "file:" << key.name << dendl;
    return ret;
  }
  
  ldout(store->ctx(), 0) <<"custom get obj len="<< cb.get_len() << dendl;
  ret = custom_access->put_obj(bucket_info.bucket.name, key.name, &(cb.get_data()), cb.get_len());
  if (ret < 0) {
    ldout(store->ctx(),0) << "custom error put_obj ret:" << ret << bucket_info.bucket.name << " file:" << key.name << dendl;
  }else
    ldout(store->ctx(),0) << "custom success ret:"<<ret<<" bucket:" << bucket_info.bucket.name << " file:" << key.name << dendl;
  return ret;
}

class RGWCustomSyncObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  RGWRados *store;
  string source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;

  RGWCustomSyncObj *req;
  RGWCustomAccess* custom_access;
public:
  RGWCustomSyncObjCR(RGWAsyncRadosProcessor *_async_rados, RGWRados *_store,
                     const string& _source_zone,
                     RGWBucketInfo& _bucket_info,
                     const rgw_obj_key& _key, RGWCustomAccess* _custom_access) 
                       : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
                         async_rados(_async_rados), store(_store),
                         source_zone(_source_zone),
                         bucket_info(_bucket_info),
                         key(_key),
                         req(nullptr),
                         custom_access(_custom_access) 
  { }

  ~RGWCustomSyncObjCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWCustomSyncObj(this, stack->create_completion_notifier(), store, source_zone, bucket_info, key, custom_access);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWCustomSyncCR : public RGWCoroutine {

  RGWDataSyncEnv *sync_env;

  RGWBucketInfo bucket_info;
  rgw_obj_key key;
  std::shared_ptr<RGWCustomAccess> custom_access;

public:
  RGWCustomSyncCR(RGWDataSyncEnv *_sync_env,
                  RGWBucketInfo& _bucket_info, rgw_obj_key& _key, std::shared_ptr<RGWCustomAccess>& _custom_access) 
                  :RGWCoroutine(_sync_env->cct), sync_env(_sync_env), bucket_info(_bucket_info), key(_key), custom_access(_custom_access)
  { }

  virtual ~RGWCustomSyncCR() {}

  int operate() override {
    reenter(this) {
      yield {
        call(new RGWCustomSyncObjCR(sync_env->async_rados, sync_env->store,
                                    sync_env->source_zone,
                                    bucket_info, key, custom_access.get()));
      }
      if (retcode < 0) {
        ldout(sync_env->cct, 0) << "RGWCustomSyncCR() returned " << retcode << dendl;
        return set_cr_error(retcode);
      }
      ldout(sync_env->cct, 20) << "info of remote obj: z=" << sync_env->source_zone
        << " b=" << bucket_info.bucket << " k=" << key << dendl;
      return set_cr_done();
    }
    return 0;
  }

};

class RGWCustomRemove : public RGWAsyncRadosRequest {
	CephContext *cct;
  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  RGWCustomAccess *custom_access;
protected:
  int _send_request(){
	  int ret = custom_access->remove_obj(bucket_info.bucket.name, key.name);
    return ret;
  }
public:
  RGWCustomRemove(RGWCoroutine *caller, RGWAioCompletionNotifier *cn,
                       RGWBucketInfo& _bucket_info,
                       const rgw_obj_key& _key,
                       CephContext *_cct,
                       RGWCustomAccess *_custom_access) 
                         : RGWAsyncRadosRequest(caller, cn),
                           cct(_cct),
                           bucket_info(_bucket_info),
                           key(_key),
                           custom_access(_custom_access)
  { }
};

class RGWCustomRemoveCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWBucketInfo bucket_info;

  rgw_obj_key key;

  RGWDataSyncEnv *sync_env;
  RGWCustomRemove *req;
  std::shared_ptr<RGWCustomAccess> custom_access;
public:
  RGWCustomRemoveCR(RGWDataSyncEnv *_sync_env,
                    RGWBucketInfo& _bucket_info,
                    const rgw_obj_key& _key,
                    std::shared_ptr<RGWCustomAccess>& _custom_access) 
                      : RGWSimpleCoroutine(_sync_env->store->ctx()), cct(_sync_env->store->ctx()),
                        bucket_info(_bucket_info),
                        key(_key),
                        sync_env(_sync_env),
                        req(NULL),
                        custom_access(_custom_access) {

  }
  ~RGWCustomRemoveCR() {
    request_cleanup();
  }

  void request_cleanup() {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request() {
    req = new RGWCustomRemove(this, stack->create_completion_notifier(), bucket_info, key, cct, custom_access.get());
    sync_env->async_rados->queue(req);
    return 0;
  }

  int request_complete() {
    return req->get_ret_status();
  }
};

class RGWCustomDataSyncModule : public RGWDataSyncModule {
  string prefix;
  map<string, string, ltstr_nocase>& config;
  set<string> bucket_filter;
  RGWCustomInfo custom_info;
  
  bool check_bucket_filter(string& bucket_name) {
    if (bucket_filter.empty()) {
      return true;
    }
      
    auto iter = bucket_filter.find(bucket_name);
    return iter != bucket_filter.end() ? true : false;
  }
  
public:
  RGWCustomDataSyncModule(const string& _prefix, map<string, string, ltstr_nocase>& _config) 
                          : prefix(_prefix), config(_config) {
    auto iter = config.find("src_bucket");
    if (iter == config.end())
      return;

    if(iter->second =="*" || iter->second.empty()) {
      return;
    }
    bucket_filter.insert(iter->second);
  }
  
  ~RGWCustomDataSyncModule() { }
  
  std::shared_ptr<RGWCustomAccess> create_custom_access() {
    int ret = init_custom_info();
    if (ret < 0) {
      return nullptr;
    }
    
    if (custom_info.access_type == "ufile") {
      if (custom_info.bucket_host.empty()) {
        dout(0) << "ERROR: bucket host of ufile is unknown." << dendl;
        return nullptr;
      }
      if (custom_info.bucket_region.empty()) {
        dout(0) << "ERROR: bucket region of ufile is unknown." << dendl;
        return nullptr;
      }
      return std::make_shared<RGWUfileAccess>(custom_info);
    }
    else {
      dout(0) << "ERROR: custom sync target interface type is invalid." << dendl;
      return nullptr;
    }
  }
  
  int init_custom_info() {
    auto iter = config.find("custom_type");
    if (iter == config.end() ) {
      dout(0) << "ERROR: custom sync target interface type is unknown." << dendl;
      return -EINVAL;
    }
    custom_info.access_type = iter->second;
    transform(custom_info.access_type.begin(), custom_info.access_type.end(), custom_info.access_type.begin(), ::tolower);  
  
    iter = config.find("domain_name");
    if (iter == config.end()) {
      dout(0) << "ERROR: the domain name of custom sync target is unknown." << dendl;
      return -EINVAL;
    }
    
    custom_info.domain_name = iter->second;
    
    iter = config.find("public_key");
    if (iter == config.end()) {
      dout(0) << "ERROR: the public key of custom sync target is unknown." << dendl;
      return -EINVAL;
    }
    custom_info.public_key = iter->second;
    
    iter = config.find("private_key");
    if (iter == config.end()) {
      dout(0) << "ERROR: the private key of custom sync target is unknown." << dendl;
      return -EINVAL;
    }
    custom_info.private_key = iter->second;
  
    iter = config.find("prefix_bucket");
    if (iter != config.end()) {
        custom_info.bucket_prefix = iter->second;
    }
    
    iter = config.find("dest_bucket");
    if (iter != config.end()) {
      if (iter->second != "*") {
        custom_info.dest_bucket = iter->second;
      }
    }
    
    iter = config.find("bucket_host");
    if (iter != config.end()) {
      custom_info.bucket_host = iter->second;
    }
    
    iter = config.find("bucket_region");
    if (iter != config.end()) {
      custom_info.bucket_region = iter->second;
    }
    
    return 0;
  }

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_CUSTOM: sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    std::shared_ptr<RGWCustomAccess> custom_access = create_custom_access();
    if (custom_access != nullptr && check_bucket_filter(bucket_info.bucket.name)) {
      custom_access->set_ceph_context(sync_env->cct);
      return new RGWCustomSyncCR(sync_env, bucket_info, key, custom_access);
    }
    return nullptr;
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch, rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << prefix << ": SYNC_CUSTOM: rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    std::shared_ptr<RGWCustomAccess> custom_access = create_custom_access();
    if (custom_access != nullptr && check_bucket_filter(bucket_info.bucket.name)) {
      custom_access->set_ceph_context(sync_env->cct);
      return new RGWCustomRemoveCR(sync_env, bucket_info, key, custom_access);
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

class RGWCustomSyncModuleInstance : public RGWSyncModuleInstance {
  RGWCustomDataSyncModule data_handler;
public:
  RGWCustomSyncModuleInstance(const string& _prefix, map<string, string, ltstr_nocase>& _config) : data_handler(_prefix, _config) {}
  RGWDataSyncModule *get_data_handler() override {
    return &data_handler;
  }
};

int RGWCustomSyncModule::create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config, RGWSyncModuleInstanceRef *instance) {
  string prefix;
  auto i = config.find("prefix");
  if (i != config.end()) {
    prefix = i->second;
  }
  
  instance->reset(new RGWCustomSyncModuleInstance(prefix, config));
  return 0;
}

