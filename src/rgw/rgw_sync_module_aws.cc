#include "rgw_common.h"
#include "rgw_coroutine.h"
#include "rgw_sync_module.h"
#include "rgw_data_sync.h"
#include "rgw_sync_module_aws.h"
#include "rgw_rest_conn.h"
#include "rgw_cr_rest.h"
#include "rgw_acl.h"

#include <boost/asio/yield.hpp>

#define dout_subsys ceph_subsys_rgw

// TODO: have various bucket naming schemes at a global/user and a bucket level

static string aws_bucket_name(const RGWBucketInfo& bucket_info, bool user_buckets=false){
  string bucket_name="rgwx" + bucket_info.zonegroup;
  if (user_buckets){
    bucket_name+=bucket_info.owner.tenant + bucket_info.owner.id;
  }
  bucket_name.erase(std::remove(bucket_name.begin(),bucket_name.end(),'-'));
  return bucket_name;
}

static string aws_object_name(const RGWBucketInfo& bucket_info, const rgw_obj_key&key, bool user_buckets=false){
  string bucket_name = aws_bucket_name(bucket_info, user_buckets);
  string object_name = bucket_name+"/";
  if (!user_buckets){
    object_name += bucket_info.owner.tenant + bucket_info.owner.id + "/";
  }
  object_name += bucket_info.bucket.name + "/" + key.name;
  return object_name;
}

struct AWSConfig {
  string id;
  std::unique_ptr<RGWRESTConn> conn;
};

class RGWAWSStreamPutCRF : public RGWStreamWriteHTTPResourceCRF
{
  RGWAccessKey access_key;
public:
  RGWAWSStreamPutCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWHTTPManager *_http_manager,
                               RGWAccessKey& _key,
                               RGWRESTStreamS3PutObj *_req) : RGWStreamWriteHTTPResourceCRF(_cct, _env, _caller, _http_manager, _req), access_key(_key) {}

  void send_ready(const std::map<string, string>& attrs) override {
    RGWRESTStreamS3PutObj *r = (RGWRESTStreamS3PutObj *)req;

    map<string, bufferlist> new_attrs;

    for (auto attr : attrs) {
      const string& val = attr.second;
      new_attrs[attr.first].append(bufferptr(val.c_str(), val.size() - 1));
    }

    RGWAccessControlPolicy policy;
    ::encode(policy, new_attrs[RGW_ATTR_ACL]);

    r->send_ready(access_key, new_attrs, false);
  }
};

// maybe use Fetch Remote Obj instead?
class RGWAWSHandleRemoteObjCBCR: public RGWStatRemoteObjCBCR {
  const AWSConfig& conf;
  bufferlist res;
  unordered_map <string, bool> bucket_created;
  string target_bucket_name;
  std::shared_ptr<RGWStreamReadHTTPResourceCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;
  RGWRESTStreamRWRequest *in_req{nullptr};
  RGWRESTStreamS3PutObj *out_req{nullptr};

  string obj_path;
  int ret{0};

public:
  RGWAWSHandleRemoteObjCBCR(RGWDataSyncEnv *_sync_env,
                            RGWBucketInfo& _bucket_info,
                            rgw_obj_key& _key,
                            const AWSConfig& _conf) : RGWStatRemoteObjCBCR(_sync_env, _bucket_info, _key),
                                                         conf(_conf)
  {}

  ~RGWAWSHandleRemoteObjCBCR(){
  }

#if 0
  int operate () override {

    reenter(this) {

      ldout(sync_env->cct, 0) << "AWS: download begin: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size
                              << " mtime=" << mtime << " attrs=" << attrs
                              << dendl;

      yield {
        string obj_path = bucket_info.bucket.name + "/" + key.name;

        // TODO-future: And we should do a part by part get and initiate mp on the aws side
        call(new RGWReadRawRESTResourceCR(sync_env->cct,
                                          sync_env->store->rest_master_conn,
                                          sync_env->http_manager,
                                          obj_path,
                                          nullptr,
                                          &res));

      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

      bucket_name=aws_bucket_name(bucket_info);
      if (bucket_created.find(bucket_name) == bucket_created.end()){
      //   // TODO: maybe do a head request for subsequent tries & make it configurable
        yield {
        //string bucket_name = aws_bucket_name(bucket_info);
          ldout(sync_env->cct,0) << "AWS: creating bucket" << bucket_name << dendl;
          bufferlist bl;
          call(new RGWPutRawRESTResourceCR <int> (sync_env->cct, conf.conn.get(),
                                                  sync_env->http_manager,
                                                  bucket_name, nullptr, bl, nullptr));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        bucket_created[bucket_name]=true;
      }

      yield {
        string path=aws_object_name(bucket_info, key);
        ldout(sync_env->cct,0) << "AWS: creating object at path" << path << dendl;
        call(new RGWPutRawRESTResourceCR<int> (sync_env->cct, conf.conn.get(),
                                                        sync_env->http_manager,
                                                        path, nullptr,
                                                        res, nullptr));
      }
      if (retcode < 0) {
        return set_cr_error(retcode);
      }


      return set_cr_done();
    }

    return 0;
  }
#endif

  int operate() override {

    reenter(this) {

      ldout(sync_env->cct, 0) << "AWS: download begin: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size
                              << " mtime=" << mtime << " attrs=" << attrs
                              << dendl;

      obj_path = bucket_info.bucket.name + "/" + key.name;

      target_bucket_name = aws_bucket_name(bucket_info);
      if (bucket_created.find(target_bucket_name) == bucket_created.end()){
        yield {
          ldout(sync_env->cct,0) << "AWS: creating bucket" << target_bucket_name << dendl;
          bufferlist bl;
          call(new RGWPutRawRESTResourceCR <int> (sync_env->cct, conf.conn.get(),
                                                  sync_env->http_manager,
                                                  target_bucket_name, nullptr, bl, nullptr));
        }
        if (retcode < 0) {
          return set_cr_error(retcode);
        }

        bucket_created[target_bucket_name] = true;
      }

#warning FIXME conn
      {
        /* init input connection */
        rgw_obj source_obj(bucket_info.bucket, key);
        ret = sync_env->store->rest_master_conn->get_obj(rgw_user(),  nullptr, source_obj,
                                                         nullptr /* mod_ptr */, nullptr /* unmod_ptr */, 0 /* mod_zone_id */, 0 /* mod_pg_ver */,
                                                         false /* prepend_metadata */, true /* get_op */, true /*rgwx_stat */,
                                                         false /* sync_manifest */, true /* skip_descrypt */, false /* send */,
                                                         nullptr /* cb */, &in_req);
        if (ret < 0) {
          return set_cr_error(ret);
        }

        /* init output connection */
        rgw_bucket target_bucket;
        target_bucket.name = target_bucket_name; /* this is only possible because we only use bucket name for
                                                    uri resolution */
        rgw_obj target_obj(target_bucket, aws_object_name(bucket_info, key));
        in_crf.reset(new RGWStreamReadHTTPResourceCRF(cct, get_env(), this, sync_env->http_manager, in_req));

        map<string, bufferlist> attrs;
        RGWAccessControlPolicy empty_policy;
        ::encode(empty_policy, attrs[RGW_ATTR_ACL]);
        conf.conn->put_obj_send_init(target_obj, &out_req);

        out_crf.reset(new RGWAWSStreamPutCRF(cct, get_env(), this, sync_env->http_manager, conf.conn->get_key(), out_req));
      }

      yield call(new RGWStreamSpliceCR(cct, sync_env->http_manager, in_crf, out_crf));
      if (retcode < 0) {
        return set_cr_error(retcode);
      }

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
        ldout(sync_env->cct, 0) << "AWS: removing aws object at" << path << dendl;
        call(new RGWDeleteRESTResourceCR(sync_env->cct, conf.conn.get(),
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
  CephContext *cct;
  AWSConfig conf;
  string s3_endpoint;
  RGWAccessKey key;
public:
  RGWAWSDataSyncModule(CephContext *_cct, const string& _s3_endpoint, const string& access_key, const string& secret) :
                  cct(_cct),
                  s3_endpoint(_s3_endpoint),
                  key(access_key, secret) {
  }

  void init(RGWDataSyncEnv *sync_env, uint64_t instance_id) {
    conf.id = string("s3:") + s3_endpoint;
    conf.conn.reset(new RGWRESTConn(cct,
                                    sync_env->store,
                                    conf.id,
                                    { s3_endpoint },
                                    key));
  }

  ~RGWAWSDataSyncModule() {}

  RGWCoroutine *sync_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, uint64_t versioned_epoch,
                            rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) << conf.id << ": sync_object: b=" << bucket_info.bucket << " k=" << key << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSHandleRemoteObjCR(sync_env, bucket_info, key, conf);
  }
  RGWCoroutine *remove_object(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime, bool versioned, uint64_t versioned_epoch,
                              rgw_zone_set *zones_trace) override {
    ldout(sync_env->cct, 0) <<"rm_object: b=" << bucket_info.bucket << " k=" << key << " mtime=" << mtime << " versioned=" << versioned << " versioned_epoch=" << versioned_epoch << dendl;
    return new RGWAWSRemoveRemoteObjCBCR(sync_env, bucket_info, key, mtime, conf);
  }
  RGWCoroutine *create_delete_marker(RGWDataSyncEnv *sync_env, RGWBucketInfo& bucket_info, rgw_obj_key& key, real_time& mtime,
                                     rgw_bucket_entry_owner& owner, bool versioned, uint64_t versioned_epoch,
                                     rgw_zone_set *zones_trace) override {
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

int RGWAWSSyncModule::create_instance(CephContext *cct, map<string, string, ltstr_nocase>& config,  RGWSyncModuleInstanceRef *instance){
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

  instance->reset(new RGWAWSSyncModuleInstance(cct, s3_endpoint, access_key, secret));
  return 0;
}
