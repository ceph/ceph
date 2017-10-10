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

class RGWRESTStreamGetCRF : public RGWStreamReadHTTPResourceCRF
{
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *conn;
  rgw_obj src_obj;
public:
  RGWRESTStreamGetCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWDataSyncEnv *_sync_env,
                               RGWRESTConn *_conn,
                               rgw_obj& _src_obj) : RGWStreamReadHTTPResourceCRF(_cct, _env, _caller, _sync_env->http_manager),
                                                                                 sync_env(_sync_env), conn(_conn), src_obj(_src_obj) {
  }

  int init(RGWBucketInfo& bucket_info, rgw_obj_key& key) {
    /* init input connection */
    RGWRESTStreamRWRequest *in_req;
    int ret = conn->get_obj(rgw_user(),  nullptr, src_obj,
                            nullptr /* mod_ptr */, nullptr /* unmod_ptr */, 0 /* mod_zone_id */, 0 /* mod_pg_ver */,
                            true /* prepend_metadata */, true /* get_op */, true /*rgwx_stat */,
                            false /* sync_manifest */, true /* skip_descrypt */, false /* send */,
                            nullptr /* cb */, &in_req);
    if (ret < 0) {
      ldout(sync_env->cct, 0) << "ERROR: " << __func__ << "(): conn->get_obj() returned ret=" << ret << dendl;
      return ret;
    }

    set_req(in_req);

    return 0;
  }

  bool need_extra_data() override {
    return true;
  }
};

class RGWAWSStreamPutCRF : public RGWStreamWriteHTTPResourceCRF
{
  RGWDataSyncEnv *sync_env;
  RGWRESTConn *conn;
  rgw_obj dest_obj;
public:
  RGWAWSStreamPutCRF(CephContext *_cct,
                               RGWCoroutinesEnv *_env,
                               RGWCoroutine *_caller,
                               RGWDataSyncEnv *_sync_env,
                               RGWRESTConn* _conn,
                               rgw_obj& _dest_obj) : RGWStreamWriteHTTPResourceCRF(_cct, _env, _caller, _sync_env->http_manager),
                                                     sync_env(_sync_env), conn(_conn), dest_obj(_dest_obj) {
  }

  int init() {
    /* init output connection */
    RGWRESTStreamS3PutObj *out_req{nullptr};

    conn->put_obj_send_init(dest_obj, &out_req);

    set_req(out_req);

    return 0;
  }

  void send_ready(const std::map<string, string>& attrs) override {
    RGWRESTStreamS3PutObj *r = (RGWRESTStreamS3PutObj *)req;

    map<string, bufferlist> new_attrs;

    for (auto attr : attrs) {
      const string& val = attr.second;
      new_attrs[attr.first].append(bufferptr(val.c_str(), val.size() - 1));
    }

    RGWAccessControlPolicy policy;
    ::encode(policy, new_attrs[RGW_ATTR_ACL]);

    r->send_ready(conn->get_key(), new_attrs, false);
  }
};

// maybe use Fetch Remote Obj instead?
class RGWAWSHandleRemoteObjCBCR: public RGWStatRemoteObjCBCR {
  const AWSConfig& conf;
  RGWRESTConn *source_conn;
  bufferlist res;
  unordered_map <string, bool> bucket_created;
  string target_bucket_name;
  std::shared_ptr<RGWStreamReadHTTPResourceCRF> in_crf;
  std::shared_ptr<RGWStreamWriteHTTPResourceCRF> out_crf;

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

  int operate() override {
    reenter(this) {

      ldout(sync_env->cct, 0) << "AWS: download begin: z=" << sync_env->source_zone
                              << " b=" << bucket_info.bucket << " k=" << key << " size=" << size
                              << " mtime=" << mtime << " attrs=" << attrs
                              << dendl;

      source_conn = sync_env->store->get_zone_conn_by_id(sync_env->source_zone);
      if (!source_conn) {
        ldout(sync_env->cct, 0) << "ERROR: cannot find http connection to zone " << sync_env->source_zone << dendl;
        return set_cr_error(-EINVAL);
      }

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

      {
        rgw_obj src_obj(bucket_info.bucket, key);
        /* init input */
        in_crf.reset(new RGWRESTStreamGetCRF(cct, get_env(), this, sync_env, source_conn, src_obj));

        ret = in_crf->init();
        if (ret < 0) {
          return set_cr_error(ret);
        }

        /* init output */
        rgw_bucket target_bucket;
        target_bucket.name = target_bucket_name; /* this is only possible because we only use bucket name for
                                                uri resolution */
        rgw_obj dest_obj(target_bucket, aws_object_name(bucket_info, key));

        out_crf.reset(new RGWAWSStreamPutCRF(cct, get_env(), this, sync_env, conf.conn.get(),
                                             dest_obj));
        ret = out_crf->init();
        if (ret < 0) {
          return set_cr_error(ret);
        }
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
